// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"). You may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

using System.Collections.Concurrent;
using System.Text;
using EventHorizon.RocketMQ.IntegrationTestInfrastructure;
using EventHorizon.RocketMQ.Remoting.Consumer;
using EventHorizon.RocketMQ.Remoting.Consumer.Pull;
using EventHorizon.RocketMQ.Remoting.Consumer.Pull.Lite;
using EventHorizon.RocketMQ.Remoting.Consumer.Push;
using EventHorizon.RocketMQ.Remoting.Producer;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace EventHorizon.RocketMQ.Remoting.IntegrationTests;

public sealed class RocketMQRebalanceIntegrationTests(
    RocketMQSingleBrokerContainerFixtureRegistry registry) : IAsyncLifetime
{
    private static readonly TimeSpan RebalanceTimeout = TimeSpan.FromSeconds(10);
    private RocketMQSingleBrokerContainerFixture _fixture = null!;

    public async ValueTask InitializeAsync()
    {
        _fixture = await registry.GetFixtureAsync(TestContext.Current.CancellationToken);
    }

    public ValueTask DisposeAsync()
    {
        return ValueTask.CompletedTask;
    }

    [Fact]
    [Trait("Category", "Integration")]
    public async Task PushConsumersUnderOneRegistrationRebalanceAfterMemberStops()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var scope = await _fixture.CreateTestScopeAsync(RocketMQTestTopicType.Normal, cancellationToken);
        var group = scope.CreateConsumerGroupName("remoting-push-rebalance");
        var suffix = Guid.NewGuid().ToString("N");
        var tag = $"remoting-push-rebalance-{suffix}";
        var bodyPrefix = $"{tag}-message";
        var expectedBodies = new ConcurrentDictionary<string, byte>(StringComparer.Ordinal);
        var deliveryCounts = new ConcurrentDictionary<string, int>(StringComparer.Ordinal);
        var deliveryOwners = new ConcurrentDictionary<(string Body, int Consumer), int>();
        var unexpectedDeliveries = new ConcurrentQueue<string>();
        var consumerDeliveries = new int[2];

        Func<IReadOnlyList<RemotingMessageView>, RemotingPushConsumeContext, CancellationToken, ValueTask<ConsumeResult>>
            CreateHandler(int consumerIndex) => (messages, _, _) =>
            {
                foreach (var message in messages)
                {
                    var body = Encoding.UTF8.GetString(message.Body);
                    if (message.Topic != scope.Topic ||
                        !body.StartsWith(bodyPrefix, StringComparison.Ordinal) ||
                        !expectedBodies.ContainsKey(body))
                    {
                        unexpectedDeliveries.Enqueue(
                            $"consumer={consumerIndex}, topic={message.Topic}, body={body}");
                        continue;
                    }

                    Interlocked.Increment(ref consumerDeliveries[consumerIndex]);
                    deliveryCounts.AddOrUpdate(body, 1, static (_, count) => count + 1);
                    deliveryOwners.AddOrUpdate(
                        (body, consumerIndex),
                        1,
                        static (_, count) => count + 1);
                }

                return ValueTask.FromResult(ConsumeResult.Success);
            };

        var services = new ServiceCollection();
        var rocketMQ = services.AddRocketMQRemoting(options =>
        {
            options.NamesrvAddr = _fixture.NameServerAddress;
            options.InstanceName = $"remoting-push-rebalance-{suffix}";
            options.HeartbeatBrokerInterval = TimeSpan.FromHours(1);
            options.PollNameServerInterval = TimeSpan.FromHours(1);
        });
        rocketMQ.AddRemotingProducer(options =>
            options.GroupName = scope.CreateProducerGroupName("remoting-push-rebalance-producer"));
        rocketMQ.AddRemotingPushConsumerWithTestHandler<FirstPushConsumerMarker>(options =>
        {
            ConfigurePushConsumer(options, group, scope.Topic, tag);
        }, CreateHandler(0));
        rocketMQ.AddRemotingPushConsumerWithTestHandler<SecondPushConsumerMarker>(options =>
        {
            ConfigurePushConsumer(options, group, scope.Topic, tag);
        }, CreateHandler(1));

        await using var provider = services.BuildServiceProvider(
            new ServiceProviderOptions { ValidateOnBuild = true });
        var producer = provider.GetRequiredService<IRemotingProducer>();
        var consumers = provider.GetServices<IRemotingPushConsumer>().ToArray();
        Assert.Equal(2, consumers.Length);
        var pushConsumers = consumers.Select(Assert.IsType<RemotingPushConsumer>).ToArray();
        var runningConsumers = new bool[consumers.Length];

        await producer.StartAsync(cancellationToken);
        try
        {
            var queues = (await producer.GetPublishMessageQueuesAsync(scope.Topic, cancellationToken))
                .OrderBy(static queue => queue.BrokerName, StringComparer.Ordinal)
                .ThenBy(static queue => queue.QueueId)
                .ToArray();
            Assert.True(queues.Length > 1, $"Rebalance test requires multiple queues, but found {queues.Length}.");
            var expectedQueues = queues.Select(FormatQueue).ToHashSet(StringComparer.Ordinal);

            for (var index = 0; index < consumers.Length; index++)
            {
                await consumers[index].StartAsync(cancellationToken);
                runningConsumers[index] = true;
            }

            await WaitUntilAsync(
                () => PushAssignmentsAreMutuallyExclusiveAndComplete(pushConsumers, scope.Topic, expectedQueues),
                RebalanceTimeout,
                () =>
                    "Push consumers did not reach mutually exclusive complete assignments. " +
                    FormatPushAssignmentDiagnostics(pushConsumers, scope.Topic, expectedQueues),
                cancellationToken);

            for (var round = 0;
                 round < 20 && !EveryConsumerHasDelivered(consumerDeliveries);
                 round++)
            {
                await SendRoundToEveryQueueAsync(
                    producer,
                    queues,
                    scope.Topic,
                    tag,
                    $"{bodyPrefix}-initial-{round}",
                    expectedBodies,
                    cancellationToken);
                await Task.Delay(TimeSpan.FromMilliseconds(250), cancellationToken);
            }

            await WaitUntilAsync(
                () => EveryConsumerHasDelivered(consumerDeliveries) &&
                      expectedBodies.Keys.All(deliveryCounts.ContainsKey),
                RebalanceTimeout,
                () =>
                    "Both Push consumers did not receive their initial assignment. " +
                    FormatPushDiagnostics(expectedBodies.Keys, deliveryCounts, deliveryOwners),
                cancellationToken);
            Assert.Empty(unexpectedDeliveries);

            var commitResults = await Task.WhenAll(queues.Select(async queue =>
            {
                var result = await _fixture.WaitForConsumerCommitAsync(
                    group,
                    scope.Topic,
                    queue.BrokerName,
                    queue.QueueId,
                    RebalanceTimeout,
                    cancellationToken);
                return (Queue: queue, result.Committed, result.Progress);
            }));
            Assert.All(
                commitResults,
                result => Assert.True(
                    result.Committed,
                    $"Initial offset for {FormatQueue(result.Queue)} was not committed. {result.Progress}"));

            await consumers[1].StopAsync(CancellationToken.None);
            runningConsumers[1] = false;

            await WaitUntilAsync(
                () => expectedQueues.SetEquals(pushConsumers[0].Assignment
                    .Where(queue => string.Equals(queue.Topic, scope.Topic, StringComparison.Ordinal))
                    .Select(QueueKey)),
                RebalanceTimeout,
                () =>
                    "The surviving Push consumer did not take over every queue. " +
                    FormatPushAssignmentDiagnostics(pushConsumers, scope.Topic, expectedQueues),
                cancellationToken);
            Assert.Empty(pushConsumers[1].Assignment);

            var takeoverBodies = await SendRoundToEveryQueueAsync(
                producer,
                queues,
                scope.Topic,
                tag,
                $"{bodyPrefix}-takeover",
                expectedBodies,
                cancellationToken);
            await WaitUntilAsync(
                () => takeoverBodies.All(deliveryCounts.ContainsKey),
                RebalanceTimeout,
                () =>
                    "The surviving Push consumer did not take over every queue. " +
                    FormatPushDiagnostics(takeoverBodies, deliveryCounts, deliveryOwners),
                cancellationToken);
            await Task.Delay(TimeSpan.FromSeconds(1), cancellationToken);

            Assert.Empty(unexpectedDeliveries);
            Assert.Equal(
                expectedBodies.Keys.Order(StringComparer.Ordinal),
                deliveryCounts.Keys.Order(StringComparer.Ordinal));
            Assert.All(deliveryCounts, static delivery => Assert.Equal(1, delivery.Value));
            Assert.All(
                takeoverBodies,
                body => Assert.Equal(
                    [0],
                    deliveryOwners
                        .Where(delivery => string.Equals(delivery.Key.Body, body, StringComparison.Ordinal))
                        .Select(static delivery => delivery.Key.Consumer)
                        .Order()));
        }
        finally
        {
            for (var index = consumers.Length - 1; index >= 0; index--)
            {
                if (runningConsumers[index])
                {
                    await consumers[index].StopAsync(CancellationToken.None);
                }
            }

            await producer.StopAsync(CancellationToken.None);
        }
    }

    [Fact]
    [Trait("Category", "Integration")]
    public async Task LitePullConsumersUnderOneRegistrationRebalanceAfterMemberStops()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var scope = await _fixture.CreateTestScopeAsync(RocketMQTestTopicType.Normal, cancellationToken);
        var group = scope.CreateConsumerGroupName("remoting-lite-pull-rebalance");
        var suffix = Guid.NewGuid().ToString("N");
        var tag = $"remoting-lite-pull-rebalance-{suffix}";
        var services = new ServiceCollection();
        var rocketMQ = services.AddRocketMQRemoting(options =>
        {
            options.NamesrvAddr = _fixture.NameServerAddress;
            options.InstanceName = $"remoting-lite-pull-rebalance-{suffix}";
            options.HeartbeatBrokerInterval = TimeSpan.FromHours(1);
            options.PollNameServerInterval = TimeSpan.FromHours(1);
        });
        rocketMQ.AddRemotingLitePullConsumer(options => ConfigureLitePullConsumer(
            options,
            group,
            scope.Topic,
            tag));
        rocketMQ.AddRemotingLitePullConsumer(options => ConfigureLitePullConsumer(
            options,
            group,
            scope.Topic,
            tag));

        await using var provider = services.BuildServiceProvider(
            new ServiceProviderOptions { ValidateOnBuild = true });
        var consumers = provider.GetServices<IRemotingLitePullConsumer>().ToArray();
        Assert.Equal(2, consumers.Length);
        var runningConsumers = new bool[consumers.Length];

        try
        {
            for (var index = 0; index < consumers.Length; index++)
            {
                await consumers[index].StartAsync(cancellationToken);
                runningConsumers[index] = true;
            }

            var expectedQueues = (await consumers[0].GetMessageQueuesAsync(scope.Topic, cancellationToken))
                .Select(QueueKey)
                .ToHashSet(StringComparer.Ordinal);
            Assert.True(
                expectedQueues.Count > 1,
                $"Rebalance test requires multiple queues, but found {expectedQueues.Count}.");

            await WaitUntilAsync(
                () => AssignmentsAreMutuallyExclusiveAndComplete(consumers, expectedQueues),
                RebalanceTimeout,
                () =>
                    "LitePull consumers did not reach mutually exclusive complete assignments. " +
                    FormatLitePullDiagnostics(consumers, expectedQueues),
                cancellationToken);

            await consumers[1].StopAsync(CancellationToken.None);
            runningConsumers[1] = false;

            await WaitUntilAsync(
                () => expectedQueues.SetEquals(consumers[0].Assignment.Select(QueueKey)),
                RebalanceTimeout,
                () =>
                    "The surviving LitePull consumer did not take over every queue. " +
                    FormatLitePullDiagnostics(consumers, expectedQueues),
                cancellationToken);
            Assert.Empty(consumers[1].Assignment);
        }
        finally
        {
            for (var index = consumers.Length - 1; index >= 0; index--)
            {
                if (runningConsumers[index])
                {
                    await consumers[index].StopAsync(CancellationToken.None);
                }
            }
        }
    }

    private static void ConfigurePushConsumer(
        RemotingPushConsumerOptions options,
        string group,
        string topic,
        string tag)
    {
        options.GroupName = group;
        options.InitialPosition = ConsumeFromPosition.Beginning;
        options.MaxConcurrency = 1;
        options.BatchSize = 1;
        options.ConsumeMessageBatchSize = 1;
        options.LongPollingTimeout = TimeSpan.FromSeconds(1);
        options.RetryDelay = TimeSpan.FromMilliseconds(100);
        options.Subscribe(topic, new FilterExpression(tag));
    }

    private static void ConfigureLitePullConsumer(
        RemotingLitePullConsumerOptions options,
        string group,
        string topic,
        string tag)
    {
        options.GroupName = group;
        options.InitialOffset = QueryOffsetPolicy.Beginning;
        options.LongPollingTimeout = TimeSpan.FromSeconds(1);
        options.Subscribe(topic, new FilterExpression(tag));
    }

    private static async Task<IReadOnlyList<string>> SendRoundToEveryQueueAsync(
        IRemotingProducer producer,
        IReadOnlyList<RemotingMessageQueue> queues,
        string topic,
        string tag,
        string roundPrefix,
        ConcurrentDictionary<string, byte> expectedBodies,
        CancellationToken cancellationToken)
    {
        var bodies = new List<string>(queues.Count);
        foreach (var queue in queues)
        {
            var body = $"{roundPrefix}-{queue.BrokerName}-{queue.QueueId}";
            Assert.True(expectedBodies.TryAdd(body, 0), $"Duplicate test body '{body}'.");
            bodies.Add(body);
            var result = await producer.SendAsync(
                new Message(topic, Encoding.UTF8.GetBytes(body)) { Tag = tag },
                queue,
                cancellationToken);
            Assert.Equal(RemotingSendStatus.SendOk, result.Status);
            Assert.Equal(queue.BrokerName, result.MessageQueue.BrokerName);
            Assert.Equal(queue.QueueId, result.MessageQueue.QueueId);
        }

        return bodies;
    }

    private static bool EveryConsumerHasDelivered(int[] consumerDeliveries)
    {
        for (var index = 0; index < consumerDeliveries.Length; index++)
        {
            if (Volatile.Read(ref consumerDeliveries[index]) == 0)
            {
                return false;
            }
        }

        return true;
    }

    private static bool AssignmentsAreMutuallyExclusiveAndComplete(
        IReadOnlyList<IRemotingLitePullConsumer> consumers,
        IReadOnlySet<string> expectedQueues)
    {
        var first = consumers[0].Assignment.Select(QueueKey).ToHashSet(StringComparer.Ordinal);
        var second = consumers[1].Assignment.Select(QueueKey).ToHashSet(StringComparer.Ordinal);
        return first.Count > 0 &&
               second.Count > 0 &&
               !first.Overlaps(second) &&
               expectedQueues.SetEquals(first.Concat(second));
    }

    private static bool PushAssignmentsAreMutuallyExclusiveAndComplete(
        IReadOnlyList<RemotingPushConsumer> consumers,
        string topic,
        IReadOnlySet<string> expectedQueues)
    {
        var first = consumers[0].Assignment
            .Where(queue => string.Equals(queue.Topic, topic, StringComparison.Ordinal))
            .Select(QueueKey)
            .ToHashSet(StringComparer.Ordinal);
        var second = consumers[1].Assignment
            .Where(queue => string.Equals(queue.Topic, topic, StringComparison.Ordinal))
            .Select(QueueKey)
            .ToHashSet(StringComparer.Ordinal);
        return first.Count > 0 &&
               second.Count > 0 &&
               !first.Overlaps(second) &&
               expectedQueues.SetEquals(first.Concat(second));
    }

    private static async Task WaitUntilAsync(
        Func<bool> condition,
        TimeSpan timeout,
        Func<string> diagnostic,
        CancellationToken cancellationToken)
    {
        var deadline = DateTimeOffset.UtcNow + timeout;
        while (!condition() && DateTimeOffset.UtcNow < deadline)
        {
            await Task.Delay(TimeSpan.FromMilliseconds(100), cancellationToken);
        }

        Assert.True(condition(), diagnostic());
    }

    private static string FormatPushDiagnostics(
        IEnumerable<string> expectedBodies,
        IReadOnlyDictionary<string, int> deliveryCounts,
        IReadOnlyDictionary<(string Body, int Consumer), int> deliveryOwners)
    {
        var missing = expectedBodies
            .Where(body => !deliveryCounts.ContainsKey(body))
            .Order(StringComparer.Ordinal);
        var deliveries = deliveryOwners
            .OrderBy(static delivery => delivery.Key.Body, StringComparer.Ordinal)
            .ThenBy(static delivery => delivery.Key.Consumer)
            .Select(delivery =>
                $"{delivery.Key.Body}->consumer-{delivery.Key.Consumer}({delivery.Value})");
        return $"missing=[{string.Join(", ", missing)}], deliveries=[{string.Join(", ", deliveries)}]";
    }

    private static string FormatLitePullDiagnostics(
        IReadOnlyList<IRemotingLitePullConsumer> consumers,
        IReadOnlySet<string> expectedQueues)
    {
        var expected = string.Join(", ", expectedQueues.Order(StringComparer.Ordinal));
        var first = string.Join(", ", consumers[0].Assignment.Select(QueueKey).Order(StringComparer.Ordinal));
        var second = string.Join(", ", consumers[1].Assignment.Select(QueueKey).Order(StringComparer.Ordinal));
        return $"expected=[{expected}], first=[{first}], second=[{second}]";
    }

    private static string FormatPushAssignmentDiagnostics(
        IReadOnlyList<RemotingPushConsumer> consumers,
        string topic,
        IReadOnlySet<string> expectedQueues)
    {
        var expected = string.Join(", ", expectedQueues.Order(StringComparer.Ordinal));
        var first = string.Join(", ", consumers[0].Assignment
            .Where(queue => string.Equals(queue.Topic, topic, StringComparison.Ordinal))
            .Select(QueueKey)
            .Order(StringComparer.Ordinal));
        var second = string.Join(", ", consumers[1].Assignment
            .Where(queue => string.Equals(queue.Topic, topic, StringComparison.Ordinal))
            .Select(QueueKey)
            .Order(StringComparer.Ordinal));
        return $"expected=[{expected}], first=[{first}], second=[{second}]";
    }

    private static string QueueKey(RemotingPullMessageQueue queue)
    {
        return $"{queue.Topic}/{queue.BrokerName}/{queue.QueueId}";
    }

    private static string FormatQueue(RemotingMessageQueue queue)
    {
        return $"{queue.Topic}/{queue.BrokerName}/{queue.QueueId}";
    }

    private sealed record FirstPushConsumerMarker;

    private sealed record SecondPushConsumerMarker;
}
