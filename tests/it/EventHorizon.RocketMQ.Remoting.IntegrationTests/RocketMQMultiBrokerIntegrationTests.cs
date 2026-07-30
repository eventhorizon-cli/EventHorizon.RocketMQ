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
using EventHorizon.RocketMQ.Remoting.Admin;
using EventHorizon.RocketMQ.Remoting.Consumer;
using EventHorizon.RocketMQ.Remoting.Consumer.Push;
using EventHorizon.RocketMQ.Remoting.Producer;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace EventHorizon.RocketMQ.Remoting.IntegrationTests;

[Collection(RocketMQMultiBrokerCollection.Name)]
[Trait("Topology", "MultiBroker")]
public sealed class RocketMQMultiBrokerIntegrationTests
{
    private static readonly string[] BrokerNames =
    [
        RocketMQMultiBrokerRemotingContainerFixture.BrokerAName,
        RocketMQMultiBrokerRemotingContainerFixture.BrokerBName,
        RocketMQMultiBrokerRemotingContainerFixture.BrokerCName
    ];

    private readonly RocketMQMultiBrokerRemotingContainerFixture _fixture;

    public RocketMQMultiBrokerIntegrationTests(RocketMQMultiBrokerRemotingContainerFixture fixture)
    {
        _fixture = fixture;
    }

    [Fact]
    [Trait("Category", "Integration")]
    public async Task ProducerDiscoversAndSendsToEveryQueueOnAllBrokers()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var services = new ServiceCollection();
        services
            .AddRocketMQRemoting(options => options.NamesrvAddr = _fixture.NameServerAddress)
            .AddRemotingProducer(options => options.GroupName = "remoting-multi-broker-producer-it");

        await using var provider = services.BuildServiceProvider(new ServiceProviderOptions { ValidateOnBuild = true });
        var producer = provider.GetRequiredService<IRemotingProducer>();
        await producer.StartAsync(cancellationToken);
        try
        {
            var queues = await GetExpectedQueuesAsync(producer, cancellationToken);
            foreach (var queue in queues)
            {
                var body = $"remoting-multi-broker-{queue.BrokerName}-{queue.QueueId}-{Guid.NewGuid():N}";
                var result = await producer.SendAsync(
                    new Message(RocketMQMultiBrokerRemotingContainerFixture.TestTopic, Encoding.UTF8.GetBytes(body)),
                    queue,
                    cancellationToken);

                Assert.Equal(RemotingSendStatus.SendOk, result.Status);
                Assert.Equal(queue.BrokerName, result.MessageQueue.BrokerName);
                Assert.Equal(queue.QueueId, result.MessageQueue.QueueId);
            }
        }
        finally
        {
            await producer.StopAsync(CancellationToken.None);
        }
    }

    [Fact]
    [Trait("Category", "Integration")]
    public async Task PushConsumerConsumesAndCommitsEveryQueueAcrossAllBrokers()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var suffix = Guid.NewGuid().ToString("N");
        var group = $"remoting-multi-broker-push-{suffix}";
        var tag = $"multi-broker-push-{suffix}";
        var expectedQueueCount = BrokerNames.Length * RocketMQMultiBrokerRemotingContainerFixture.QueuesPerBroker;
        var deliveries = new ConcurrentDictionary<string, int>(StringComparer.Ordinal);
        var allConsumed = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        var services = new ServiceCollection();
        services
            .AddRocketMQRemoting(options =>
            {
                options.NamesrvAddr = _fixture.NameServerAddress;
                options.InstanceName = $"remoting-multi-broker-push-{suffix}";
                options.HeartbeatBrokerInterval = TimeSpan.FromMilliseconds(250);
                options.PollNameServerInterval = TimeSpan.FromMilliseconds(250);
            })
            .AddRemotingAdmin()
            .AddRemotingProducer(options => options.GroupName = $"remoting-multi-broker-producer-{suffix}")
            .AddTestRemotingPushConsumer<RocketMQMultiBrokerIntegrationTests>(options =>
            {
                options.GroupName = group;
                options.InitialPosition = ConsumeFromPosition.Beginning;
                options.MaxConcurrency = 4;
                options.BatchSize = 1;
                options.ConsumeMessageBatchSize = 1;
                options.LongPollingTimeout = TimeSpan.FromSeconds(1);
                options.RetryDelay = TimeSpan.FromMilliseconds(100);
                options.Subscribe(RocketMQMultiBrokerRemotingContainerFixture.TestTopic, new FilterExpression(tag));
            }, (messages, _, _) =>
                {
                    foreach (var message in messages)
                    {
                        var body = Encoding.UTF8.GetString(message.Body);
                        deliveries.AddOrUpdate(body, 1, static (_, count) => count + 1);
                    }

                    if (deliveries.Count == expectedQueueCount)
                    {
                        allConsumed.TrySetResult();
                    }

                    return ValueTask.FromResult(ConsumeResult.Success);
                });

        await using var provider = services.BuildServiceProvider(new ServiceProviderOptions { ValidateOnBuild = true });
        var admin = provider.GetRequiredService<IRemotingAdmin>();
        var producer = provider.GetRequiredService<IRemotingProducer>();
        var consumer = provider.GetRequiredService<IRemotingPushConsumer>();
        await producer.StartAsync(cancellationToken);
        await consumer.StartAsync(cancellationToken);
        try
        {
            var queues = await GetExpectedQueuesAsync(producer, cancellationToken);
            var expectedBodies = await SendMessageToEveryQueueAsync(producer, queues, tag, cancellationToken);

            await allConsumed.Task.WaitAsync(TimeSpan.FromSeconds(30), cancellationToken);
            await Task.Delay(TimeSpan.FromSeconds(1), cancellationToken);

            Assert.Equal(
                expectedBodies.OrderBy(static body => body, StringComparer.Ordinal),
                deliveries.Keys.OrderBy(static body => body, StringComparer.Ordinal));
            Assert.All(deliveries, static delivery => Assert.Equal(1, delivery.Value));
            await AssertOffsetsCommittedAsync(admin, group, queues, cancellationToken);
        }
        finally
        {
            await consumer.StopAsync(CancellationToken.None);
            await producer.StopAsync(CancellationToken.None);
        }
    }

    [Fact]
    [Trait("Category", "Integration")]
    public async Task BlockedQueueDoesNotStallOtherQueuesOrAdvanceItsOffset()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var suffix = Guid.NewGuid().ToString("N");
        var group = $"remoting-multi-broker-blocked-{suffix}";
        var tag = $"multi-broker-blocked-{suffix}";
        var expectedQueueCount = BrokerNames.Length * RocketMQMultiBrokerRemotingContainerFixture.QueuesPerBroker;
        var deliveries = new ConcurrentDictionary<string, int>(StringComparer.Ordinal);
        var slowStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseSlow = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var fastConsumed = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var allConsumed = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        string? slowBody = null;

        var services = new ServiceCollection();
        services
            .AddRocketMQRemoting(options =>
            {
                options.NamesrvAddr = _fixture.NameServerAddress;
                options.InstanceName = $"remoting-multi-broker-blocked-{suffix}";
                options.HeartbeatBrokerInterval = TimeSpan.FromMilliseconds(250);
                options.PollNameServerInterval = TimeSpan.FromMilliseconds(250);
            })
            .AddRemotingAdmin()
            .AddRemotingProducer(options => options.GroupName = $"remoting-multi-broker-producer-{suffix}")
            .AddTestRemotingPushConsumer<RocketMQMultiBrokerIntegrationTests>(options =>
            {
                options.GroupName = group;
                options.InitialPosition = ConsumeFromPosition.Beginning;
                options.MaxConcurrency = 4;
                options.BatchSize = 1;
                options.ConsumeMessageBatchSize = 1;
                options.MaxCachedMessages = expectedQueueCount;
                options.LongPollingTimeout = TimeSpan.FromSeconds(1);
                options.RetryDelay = TimeSpan.FromMilliseconds(100);
                options.Subscribe(RocketMQMultiBrokerRemotingContainerFixture.TestTopic, new FilterExpression(tag));
            }, async (messages, _, token) =>
                {
                    var body = Encoding.UTF8.GetString(Assert.Single(messages).Body);
                    if (string.Equals(body, slowBody, StringComparison.Ordinal))
                    {
                        slowStarted.TrySetResult();
                        await releaseSlow.Task.WaitAsync(token);
                    }

                    deliveries.AddOrUpdate(body, 1, static (_, count) => count + 1);
                    if (deliveries.Count == expectedQueueCount - 1)
                    {
                        fastConsumed.TrySetResult();
                    }

                    if (deliveries.Count == expectedQueueCount)
                    {
                        allConsumed.TrySetResult();
                    }

                    return ConsumeResult.Success;
                });

        await using var provider = services.BuildServiceProvider(new ServiceProviderOptions { ValidateOnBuild = true });
        var admin = provider.GetRequiredService<IRemotingAdmin>();
        var producer = provider.GetRequiredService<IRemotingProducer>();
        var consumer = provider.GetRequiredService<IRemotingPushConsumer>();
        await producer.StartAsync(cancellationToken);
        var queues = await GetExpectedQueuesAsync(producer, cancellationToken);
        var slowQueue = queues.First(queue =>
            queue.BrokerName == RocketMQMultiBrokerRemotingContainerFixture.BrokerAName && queue.QueueId == 0);
        slowBody = $"{tag}-{slowQueue.BrokerName}-{slowQueue.QueueId}";
        var expectedOffsets = new Dictionary<(string BrokerName, int QueueId), long>();
        foreach (var queue in queues)
        {
            expectedOffsets[(queue.BrokerName, queue.QueueId)] =
                await admin.GetMaxOffsetAsync(queue, cancellationToken: cancellationToken) + 1;
        }

        await consumer.StartAsync(cancellationToken);
        try
        {
            await SendMessageToEveryQueueAsync(producer, queues, tag, cancellationToken);
            await slowStarted.Task.WaitAsync(TimeSpan.FromSeconds(30), cancellationToken);
            await fastConsumed.Task.WaitAsync(TimeSpan.FromSeconds(30), cancellationToken);

            foreach (var queue in queues.Where(queue =>
                         queue.BrokerName != slowQueue.BrokerName || queue.QueueId != slowQueue.QueueId))
            {
                await AssertOffsetCommittedAsync(
                    admin,
                    group,
                    queue,
                    expectedOffsets[(queue.BrokerName, queue.QueueId)],
                    cancellationToken);
            }

            var slowCommitted = await admin.GetConsumerOffsetAsync(group, slowQueue, cancellationToken);
            Assert.True(
                slowCommitted < expectedOffsets[(slowQueue.BrokerName, slowQueue.QueueId)],
                $"Blocked queue {slowQueue.BrokerName}/{slowQueue.QueueId} advanced to {slowCommitted} before its handler completed.");

            releaseSlow.TrySetResult();
            await allConsumed.Task.WaitAsync(TimeSpan.FromSeconds(30), cancellationToken);
            Assert.All(deliveries, static delivery => Assert.Equal(1, delivery.Value));
            await AssertOffsetsCommittedAsync(admin, group, queues, cancellationToken);
        }
        finally
        {
            releaseSlow.TrySetResult();
            await consumer.StopAsync(CancellationToken.None);
            await producer.StopAsync(CancellationToken.None);
        }
    }

    [Fact]
    [Trait("Category", "Integration")]
    public async Task RetriedQueueDoesNotDuplicateSuccessfulQueues()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var suffix = Guid.NewGuid().ToString("N");
        var group = $"remoting-multi-broker-retry-{suffix}";
        var tag = $"multi-broker-retry-{suffix}";
        var expectedQueueCount = BrokerNames.Length * RocketMQMultiBrokerRemotingContainerFixture.QueuesPerBroker;
        var attempts = new ConcurrentDictionary<string, int>(StringComparer.Ordinal);
        var successful = new ConcurrentDictionary<string, int>(StringComparer.Ordinal);
        var retryRequested = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var fastConsumed = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var allConsumed = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        string? retryBody = null;

        var services = new ServiceCollection();
        services
            .AddRocketMQRemoting(options =>
            {
                options.NamesrvAddr = _fixture.NameServerAddress;
                options.InstanceName = $"remoting-multi-broker-retry-{suffix}";
                options.HeartbeatBrokerInterval = TimeSpan.FromMilliseconds(250);
                options.PollNameServerInterval = TimeSpan.FromMilliseconds(250);
            })
            .AddRemotingAdmin()
            .AddRemotingProducer(options => options.GroupName = $"remoting-multi-broker-producer-{suffix}")
            .AddTestRemotingPushConsumer<RocketMQMultiBrokerIntegrationTests>(options =>
            {
                options.GroupName = group;
                options.InitialPosition = ConsumeFromPosition.Beginning;
                options.MaxConcurrency = 4;
                options.BatchSize = 1;
                options.ConsumeMessageBatchSize = 1;
                options.MaxCachedMessages = expectedQueueCount;
                options.MaxDeliveryAttempts = 3;
                options.LongPollingTimeout = TimeSpan.FromSeconds(1);
                options.RetryDelay = TimeSpan.FromMilliseconds(100);
                options.Subscribe(RocketMQMultiBrokerRemotingContainerFixture.TestTopic, new FilterExpression(tag));
            }, (messages, _, _) =>
                {
                    var body = Encoding.UTF8.GetString(Assert.Single(messages).Body);
                    var attempt = attempts.AddOrUpdate(body, 1, static (_, count) => count + 1);
                    if (string.Equals(body, retryBody, StringComparison.Ordinal) && attempt == 1)
                    {
                        retryRequested.TrySetResult();
                        return ValueTask.FromResult(ConsumeResult.Retry);
                    }

                    successful.AddOrUpdate(body, 1, static (_, count) => count + 1);
                    if (successful.Count == expectedQueueCount - 1)
                    {
                        fastConsumed.TrySetResult();
                    }

                    if (successful.Count == expectedQueueCount)
                    {
                        allConsumed.TrySetResult();
                    }

                    return ValueTask.FromResult(ConsumeResult.Success);
                });

        await using var provider = services.BuildServiceProvider(new ServiceProviderOptions { ValidateOnBuild = true });
        var admin = provider.GetRequiredService<IRemotingAdmin>();
        var producer = provider.GetRequiredService<IRemotingProducer>();
        var consumer = provider.GetRequiredService<IRemotingPushConsumer>();
        await producer.StartAsync(cancellationToken);
        var queues = await GetExpectedQueuesAsync(producer, cancellationToken);
        var retryQueue = queues.First(queue =>
            queue.BrokerName == RocketMQMultiBrokerRemotingContainerFixture.BrokerAName && queue.QueueId == 0);
        retryBody = $"{tag}-{retryQueue.BrokerName}-{retryQueue.QueueId}";
        await consumer.StartAsync(cancellationToken);
        try
        {
            var expectedBodies = await SendMessageToEveryQueueAsync(producer, queues, tag, cancellationToken);
            await retryRequested.Task.WaitAsync(TimeSpan.FromSeconds(30), cancellationToken);
            await fastConsumed.Task.WaitAsync(TimeSpan.FromSeconds(30), cancellationToken);
            await allConsumed.Task.WaitAsync(TimeSpan.FromSeconds(30), cancellationToken);
            await Task.Delay(TimeSpan.FromSeconds(1), cancellationToken);

            Assert.Equal(expectedBodies.Order(StringComparer.Ordinal), successful.Keys.Order(StringComparer.Ordinal));
            Assert.All(successful, static delivery => Assert.Equal(1, delivery.Value));
            Assert.Equal(2, attempts[retryBody]);
            Assert.All(
                attempts.Where(attempt => !string.Equals(attempt.Key, retryBody, StringComparison.Ordinal)),
                static attempt => Assert.Equal(1, attempt.Value));
            await AssertOffsetsCommittedAsync(admin, group, queues, cancellationToken);
        }
        finally
        {
            await consumer.StopAsync(CancellationToken.None);
            await producer.StopAsync(CancellationToken.None);
        }
    }

    [Theory]
    [InlineData(3)]
    [InlineData(10)]
    [Trait("Category", "Integration")]
    public async Task PushConsumersInSameGroupShareQueuesWithoutDuplicates(int consumerCount)
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var suffix = Guid.NewGuid().ToString("N");
        var group = $"remoting-multi-broker-group-{consumerCount}-{suffix}";
        var tag = $"multi-broker-group-{consumerCount}-{suffix}";
        var expectedQueueCount = BrokerNames.Length * RocketMQMultiBrokerRemotingContainerFixture.QueuesPerBroker;
        var deliveries = new ConcurrentDictionary<string, int>(StringComparer.Ordinal);
        var deliveriesByConsumer = new ConcurrentDictionary<int, int>();
        var allConsumed = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var consumerProviders = new List<ServiceProvider>(consumerCount);
        var consumers = new List<IRemotingPushConsumer>(consumerCount);

        var producerServices = new ServiceCollection();
        producerServices
            .AddRocketMQRemoting(options => options.NamesrvAddr = _fixture.NameServerAddress)
            .AddRemotingAdmin()
            .AddRemotingProducer(options => options.GroupName = $"remoting-multi-broker-producer-{suffix}");
        await using var producerProvider = producerServices.BuildServiceProvider(
            new ServiceProviderOptions { ValidateOnBuild = true });
        var admin = producerProvider.GetRequiredService<IRemotingAdmin>();
        var producer = producerProvider.GetRequiredService<IRemotingProducer>();

        try
        {
            for (var index = 0; index < consumerCount; index++)
            {
                var consumerIndex = index;
                var provider = CreatePushConsumerProvider(
                    group,
                    $"remoting-multi-broker-group-{suffix}-{consumerIndex}",
                    tag,
                    body =>
                    {
                        deliveries.AddOrUpdate(body, 1, static (_, count) => count + 1);
                        deliveriesByConsumer.AddOrUpdate(consumerIndex, 1, static (_, count) => count + 1);
                        if (deliveries.Count == expectedQueueCount)
                        {
                            allConsumed.TrySetResult();
                        }
                    });
                consumerProviders.Add(provider);
                consumers.Add(provider.GetRequiredService<IRemotingPushConsumer>());
            }

            await producer.StartAsync(cancellationToken);
            await Task.WhenAll(consumers.Select(consumer => consumer.StartAsync(cancellationToken).AsTask()));
            await Task.Delay(TimeSpan.FromSeconds(3), cancellationToken);

            var queues = await GetExpectedQueuesAsync(producer, cancellationToken);
            var expectedBodies = await SendMessageToEveryQueueAsync(producer, queues, tag, cancellationToken);

            await allConsumed.Task.WaitAsync(TimeSpan.FromSeconds(30), cancellationToken);
            await Task.Delay(TimeSpan.FromSeconds(1), cancellationToken);

            Assert.Equal(
                expectedBodies.OrderBy(static body => body, StringComparer.Ordinal),
                deliveries.Keys.OrderBy(static body => body, StringComparer.Ordinal));
            Assert.All(deliveries, static delivery => Assert.Equal(1, delivery.Value));
            Assert.Equal(Math.Min(consumerCount, queues.Count), deliveriesByConsumer.Count);
            Assert.Equal(Math.Max(0, consumerCount - queues.Count), consumerCount - deliveriesByConsumer.Count);
            await AssertOffsetsCommittedAsync(admin, group, queues, cancellationToken);
        }
        finally
        {
            await Task.WhenAll(consumers.Select(consumer => consumer.StopAsync(CancellationToken.None).AsTask()));
            await Task.WhenAll(consumerProviders.Select(provider => provider.DisposeAsync().AsTask()));
            await producer.StopAsync(CancellationToken.None);
        }
    }

    private ServiceProvider CreatePushConsumerProvider(
        string group,
        string instanceName,
        string tag,
        Action<string> onMessage)
    {
        var services = new ServiceCollection();
        services
            .AddRocketMQRemoting(options =>
            {
                options.NamesrvAddr = _fixture.NameServerAddress;
                options.InstanceName = instanceName;
                options.HeartbeatBrokerInterval = TimeSpan.FromMilliseconds(250);
                options.PollNameServerInterval = TimeSpan.FromMilliseconds(250);
            })
            .AddTestRemotingPushConsumer<RocketMQMultiBrokerIntegrationTests>(options =>
            {
                options.GroupName = group;
                options.InitialPosition = ConsumeFromPosition.Beginning;
                options.MaxConcurrency = 1;
                options.BatchSize = 1;
                options.ConsumeMessageBatchSize = 1;
                options.LongPollingTimeout = TimeSpan.FromSeconds(1);
                options.RetryDelay = TimeSpan.FromMilliseconds(100);
                options.Subscribe(RocketMQMultiBrokerRemotingContainerFixture.TestTopic, new FilterExpression(tag));
            }, (messages, _, _) =>
                {
                    foreach (var message in messages)
                    {
                        onMessage(Encoding.UTF8.GetString(message.Body));
                    }

                    return ValueTask.FromResult(ConsumeResult.Success);
                });

        return services.BuildServiceProvider(new ServiceProviderOptions { ValidateOnBuild = true });
    }

    private async Task<IReadOnlyList<RemotingMessageQueue>> GetExpectedQueuesAsync(
        IRemotingProducer producer,
        CancellationToken cancellationToken)
    {
        var queues = await producer.GetPublishMessageQueuesAsync(
            RocketMQMultiBrokerRemotingContainerFixture.TestTopic,
            cancellationToken);
        Assert.Equal(BrokerNames.Length * RocketMQMultiBrokerRemotingContainerFixture.QueuesPerBroker, queues.Count);

        foreach (var brokerName in BrokerNames)
        {
            var brokerQueues = queues
                .Where(queue => queue.BrokerName == brokerName)
                .OrderBy(queue => queue.QueueId)
                .ToArray();
            Assert.Equal(RocketMQMultiBrokerRemotingContainerFixture.QueuesPerBroker, brokerQueues.Length);
            Assert.Equal(
                Enumerable.Range(0, RocketMQMultiBrokerRemotingContainerFixture.QueuesPerBroker),
                brokerQueues.Select(queue => queue.QueueId));
        }

        return queues
            .OrderBy(queue => queue.BrokerName, StringComparer.Ordinal)
            .ThenBy(queue => queue.QueueId)
            .ToArray();
    }

    private static async Task<IReadOnlySet<string>> SendMessageToEveryQueueAsync(
        IRemotingProducer producer,
        IReadOnlyList<RemotingMessageQueue> queues,
        string tag,
        CancellationToken cancellationToken)
    {
        var expectedBodies = new HashSet<string>(StringComparer.Ordinal);
        foreach (var queue in queues)
        {
            var body = $"{tag}-{queue.BrokerName}-{queue.QueueId}";
            Assert.True(expectedBodies.Add(body));
            var result = await producer.SendAsync(
                new Message(RocketMQMultiBrokerRemotingContainerFixture.TestTopic, Encoding.UTF8.GetBytes(body))
                {
                    Tag = tag
                },
                queue,
                cancellationToken);
            Assert.Equal(RemotingSendStatus.SendOk, result.Status);
            Assert.Equal(queue.BrokerName, result.MessageQueue.BrokerName);
            Assert.Equal(queue.QueueId, result.MessageQueue.QueueId);
        }

        return expectedBodies;
    }

    private static async Task AssertOffsetsCommittedAsync(
        IRemotingAdmin admin,
        string group,
        IReadOnlyList<RemotingMessageQueue> queues,
        CancellationToken cancellationToken)
    {
        foreach (var queue in queues)
        {
            var expectedOffset = await admin.GetMaxOffsetAsync(queue, cancellationToken: cancellationToken);
            var deadline = DateTimeOffset.UtcNow + TimeSpan.FromSeconds(10);
            long? committedOffset;
            do
            {
                committedOffset = await admin.GetConsumerOffsetAsync(group, queue, cancellationToken);
                if (committedOffset >= expectedOffset)
                {
                    break;
                }

                await Task.Delay(TimeSpan.FromMilliseconds(250), cancellationToken);
            }
            while (DateTimeOffset.UtcNow < deadline);

            Assert.True(
                committedOffset >= expectedOffset,
                $"Consumer group '{group}' did not commit {queue.BrokerName}/{queue.QueueId}. " +
                $"Expected at least {expectedOffset}, actual {committedOffset?.ToString() ?? "<none>"}.");
        }
    }

    private static async Task AssertOffsetCommittedAsync(
        IRemotingAdmin admin,
        string group,
        RemotingMessageQueue queue,
        long expectedOffset,
        CancellationToken cancellationToken)
    {
        var deadline = DateTimeOffset.UtcNow + TimeSpan.FromSeconds(10);
        long? committedOffset;
        do
        {
            committedOffset = await admin.GetConsumerOffsetAsync(group, queue, cancellationToken);
            if (committedOffset >= expectedOffset)
            {
                return;
            }

            await Task.Delay(TimeSpan.FromMilliseconds(250), cancellationToken);
        }
        while (DateTimeOffset.UtcNow < deadline);

        Assert.Fail(
            $"Consumer group '{group}' did not commit {queue.BrokerName}/{queue.QueueId}. " +
            $"Expected at least {expectedOffset}, actual {committedOffset?.ToString() ?? "<none>"}.");
    }
}
