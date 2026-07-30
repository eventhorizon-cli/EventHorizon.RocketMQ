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
using EventHorizon.RocketMQ.Remoting.Producer;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace EventHorizon.RocketMQ.Remoting.IntegrationTests;

public sealed class RocketMQCrossProcessRebalanceIntegrationTests(
    RocketMQContainerFixtureRegistry registry)
{
    private static readonly TimeSpan ProcessStartupTimeout = TimeSpan.FromSeconds(20);
    private static readonly TimeSpan RebalanceTimeout = TimeSpan.FromSeconds(10);

    [Theory]
    [InlineData(false, false)]
    [InlineData(true, false)]
    [InlineData(false, true)]
    [Trait("Category", "Integration")]
    public async Task PushConsumersInSeparateProcessesRebalanceAfterMemberLeaves(
        bool consumeOrderly,
        bool terminateSecondProcess)
    {
        Assert.True(
            File.Exists(RemotingConsumerProcess.HostAssemblyPath),
            $"Cross-process consumer host was not built at '{RemotingConsumerProcess.HostAssemblyPath}'.");

        var cancellationToken = TestContext.Current.CancellationToken;
        var fixture = await registry.GetFixtureAsync(cancellationToken);
        var scope = await fixture.CreateTestScopeAsync(RocketMQTestTopicType.Normal, cancellationToken);
        var suffix = Guid.NewGuid().ToString("N");
        var group = scope.CreateConsumerGroupName("remoting-cross-process-rebalance");
        var tag = $"remoting-cross-process-rebalance-{suffix}";
        var expectedBodies = new ConcurrentDictionary<string, byte>(StringComparer.Ordinal);

        var services = new ServiceCollection();
        var rocketMQ = services.AddRocketMQRemoting(options =>
        {
            options.NamesrvAddr = fixture.NameServerAddress;
            options.InstanceName = $"remoting-cross-process-producer-{suffix}";
        });
        rocketMQ.AddRemotingProducer(options =>
            options.GroupName = scope.CreateProducerGroupName("remoting-cross-process-rebalance"));

        await using var provider = services.BuildServiceProvider(
            new ServiceProviderOptions { ValidateOnBuild = true });
        var producer = provider.GetRequiredService<IRemotingProducer>();
        await producer.StartAsync(cancellationToken);
        try
        {
            var queues = (await producer.GetPublishMessageQueuesAsync(scope.Topic, cancellationToken))
                .OrderBy(static queue => queue.BrokerName, StringComparer.Ordinal)
                .ThenBy(static queue => queue.QueueId)
                .ToArray();
            Assert.True(
                queues.Length > 1,
                $"Cross-process rebalance test requires multiple queues, but found {queues.Length}.");
            var expectedQueues = queues.Select(FormatQueue).ToHashSet(StringComparer.Ordinal);

            await using var first = await RemotingConsumerProcess.StartAsync(
                "first",
                fixture.NameServerAddress,
                $"remoting-cross-process-{suffix}-first",
                group,
                scope.Topic,
                tag,
                consumeOrderly,
                ProcessStartupTimeout,
                cancellationToken);
            await using var second = await RemotingConsumerProcess.StartAsync(
                "second",
                fixture.NameServerAddress,
                $"remoting-cross-process-{suffix}-second",
                group,
                scope.Topic,
                tag,
                consumeOrderly,
                ProcessStartupTimeout,
                cancellationToken);

            Assert.NotEqual(Environment.ProcessId, first.ProcessId);
            Assert.NotEqual(Environment.ProcessId, second.ProcessId);
            Assert.NotEqual(first.ProcessId, second.ProcessId);

            await WaitUntilAsync(
                () => AssignmentsAreMutuallyExclusiveAndComplete(first, second, expectedQueues),
                RebalanceTimeout,
                () =>
                    "Cross-process consumers did not reach mutually exclusive complete assignments. " +
                    FormatDiagnostics(first, second, expectedQueues),
                cancellationToken);

            var initialBodies = await SendRoundToEveryQueueAsync(
                producer,
                queues,
                scope.Topic,
                tag,
                $"remoting-cross-process-{suffix}-initial",
                expectedBodies,
                cancellationToken);
            await WaitUntilAsync(
                () => initialBodies.All(body => TotalDeliveryCount(body, first, second) == 1),
                RebalanceTimeout,
                () =>
                    "Cross-process consumers did not consume the initial round exactly once. " +
                    FormatDeliveryDiagnostics(initialBodies, first, second),
                cancellationToken);
            await WaitForCommitsAsync(fixture, group, queues, RebalanceTimeout, cancellationToken);

            if (terminateSecondProcess)
            {
                await second.TerminateAsync(CancellationToken.None);
            }
            else
            {
                await second.StopAsync(CancellationToken.None);
            }
            await WaitUntilAsync(
                () => expectedQueues.SetEquals(first.Assignment),
                RebalanceTimeout,
                () =>
                    "The surviving cross-process consumer did not take over every queue after the member left. " +
                    FormatDiagnostics(first, second, expectedQueues),
                cancellationToken);

            var takeoverBodies = await SendRoundToEveryQueueAsync(
                producer,
                queues,
                scope.Topic,
                tag,
                $"remoting-cross-process-{suffix}-takeover",
                expectedBodies,
                cancellationToken);
            await WaitUntilAsync(
                () => takeoverBodies.All(body => first.GetDeliveryCount(body) == 1),
                RebalanceTimeout,
                () =>
                    "The surviving cross-process consumer did not consume the takeover round. " +
                    FormatDeliveryDiagnostics(takeoverBodies, first, second),
                cancellationToken);
            await WaitForCommitsAsync(fixture, group, queues, RebalanceTimeout, cancellationToken);
            await Task.Delay(TimeSpan.FromSeconds(1), cancellationToken);

            Assert.All(expectedBodies.Keys, body => Assert.Equal(1, TotalDeliveryCount(body, first, second)));
            Assert.All(takeoverBodies, body => Assert.Equal(0, second.GetDeliveryCount(body)));
        }
        finally
        {
            await producer.StopAsync(CancellationToken.None);
        }
    }

    private static bool AssignmentsAreMutuallyExclusiveAndComplete(
        RemotingConsumerProcess first,
        RemotingConsumerProcess second,
        IReadOnlySet<string> expectedQueues)
    {
        first.ThrowIfFaulted();
        second.ThrowIfFaulted();
        var firstAssignment = first.Assignment.ToHashSet(StringComparer.Ordinal);
        var secondAssignment = second.Assignment.ToHashSet(StringComparer.Ordinal);
        return firstAssignment.Count > 0 &&
               secondAssignment.Count > 0 &&
               !firstAssignment.Overlaps(secondAssignment) &&
               expectedQueues.SetEquals(firstAssignment.Concat(secondAssignment));
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
        }

        return bodies;
    }

    private static async Task WaitForCommitsAsync(
        RocketMQContainerFixture fixture,
        string group,
        IReadOnlyList<RemotingMessageQueue> queues,
        TimeSpan timeout,
        CancellationToken cancellationToken)
    {
        var results = await Task.WhenAll(queues.Select(async queue =>
        {
            var result = await fixture.WaitForConsumerCommitAsync(
                group,
                queue.Topic,
                queue.BrokerName,
                queue.QueueId,
                timeout,
                cancellationToken);
            return (Queue: queue, result.Committed, result.Progress);
        }));
        Assert.All(
            results,
            result => Assert.True(
                result.Committed,
                $"Offset for {FormatQueue(result.Queue)} was not committed. {result.Progress}"));
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

    private static int TotalDeliveryCount(
        string body,
        RemotingConsumerProcess first,
        RemotingConsumerProcess second)
    {
        first.ThrowIfFaulted();
        second.ThrowIfFaulted();
        return first.GetDeliveryCount(body) + second.GetDeliveryCount(body);
    }

    private static string FormatDiagnostics(
        RemotingConsumerProcess first,
        RemotingConsumerProcess second,
        IReadOnlySet<string> expectedQueues)
    {
        var expected = string.Join(", ", expectedQueues.Order(StringComparer.Ordinal));
        return $"expected=[{expected}], first=({first.GetDiagnostics()}), second=({second.GetDiagnostics()})";
    }

    private static string FormatDeliveryDiagnostics(
        IEnumerable<string> bodies,
        RemotingConsumerProcess first,
        RemotingConsumerProcess second)
    {
        return string.Join(
            ", ",
            bodies.Select(body =>
                $"{body}->first({first.GetDeliveryCount(body)}),second({second.GetDeliveryCount(body)})"));
    }

    private static string FormatQueue(RemotingMessageQueue queue)
    {
        return $"{queue.Topic}/{queue.BrokerName}/{queue.QueueId}";
    }
}
