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
using EventHorizon.RocketMQ.Remoting.Consumer.Push.Assignment;
using EventHorizon.RocketMQ.Remoting.IntegrationTests.Consumer.Push.Pop.Support;
using EventHorizon.RocketMQ.Remoting.Producer;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace EventHorizon.RocketMQ.Remoting.IntegrationTests.Consumer.Push.Pop;

public sealed class RocketMQBrokerAssignedMixedModeIntegrationTests(
    RocketMQSingleBrokerContainerFixtureRegistry registry)
{
    [Fact]
    [Trait("Category", "Integration")]
    public async Task BrokerAssignedPush_PullAndPopTopics_UsesOneHandlerAndRespectsReceiveModes()
    {
        using var settlements = new RemotingSettlementObserver();
        var cancellationToken = TestContext.Current.CancellationToken;
        var fixture = await registry.GetFixtureAsync(cancellationToken);
        var pullScope = await fixture.CreateTestScopeAsync(RocketMQTestTopicType.Normal, cancellationToken);
        var popScope = await fixture.CreateTestScopeAsync(RocketMQTestTopicType.Normal, cancellationToken);
        Assert.NotEqual(pullScope.Topic, popScope.Topic);

        var consumerGroup = pullScope.CreateConsumerGroupName("remoting-broker-mixed-mode");
        var pullTag = $"remoting-broker-pull-{Guid.NewGuid():N}";
        var popTag = $"remoting-broker-pop-{Guid.NewGuid():N}";
        var pullBody = $"pull-{Guid.NewGuid():N}";
        var popBody = $"pop-{Guid.NewGuid():N}";
        var expectedTopics = new Dictionary<string, string>(StringComparer.Ordinal)
        {
            [pullBody] = pullScope.Topic,
            [popBody] = popScope.Topic
        };
        var deliveries = new ConcurrentDictionary<string, RemotingMessageView>(StringComparer.Ordinal);
        var allDelivered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var pullDeliveryCount = 0;
        var services = new ServiceCollection();
        var rocketMQ = BrokerAssignedPopIntegrationTestSupport.AddRemotingClient(
            services,
            fixture,
            $"remoting-broker-mixed-{Guid.NewGuid():N}");
        rocketMQ.AddRemotingAdmin();
        rocketMQ.AddRemotingProducer(options =>
            options.GroupName = pullScope.CreateProducerGroupName("remoting-broker-mixed-producer"));
        rocketMQ.AddRemotingPushConsumerWithTestHandler<MixedModeConsumerMarker>(options =>
        {
            options.GroupName = consumerGroup;
            options.QueueAssignmentMode = RemotingPushQueueAssignmentMode.Broker;
            options.InitialPosition = ConsumeFromPosition.Beginning;
            options.MaxConcurrency = 2;
            options.PullBatchSize = 1;
            options.PopBatchSize = 1;
            options.PopInvisibleDuration = BrokerAssignedPopIntegrationTestSupport.PopInvisibleDuration;
            options.PopMaxInflightMessagesPerAssignment = 8;
            options.ConsumeMessageBatchSize = 1;
            options.LongPollingTimeout = TimeSpan.FromSeconds(1);
            options.Subscribe(pullScope.Topic, new FilterExpression(pullTag));
            options.Subscribe(popScope.Topic, new FilterExpression(popTag));
        }, (messages, _, _) =>
            {
                foreach (var message in messages)
                {
                    var body = Encoding.UTF8.GetString(message.Body);
                    if (expectedTopics.TryGetValue(body, out var expectedTopic) &&
                        string.Equals(message.Topic, expectedTopic, StringComparison.Ordinal))
                    {
                        if (string.Equals(body, pullBody, StringComparison.Ordinal) &&
                            Interlocked.Increment(ref pullDeliveryCount) == 1)
                        {
                            return ValueTask.FromResult(ConsumeResult.Retry);
                        }

                        deliveries[body] = message;
                    }
                }

                if (expectedTopics.Keys.All(deliveries.ContainsKey))
                {
                    allDelivered.TrySetResult();
                }

                return ValueTask.FromResult(ConsumeResult.Success);
            });

        await using var provider = services.BuildServiceProvider(
            new ServiceProviderOptions { ValidateOnBuild = true });
        await using var requestMode = await RemotingMessageRequestModeScope.UsePopAsync(
            provider,
            fixture.BrokerAddress,
            popScope.Topic,
            consumerGroup,
            cancellationToken);
        var admin = provider.GetRequiredService<IRemotingAdmin>();
        var producer = provider.GetRequiredService<IRemotingProducer>();
        var consumer = provider.GetRequiredService<IRemotingPushConsumer>();
        await producer.StartAsync(cancellationToken);
        await consumer.StartAsync(cancellationToken);
        try
        {
            var pushConsumer = Assert.IsType<RemotingPushConsumer>(consumer);
            await BrokerAssignedPopIntegrationTestSupport.WaitUntilAsync(
                () => Task.FromResult(
                    pushConsumer.ReceiveAssignments.Any(assignment =>
                        string.Equals(assignment.Topic, pullScope.Topic, StringComparison.Ordinal)) &&
                    pushConsumer.ReceiveAssignments.Any(assignment =>
                        string.Equals(assignment.Topic, popScope.Topic, StringComparison.Ordinal))),
                BrokerAssignedPopIntegrationTestSupport.DeliveryTimeout,
                cancellationToken);

            var pullAssignments = pushConsumer.ReceiveAssignments
                .Where(assignment => string.Equals(assignment.Topic, pullScope.Topic, StringComparison.Ordinal))
                .ToArray();
            Assert.NotEmpty(pullAssignments);
            Assert.All(pullAssignments, static assignment =>
                Assert.Equal(RemotingPushReceiveMode.Pull, assignment.Mode));
            var popAssignment = Assert.Single(pushConsumer.ReceiveAssignments, assignment =>
                string.Equals(assignment.Topic, popScope.Topic, StringComparison.Ordinal));
            Assert.Equal(RemotingPushReceiveMode.Pop, popAssignment.Mode);
            Assert.Equal(-1, popAssignment.QueueId);

            var pullSent = await producer.SendAsync(
                new Message(pullScope.Topic, Encoding.UTF8.GetBytes(pullBody)) { Tag = pullTag },
                cancellationToken);
            var popSent = await producer.SendAsync(
                new Message(popScope.Topic, Encoding.UTF8.GetBytes(popBody)) { Tag = popTag },
                cancellationToken);

            await allDelivered.Task.WaitAsync(
                BrokerAssignedPopIntegrationTestSupport.DeliveryTimeout,
                cancellationToken);
            await settlements.WaitForAsync(
                "nack",
                pullScope.Topic,
                consumerGroup,
                1,
                BrokerAssignedPopIntegrationTestSupport.DeliveryTimeout,
                cancellationToken,
                pullSent.MessageId);
            await settlements.WaitForAsync(
                "ack",
                popScope.Topic,
                consumerGroup,
                1,
                BrokerAssignedPopIntegrationTestSupport.DeliveryTimeout,
                cancellationToken,
                popSent.MessageId);
            var retryTopic = $"%RETRY%{consumerGroup}";
            await settlements.WaitForAsync(
                "commit",
                retryTopic,
                consumerGroup,
                2,
                BrokerAssignedPopIntegrationTestSupport.DeliveryTimeout,
                cancellationToken);
            var retryQueue = new RemotingConsumerQueue(
                retryTopic,
                pullSent.MessageQueue.BrokerName,
                queueId: 0);
            var retryOffset = await WaitForConsumerOffsetAsync(
                admin,
                consumerGroup,
                retryQueue,
                expectedOffset: 1,
                BrokerAssignedPopIntegrationTestSupport.DeliveryTimeout,
                cancellationToken);

            Assert.True(retryOffset >= 1, $"Broker-assigned retry offset was {retryOffset?.ToString() ?? "<none>"}.");
            Assert.Equal(pullScope.Topic, deliveries[pullBody].Topic);
            Assert.True(deliveries[pullBody].DeliveryAttempt >= 2);
            Assert.Equal(2, Volatile.Read(ref pullDeliveryCount));
            Assert.Equal(popScope.Topic, deliveries[popBody].Topic);
        }
        finally
        {
            await consumer.StopAsync(CancellationToken.None);
            await producer.StopAsync(CancellationToken.None);
        }
    }

    private static async Task<long?> WaitForConsumerOffsetAsync(
        IRemotingAdmin admin,
        string consumerGroup,
        RemotingConsumerQueue queue,
        long expectedOffset,
        TimeSpan timeout,
        CancellationToken cancellationToken)
    {
        var deadline = DateTimeOffset.UtcNow + timeout;
        long? committedOffset;
        do
        {
            committedOffset = await admin.GetConsumerOffsetAsync(consumerGroup, queue, cancellationToken);
            if (committedOffset >= expectedOffset)
            {
                return committedOffset;
            }

            await Task.Delay(TimeSpan.FromMilliseconds(250), cancellationToken);
        }
        while (DateTimeOffset.UtcNow < deadline);

        return committedOffset;
    }

    private sealed record MixedModeConsumerMarker;
}
