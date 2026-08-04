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
using EventHorizon.RocketMQ.Remoting.Consumer.Push;
using EventHorizon.RocketMQ.Remoting.IntegrationTests.Consumer.Push.Pop.Support;
using EventHorizon.RocketMQ.Remoting.Producer;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace EventHorizon.RocketMQ.Remoting.IntegrationTests.Consumer.Push.Pop;

public sealed class RocketMQBrokerAssignedPopWildcardIntegrationTests(
    RocketMQSingleBrokerContainerFixtureRegistry registry)
{
    [Fact]
    [Trait("Category", "Integration")]
    public async Task BrokerAssignedPop_WildcardAssignment_ConsumesEveryPhysicalQueueAndAcknowledges()
    {
        using var settlements = new RemotingSettlementObserver();
        var cancellationToken = TestContext.Current.CancellationToken;
        var fixture = await registry.GetFixtureAsync(cancellationToken);
        var scope = await fixture.CreateTestScopeAsync(RocketMQTestTopicType.Normal, cancellationToken);
        var consumerGroup = scope.CreateConsumerGroupName("remoting-broker-pop-wildcard");
        var tag = $"remoting-broker-pop-wildcard-{Guid.NewGuid():N}";
        var expected = new ConcurrentDictionary<string, ExpectedDelivery>(StringComparer.Ordinal);
        var deliveries = new ConcurrentDictionary<string, ObservedDelivery>(StringComparer.Ordinal);
        var deliveryCounts = new ConcurrentDictionary<string, int>(StringComparer.Ordinal);
        var allDelivered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var services = new ServiceCollection();
        var rocketMQ = BrokerAssignedPopIntegrationTestSupport.AddRemotingClient(
            services,
            fixture,
            $"remoting-broker-pop-wildcard-{Guid.NewGuid():N}");
        rocketMQ.AddRemotingProducer(options =>
            options.GroupName = scope.CreateProducerGroupName("remoting-broker-pop-wildcard-producer"));
        rocketMQ.AddRemotingPushConsumerWithTestHandler<WildcardConsumerMarker>(options =>
        {
            BrokerAssignedPopIntegrationTestSupport.ConfigureBrokerAssignedPop(options, consumerGroup, scope.Topic, tag);
            options.PopBatchSize = 32;
            options.ConsumeMessageBatchSize = 32;
        }, (messages, _, _) =>
            {
                foreach (var message in messages)
                {
                    var body = Encoding.UTF8.GetString(message.Body);
                    if (!expected.ContainsKey(body))
                    {
                        continue;
                    }

                    deliveries[body] = new(message.Topic, message.BrokerName, message.QueueId);
                    deliveryCounts.AddOrUpdate(body, 1, static (_, count) => count + 1);
                }

                if (!expected.IsEmpty && expected.Keys.All(deliveries.ContainsKey))
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
            scope.Topic,
            consumerGroup,
            cancellationToken);
        var producer = provider.GetRequiredService<IRemotingProducer>();
        var consumer = provider.GetRequiredService<IRemotingPushConsumer>();
        await producer.StartAsync(cancellationToken);
        await consumer.StartAsync(cancellationToken);
        try
        {
            await BrokerAssignedPopIntegrationTestSupport.AssertWildcardPopAssignmentAsync(
                consumer,
                scope.Topic,
                cancellationToken);

            var queues = await producer.GetPublishMessageQueuesAsync(scope.Topic, cancellationToken);
            Assert.True(queues.Count > 1, $"Wildcard POP test requires multiple queues, but found {queues.Count}.");
            var plannedMessages = queues.Select(queue => new
            {
                Queue = queue,
                Body = $"wildcard-{queue.BrokerName}-{queue.QueueId}-{Guid.NewGuid():N}"
            }).ToArray();
            foreach (var planned in plannedMessages)
            {
                Assert.True(expected.TryAdd(
                    planned.Body,
                    new(planned.Queue.BrokerName, planned.Queue.QueueId)));
            }

            var sentMessages = new List<RemotingSendResult>(plannedMessages.Length);
            foreach (var planned in plannedMessages)
            {
                var result = await producer.SendAsync(
                    new Message(scope.Topic, Encoding.UTF8.GetBytes(planned.Body)) { Tag = tag },
                    planned.Queue,
                    cancellationToken);
                Assert.Equal(RemotingSendStatus.SendOk, result.Status);
                sentMessages.Add(result);
            }

            await allDelivered.Task.WaitAsync(
                BrokerAssignedPopIntegrationTestSupport.DeliveryTimeout,
                cancellationToken);
            foreach (var sent in sentMessages)
            {
                await settlements.WaitForAsync(
                    "ack",
                    scope.Topic,
                    consumerGroup,
                    1,
                    BrokerAssignedPopIntegrationTestSupport.DeliveryTimeout,
                    cancellationToken,
                    sent.MessageId);
            }

            Assert.Equal(expected.Count, deliveries.Count);
            foreach (var (body, expectedDelivery) in expected)
            {
                var observed = deliveries[body];
                Assert.Equal(scope.Topic, observed.Topic);
                Assert.Equal(expectedDelivery.BrokerName, observed.BrokerName);
                Assert.Equal(expectedDelivery.QueueId, observed.QueueId);
                Assert.Equal(1, deliveryCounts[body]);
            }
        }
        finally
        {
            await consumer.StopAsync(CancellationToken.None);
            await producer.StopAsync(CancellationToken.None);
        }
    }

    private sealed record ExpectedDelivery(string BrokerName, int QueueId);

    private sealed record ObservedDelivery(string Topic, string? BrokerName, int QueueId);

    private sealed record WildcardConsumerMarker;
}
