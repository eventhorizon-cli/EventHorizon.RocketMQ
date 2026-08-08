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

using System.Text;
using EventHorizon.RocketMQ.IntegrationTestInfrastructure;
using EventHorizon.RocketMQ.Remoting.Consumer;
using EventHorizon.RocketMQ.Remoting.Consumer.Push;
using EventHorizon.RocketMQ.Remoting.IntegrationTests.Consumer.Push.Pop.Support;
using EventHorizon.RocketMQ.Remoting.Producer;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace EventHorizon.RocketMQ.Remoting.IntegrationTests.Consumer.Push.Pop;

public sealed class RocketMQBrokerAssignedPopNegativeDelayIntegrationTests(
    RocketMQSingleBrokerContainerFixtureRegistry registry)
{
    [Fact]
    [Trait("Category", "Integration")]
    public async Task BrokerAssignedPop_HandlerRequestsNegativeDelay_RedeliversWithoutDeadLetterAndAcknowledgesReceipt()
    {
        using var settlements = new RemotingSettlementObserver();
        var cancellationToken = TestContext.Current.CancellationToken;
        var fixture = await registry.GetFixtureAsync(cancellationToken);
        var scope = await fixture.CreateTestScopeAsync(RocketMQTestTopicType.Normal, cancellationToken);
        var consumerGroup = scope.CreateConsumerGroupName("remoting-broker-pop-negative-delay");
        var deadLetterTopic = $"%DLQ%{consumerGroup}";
        var tag = $"remoting-broker-pop-negative-delay-{Guid.NewGuid():N}";
        var body = $"remoting-broker-pop-negative-delay-{Guid.NewGuid():N}";
        var firstAttempt = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var redelivered = new TaskCompletionSource<RemotingMessageView>(TaskCreationOptions.RunContinuationsAsynchronously);
        var deliveryCount = 0;
        var services = new ServiceCollection();
        var rocketMQ = BrokerAssignedPopIntegrationTestSupport.AddRemotingClient(
            services,
            fixture,
            $"remoting-broker-pop-negative-delay-{Guid.NewGuid():N}");
        rocketMQ.AddRemotingProducer(options =>
            options.GroupName = scope.CreateProducerGroupName("remoting-broker-pop-negative-delay-producer"));
        rocketMQ.AddRemotingPushConsumerWithTestHandler<NegativeDelayConsumerMarker>(options =>
        {
            BrokerAssignedPopIntegrationTestSupport.ConfigureBrokerAssignedPop(
                options,
                consumerGroup,
                scope.Topic,
                tag);
            options.PopBatchSize = 1;
            options.ConsumeMessageBatchSize = 1;
        }, (messages, context, _) =>
            {
                var message = Assert.Single(messages);
                if (Encoding.UTF8.GetString(message.Body) != body)
                {
                    return ValueTask.FromResult(ConsumeResult.Success);
                }

                var attempt = Interlocked.Increment(ref deliveryCount);
                if (attempt == 1)
                {
                    firstAttempt.TrySetResult();
                    context.DelayLevelWhenNextConsume = -1;
                    return ValueTask.FromResult(ConsumeResult.Retry);
                }

                redelivered.TrySetResult(message);
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
            var sent = await producer.SendAsync(
                new Message(scope.Topic, Encoding.UTF8.GetBytes(body)) { Tag = tag },
                cancellationToken);
            await firstAttempt.Task.WaitAsync(
                BrokerAssignedPopIntegrationTestSupport.DeliveryTimeout,
                cancellationToken);
            var redeliveredMessage = await redelivered.Task.WaitAsync(
                BrokerAssignedPopIntegrationTestSupport.DeliveryTimeout,
                cancellationToken);

            Assert.Equal(sent.MessageId, redeliveredMessage.MessageId);
            Assert.Equal(scope.Topic, redeliveredMessage.Topic);
            Assert.Equal(body, Encoding.UTF8.GetString(redeliveredMessage.Body));
            Assert.Equal("broker-a", redeliveredMessage.BrokerName);
            Assert.True(redeliveredMessage.QueueId >= 0);
            Assert.True(redeliveredMessage.DeliveryAttempt >= 2);
            await settlements.WaitForAsync(
                "nack",
                scope.Topic,
                consumerGroup,
                1,
                BrokerAssignedPopIntegrationTestSupport.DeliveryTimeout,
                cancellationToken,
                sent.MessageId);
            await settlements.WaitForAsync(
                "ack",
                scope.Topic,
                consumerGroup,
                1,
                BrokerAssignedPopIntegrationTestSupport.DeliveryTimeout,
                cancellationToken,
                sent.MessageId);
            Assert.Equal(0, settlements.Count("reject", scope.Topic, consumerGroup, sent.MessageId));
            Assert.Equal(2, Volatile.Read(ref deliveryCount));
            Assert.DoesNotContain(
                deadLetterTopic,
                await fixture.GetTopicListAsync(cancellationToken),
                StringComparison.Ordinal);
        }
        finally
        {
            await consumer.StopAsync(CancellationToken.None);
            await producer.StopAsync(CancellationToken.None);
        }
    }

    private sealed record NegativeDelayConsumerMarker;
}
