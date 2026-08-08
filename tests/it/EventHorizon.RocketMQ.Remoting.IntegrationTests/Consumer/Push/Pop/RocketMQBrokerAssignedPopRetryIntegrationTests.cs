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

public sealed class RocketMQBrokerAssignedPopRetryIntegrationTests(
    RocketMQSingleBrokerContainerFixtureRegistry registry)
{
    [Fact]
    [Trait("Category", "Integration")]
    public async Task BrokerAssignedPop_HandlerRequestsRetryAtDeliveryLimit_RedeliversWithoutDeadLetterAndAcknowledgesLatestReceipt()
    {
        using var settlements = new RemotingSettlementObserver();
        var cancellationToken = TestContext.Current.CancellationToken;
        var fixture = await registry.GetFixtureAsync(cancellationToken);
        var scope = await fixture.CreateTestScopeAsync(RocketMQTestTopicType.Normal, cancellationToken);
        var consumerGroup = scope.CreateConsumerGroupName("remoting-broker-pop-retry");
        var tag = $"remoting-broker-pop-retry-{Guid.NewGuid():N}";
        var body = $"remoting-broker-pop-retry-{Guid.NewGuid():N}";
        var retryRequested = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var redelivered = new TaskCompletionSource<RemotingMessageView>(TaskCreationOptions.RunContinuationsAsynchronously);
        var deliveryCount = 0;
        var services = new ServiceCollection();
        var rocketMQ = BrokerAssignedPopIntegrationTestSupport.AddRemotingClient(
            services,
            fixture,
            $"remoting-broker-pop-retry-{Guid.NewGuid():N}");
        rocketMQ.AddRemotingProducer(options =>
            options.GroupName = scope.CreateProducerGroupName("remoting-broker-pop-retry-producer"));
        rocketMQ.AddRemotingPushConsumerWithTestHandler<RetryConsumerMarker>(options =>
        {
            BrokerAssignedPopIntegrationTestSupport.ConfigureBrokerAssignedPop(options, consumerGroup, scope.Topic, tag);
            options.PopBatchSize = 1;
            options.ConsumeMessageBatchSize = 1;
            options.MaxDeliveryAttempts = 1;
            options.RetryDelay = TimeSpan.FromSeconds(10);
        }, (messages, _, _) =>
            {
                var message = Assert.Single(messages);
                if (Encoding.UTF8.GetString(message.Body) != body)
                {
                    return ValueTask.FromResult(ConsumeResult.Success);
                }

                if (Interlocked.Increment(ref deliveryCount) == 1)
                {
                    retryRequested.TrySetResult();
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
            await retryRequested.Task.WaitAsync(
                BrokerAssignedPopIntegrationTestSupport.DeliveryTimeout,
                cancellationToken);
            var retry = await redelivered.Task.WaitAsync(
                BrokerAssignedPopIntegrationTestSupport.DeliveryTimeout,
                cancellationToken);

            Assert.Equal(sent.MessageId, retry.MessageId);
            Assert.Equal(scope.Topic, retry.Topic);
            Assert.Equal("broker-a", retry.BrokerName);
            Assert.True(retry.QueueId >= 0);
            Assert.True(retry.DeliveryAttempt >= 2);
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
            Assert.Equal(
                0,
                settlements.Count("reject", scope.Topic, consumerGroup, sent.MessageId));
            Assert.Equal(2, Volatile.Read(ref deliveryCount));
        }
        finally
        {
            await consumer.StopAsync(CancellationToken.None);
            await producer.StopAsync(CancellationToken.None);
        }
    }

    private sealed record RetryConsumerMarker;
}
