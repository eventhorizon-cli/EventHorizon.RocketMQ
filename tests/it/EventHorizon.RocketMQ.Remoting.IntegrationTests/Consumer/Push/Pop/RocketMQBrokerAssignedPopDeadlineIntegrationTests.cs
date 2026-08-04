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

public sealed class RocketMQBrokerAssignedPopDeadlineIntegrationTests(
    RocketMQSingleBrokerContainerFixtureRegistry registry)
{
    [Fact]
    [Trait("Category", "Integration")]
    public async Task BrokerAssignedPop_HandlerExceedsInvisibleDuration_AllowsBrokerRedelivery()
    {
        using var settlements = new RemotingSettlementObserver();
        var cancellationToken = TestContext.Current.CancellationToken;
        var fixture = await registry.GetFixtureAsync(cancellationToken);
        var scope = await fixture.CreateTestScopeAsync(RocketMQTestTopicType.Normal, cancellationToken);
        var consumerGroup = scope.CreateConsumerGroupName("remoting-broker-pop-deadline");
        var tag = $"remoting-broker-pop-deadline-{Guid.NewGuid():N}";
        var body = $"remoting-broker-pop-deadline-{Guid.NewGuid():N}";
        var firstHandlerStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseFirstHandler = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var redelivered = new TaskCompletionSource<RemotingMessageView>(TaskCreationOptions.RunContinuationsAsynchronously);
        var deliveryCount = 0;

        async ValueTask<ConsumeResult> HandleAsync(
            IReadOnlyList<RemotingMessageView> messages,
            RemotingPushConsumeContext _,
            CancellationToken handlerCancellation)
        {
            var message = Assert.Single(messages);
            if (Encoding.UTF8.GetString(message.Body) != body)
            {
                return ConsumeResult.Success;
            }

            if (Interlocked.Increment(ref deliveryCount) == 1)
            {
                firstHandlerStarted.TrySetResult();
                await releaseFirstHandler.Task.WaitAsync(handlerCancellation);
            }
            else
            {
                redelivered.TrySetResult(message);
            }

            return ConsumeResult.Success;
        }

        var services = new ServiceCollection();
        var rocketMQ = BrokerAssignedPopIntegrationTestSupport.AddRemotingClient(
            services,
            fixture,
            $"remoting-broker-pop-deadline-{Guid.NewGuid():N}");
        rocketMQ.AddRemotingProducer(options =>
            options.GroupName = scope.CreateProducerGroupName("remoting-broker-pop-deadline-producer"));
        rocketMQ.AddRemotingPushConsumerWithTestHandler<FirstDeadlineConsumerMarker>(
            options => BrokerAssignedPopIntegrationTestSupport.ConfigureBrokerAssignedPop(
                options,
                consumerGroup,
                scope.Topic,
                tag),
            HandleAsync);
        rocketMQ.AddRemotingPushConsumerWithTestHandler<SecondDeadlineConsumerMarker>(
            options => BrokerAssignedPopIntegrationTestSupport.ConfigureBrokerAssignedPop(
                options,
                consumerGroup,
                scope.Topic,
                tag),
            HandleAsync);

        await using var provider = services.BuildServiceProvider(
            new ServiceProviderOptions { ValidateOnBuild = true });
        await using var requestMode = await RemotingMessageRequestModeScope.UsePopAsync(
            provider,
            fixture.BrokerAddress,
            scope.Topic,
            consumerGroup,
            cancellationToken);
        var producer = provider.GetRequiredService<IRemotingProducer>();
        var consumers = provider.GetServices<IRemotingPushConsumer>().ToArray();
        Assert.Equal(2, consumers.Length);
        await producer.StartAsync(cancellationToken);
        try
        {
            foreach (var consumer in consumers)
            {
                await consumer.StartAsync(cancellationToken);
                await BrokerAssignedPopIntegrationTestSupport.AssertWildcardPopAssignmentAsync(
                    consumer,
                    scope.Topic,
                    cancellationToken);
            }

            var sent = await producer.SendAsync(
                new Message(scope.Topic, Encoding.UTF8.GetBytes(body)) { Tag = tag },
                cancellationToken);
            await firstHandlerStarted.Task.WaitAsync(
                BrokerAssignedPopIntegrationTestSupport.DeliveryTimeout,
                cancellationToken);
            var redelivery = await redelivered.Task.WaitAsync(
                BrokerAssignedPopIntegrationTestSupport.DeliveryTimeout,
                cancellationToken);

            Assert.Equal(sent.MessageId, redelivery.MessageId);
            Assert.Equal(scope.Topic, redelivery.Topic);
            Assert.True(redelivery.DeliveryAttempt >= 2);

            releaseFirstHandler.TrySetResult();
            await settlements.WaitForAsync(
                "ack",
                scope.Topic,
                consumerGroup,
                1,
                BrokerAssignedPopIntegrationTestSupport.DeliveryTimeout,
                cancellationToken,
                sent.MessageId);
            Assert.True(Volatile.Read(ref deliveryCount) >= 2);
        }
        finally
        {
            releaseFirstHandler.TrySetResult();
            foreach (var consumer in consumers.Reverse())
            {
                await consumer.StopAsync(CancellationToken.None);
            }

            await producer.StopAsync(CancellationToken.None);
        }
    }

    private sealed record FirstDeadlineConsumerMarker;

    private sealed record SecondDeadlineConsumerMarker;
}
