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
using EventHorizon.RocketMQ.Remoting.Consumer.LitePull;
using EventHorizon.RocketMQ.Remoting.Consumer.Push;
using EventHorizon.RocketMQ.Remoting.IntegrationTests.Consumer.Push.Pop.Support;
using EventHorizon.RocketMQ.Remoting.Producer;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace EventHorizon.RocketMQ.Remoting.IntegrationTests.Consumer.Push.Pop;

public sealed class RocketMQBrokerAssignedPopDeadLetterIntegrationTests(
    RocketMQSingleBrokerContainerFixtureRegistry registry)
{
    [Fact]
    [Trait("Category", "Integration")]
    public async Task BrokerAssignedPop_HandlerRequestsDeadLetter_ForwardsBeforeAcknowledgingReceipt()
    {
        using var settlements = new RemotingSettlementObserver();
        var cancellationToken = TestContext.Current.CancellationToken;
        var fixture = await registry.GetFixtureAsync(cancellationToken);
        var scope = await fixture.CreateTestScopeAsync(RocketMQTestTopicType.Normal, cancellationToken);
        var consumerGroup = scope.CreateConsumerGroupName("remoting-broker-pop-dead-letter");
        var deadLetterTopic = $"%DLQ%{consumerGroup}";
        var tag = $"remoting-broker-pop-dead-letter-{Guid.NewGuid():N}";
        var body = $"remoting-broker-pop-dead-letter-{Guid.NewGuid():N}";
        var deadLetterRequested = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var deliveryCount = 0;
        var services = new ServiceCollection();
        var rocketMQ = BrokerAssignedPopIntegrationTestSupport.AddRemotingClient(
            services,
            fixture,
            $"remoting-broker-pop-dead-letter-{Guid.NewGuid():N}");
        rocketMQ.AddRemotingProducer(options =>
            options.GroupName = scope.CreateProducerGroupName("remoting-broker-pop-dead-letter-producer"));
        rocketMQ.AddRemotingPushConsumerWithTestHandler<DeadLetterConsumerMarker>(options =>
        {
            BrokerAssignedPopIntegrationTestSupport.ConfigureBrokerAssignedPop(options, consumerGroup, scope.Topic, tag);
            options.PopBatchSize = 1;
            options.ConsumeMessageBatchSize = 1;
        }, (messages, _, _) =>
            {
                var message = Assert.Single(messages);
                if (Encoding.UTF8.GetString(message.Body) != body)
                {
                    return ValueTask.FromResult(ConsumeResult.Success);
                }

                Interlocked.Increment(ref deliveryCount);
                deadLetterRequested.TrySetResult();
                return ValueTask.FromResult(ConsumeResult.DeadLetter);
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
            await deadLetterRequested.Task.WaitAsync(
                BrokerAssignedPopIntegrationTestSupport.DeliveryTimeout,
                cancellationToken);
            await BrokerAssignedPopIntegrationTestSupport.WaitUntilAsync(
                async () => (await fixture.GetTopicListAsync(cancellationToken)).Contains(
                    deadLetterTopic,
                    StringComparison.Ordinal),
                BrokerAssignedPopIntegrationTestSupport.DeliveryTimeout,
                cancellationToken);

            var deadLetter = await ConsumeDeadLetterAsync(
                fixture,
                scope,
                deadLetterTopic,
                body,
                cancellationToken);
            Assert.Equal(scope.Topic, deadLetter.Topic);
            Assert.Equal(body, Encoding.UTF8.GetString(deadLetter.Body));
            await settlements.WaitForAsync(
                "reject",
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
            Assert.Equal(1, Volatile.Read(ref deliveryCount));
        }
        finally
        {
            await consumer.StopAsync(CancellationToken.None);
            await producer.StopAsync(CancellationToken.None);
        }
    }

    private static async Task<RemotingMessageView> ConsumeDeadLetterAsync(
        RocketMQSingleBrokerContainerFixture fixture,
        RocketMQTestScope scope,
        string deadLetterTopic,
        string expectedBody,
        CancellationToken cancellationToken)
    {
        var services = new ServiceCollection();
        services
            .AddRocketMQRemoting(options => options.NamesrvAddr = fixture.NameServerAddress)
            .AddRemotingLitePullConsumer(options =>
            {
                options.GroupName = scope.CreateConsumerGroupName("remoting-dead-letter-observer");
                options.InitialPosition = ConsumeFromPosition.Beginning;
                options.EnableAutoCommit = false;
                options.LongPollingTimeout = TimeSpan.FromSeconds(1);
                options.PollTimeout = TimeSpan.FromSeconds(2);
                options.Subscribe(deadLetterTopic);
            });

        await using var provider = services.BuildServiceProvider(
            new ServiceProviderOptions { ValidateOnBuild = true });
        var observer = provider.GetRequiredService<IRemotingLitePullConsumer>();
        await observer.StartAsync(cancellationToken);
        try
        {
            var deadline = DateTimeOffset.UtcNow + BrokerAssignedPopIntegrationTestSupport.DeliveryTimeout;
            do
            {
                var messages = await observer.PollAsync(cancellationToken: cancellationToken);
                var matching = messages.SingleOrDefault(message =>
                    Encoding.UTF8.GetString(message.Body) == expectedBody);
                if (matching is not null)
                {
                    await observer.CommitAsync(cancellationToken);
                    return matching;
                }
            }
            while (DateTimeOffset.UtcNow < deadline);

            throw new TimeoutException($"No matching message was consumed from dead-letter topic '{deadLetterTopic}'.");
        }
        finally
        {
            await observer.StopAsync(CancellationToken.None);
        }
    }

    private sealed record DeadLetterConsumerMarker;
}
