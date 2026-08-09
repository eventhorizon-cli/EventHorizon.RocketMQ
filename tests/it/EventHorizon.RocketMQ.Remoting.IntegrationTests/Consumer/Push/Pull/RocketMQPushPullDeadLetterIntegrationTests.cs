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
using EventHorizon.RocketMQ.Remoting.Consumer.Push.Assignment;
using EventHorizon.RocketMQ.Remoting.Consumer.Push.Pull.Offset;
using EventHorizon.RocketMQ.Remoting.IntegrationTests.Consumer.Push.Pop.Support;
using EventHorizon.RocketMQ.Remoting.Producer;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace EventHorizon.RocketMQ.Remoting.IntegrationTests.Consumer.Push.Pull;

public sealed class RocketMQPushPullDeadLetterIntegrationTests(
    RocketMQSingleBrokerContainerFixtureRegistry registry)
{
    [Fact]
    [Trait("Category", "Integration")]
    public async Task PushPullConsumer_HandlerRequestsNegativeDelay_ForwardsToDeadLetterQueue()
    {
        await RunDeadLetterWorkflowAsync(
            DeadLetterScenario.Explicit,
            TestContext.Current.CancellationToken);
    }

    [Fact]
    [Trait("Category", "Integration")]
    public async Task PushPullConsumer_ConcurrentRetryFailsAgain_ForwardsToDeadLetterQueue()
    {
        await RunDeadLetterWorkflowAsync(
            DeadLetterScenario.ConcurrentRetryExhausted,
            TestContext.Current.CancellationToken);
    }

    [Fact]
    [Trait("Category", "Integration")]
    public async Task PushPullConsumer_MessageGroupRetryFailsAgain_ForwardsToDeadLetterQueue()
    {
        await RunDeadLetterWorkflowAsync(
            DeadLetterScenario.MessageGroupRetryExhausted,
            TestContext.Current.CancellationToken);
    }

    [Fact]
    [Trait("Category", "Integration")]
    public async Task PushPullConsumer_OrderlyRetryFailsAgain_ForwardsToDeadLetterQueue()
    {
        await RunDeadLetterWorkflowAsync(
            DeadLetterScenario.OrderlyRetryExhausted,
            TestContext.Current.CancellationToken);
    }

    [Fact]
    [Trait("Category", "Integration")]
    public async Task PushPullBroadcastConsumer_OrderlyRetryFailsAgain_ForwardsToDeadLetterQueue()
    {
        await RunDeadLetterWorkflowAsync(
            DeadLetterScenario.OrderlyBroadcastRetryExhausted,
            TestContext.Current.CancellationToken);
    }

    private async Task RunDeadLetterWorkflowAsync(
        DeadLetterScenario scenario,
        CancellationToken cancellationToken)
    {
        using var settlements = new RemotingSettlementObserver();
        var fixture = await registry.GetFixtureAsync(cancellationToken);
        var role = scenario switch
        {
            DeadLetterScenario.Explicit => "direct",
            DeadLetterScenario.ConcurrentRetryExhausted => "concurrent",
            DeadLetterScenario.MessageGroupRetryExhausted => "message-group",
            DeadLetterScenario.OrderlyRetryExhausted => "orderly",
            DeadLetterScenario.OrderlyBroadcastRetryExhausted => "orderly-broadcast",
            _ => throw new ArgumentOutOfRangeException(nameof(scenario))
        };
        var topicType = scenario == DeadLetterScenario.MessageGroupRetryExhausted
            ? RocketMQTestTopicType.Fifo
            : RocketMQTestTopicType.Normal;
        var scope = await fixture.CreateTestScopeAsync(topicType, cancellationToken);
        var consumerGroup = scope.CreateConsumerGroupName($"remoting-push-pull-dlq-{role}");
        var deadLetterTopic = $"%DLQ%{consumerGroup}";
        var tag = $"remoting-push-pull-dlq-{role}-{Guid.NewGuid():N}";
        var body = $"remoting-push-pull-dlq-{role}-{Guid.NewGuid():N}";
        var expectedDeliveryCount = scenario == DeadLetterScenario.Explicit ? 1 : 2;
        var terminalAttempt = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var deliveryCount = 0;
        var isOrderlyBroadcast = scenario == DeadLetterScenario.OrderlyBroadcastRetryExhausted;
        var localOffsetPath = isOrderlyBroadcast
            ? Path.Combine(Path.GetTempPath(), $"rocketmq-remoting-dlq-{Guid.NewGuid():N}.json")
            : null;
        var services = new ServiceCollection();
        var rocketMQ = services.AddRocketMQRemoting(options =>
        {
            options.NamesrvAddr = fixture.NameServerAddress;
            options.InstanceName = $"remoting-push-pull-dlq-{role}-{Guid.NewGuid():N}";
        });
        rocketMQ.AddRemotingProducer(options =>
            options.GroupName = scope.CreateProducerGroupName($"remoting-push-pull-dlq-{role}-producer"));
        rocketMQ.AddRemotingPushConsumerWithTestHandler<DeadLetterConsumerMarker>(options =>
        {
            options.GroupName = consumerGroup;
            options.InitialPosition = ConsumeFromPosition.Beginning;
            options.PullBatchSize = 1;
            options.ConsumeMessageBatchSize = 1;
            options.MaxConcurrency = 1;
            options.MaxDeliveryAttempts = expectedDeliveryCount;
            options.LongPollingTimeout = TimeSpan.FromSeconds(1);
            options.RetryDelay = TimeSpan.FromMilliseconds(100);
            options.ConsumerMode = isOrderlyBroadcast ? ConsumerMode.Broadcasting : ConsumerMode.Clustering;
            options.LocalOffsetStorePath = localOffsetPath;
            options.ConsumeOrderly = scenario is
                DeadLetterScenario.OrderlyRetryExhausted or
                DeadLetterScenario.OrderlyBroadcastRetryExhausted;
            options.Subscribe(scope.Topic, new FilterExpression(tag));
        }, (messages, context, _) =>
            {
                var message = Assert.Single(messages);
                if (Encoding.UTF8.GetString(message.Body) != body)
                {
                    return ValueTask.FromResult(ConsumeResult.Success);
                }

                if (Interlocked.Increment(ref deliveryCount) == expectedDeliveryCount)
                {
                    terminalAttempt.TrySetResult();
                }

                if (scenario is
                    DeadLetterScenario.OrderlyRetryExhausted or
                    DeadLetterScenario.OrderlyBroadcastRetryExhausted)
                {
                    context.SuspendCurrentQueueDuration = TimeSpan.FromMilliseconds(50);
                }

                context.DelayLevelWhenNextConsume = scenario switch
                {
                    DeadLetterScenario.Explicit => -1,
                    DeadLetterScenario.ConcurrentRetryExhausted => 1,
                    _ => 0
                };
                return ValueTask.FromResult(ConsumeResult.Retry);
            });

        await using var provider = services.BuildServiceProvider(
            new ServiceProviderOptions { ValidateOnBuild = true });
        var producer = provider.GetRequiredService<IRemotingProducer>();
        var consumer = provider.GetRequiredService<IRemotingPushConsumer>();
        await producer.StartAsync(cancellationToken);
        await consumer.StartAsync(cancellationToken);
        try
        {
            var pushConsumer = Assert.IsType<RemotingPushConsumer>(consumer);
            await BrokerAssignedPopIntegrationTestSupport.WaitUntilAsync(
                () => Task.FromResult(pushConsumer.ReceiveAssignments.Any(assignment =>
                    string.Equals(assignment.Topic, scope.Topic, StringComparison.Ordinal) &&
                    assignment.Mode == RemotingPushReceiveMode.Pull)),
                BrokerAssignedPopIntegrationTestSupport.DeliveryTimeout,
                cancellationToken);

            var sent = await producer.SendAsync(new Message(scope.Topic, Encoding.UTF8.GetBytes(body))
            {
                Tag = tag,
                MessageGroup = scenario == DeadLetterScenario.MessageGroupRetryExhausted
                    ? $"remoting-push-pull-dlq-{Guid.NewGuid():N}"
                    : null
            }, cancellationToken);
            await terminalAttempt.Task.WaitAsync(
                BrokerAssignedPopIntegrationTestSupport.DeliveryTimeout,
                cancellationToken);
            if (scenario == DeadLetterScenario.ConcurrentRetryExhausted)
            {
                await settlements.WaitForAsync(
                    "nack",
                    scope.Topic,
                    consumerGroup,
                    1,
                    BrokerAssignedPopIntegrationTestSupport.DeliveryTimeout,
                    cancellationToken,
                    sent.MessageId);
            }

            await settlements.WaitForAsync(
                "reject",
                scope.Topic,
                consumerGroup,
                1,
                BrokerAssignedPopIntegrationTestSupport.DeliveryTimeout,
                cancellationToken,
                sent.MessageId);
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
            if (isOrderlyBroadcast)
            {
                var queue = new RemotingConsumerQueue(
                    scope.Topic,
                    sent.MessageQueue.BrokerName,
                    sent.MessageQueue.QueueId);
                await BrokerAssignedPopIntegrationTestSupport.WaitUntilAsync(
                    async () => await new BroadcastOffsetStore(localOffsetPath, "unused", "unused")
                        .ReadAsync(queue, cancellationToken) == sent.QueueOffset + 1,
                    BrokerAssignedPopIntegrationTestSupport.DeliveryTimeout,
                    cancellationToken);
            }
            else
            {
                var commit = await fixture.WaitForConsumerCommitAsync(
                    consumerGroup,
                    scope.Topic,
                    sent.MessageQueue.BrokerName,
                    sent.MessageQueue.QueueId,
                    BrokerAssignedPopIntegrationTestSupport.DeliveryTimeout,
                    cancellationToken);
                Assert.True(commit.Committed, $"PULL dead-letter source offset was not committed. {commit.Progress}");
            }

            Assert.Equal(1, settlements.Count("reject", scope.Topic, consumerGroup, sent.MessageId));
            Assert.Equal(expectedDeliveryCount, Volatile.Read(ref deliveryCount));
        }
        finally
        {
            await consumer.StopAsync(CancellationToken.None);
            await producer.StopAsync(CancellationToken.None);
            if (localOffsetPath is not null)
            {
                File.Delete(localOffsetPath);
            }
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
                options.GroupName = scope.CreateConsumerGroupName("remoting-push-pull-dead-letter-observer");
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

    private enum DeadLetterScenario
    {
        Explicit,
        ConcurrentRetryExhausted,
        MessageGroupRetryExhausted,
        OrderlyRetryExhausted,
        OrderlyBroadcastRetryExhausted
    }
}
