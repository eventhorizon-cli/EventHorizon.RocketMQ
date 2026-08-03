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
using EventHorizon.RocketMQ.Remoting.Consumer.Pull.Lite;
using EventHorizon.RocketMQ.Remoting.Consumer.Push;
using EventHorizon.RocketMQ.Remoting.Consumer.Push.Assignment;
using EventHorizon.RocketMQ.Remoting.Producer;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace EventHorizon.RocketMQ.Remoting.IntegrationTests;

public sealed class RocketMQBrokerAssignedPopIntegrationTests(
    RocketMQSingleBrokerContainerFixtureRegistry registry)
{
    private static readonly TimeSpan PopInvisibleDuration = TimeSpan.FromSeconds(5);
    private static readonly TimeSpan DeliveryTimeout = TimeSpan.FromSeconds(45);
    private static readonly TimeSpan SettlementObservationWindow = TimeSpan.FromSeconds(7);

    [Fact]
    [Trait("Category", "Integration")]
    public async Task BrokerAssignedPush_PullAndPopTopics_UsesOneHandlerAndRespectsReceiveModes()
    {
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
        var rocketMQ = AddRemotingClient(services, fixture, $"remoting-broker-mixed-{Guid.NewGuid():N}");
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
            options.PopInvisibleDuration = PopInvisibleDuration;
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
        var producer = provider.GetRequiredService<IRemotingProducer>();
        var consumer = provider.GetRequiredService<IRemotingPushConsumer>();
        await producer.StartAsync(cancellationToken);
        await consumer.StartAsync(cancellationToken);
        try
        {
            var pushConsumer = Assert.IsType<RemotingPushConsumer>(consumer);
            await WaitUntilAsync(
                () => Task.FromResult(
                    pushConsumer.ReceiveAssignments.Any(assignment =>
                        string.Equals(assignment.Topic, pullScope.Topic, StringComparison.Ordinal)) &&
                    pushConsumer.ReceiveAssignments.Any(assignment =>
                        string.Equals(assignment.Topic, popScope.Topic, StringComparison.Ordinal))),
                DeliveryTimeout,
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

            await producer.SendAsync(
                new Message(pullScope.Topic, Encoding.UTF8.GetBytes(pullBody)) { Tag = pullTag },
                cancellationToken);
            await producer.SendAsync(
                new Message(popScope.Topic, Encoding.UTF8.GetBytes(popBody)) { Tag = popTag },
                cancellationToken);

            await allDelivered.Task.WaitAsync(DeliveryTimeout, cancellationToken);
            await Task.Delay(SettlementObservationWindow, cancellationToken);

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

    [Fact]
    [Trait("Category", "Integration")]
    public async Task BrokerAssignedPop_WildcardAssignment_ConsumesEveryPhysicalQueueAndAcknowledges()
    {
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
        var rocketMQ = AddRemotingClient(services, fixture, $"remoting-broker-pop-wildcard-{Guid.NewGuid():N}");
        rocketMQ.AddRemotingProducer(options =>
            options.GroupName = scope.CreateProducerGroupName("remoting-broker-pop-wildcard-producer"));
        rocketMQ.AddRemotingPushConsumerWithTestHandler<WildcardConsumerMarker>(options =>
        {
            ConfigureBrokerAssignedPop(options, consumerGroup, scope.Topic, tag);
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
            await AssertWildcardPopAssignmentAsync(consumer, scope.Topic, cancellationToken);

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

            foreach (var planned in plannedMessages)
            {
                var result = await producer.SendAsync(
                    new Message(scope.Topic, Encoding.UTF8.GetBytes(planned.Body)) { Tag = tag },
                    planned.Queue,
                    cancellationToken);
                Assert.Equal(RemotingSendStatus.SendOk, result.Status);
            }

            await allDelivered.Task.WaitAsync(DeliveryTimeout, cancellationToken);
            await Task.Delay(SettlementObservationWindow, cancellationToken);

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

    [Fact]
    [Trait("Category", "Integration")]
    public async Task BrokerAssignedPop_HandlerRequestsRetry_RedeliversAndAcknowledgesLatestReceipt()
    {
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
        var rocketMQ = AddRemotingClient(services, fixture, $"remoting-broker-pop-retry-{Guid.NewGuid():N}");
        rocketMQ.AddRemotingProducer(options =>
            options.GroupName = scope.CreateProducerGroupName("remoting-broker-pop-retry-producer"));
        rocketMQ.AddRemotingPushConsumerWithTestHandler<RetryConsumerMarker>(options =>
        {
            ConfigureBrokerAssignedPop(options, consumerGroup, scope.Topic, tag);
            options.PopBatchSize = 1;
            options.ConsumeMessageBatchSize = 1;
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
            await AssertWildcardPopAssignmentAsync(consumer, scope.Topic, cancellationToken);
            var sent = await producer.SendAsync(
                new Message(scope.Topic, Encoding.UTF8.GetBytes(body)) { Tag = tag },
                cancellationToken);
            await retryRequested.Task.WaitAsync(DeliveryTimeout, cancellationToken);
            var retry = await redelivered.Task.WaitAsync(DeliveryTimeout, cancellationToken);

            Assert.Equal(sent.MessageId, retry.MessageId);
            Assert.Equal(scope.Topic, retry.Topic);
            Assert.Equal("broker-a", retry.BrokerName);
            Assert.True(retry.QueueId >= 0);
            Assert.True(retry.DeliveryAttempt >= 2);
            await Task.Delay(SettlementObservationWindow, cancellationToken);
            Assert.Equal(2, Volatile.Read(ref deliveryCount));
        }
        finally
        {
            await consumer.StopAsync(CancellationToken.None);
            await producer.StopAsync(CancellationToken.None);
        }
    }

    [Fact]
    [Trait("Category", "Integration")]
    public async Task BrokerAssignedPop_HandlerExceedsInvisibleDuration_RenewsAndPreventsDuplicateDelivery()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var fixture = await registry.GetFixtureAsync(cancellationToken);
        var scope = await fixture.CreateTestScopeAsync(RocketMQTestTopicType.Normal, cancellationToken);
        var consumerGroup = scope.CreateConsumerGroupName("remoting-broker-pop-renewal");
        var tag = $"remoting-broker-pop-renewal-{Guid.NewGuid():N}";
        var body = $"remoting-broker-pop-renewal-{Guid.NewGuid():N}";
        var handlerStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseHandler = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var handlerReturned = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
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

            Interlocked.Increment(ref deliveryCount);
            handlerStarted.TrySetResult();
            await releaseHandler.Task.WaitAsync(handlerCancellation);
            handlerReturned.TrySetResult();
            return ConsumeResult.Success;
        }

        var services = new ServiceCollection();
        var rocketMQ = AddRemotingClient(services, fixture, $"remoting-broker-pop-renewal-{Guid.NewGuid():N}");
        rocketMQ.AddRemotingProducer(options =>
            options.GroupName = scope.CreateProducerGroupName("remoting-broker-pop-renewal-producer"));
        rocketMQ.AddRemotingPushConsumerWithTestHandler<FirstRenewalConsumerMarker>(
            options => ConfigureBrokerAssignedPop(options, consumerGroup, scope.Topic, tag),
            HandleAsync);
        rocketMQ.AddRemotingPushConsumerWithTestHandler<SecondRenewalConsumerMarker>(
            options => ConfigureBrokerAssignedPop(options, consumerGroup, scope.Topic, tag),
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
                await AssertWildcardPopAssignmentAsync(consumer, scope.Topic, cancellationToken);
            }

            await producer.SendAsync(
                new Message(scope.Topic, Encoding.UTF8.GetBytes(body)) { Tag = tag },
                cancellationToken);
            await handlerStarted.Task.WaitAsync(DeliveryTimeout, cancellationToken);
            await Task.Delay(PopInvisibleDuration + TimeSpan.FromSeconds(3), cancellationToken);
            Assert.Equal(1, Volatile.Read(ref deliveryCount));

            releaseHandler.TrySetResult();
            await handlerReturned.Task.WaitAsync(TimeSpan.FromSeconds(10), cancellationToken);
            await Task.Delay(SettlementObservationWindow, cancellationToken);
            Assert.Equal(1, Volatile.Read(ref deliveryCount));
        }
        finally
        {
            releaseHandler.TrySetResult();
            foreach (var consumer in consumers.Reverse())
            {
                await consumer.StopAsync(CancellationToken.None);
            }

            await producer.StopAsync(CancellationToken.None);
        }
    }

    [Fact]
    [Trait("Category", "Integration")]
    public async Task BrokerAssignedPop_HandlerRequestsDeadLetter_ForwardsBeforeAcknowledgingReceipt()
    {
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
        var rocketMQ = AddRemotingClient(services, fixture, $"remoting-broker-pop-dead-letter-{Guid.NewGuid():N}");
        rocketMQ.AddRemotingProducer(options =>
            options.GroupName = scope.CreateProducerGroupName("remoting-broker-pop-dead-letter-producer"));
        rocketMQ.AddRemotingPushConsumerWithTestHandler<DeadLetterConsumerMarker>(options =>
        {
            ConfigureBrokerAssignedPop(options, consumerGroup, scope.Topic, tag);
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
            await AssertWildcardPopAssignmentAsync(consumer, scope.Topic, cancellationToken);
            await producer.SendAsync(
                new Message(scope.Topic, Encoding.UTF8.GetBytes(body)) { Tag = tag },
                cancellationToken);
            await deadLetterRequested.Task.WaitAsync(DeliveryTimeout, cancellationToken);
            await WaitUntilAsync(
                async () => (await fixture.GetTopicListAsync(cancellationToken)).Contains(
                    deadLetterTopic,
                    StringComparison.Ordinal),
                DeliveryTimeout,
                cancellationToken);

            var deadLetter = await ConsumeDeadLetterAsync(
                fixture,
                scope,
                deadLetterTopic,
                body,
                cancellationToken);
            Assert.Equal(scope.Topic, deadLetter.Topic);
            Assert.Equal(body, Encoding.UTF8.GetString(deadLetter.Body));
            await Task.Delay(SettlementObservationWindow, cancellationToken);
            Assert.Equal(1, Volatile.Read(ref deliveryCount));
        }
        finally
        {
            await consumer.StopAsync(CancellationToken.None);
            await producer.StopAsync(CancellationToken.None);
        }
    }

    private static RemotingRocketMQBuilder AddRemotingClient(
        ServiceCollection services,
        RocketMQSingleBrokerContainerFixture fixture,
        string instanceName) => services.AddRocketMQRemoting(options =>
        {
            options.NamesrvAddr = fixture.NameServerAddress;
            options.InstanceName = instanceName;
        });

    private static void ConfigureBrokerAssignedPop(
        RemotingPushConsumerOptions options,
        string consumerGroup,
        string topic,
        string tag)
    {
        options.GroupName = consumerGroup;
        options.QueueAssignmentMode = RemotingPushQueueAssignmentMode.Broker;
        options.InitialPosition = ConsumeFromPosition.End;
        options.MaxConcurrency = 1;
        options.PopBatchSize = 1;
        options.PopInvisibleDuration = PopInvisibleDuration;
        options.PopMaxInflightMessagesPerAssignment = 32;
        options.ConsumeMessageBatchSize = 1;
        options.LongPollingTimeout = TimeSpan.FromSeconds(1);
        options.Subscribe(topic, new FilterExpression(tag));
    }

    private static async Task AssertWildcardPopAssignmentAsync(
        IRemotingPushConsumer consumer,
        string topic,
        CancellationToken cancellationToken)
    {
        var pushConsumer = Assert.IsType<RemotingPushConsumer>(consumer);
        await WaitUntilAsync(
            () => Task.FromResult(pushConsumer.ReceiveAssignments.Any(assignment =>
                string.Equals(assignment.Topic, topic, StringComparison.Ordinal))),
            DeliveryTimeout,
            cancellationToken);
        var assignment = Assert.Single(pushConsumer.ReceiveAssignments, assignment =>
            string.Equals(assignment.Topic, topic, StringComparison.Ordinal));
        Assert.Equal("broker-a", assignment.BrokerName);
        Assert.Equal(-1, assignment.QueueId);
        Assert.Equal(RemotingPushReceiveMode.Pop, assignment.Mode);
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
            var deadline = DateTimeOffset.UtcNow + DeliveryTimeout;
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

    private static async Task WaitUntilAsync(
        Func<Task<bool>> condition,
        TimeSpan timeout,
        CancellationToken cancellationToken)
    {
        var deadline = DateTimeOffset.UtcNow + timeout;
        do
        {
            if (await condition())
            {
                return;
            }

            await Task.Delay(TimeSpan.FromMilliseconds(250), cancellationToken);
        }
        while (DateTimeOffset.UtcNow < deadline);

        Assert.Fail("The expected Broker state was not observed before the timeout.");
    }

    private sealed record ExpectedDelivery(string BrokerName, int QueueId);

    private sealed record ObservedDelivery(string Topic, string? BrokerName, int QueueId);

    private sealed record MixedModeConsumerMarker;

    private sealed record WildcardConsumerMarker;

    private sealed record RetryConsumerMarker;

    private sealed record FirstRenewalConsumerMarker;

    private sealed record SecondRenewalConsumerMarker;

    private sealed record DeadLetterConsumerMarker;
}
