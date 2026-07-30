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
using EventHorizon.RocketMQ.Grpc.Consumer;
using EventHorizon.RocketMQ.Grpc.Consumer.Push;
using EventHorizon.RocketMQ.Grpc.Consumer.Simple;
using EventHorizon.RocketMQ.Grpc.Producer;
using EventHorizon.RocketMQ.Grpc.Producer.Transactions;
using EventHorizon.RocketMQ.IntegrationTestInfrastructure;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace EventHorizon.RocketMQ.Grpc.IntegrationTests;

public sealed class RocketMQContainerTests(RocketMQContainerFixtureRegistry registry)
{

    [Fact]
    [Trait("Category", "Integration")]
    public async Task GrpcProducerAndSimpleConsumerRoundTrip()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var fixture = await registry.GetFixtureAsync(cancellationToken);
        var scope = await fixture.CreateTestScopeAsync(RocketMQTestTopicType.Normal, cancellationToken);
        var consumerGroup = scope.CreateConsumerGroupName("grpc-simple-consumer");
        var services = new ServiceCollection();
        services
            .AddRocketMQGrpc(options => options.Endpoint = fixture.GrpcEndpoint)
            .AddGrpcProducer()
            .AddGrpcSimpleConsumer(options =>
            {
                options.GroupName = consumerGroup;
                options.AwaitDuration = TimeSpan.FromSeconds(3);
                options.Subscribe(scope.Topic, new FilterExpression("grpc-simple"));
            });

        await using var provider = services.BuildServiceProvider(new ServiceProviderOptions { ValidateOnBuild = true });
        var producer = provider.GetRequiredService<IGrpcProducer>();
        var consumer = provider.GetRequiredService<IGrpcSimpleConsumer>();
        await producer.StartAsync(cancellationToken);
        await consumer.StartAsync(cancellationToken);
        try
        {
            var body = $"grpc-simple-{Guid.NewGuid():N}";
            var receipt = await producer.SendAsync(new Message(
                scope.Topic,
                Encoding.UTF8.GetBytes(body))
            {
                Tag = "grpc-simple"
            }, cancellationToken);
            Assert.NotEmpty(receipt.MessageId);
            Assert.NotEmpty(receipt.Endpoints);

            GrpcMessageView? received = null;
            for (var attempt = 0; attempt < 10 && received is null; attempt++)
            {
                var messages = await consumer.ReceiveAsync(16, cancellationToken: cancellationToken);
                received = messages.FirstOrDefault(message => Encoding.UTF8.GetString(message.Body) == body);
                foreach (var message in messages)
                {
                    await consumer.AckAsync(message, cancellationToken);
                }
            }

            Assert.NotNull(received);
            Assert.Equal(receipt.MessageId, received.MessageId);
        }
        finally
        {
            await consumer.StopAsync(CancellationToken.None);
            await producer.StopAsync(CancellationToken.None);
        }
    }

    [Fact]
    [Trait("Category", "Integration")]
    public async Task MultipleGrpcSimpleConsumersWithIndependentTopicsReceiveTheirOwnMessages()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var fixture = await registry.GetFixtureAsync(cancellationToken);
        var ordersScope = await fixture.CreateTestScopeAsync(RocketMQTestTopicType.Normal, cancellationToken);
        var auditScope = await fixture.CreateTestScopeAsync(RocketMQTestTopicType.Normal, cancellationToken);
        var ordersGroup = ordersScope.CreateConsumerGroupName("grpc-orders-simple-consumer");
        var auditGroup = auditScope.CreateConsumerGroupName("grpc-audit-simple-consumer");
        var ordersTag = $"grpc-orders-simple-{Guid.NewGuid():N}";
        var auditTag = $"grpc-audit-simple-{Guid.NewGuid():N}";
        var services = new ServiceCollection();
        var rocketMQ = services
            .AddRocketMQGrpc(options => options.Endpoint = fixture.GrpcEndpoint)
            .AddGrpcProducer();
        rocketMQ.AddGrpcSimpleConsumer(options =>
        {
            options.GroupName = ordersGroup;
            options.AwaitDuration = TimeSpan.FromSeconds(3);
            options.Subscribe(ordersScope.Topic, new FilterExpression(ordersTag));
        });
        rocketMQ.AddGrpcSimpleConsumer(options =>
        {
            options.GroupName = auditGroup;
            options.AwaitDuration = TimeSpan.FromSeconds(3);
            options.Subscribe(auditScope.Topic, new FilterExpression(auditTag));
        });

        await using var provider = services.BuildServiceProvider(new ServiceProviderOptions { ValidateOnBuild = true });
        var producer = provider.GetRequiredService<IGrpcProducer>();
        var consumers = provider.GetServices<IGrpcSimpleConsumer>().ToArray();
        Assert.Equal(2, consumers.Length);
        await producer.StartAsync(cancellationToken);
        try
        {
            foreach (var consumer in consumers)
            {
                await consumer.StartAsync(cancellationToken);
            }

            var ordersBody = $"grpc-orders-simple-body-{Guid.NewGuid():N}";
            var auditBody = $"grpc-audit-simple-body-{Guid.NewGuid():N}";
            var ordersReceipt = await producer.SendAsync(new Message(
                ordersScope.Topic,
                Encoding.UTF8.GetBytes(ordersBody))
            {
                Tag = ordersTag
            }, cancellationToken);
            var auditReceipt = await producer.SendAsync(new Message(
                auditScope.Topic,
                Encoding.UTF8.GetBytes(auditBody))
            {
                Tag = auditTag
            }, cancellationToken);

            var ordersMessage = await ReceiveExpectedMessageAsync(
                consumers[0],
                ordersScope.Topic,
                ordersBody,
                cancellationToken);
            var auditMessage = await ReceiveExpectedMessageAsync(
                consumers[1],
                auditScope.Topic,
                auditBody,
                cancellationToken);

            Assert.Equal(ordersReceipt.MessageId, ordersMessage.MessageId);
            Assert.Equal(auditReceipt.MessageId, auditMessage.MessageId);
        }
        finally
        {
            foreach (var consumer in consumers.Reverse())
            {
                await consumer.StopAsync(CancellationToken.None);
            }

            await producer.StopAsync(CancellationToken.None);
        }
    }

    [Fact]
    [Trait("Category", "Integration")]
    public async Task GrpcTransactionCommitMakesMessageVisible()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var fixture = await registry.GetFixtureAsync(cancellationToken);
        var scope = await fixture.CreateTestScopeAsync(RocketMQTestTopicType.Transaction, cancellationToken);
        var consumerGroup = scope.CreateConsumerGroupName("grpc-transaction-consumer");
        var services = new ServiceCollection();
        services
            .AddRocketMQGrpc(options => options.Endpoint = fixture.GrpcEndpoint)
            .AddGrpcProducer(options =>
            {
                options.Topics.Add(scope.Topic);
                options.TransactionChecker = static (_, _) =>
                    ValueTask.FromResult(TransactionResolution.Unknown);
            })
            .AddGrpcSimpleConsumer(options =>
            {
                options.GroupName = consumerGroup;
                options.AwaitDuration = TimeSpan.FromSeconds(3);
                options.Subscribe(scope.Topic, new FilterExpression("transaction"));
            });

        await using var provider = services.BuildServiceProvider(new ServiceProviderOptions { ValidateOnBuild = true });
        var producer = provider.GetRequiredService<IGrpcProducer>();
        var consumer = provider.GetRequiredService<IGrpcSimpleConsumer>();
        await producer.StartAsync(cancellationToken);
        await consumer.StartAsync(cancellationToken);
        try
        {
            var body = $"transaction-{Guid.NewGuid():N}";
            var transaction = await producer.SendTransactionAsync(new Message(
                scope.Topic,
                Encoding.UTF8.GetBytes(body))
            {
                Tag = "transaction"
            }, cancellationToken);
            await transaction.CommitAsync(cancellationToken);

            GrpcMessageView? received = null;
            for (var attempt = 0; attempt < 10 && received is null; attempt++)
            {
                var messages = await consumer.ReceiveAsync(16, cancellationToken: cancellationToken);
                received = messages.FirstOrDefault(message => Encoding.UTF8.GetString(message.Body) == body);
                foreach (var message in messages)
                {
                    await consumer.AckAsync(message, cancellationToken);
                }
            }

            Assert.NotNull(received);
            Assert.Equal(transaction.MessageId, received.MessageId);
        }
        finally
        {
            await consumer.StopAsync(CancellationToken.None);
            await producer.StopAsync(CancellationToken.None);
        }
    }

    [Fact]
    [Trait("Category", "Integration")]
    public async Task GrpcPushConsumerDispatchesAndAcknowledgesMessage()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var fixture = await registry.GetFixtureAsync(cancellationToken);
        var scope = await fixture.CreateTestScopeAsync(RocketMQTestTopicType.Normal, cancellationToken);
        var consumerGroup = scope.CreateConsumerGroupName("grpc-push-consumer");
        var consumed = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
        var expected = $"grpc-push-{Guid.NewGuid():N}";
        var services = new ServiceCollection();
        services.AddSingleton(new PushMessageObservation(expected, consumed));
        services
            .AddRocketMQGrpc(options => options.Endpoint = fixture.GrpcEndpoint)
            .AddGrpcProducer()
            .AddGrpcPushConsumer<PushMessageHandler>(ServiceLifetime.Singleton, options =>
            {
                options.GroupName = consumerGroup;
                options.LongPollingTimeout = TimeSpan.FromSeconds(3);
                options.Subscribe(scope.Topic, new FilterExpression("grpc-push"));
            });

        await using var provider = services.BuildServiceProvider(new ServiceProviderOptions { ValidateOnBuild = true });
        var producer = provider.GetRequiredService<IGrpcProducer>();
        var consumer = provider.GetRequiredService<IGrpcPushConsumer>();
        await producer.StartAsync(cancellationToken);
        await consumer.StartAsync(cancellationToken);
        try
        {
            var sent = await producer.SendAsync(new Message(
                scope.Topic,
                Encoding.UTF8.GetBytes(expected))
            {
                Tag = "grpc-push"
            }, cancellationToken);
            var messageId = await consumed.Task.WaitAsync(TimeSpan.FromSeconds(30), cancellationToken);
            Assert.Equal(sent.MessageId, messageId);
        }
        finally
        {
            await consumer.StopAsync(CancellationToken.None);
            await producer.StopAsync(CancellationToken.None);
        }
    }

    [Fact]
    [Trait("Category", "Integration")]
    public async Task MultipleGrpcPushConsumersDispatchOnlyTheirOwnTopics()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var fixture = await registry.GetFixtureAsync(cancellationToken);
        var ordersScope = await fixture.CreateTestScopeAsync(RocketMQTestTopicType.Normal, cancellationToken);
        var auditScope = await fixture.CreateTestScopeAsync(RocketMQTestTopicType.Normal, cancellationToken);
        var ordersGroup = ordersScope.CreateConsumerGroupName("grpc-orders-push-consumer");
        var ordersObserverGroup = ordersScope.CreateConsumerGroupName("grpc-orders-observer-push-consumer");
        var auditGroup = auditScope.CreateConsumerGroupName("grpc-audit-push-consumer");
        var ordersTag = $"orders-{Guid.NewGuid():N}";
        var auditTag = $"audit-{Guid.NewGuid():N}";
        var ordersBody = $"orders-{Guid.NewGuid():N}";
        var auditBody = $"audit-{Guid.NewGuid():N}";
        var ordersConsumed = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
        var ordersObserved = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
        var auditConsumed = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
        var unexpectedDelivery = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
        var ordersObservedTopics = new ConcurrentQueue<string>();
        var ordersObserverObservedTopics = new ConcurrentQueue<string>();
        var auditObservedTopics = new ConcurrentQueue<string>();
        var services = new ServiceCollection();
        services.AddSingleton(new MultipleTopicObservation(
            ordersScope.Topic,
            auditScope.Topic,
            ordersBody,
            auditBody,
            ordersConsumed,
            ordersObserved,
            auditConsumed,
            unexpectedDelivery,
            ordersObservedTopics,
            ordersObserverObservedTopics,
            auditObservedTopics));
        services
            .AddRocketMQGrpc(options => options.Endpoint = fixture.GrpcEndpoint)
            .AddGrpcProducer()
            .AddGrpcPushConsumer<OrdersMessageHandler>(ServiceLifetime.Singleton, options =>
            {
                options.GroupName = ordersGroup;
                options.LongPollingTimeout = TimeSpan.FromSeconds(3);
                options.Subscribe(ordersScope.Topic, new FilterExpression(ordersTag));
            })
            .AddGrpcPushConsumer<OrdersObserverMessageHandler>(ServiceLifetime.Singleton, options =>
            {
                options.GroupName = ordersObserverGroup;
                options.LongPollingTimeout = TimeSpan.FromSeconds(3);
                options.Subscribe(ordersScope.Topic, new FilterExpression(ordersTag));
            })
            .AddGrpcPushConsumer<AuditMessageHandler>(ServiceLifetime.Singleton, options =>
            {
                options.GroupName = auditGroup;
                options.LongPollingTimeout = TimeSpan.FromSeconds(3);
                options.Subscribe(auditScope.Topic, new FilterExpression(auditTag));
            });

        await using var provider = services.BuildServiceProvider(new ServiceProviderOptions { ValidateOnBuild = true });
        var producer = provider.GetRequiredService<IGrpcProducer>();
        var consumers = provider.GetServices<IGrpcPushConsumer>().ToArray();
        Assert.Equal(3, consumers.Length);
        var startedConsumers = new List<IGrpcPushConsumer>();

        await producer.StartAsync(cancellationToken);
        try
        {
            foreach (var consumer in consumers)
            {
                await consumer.StartAsync(cancellationToken);
                startedConsumers.Add(consumer);
            }

            var ordersReceipt = await producer.SendAsync(new Message(
                ordersScope.Topic,
                Encoding.UTF8.GetBytes(ordersBody))
            {
                Tag = ordersTag
            }, cancellationToken);
            var auditReceipt = await producer.SendAsync(new Message(
                auditScope.Topic,
                Encoding.UTF8.GetBytes(auditBody))
            {
                Tag = auditTag
            }, cancellationToken);

            var expectedDeliveries = Task.WhenAll(ordersConsumed.Task, ordersObserved.Task, auditConsumed.Task);
            var firstCompletion = await Task.WhenAny(expectedDeliveries, unexpectedDelivery.Task)
                .WaitAsync(TimeSpan.FromSeconds(30), cancellationToken);
            if (ReferenceEquals(firstCompletion, unexpectedDelivery.Task))
            {
                Assert.Fail(await unexpectedDelivery.Task);
            }

            var messageIds = await expectedDeliveries;
            Assert.Equal(ordersReceipt.MessageId, messageIds[0]);
            Assert.Equal(ordersReceipt.MessageId, messageIds[1]);
            Assert.Equal(auditReceipt.MessageId, messageIds[2]);
            await Task.Delay(TimeSpan.FromSeconds(1), cancellationToken);
            if (unexpectedDelivery.Task.IsCompletedSuccessfully)
            {
                Assert.Fail(await unexpectedDelivery.Task);
            }
        }
        finally
        {
            foreach (var consumer in startedConsumers.AsEnumerable().Reverse())
            {
                await consumer.StopAsync(CancellationToken.None);
            }

            await producer.StopAsync(CancellationToken.None);
        }

        Assert.NotEmpty(ordersObservedTopics);
        Assert.All(ordersObservedTopics, topic => Assert.Equal(ordersScope.Topic, topic));
        Assert.NotEmpty(ordersObserverObservedTopics);
        Assert.All(ordersObserverObservedTopics, topic => Assert.Equal(ordersScope.Topic, topic));
        Assert.NotEmpty(auditObservedTopics);
        Assert.All(auditObservedTopics, topic => Assert.Equal(auditScope.Topic, topic));
    }

    [Fact]
    [Trait("Category", "Integration")]
    public async Task MultipleGrpcPushConsumersWithSameGroupShareMessagesWithoutDuplicates()
    {
        const int messagesPerRound = 16;
        const int maximumRounds = 3;
        var cancellationToken = TestContext.Current.CancellationToken;
        var fixture = await registry.GetFixtureAsync(cancellationToken);
        var scope = await fixture.CreateTestScopeAsync(RocketMQTestTopicType.Normal, cancellationToken);
        var group = scope.CreateConsumerGroupName("grpc-shared-push-consumer");
        var tag = $"grpc-shared-{Guid.NewGuid():N}";
        var expectedBodies = new ConcurrentDictionary<string, byte>(StringComparer.Ordinal);
        var deliveries = new ConcurrentDictionary<string, int>(StringComparer.Ordinal);
        var unexpectedDelivery = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
        var observation = new SharedGroupObservation(
            scope.Topic,
            expectedBodies,
            deliveries,
            unexpectedDelivery);

        var services = new ServiceCollection();
        services.AddSingleton(observation);
        var rocketMQ = services
            .AddRocketMQGrpc(options => options.Endpoint = fixture.GrpcEndpoint)
            .AddGrpcProducer();
        rocketMQ.AddGrpcPushConsumer<FirstSharedGroupMessageHandler>(ServiceLifetime.Singleton, options =>
        {
            options.GroupName = group;
            options.MaxConcurrency = 4;
            options.LongPollingTimeout = TimeSpan.FromSeconds(1);
            options.Subscribe(scope.Topic, new FilterExpression(tag));
        });
        rocketMQ.AddGrpcPushConsumer<SecondSharedGroupMessageHandler>(ServiceLifetime.Singleton, options =>
        {
            options.GroupName = group;
            options.MaxConcurrency = 4;
            options.LongPollingTimeout = TimeSpan.FromSeconds(1);
            options.Subscribe(scope.Topic, new FilterExpression(tag));
        });

        await using var provider = services.BuildServiceProvider(
            new ServiceProviderOptions { ValidateOnBuild = true });
        var producer = provider.GetRequiredService<IGrpcProducer>();
        var consumers = provider.GetServices<IGrpcPushConsumer>().ToArray();
        Assert.Equal(2, consumers.Length);
        var startedConsumers = new List<IGrpcPushConsumer>();

        await producer.StartAsync(cancellationToken);
        try
        {
            foreach (var consumer in consumers)
            {
                await consumer.StartAsync(cancellationToken);
                startedConsumers.Add(consumer);
            }

            for (var round = 0;
                 round < maximumRounds &&
                 (observation.FirstConsumerCalls == 0 || observation.SecondConsumerCalls == 0);
                 round++)
            {
                var roundBodies = Enumerable.Range(0, messagesPerRound)
                    .Select(index => $"{tag}-{round}-{index}")
                    .ToArray();
                foreach (var body in roundBodies)
                {
                    Assert.True(expectedBodies.TryAdd(body, 0), $"Duplicate expected body '{body}'.");
                    await producer.SendAsync(new Message(scope.Topic, Encoding.UTF8.GetBytes(body))
                    {
                        Tag = tag
                    }, cancellationToken);
                }

                await WaitUntilAsync(
                    () => roundBodies.All(deliveries.ContainsKey),
                    TimeSpan.FromSeconds(30),
                    () => $"A shared-group Push round was not completely delivered: " +
                          $"{string.Join(", ", roundBodies.Where(body => !deliveries.ContainsKey(body)))}.",
                    cancellationToken);
            }

            if (unexpectedDelivery.Task.IsCompletedSuccessfully)
            {
                Assert.Fail(await unexpectedDelivery.Task);
            }

            Assert.True(observation.FirstConsumerCalls > 0, "The first consumer did not receive an assigned queue.");
            Assert.True(observation.SecondConsumerCalls > 0, "The second consumer did not receive an assigned queue.");
            await Task.Delay(TimeSpan.FromSeconds(1), cancellationToken);
            if (unexpectedDelivery.Task.IsCompletedSuccessfully)
            {
                Assert.Fail(await unexpectedDelivery.Task);
            }

            Assert.True(expectedBodies.Keys.ToHashSet(StringComparer.Ordinal).SetEquals(deliveries.Keys));
            Assert.All(deliveries.Values, count => Assert.Equal(1, count));
        }
        finally
        {
            foreach (var consumer in startedConsumers.AsEnumerable().Reverse())
            {
                await consumer.StopAsync(CancellationToken.None);
            }

            await producer.StopAsync(CancellationToken.None);
        }
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

    private static async Task<GrpcMessageView> ReceiveExpectedMessageAsync(
        IGrpcSimpleConsumer consumer,
        string expectedTopic,
        string expectedBody,
        CancellationToken cancellationToken)
    {
        for (var attempt = 0; attempt < 10; attempt++)
        {
            var messages = await consumer.ReceiveAsync(16, cancellationToken: cancellationToken);
            foreach (var message in messages)
            {
                Assert.Equal(expectedTopic, message.Topic);
                var body = Encoding.UTF8.GetString(message.Body);
                await consumer.AckAsync(message, cancellationToken);
                if (body == expectedBody)
                {
                    return message;
                }
            }
        }

        throw new Xunit.Sdk.XunitException(
            $"Simple Consumer did not receive expected message '{expectedBody}' from topic '{expectedTopic}'.");
    }

    private sealed record PushMessageObservation(
        string ExpectedBody,
        TaskCompletionSource<string> Consumed);

    private sealed class PushMessageHandler(PushMessageObservation observation) : IGrpcPushMessageHandler
    {
        public ValueTask<ConsumeResult> HandleAsync(
            GrpcMessageView message,
            CancellationToken cancellationToken)
        {
            if (Encoding.UTF8.GetString(message.Body) == observation.ExpectedBody)
            {
                observation.Consumed.TrySetResult(message.MessageId);
            }

            return ValueTask.FromResult(ConsumeResult.Success);
        }
    }

    private sealed class MultipleTopicObservation(
        string ordersTopic,
        string auditTopic,
        string ordersBody,
        string auditBody,
        TaskCompletionSource<string> ordersConsumed,
        TaskCompletionSource<string> ordersObserved,
        TaskCompletionSource<string> auditConsumed,
        TaskCompletionSource<string> unexpectedDelivery,
        ConcurrentQueue<string> ordersObservedTopics,
        ConcurrentQueue<string> ordersObserverObservedTopics,
        ConcurrentQueue<string> auditObservedTopics)
    {
        public ValueTask<ConsumeResult> HandleOrders(GrpcMessageView message, bool observer)
        {
            var observedTopics = observer ? ordersObserverObservedTopics : ordersObservedTopics;
            var completion = observer ? ordersObserved : ordersConsumed;
            observedTopics.Enqueue(message.Topic);
            if (message.Topic != ordersTopic)
            {
                var consumerName = observer ? "Orders observer" : "Orders consumer";
                unexpectedDelivery.TrySetResult($"{consumerName} received topic '{message.Topic}'.");
            }

            if (Encoding.UTF8.GetString(message.Body) == ordersBody)
            {
                completion.TrySetResult(message.MessageId);
            }

            return ValueTask.FromResult(ConsumeResult.Success);
        }

        public ValueTask<ConsumeResult> HandleAudit(GrpcMessageView message)
        {
            auditObservedTopics.Enqueue(message.Topic);
            if (message.Topic != auditTopic)
            {
                unexpectedDelivery.TrySetResult($"Audit consumer received topic '{message.Topic}'.");
            }

            if (Encoding.UTF8.GetString(message.Body) == auditBody)
            {
                auditConsumed.TrySetResult(message.MessageId);
            }

            return ValueTask.FromResult(ConsumeResult.Success);
        }
    }

    private sealed class OrdersMessageHandler(MultipleTopicObservation observation) : IGrpcPushMessageHandler
    {
        public ValueTask<ConsumeResult> HandleAsync(
            GrpcMessageView message,
            CancellationToken cancellationToken) =>
            observation.HandleOrders(message, false);
    }

    private sealed class OrdersObserverMessageHandler(MultipleTopicObservation observation) : IGrpcPushMessageHandler
    {
        public ValueTask<ConsumeResult> HandleAsync(
            GrpcMessageView message,
            CancellationToken cancellationToken) =>
            observation.HandleOrders(message, true);
    }

    private sealed class AuditMessageHandler(MultipleTopicObservation observation) : IGrpcPushMessageHandler
    {
        public ValueTask<ConsumeResult> HandleAsync(
            GrpcMessageView message,
            CancellationToken cancellationToken) =>
            observation.HandleAudit(message);
    }

    private sealed class SharedGroupObservation(
        string topic,
        ConcurrentDictionary<string, byte> expectedBodies,
        ConcurrentDictionary<string, int> deliveries,
        TaskCompletionSource<string> unexpectedDelivery)
    {
        private int _firstConsumerCalls;
        private int _secondConsumerCalls;

        public int FirstConsumerCalls => Volatile.Read(ref _firstConsumerCalls);

        public int SecondConsumerCalls => Volatile.Read(ref _secondConsumerCalls);

        public ValueTask<ConsumeResult> Handle(GrpcMessageView message, bool firstConsumer)
        {
            var body = Encoding.UTF8.GetString(message.Body);
            if (message.Topic != topic || !expectedBodies.ContainsKey(body))
            {
                unexpectedDelivery.TrySetResult(
                    $"Shared-group consumer received topic '{message.Topic}' with body '{body}'.");
                return ValueTask.FromResult(ConsumeResult.Success);
            }

            if (firstConsumer)
            {
                Interlocked.Increment(ref _firstConsumerCalls);
            }
            else
            {
                Interlocked.Increment(ref _secondConsumerCalls);
            }

            deliveries.AddOrUpdate(body, 1, static (_, count) => count + 1);
            return ValueTask.FromResult(ConsumeResult.Success);
        }
    }

    private sealed class FirstSharedGroupMessageHandler(SharedGroupObservation observation) : IGrpcPushMessageHandler
    {
        public ValueTask<ConsumeResult> HandleAsync(
            GrpcMessageView message,
            CancellationToken cancellationToken) =>
            observation.Handle(message, true);
    }

    private sealed class SecondSharedGroupMessageHandler(SharedGroupObservation observation) : IGrpcPushMessageHandler
    {
        public ValueTask<ConsumeResult> HandleAsync(
            GrpcMessageView message,
            CancellationToken cancellationToken) =>
            observation.Handle(message, false);
    }
}
