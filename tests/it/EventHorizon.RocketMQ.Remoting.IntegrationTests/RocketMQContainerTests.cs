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
using System.Net.Sockets;
using System.Text;
using System.Text.Json;
using EventHorizon.RocketMQ.IntegrationTestInfrastructure;
using EventHorizon.RocketMQ.Remoting.Consumer;
using EventHorizon.RocketMQ.Remoting.Consumer.LitePull;
using EventHorizon.RocketMQ.Remoting.Consumer.Push;
using EventHorizon.RocketMQ.Remoting.Producer;
using EventHorizon.RocketMQ.Remoting.Protocol;
using EventHorizon.RocketMQ.Remoting.Protocol.Route;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Xunit;

namespace EventHorizon.RocketMQ.Remoting.IntegrationTests;

public sealed class RocketMQContainerTests(RocketMQSingleBrokerContainerFixtureRegistry registry) : IAsyncLifetime
{
    private RocketMQSingleBrokerContainerFixture _fixture = null!;

    public async ValueTask InitializeAsync()
    {
        _fixture = await registry.GetFixtureAsync(TestContext.Current.CancellationToken);
    }

    public ValueTask DisposeAsync()
    {
        return ValueTask.CompletedTask;
    }

    [Fact]
    [Trait("Category", "Integration")]
    public async Task Producer_ContainerizedBroker_SendsMessages()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        await AssertReachableAsync(_fixture.NameServerAddress, cancellationToken);
        await AssertReachableAsync(_fixture.BrokerAddress, cancellationToken);

        var services = new ServiceCollection();
        services
            .AddRocketMQRemoting(options =>
            {
                options.NamesrvAddr = _fixture.NameServerAddress;
            })
            .AddRemotingProducer(options => options.GroupName = "integration-tests");

        await using var provider = services.BuildServiceProvider();
        var producer = provider.GetRequiredService<IRemotingProducer>();

        var nameServer = provider.GetRequiredKeyedService<INameServer>(Options.DefaultName);
        var route = await nameServer.GetTopicRouteInfoAsync(
            RocketMQSingleBrokerContainerFixture.TestTopic,
            TimeSpan.FromSeconds(10),
            cancellationToken: cancellationToken);
        Assert.NotEmpty(route.BrokerDatas);
        Assert.NotEmpty(route.QueueDatas);

        await producer.StartAsync(cancellationToken);
        try
        {
            var result = await producer.SendAsync(new Message(
                RocketMQSingleBrokerContainerFixture.TestTopic,
                "hello from dotnet"u8.ToArray()), cancellationToken);
            Assert.Equal(RemotingSendStatus.SendOk, result.Status);
            Assert.NotEmpty(result.MessageId);
            Assert.NotEmpty(result.OffsetMessageId);
            Assert.Equal(RocketMQSingleBrokerContainerFixture.TestTopic, result.MessageQueue.Topic);

            await producer.SendOnewayAsync(new Message(
                RocketMQSingleBrokerContainerFixture.TestTopic,
                "one way"u8.ToArray()), cancellationToken);
        }
        finally
        {
            await producer.StopAsync(CancellationToken.None);
        }
    }

    [Fact]
    [Trait("Category", "Integration")]
    public async Task LitePullConsumer_ManualAssignment_ControlsPositionsAndCommits()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var scope = await _fixture.CreateTestScopeAsync(RocketMQTestTopicType.Normal, cancellationToken);
        var group = scope.CreateConsumerGroupName("remoting-manual-lite-pull-consumer");
        var tag = $"remoting-manual-lite-pull-{Guid.NewGuid():N}";
        var services = new ServiceCollection();
        services
            .AddRocketMQRemoting(options =>
            {
                options.NamesrvAddr = _fixture.NameServerAddress;
            })
            .AddRemotingProducer(options =>
                options.GroupName = scope.CreateProducerGroupName("remoting-manual-lite-pull-producer"))
            .AddRemotingLitePullConsumer(options =>
            {
                options.GroupName = group;
                options.InitialPosition = ConsumeFromPosition.End;
                options.EnableAutoCommit = false;
                options.LongPollingTimeout = TimeSpan.FromSeconds(1);
                options.PollTimeout = TimeSpan.FromSeconds(2);
            });

        await using var provider = services.BuildServiceProvider(new ServiceProviderOptions { ValidateOnBuild = true });
        var producer = provider.GetRequiredService<IRemotingProducer>();
        var consumer = provider.GetRequiredService<IRemotingLitePullConsumer>();
        await producer.StartAsync(cancellationToken);
        await consumer.StartAsync(cancellationToken);
        try
        {
            var queues = await consumer.GetMessageQueuesAsync(
                scope.Topic,
                cancellationToken);
            await consumer.AssignAsync(
                queues,
                new Dictionary<string, FilterExpression>(StringComparer.Ordinal)
                {
                    [scope.Topic] = new(tag)
                },
                cancellationToken);
            foreach (var assignedQueue in queues)
            {
                await consumer.SeekToEndAsync(assignedQueue, cancellationToken);
            }

            var body = $"remoting-manual-lite-pull-{Guid.NewGuid():N}";
            var sent = await producer.SendAsync(new Message(scope.Topic, Encoding.UTF8.GetBytes(body))
            {
                Tag = tag
            }, cancellationToken);
            var queue = queues.Single(value => value.QueueId == sent.MessageQueue.QueueId && value.BrokerName == sent.MessageQueue.BrokerName);

            RemotingMessageView? matching = null;
            for (var attempt = 0; attempt < 10 && matching is null; attempt++)
            {
                var messages = await consumer.PollAsync(cancellationToken: cancellationToken);
                matching = messages.SingleOrDefault(message => Encoding.UTF8.GetString(message.Body) == body);
            }

            var message = Assert.IsType<RemotingMessageView>(matching);
            Assert.Equal(message.QueueOffset + 1, consumer.Position(queue));
            await consumer.CommitAsync(queue, cancellationToken);
            Assert.Equal(message.QueueOffset + 1, await consumer.GetCommittedOffsetAsync(queue, cancellationToken));
        }
        finally
        {
            await consumer.StopAsync(CancellationToken.None);
            await producer.StopAsync(CancellationToken.None);
        }
    }

    [Fact]
    [Trait("Category", "Integration")]
    public async Task LitePullConsumer_Subscription_PollsAndCommitsExplicitly()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var suffix = Guid.NewGuid().ToString("N");
        var services = new ServiceCollection();
        services
            .AddRocketMQRemoting(options =>
            {
                options.NamesrvAddr = _fixture.NameServerAddress;
            })
            .AddRemotingProducer(options => options.GroupName = $"legacy-lite-pull-producer-{suffix}")
            .AddRemotingLitePullConsumer(options =>
            {
                options.GroupName = $"legacy-lite-pull-consumer-{suffix}";
                options.EnableAutoCommit = false;
                options.LongPollingTimeout = TimeSpan.FromSeconds(3);
                options.PollTimeout = TimeSpan.FromSeconds(3);
                options.Subscribe(RocketMQSingleBrokerContainerFixture.TestTopic, new FilterExpression("remoting-lite-pull"));
            });

        await using var provider = services.BuildServiceProvider(new ServiceProviderOptions { ValidateOnBuild = true });
        var producer = provider.GetRequiredService<IRemotingProducer>();
        var consumer = provider.GetRequiredService<IRemotingLitePullConsumer>();
        await producer.StartAsync(cancellationToken);
        await consumer.StartAsync(cancellationToken);
        try
        {
            Assert.NotEmpty(consumer.Assignment);
            var body = $"remoting-lite-pull-{suffix}";
            await producer.SendAsync(new Message(RocketMQSingleBrokerContainerFixture.TestTopic, Encoding.UTF8.GetBytes(body))
            {
                Tag = "remoting-lite-pull"
            }, cancellationToken);

            RemotingMessageView? matching = null;
            for (var attempt = 0; attempt < 10 && matching is null; attempt++)
            {
                var messages = await consumer.PollAsync(cancellationToken: cancellationToken);
                matching = messages.SingleOrDefault(message => Encoding.UTF8.GetString(message.Body) == body);
            }

            Assert.NotNull(matching);
            await consumer.CommitAsync(cancellationToken);
        }
        finally
        {
            await consumer.StopAsync(CancellationToken.None);
            await producer.StopAsync(CancellationToken.None);
        }
    }

    [Fact]
    [Trait("Category", "Integration")]
    public async Task LegacyPushConsumer_Message_DispatchesAndCommits()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var consumed = new TaskCompletionSource<(string MessageId, int DeliveryAttempt)>(TaskCreationOptions.RunContinuationsAsynchronously);
        var firstAttempt = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var handlerCalls = 0;
        var expected = $"legacy-push-{Guid.NewGuid():N}";
        var logs = new RecordingLoggerFactory();
        var services = new ServiceCollection();
        services.AddSingleton<ILoggerFactory>(logs);
        services
            .AddRocketMQRemoting(options =>
            {
                options.NamesrvAddr = _fixture.NameServerAddress;
            })
            .AddRemotingProducer(options => options.GroupName = "legacy-push-producer-it")
            .AddRemotingPushConsumerWithTestHandler<RocketMQContainerTests>(options =>
            {
                options.GroupName = "legacy-push-consumer-it";
                options.InitialPosition = ConsumeFromPosition.Beginning;
                options.LongPollingTimeout = TimeSpan.FromSeconds(3);
                options.RetryDelay = TimeSpan.FromMilliseconds(100);
                options.Subscribe(RocketMQSingleBrokerContainerFixture.TestTopic, new FilterExpression("legacy-push"));
            }, (messages, _, _) =>
                {
                    var message = Assert.Single(messages);
                    var body = Encoding.UTF8.GetString(message.Body);
                    if (body == expected && Interlocked.Increment(ref handlerCalls) == 1)
                    {
                        firstAttempt.TrySetResult();
                        return ValueTask.FromResult(ConsumeResult.Retry);
                    }

                    if (body == expected)
                    {
                        consumed.TrySetResult((message.MessageId, message.DeliveryAttempt));
                    }

                    return ValueTask.FromResult(ConsumeResult.Success);
                });

        await using var provider = services.BuildServiceProvider(new ServiceProviderOptions { ValidateOnBuild = true });
        var producer = provider.GetRequiredService<IRemotingProducer>();
        var consumer = provider.GetRequiredService<IRemotingPushConsumer>();
        await producer.StartAsync(cancellationToken);
        await consumer.StartAsync(cancellationToken);
        try
        {
            var sent = await producer.SendAsync(new Message(RocketMQSingleBrokerContainerFixture.TestTopic, Encoding.UTF8.GetBytes(expected))
            {
                Tag = "legacy-push"
            }, cancellationToken);
            await firstAttempt.Task.WaitAsync(TimeSpan.FromSeconds(30), cancellationToken);
            var topics = await _fixture.GetTopicListAsync(cancellationToken);
            const string retryTopic = "%RETRY%legacy-push-consumer-it";
            Assert.Contains(retryTopic, topics);
            (string MessageId, int DeliveryAttempt) delivery;
            try
            {
                delivery = await consumed.Task.WaitAsync(TimeSpan.FromSeconds(20), cancellationToken);
            }
            catch (TimeoutException exception)
            {
                var status = await _fixture.GetTopicStatusAsync(retryTopic, cancellationToken);
                var progress = await _fixture.GetConsumerProgressAsync("legacy-push-consumer-it", cancellationToken);
                throw new InvalidOperationException(
                    $"Retry delivery timed out. Topic status: {status}. Consumer progress: {progress}. Client logs: {logs.Snapshot()}",
                    exception);
            }
            Assert.Equal(sent.MessageId, delivery.MessageId);
            Assert.True(delivery.DeliveryAttempt >= 2);
            var commit = await _fixture.WaitForConsumerCommitAsync(
                "legacy-push-consumer-it",
                RocketMQSingleBrokerContainerFixture.TestTopic,
                sent.MessageQueue.BrokerName,
                sent.MessageQueue.QueueId,
                TimeSpan.FromSeconds(10),
                cancellationToken);
            Assert.True(commit.Committed, $"Legacy push offset was not committed. {commit.Progress}");
        }
        finally
        {
            await consumer.StopAsync(CancellationToken.None);
            await producer.StopAsync(CancellationToken.None);
        }
    }

    [Fact]
    [Trait("Category", "Integration")]
    public async Task LegacyPushConsumers_OneRegistration_ConsumeIndependentTopics()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var orders = await _fixture.CreateTestScopeAsync(RocketMQTestTopicType.Normal, cancellationToken);
        var payments = await _fixture.CreateTestScopeAsync(RocketMQTestTopicType.Normal, cancellationToken);
        var ordersBody = $"orders-{Guid.NewGuid():N}";
        var paymentsBody = $"payments-{Guid.NewGuid():N}";
        var ordersConsumed = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
        var ordersObserved = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
        var paymentsConsumed = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
        var unexpectedDelivery = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
        var services = new ServiceCollection();
        var rocketMQ = services.AddRocketMQRemoting(options =>
        {
            options.NamesrvAddr = _fixture.NameServerAddress;
        });
        rocketMQ.AddRemotingProducer(options =>
            options.GroupName = orders.CreateProducerGroupName("multiple-remoting-push-producer"));
        rocketMQ.AddRemotingPushConsumerWithTestHandler<OrdersPushConsumerMarker>(options =>
        {
            options.GroupName = orders.CreateConsumerGroupName("orders-remoting-push-consumer");
            options.LongPollingTimeout = TimeSpan.FromSeconds(1);
            options.Subscribe(orders.Topic, new FilterExpression("orders"));
        }, (messages, _, _) =>
            {
                foreach (var message in messages)
                {
                    var body = Encoding.UTF8.GetString(message.Body);
                    if (message.Topic == orders.Topic && body == ordersBody)
                    {
                        ordersConsumed.TrySetResult(body);
                    }
                    else
                    {
                        unexpectedDelivery.TrySetResult(
                            $"Orders consumer received topic '{message.Topic}' with body '{body}'.");
                    }
                }

                return ValueTask.FromResult(ConsumeResult.Success);
            });
        rocketMQ.AddRemotingPushConsumerWithTestHandler<OrdersObserverPushConsumerMarker>(options =>
        {
            options.GroupName = orders.CreateConsumerGroupName("orders-observer-remoting-push-consumer");
            options.LongPollingTimeout = TimeSpan.FromSeconds(1);
            options.Subscribe(orders.Topic, new FilterExpression("orders"));
        }, (messages, _, _) =>
            {
                foreach (var message in messages)
                {
                    var body = Encoding.UTF8.GetString(message.Body);
                    if (message.Topic == orders.Topic && body == ordersBody)
                    {
                        ordersObserved.TrySetResult(body);
                    }
                    else
                    {
                        unexpectedDelivery.TrySetResult(
                            $"Orders observer received topic '{message.Topic}' with body '{body}'.");
                    }
                }

                return ValueTask.FromResult(ConsumeResult.Success);
            });
        rocketMQ.AddRemotingPushConsumerWithTestHandler<PaymentsPushConsumerMarker>(options =>
        {
            options.GroupName = payments.CreateConsumerGroupName("payments-remoting-push-consumer");
            options.LongPollingTimeout = TimeSpan.FromSeconds(1);
            options.Subscribe(payments.Topic, new FilterExpression("payments"));
        }, (messages, _, _) =>
            {
                foreach (var message in messages)
                {
                    var body = Encoding.UTF8.GetString(message.Body);
                    if (message.Topic == payments.Topic && body == paymentsBody)
                    {
                        paymentsConsumed.TrySetResult(body);
                    }
                    else
                    {
                        unexpectedDelivery.TrySetResult(
                            $"Payments consumer received topic '{message.Topic}' with body '{body}'.");
                    }
                }

                return ValueTask.FromResult(ConsumeResult.Success);
            });

        await using var provider = services.BuildServiceProvider(
            new ServiceProviderOptions { ValidateOnBuild = true });
        var producer = provider.GetRequiredService<IRemotingProducer>();
        var consumers = provider.GetServices<IRemotingPushConsumer>().ToArray();
        Assert.Equal(3, consumers.Length);
        var startedConsumers = new List<IRemotingPushConsumer>();

        await producer.StartAsync(cancellationToken);
        try
        {
            foreach (var consumer in consumers)
            {
                await consumer.StartAsync(cancellationToken);
                startedConsumers.Add(consumer);
            }

            var ordersReceipt = await producer.SendAsync(new Message(
                orders.Topic,
                Encoding.UTF8.GetBytes(ordersBody))
            {
                Tag = "orders"
            }, cancellationToken);
            var paymentsReceipt = await producer.SendAsync(new Message(
                payments.Topic,
                Encoding.UTF8.GetBytes(paymentsBody))
            {
                Tag = "payments"
            }, cancellationToken);
            Assert.Equal(RemotingSendStatus.SendOk, ordersReceipt.Status);
            Assert.Equal(RemotingSendStatus.SendOk, paymentsReceipt.Status);

            var expectedDeliveries = Task.WhenAll(ordersConsumed.Task, ordersObserved.Task, paymentsConsumed.Task);
            var firstCompletion = await Task.WhenAny(expectedDeliveries, unexpectedDelivery.Task)
                .WaitAsync(TimeSpan.FromSeconds(30), cancellationToken);
            if (ReferenceEquals(firstCompletion, unexpectedDelivery.Task))
            {
                Assert.Fail(await unexpectedDelivery.Task);
            }

            var bodies = await expectedDeliveries;
            Assert.Equal([ordersBody, ordersBody, paymentsBody], bodies);
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
    }

    [Fact]
    [Trait("Category", "Integration")]
    public async Task LegacyPushConsumers_SameGroup_ShareMessagesWithoutDuplicates()
    {
        const int messageCount = 16;
        var cancellationToken = TestContext.Current.CancellationToken;
        var scope = await _fixture.CreateTestScopeAsync(RocketMQTestTopicType.Normal, cancellationToken);
        var group = scope.CreateConsumerGroupName("remoting-shared-push-consumer");
        var tag = $"remoting-shared-{Guid.NewGuid():N}";
        var expectedBodies = Enumerable.Range(0, messageCount)
            .Select(index => $"{tag}-{index}")
            .ToHashSet(StringComparer.Ordinal);
        var deliveries = new ConcurrentDictionary<string, int>(StringComparer.Ordinal);
        var allConsumed = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var unexpectedDelivery = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
        var firstConsumerCalls = 0;
        var secondConsumerCalls = 0;

        Func<IReadOnlyList<RemotingMessageView>, RemotingPushConsumeContext, CancellationToken, ValueTask<ConsumeResult>>
            CreateHandler(Action onDelivery) => (messages, _, _) =>
            {
                foreach (var message in messages)
                {
                    var body = Encoding.UTF8.GetString(message.Body);
                    if (message.Topic != scope.Topic || !expectedBodies.Contains(body))
                    {
                        unexpectedDelivery.TrySetResult(
                            $"Shared-group consumer received topic '{message.Topic}' with body '{body}'.");
                        continue;
                    }

                    onDelivery();
                    deliveries.AddOrUpdate(body, 1, static (_, count) => count + 1);
                    if (deliveries.Count == messageCount)
                    {
                        allConsumed.TrySetResult();
                    }
                }

                return ValueTask.FromResult(ConsumeResult.Success);
            };

        var services = new ServiceCollection();
        var rocketMQ = services.AddRocketMQRemoting(options =>
        {
            options.NamesrvAddr = _fixture.NameServerAddress;
            options.HeartbeatBrokerInterval = TimeSpan.FromMilliseconds(250);
            options.PollNameServerInterval = TimeSpan.FromMilliseconds(250);
        });
        rocketMQ.AddRemotingProducer(options =>
            options.GroupName = scope.CreateProducerGroupName("remoting-shared-push-producer"));
        rocketMQ.AddRemotingPushConsumerWithTestHandler<FirstSharedGroupPushConsumerMarker>(options =>
        {
            options.GroupName = group;
            options.MaxConcurrency = 4;
            options.LongPollingTimeout = TimeSpan.FromSeconds(1);
            options.Subscribe(scope.Topic, new FilterExpression(tag));
        }, CreateHandler(() => Interlocked.Increment(ref firstConsumerCalls)));
        rocketMQ.AddRemotingPushConsumerWithTestHandler<SecondSharedGroupPushConsumerMarker>(options =>
        {
            options.GroupName = group;
            options.MaxConcurrency = 4;
            options.LongPollingTimeout = TimeSpan.FromSeconds(1);
            options.Subscribe(scope.Topic, new FilterExpression(tag));
        }, CreateHandler(() => Interlocked.Increment(ref secondConsumerCalls)));

        await using var provider = services.BuildServiceProvider(
            new ServiceProviderOptions { ValidateOnBuild = true });
        var producer = provider.GetRequiredService<IRemotingProducer>();
        var consumers = provider.GetServices<IRemotingPushConsumer>().ToArray();
        Assert.Equal(2, consumers.Length);
        var startedConsumers = new List<IRemotingPushConsumer>();

        await producer.StartAsync(cancellationToken);
        try
        {
            foreach (var consumer in consumers)
            {
                await consumer.StartAsync(cancellationToken);
                startedConsumers.Add(consumer);
            }

            await Task.Delay(TimeSpan.FromSeconds(2), cancellationToken);
            foreach (var body in expectedBodies)
            {
                var receipt = await producer.SendAsync(new Message(scope.Topic, Encoding.UTF8.GetBytes(body))
                {
                    Tag = tag
                }, cancellationToken);
                Assert.Equal(RemotingSendStatus.SendOk, receipt.Status);
            }

            var firstCompletion = await Task.WhenAny(allConsumed.Task, unexpectedDelivery.Task)
                .WaitAsync(TimeSpan.FromSeconds(30), cancellationToken);
            if (ReferenceEquals(firstCompletion, unexpectedDelivery.Task))
            {
                Assert.Fail(await unexpectedDelivery.Task);
            }

            await allConsumed.Task;
            await Task.Delay(TimeSpan.FromSeconds(1), cancellationToken);
            if (unexpectedDelivery.Task.IsCompletedSuccessfully)
            {
                Assert.Fail(await unexpectedDelivery.Task);
            }

            Assert.True(expectedBodies.SetEquals(deliveries.Keys));
            Assert.All(deliveries.Values, count => Assert.Equal(1, count));
            Assert.True(firstConsumerCalls > 0, "The first consumer did not receive an assigned queue.");
            Assert.True(secondConsumerCalls > 0, "The second consumer did not receive an assigned queue.");
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

    [Fact]
    [Trait("Category", "Integration")]
    public async Task LegacyPushConsumer_ConfiguredMessageBatch_DispatchesMessages()
    {
        const int messageCount = 3;
        var cancellationToken = TestContext.Current.CancellationToken;
        var suffix = Guid.NewGuid().ToString("N");
        var group = $"legacy-push-batch-{suffix}";
        var tag = $"legacy-push-batch-{suffix}";
        var expectedBodies = Enumerable.Range(0, messageCount)
            .Select(index => $"{tag}-{index}")
            .ToArray();
        var consumed = new TaskCompletionSource<string[]>(TaskCreationOptions.RunContinuationsAsynchronously);
        var services = new ServiceCollection();
        services
            .AddRocketMQRemoting(options =>
            {
                options.NamesrvAddr = _fixture.NameServerAddress;
            })
            .AddRemotingProducer(options => options.GroupName = $"legacy-push-batch-producer-{suffix}")
            .AddRemotingPushConsumerWithTestHandler<RocketMQContainerTests>(options =>
            {
                options.GroupName = group;
                options.InitialPosition = ConsumeFromPosition.Beginning;
                options.PullBatchSize = messageCount;
                options.ConsumeMessageBatchSize = messageCount;
                options.LongPollingTimeout = TimeSpan.FromSeconds(1);
                options.Subscribe(RocketMQSingleBrokerContainerFixture.TestTopic, new FilterExpression(tag));
            }, (messages, _, _) =>
                {
                    var bodies = messages.Select(message => Encoding.UTF8.GetString(message.Body)).ToArray();
                    if (bodies.SequenceEqual(expectedBodies))
                    {
                        consumed.TrySetResult(bodies);
                    }

                    return ValueTask.FromResult(ConsumeResult.Success);
                });

        await using var provider = services.BuildServiceProvider(new ServiceProviderOptions { ValidateOnBuild = true });
        var producer = provider.GetRequiredService<IRemotingProducer>();
        var consumer = provider.GetRequiredService<IRemotingPushConsumer>();
        await producer.StartAsync(cancellationToken);
        try
        {
            var queue = (await producer.GetPublishMessageQueuesAsync(
                RocketMQSingleBrokerContainerFixture.TestTopic,
                cancellationToken)).First();
            var sent = await producer.SendAsync(
                expectedBodies.Select(body => new Message(
                RocketMQSingleBrokerContainerFixture.TestTopic,
                Encoding.UTF8.GetBytes(body))
                {
                    Tag = tag
                }).ToArray(),
                queue,
                cancellationToken);
            Assert.Equal(RemotingSendStatus.SendOk, sent.Status);

            await consumer.StartAsync(cancellationToken);
            var actualBodies = await consumed.Task.WaitAsync(TimeSpan.FromSeconds(20), cancellationToken);
            Assert.Equal(expectedBodies, actualBodies);

            var commit = await _fixture.WaitForConsumerCommitAsync(
                group,
                RocketMQSingleBrokerContainerFixture.TestTopic,
                queue.BrokerName,
                queue.QueueId,
                TimeSpan.FromSeconds(10),
                cancellationToken);
            Assert.True(commit.Committed, $"Legacy push batch offset was not committed. {commit.Progress}");
        }
        finally
        {
            await consumer.StopAsync(CancellationToken.None);
            await producer.StopAsync(CancellationToken.None);
        }
    }

    [Fact]
    [Trait("Category", "Integration")]
    public async Task LegacyPushConsumer_BatchPrefixAndRemainingMessages_AcknowledgesAndRetries()
    {
        const int messageCount = 3;
        var cancellationToken = TestContext.Current.CancellationToken;
        var suffix = Guid.NewGuid().ToString("N");
        var group = $"legacy-push-partial-ack-{suffix}";
        var tag = $"legacy-push-partial-ack-{suffix}";
        var expectedBodies = Enumerable.Range(0, messageCount)
            .Select(index => $"{tag}-{index}")
            .ToArray();
        var expectedRetriedBodies = expectedBodies.Skip(1).ToHashSet(StringComparer.Ordinal);
        var firstBatch = new TaskCompletionSource<(string[] Bodies, int DelayLevel)>(
            TaskCreationOptions.RunContinuationsAsynchronously);
        var tailRedelivered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var redeliveredAttempts = new ConcurrentDictionary<string, int>(StringComparer.Ordinal);
        var handlerInvocations = 0;
        var services = new ServiceCollection();
        services
            .AddRocketMQRemoting(options =>
            {
                options.NamesrvAddr = _fixture.NameServerAddress;
            })
            .AddRemotingProducer(options => options.GroupName = $"legacy-push-partial-ack-producer-{suffix}")
            .AddRemotingPushConsumerWithTestHandler<RocketMQContainerTests>(options =>
            {
                options.GroupName = group;
                options.InitialPosition = ConsumeFromPosition.Beginning;
                options.PullBatchSize = messageCount;
                options.ConsumeMessageBatchSize = messageCount;
                options.MaxConcurrency = 1;
                options.PullMaxCachedMessages = messageCount;
                options.LongPollingTimeout = TimeSpan.FromSeconds(1);
                options.RetryDelay = TimeSpan.FromSeconds(1);
                options.Subscribe(RocketMQSingleBrokerContainerFixture.TestTopic, new FilterExpression(tag));
            }, (messages, context, _) =>
                {
                    var deliveries = messages
                        .Select(message => (Body: Encoding.UTF8.GetString(message.Body), message.DeliveryAttempt))
                        .ToArray();
                    if (Interlocked.Increment(ref handlerInvocations) == 1)
                    {
                        firstBatch.TrySetResult((deliveries.Select(static delivery => delivery.Body).ToArray(), context.DelayLevelWhenNextConsume));
                        context.AckIndex = 0;
                    }
                    else
                    {
                        foreach (var delivery in deliveries)
                        {
                            redeliveredAttempts.AddOrUpdate(
                                delivery.Body,
                                delivery.DeliveryAttempt,
                                (_, currentAttempt) => Math.Max(currentAttempt, delivery.DeliveryAttempt));
                        }

                        if (expectedRetriedBodies.All(body =>
                                redeliveredAttempts.TryGetValue(body, out var attempt) && attempt >= 2))
                        {
                            tailRedelivered.TrySetResult();
                        }
                    }

                    return ValueTask.FromResult(ConsumeResult.Success);
                });

        await using var provider = services.BuildServiceProvider(new ServiceProviderOptions { ValidateOnBuild = true });
        var producer = provider.GetRequiredService<IRemotingProducer>();
        var consumer = provider.GetRequiredService<IRemotingPushConsumer>();
        await producer.StartAsync(cancellationToken);
        try
        {
            var queue = (await producer.GetPublishMessageQueuesAsync(
                RocketMQSingleBrokerContainerFixture.TestTopic,
                cancellationToken)).First();
            var sent = await producer.SendAsync(
                expectedBodies.Select(body => new Message(
                    RocketMQSingleBrokerContainerFixture.TestTopic,
                    Encoding.UTF8.GetBytes(body))
                {
                    Tag = tag
                }).ToArray(),
                queue,
                cancellationToken);
            Assert.Equal(RemotingSendStatus.SendOk, sent.Status);

            await consumer.StartAsync(cancellationToken);
            var initialDelivery = await firstBatch.Task.WaitAsync(TimeSpan.FromSeconds(20), cancellationToken);
            Assert.Equal(expectedBodies, initialDelivery.Bodies);
            Assert.Equal(1, initialDelivery.DelayLevel);

            await tailRedelivered.Task.WaitAsync(TimeSpan.FromSeconds(30), cancellationToken);
            var retryTopic = $"%RETRY%{group}";
            var retryCommit = await _fixture.WaitForConsumerCommitAsync(
                group,
                retryTopic,
                queue.BrokerName,
                0,
                TimeSpan.FromSeconds(10),
                cancellationToken);
            Assert.True(retryCommit.Committed, $"Legacy push retry offset was not committed. {retryCommit.Progress}");
            Assert.All(
                expectedRetriedBodies,
                body => Assert.True(
                    redeliveredAttempts.TryGetValue(body, out var attempt) && attempt >= 2,
                    $"Message '{body}' was not redelivered from the retry topic."));
            Assert.DoesNotContain(expectedBodies[0], redeliveredAttempts.Keys);

            var commit = await _fixture.WaitForConsumerCommitAsync(
                group,
                RocketMQSingleBrokerContainerFixture.TestTopic,
                queue.BrokerName,
                queue.QueueId,
                TimeSpan.FromSeconds(10),
                cancellationToken);
            Assert.True(commit.Committed, $"Legacy push partial acknowledgement offset was not committed. {commit.Progress}");
        }
        finally
        {
            await consumer.StopAsync(CancellationToken.None);
            await producer.StopAsync(CancellationToken.None);
        }
    }

    [Fact]
    [Trait("Category", "Integration")]
    public async Task LegacyOrderlyPushConsumer_BrokerQueueLock_WaitsForLock()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var suffix = Guid.NewGuid().ToString("N");
        var group = $"legacy-orderly-lock-{suffix}";
        var tag = $"legacy-orderly-lock-{suffix}";
        var expected = $"legacy-orderly-message-{suffix}";
        var lockOwnerClientId = $"127.0.0.1@legacy-orderly-lock-owner-{suffix}";
        var delivered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var services = new ServiceCollection();
        services
            .AddRocketMQRemoting(options =>
            {
                options.NamesrvAddr = _fixture.NameServerAddress;
                options.InstanceName = $"legacy-orderly-lock-{suffix}";
                options.HeartbeatBrokerInterval = TimeSpan.FromMilliseconds(250);
                options.PollNameServerInterval = TimeSpan.FromMilliseconds(250);
            })
            .AddRemotingProducer(options => options.GroupName = $"legacy-orderly-producer-{suffix}")
            .AddRemotingPushConsumerWithTestHandler<RocketMQContainerTests>(options =>
            {
                options.GroupName = group;
                options.ConsumeOrderly = true;
                options.InitialPosition = ConsumeFromPosition.Beginning;
                options.LongPollingTimeout = TimeSpan.FromSeconds(1);
                options.Subscribe(RocketMQSingleBrokerContainerFixture.TestTopic, new FilterExpression(tag));
            }, (messages, _, _) =>
                {
                    var message = Assert.Single(messages);
                    if (Encoding.UTF8.GetString(message.Body) == expected)
                    {
                        delivered.TrySetResult();
                    }

                    return ValueTask.FromResult(ConsumeResult.Success);
                });

        await using var provider = services.BuildServiceProvider(
            new ServiceProviderOptions { ValidateOnBuild = true });
        var producer = provider.GetRequiredService<IRemotingProducer>();
        var consumer = provider.GetRequiredService<IRemotingPushConsumer>();
        var lockClient = provider.GetRequiredKeyedService<RemotingClientRegistry>(Options.DefaultName).SharedClient;
        var brokerEndpoint = EndpointParser.Parse(_fixture.BrokerAddress);
        byte[]? lockBody = null;
        var lockHeld = false;

        await producer.StartAsync(cancellationToken);
        try
        {
            var queue = (await producer.GetPublishMessageQueuesAsync(
                RocketMQSingleBrokerContainerFixture.TestTopic,
                cancellationToken)).First();
            lockBody = Encoding.UTF8.GetBytes(new
            {
                consumerGroup = group,
                clientId = lockOwnerClientId,
                onlyThisBroker = false,
                mqSet = new[]
                {
                    new
                    {
                        topic = queue.Topic,
                        brokerName = queue.BrokerName,
                        queueId = queue.QueueId
                    }
                }
            }.ToJson());
            var lockRequest = new RemotingCommand(RequestCode.LockBatchMq)
            {
                Body = lockBody
            };
            var unlockRequest = new RemotingCommand(RequestCode.UnlockBatchMq)
            {
                Body = lockBody
            };

            var lockResponse = await lockClient.InvokeAsync(
                brokerEndpoint,
                lockRequest,
                TimeSpan.FromSeconds(3),
                cancellationToken);
            Assert.Equal(ResponseCodes.ResSuccess, lockResponse.Code);
            var responseBody = Assert.IsType<byte[]>(lockResponse.Body);
            using (var lockResponseJson = JsonDocument.Parse(responseBody))
            {
                Assert.Contains(
                    lockResponseJson.RootElement.GetProperty("lockOKMQSet").EnumerateArray(),
                    entry =>
                        entry.GetProperty("topic").GetString() == queue.Topic &&
                        entry.GetProperty("brokerName").GetString() == queue.BrokerName &&
                        entry.GetProperty("queueId").GetInt32() == queue.QueueId);
            }

            lockHeld = true;
            await consumer.StartAsync(cancellationToken);
            await producer.SendAsync(new Message(RocketMQSingleBrokerContainerFixture.TestTopic, Encoding.UTF8.GetBytes(expected))
            {
                Tag = tag
            }, queue, cancellationToken);

            await Task.Delay(TimeSpan.FromSeconds(1), cancellationToken);
            Assert.False(
                delivered.Task.IsCompleted,
                "The consumer dispatched a message while another client held the Broker lock.");

            var unlockResponse = await lockClient.InvokeAsync(
                brokerEndpoint,
                unlockRequest,
                TimeSpan.FromSeconds(3),
                cancellationToken);
            Assert.Equal(ResponseCodes.ResSuccess, unlockResponse.Code);
            lockHeld = false;

            await delivered.Task.WaitAsync(TimeSpan.FromSeconds(10), cancellationToken);
        }
        finally
        {
            try
            {
                if (lockHeld && lockBody is not null)
                {
                    await lockClient.InvokeAsync(
                        brokerEndpoint,
                        new RemotingCommand(RequestCode.UnlockBatchMq)
                        {
                            Body = lockBody
                        },
                        TimeSpan.FromSeconds(3),
                        CancellationToken.None);
                }
            }
            finally
            {
                await consumer.StopAsync(CancellationToken.None);
                await producer.StopAsync(CancellationToken.None);
            }
        }
    }

    [Fact]
    [Trait("Category", "Integration")]
    public async Task LegacyPushConsumers_SameGroup_ShareQueuesWithoutDuplicates()
    {
        const int messageCount = 16;
        var cancellationToken = TestContext.Current.CancellationToken;
        var suffix = Guid.NewGuid().ToString("N");
        var tag = $"legacy-cluster-{suffix}";
        var expectedBodies = Enumerable.Range(0, messageCount)
            .Select(index => $"{tag}-{index}")
            .ToHashSet(StringComparer.Ordinal);
        var deliveries = new ConcurrentDictionary<string, int>(StringComparer.Ordinal);
        var allConsumed = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var firstConsumerCalls = 0;
        var secondConsumerCalls = 0;

        ServiceProvider CreateConsumerProvider(string instanceName, Action onDelivery)
        {
            var services = new ServiceCollection();
            services
                .AddRocketMQRemoting(options =>
                {
                    options.NamesrvAddr = _fixture.NameServerAddress;
                    options.InstanceName = instanceName;
                    options.HeartbeatBrokerInterval = TimeSpan.FromMilliseconds(250);
                    options.PollNameServerInterval = TimeSpan.FromMilliseconds(250);
                })
                .AddRemotingPushConsumerWithTestHandler<RocketMQContainerTests>(options =>
                {
                    options.GroupName = "legacy-push-cluster-it";
                    options.MaxConcurrency = 4;
                    options.LongPollingTimeout = TimeSpan.FromSeconds(1);
                    options.Subscribe(RocketMQSingleBrokerContainerFixture.TestTopic, new FilterExpression(tag));
                }, (messages, _, _) =>
                    {
                        foreach (var message in messages)
                        {
                            var body = Encoding.UTF8.GetString(message.Body);
                            if (expectedBodies.Contains(body))
                            {
                                onDelivery();
                                deliveries.AddOrUpdate(body, 1, static (_, count) => count + 1);
                                if (deliveries.Count == messageCount)
                                {
                                    allConsumed.TrySetResult();
                                }
                            }
                        }

                        return ValueTask.FromResult(ConsumeResult.Success);
                    });
            return services.BuildServiceProvider(new ServiceProviderOptions { ValidateOnBuild = true });
        }

        await using var firstProvider = CreateConsumerProvider(
            $"legacy-cluster-a-{suffix}",
            () => Interlocked.Increment(ref firstConsumerCalls));
        await using var secondProvider = CreateConsumerProvider(
            $"legacy-cluster-b-{suffix}",
            () => Interlocked.Increment(ref secondConsumerCalls));
        var producerServices = new ServiceCollection();
        producerServices
            .AddRocketMQRemoting(options =>
            {
                options.NamesrvAddr = _fixture.NameServerAddress;
            })
            .AddRemotingProducer(options => options.GroupName = "legacy-push-cluster-producer-it");
        await using var producerProvider = producerServices.BuildServiceProvider(
            new ServiceProviderOptions { ValidateOnBuild = true });
        var firstConsumer = firstProvider.GetRequiredService<IRemotingPushConsumer>();
        var secondConsumer = secondProvider.GetRequiredService<IRemotingPushConsumer>();
        var producer = producerProvider.GetRequiredService<IRemotingProducer>();

        await producer.StartAsync(cancellationToken);
        await firstConsumer.StartAsync(cancellationToken);
        await secondConsumer.StartAsync(cancellationToken);
        try
        {
            await Task.Delay(TimeSpan.FromSeconds(2), cancellationToken);
            foreach (var body in expectedBodies)
            {
                await producer.SendAsync(new Message(
                    RocketMQSingleBrokerContainerFixture.TestTopic,
                    Encoding.UTF8.GetBytes(body))
                {
                    Tag = tag
                }, cancellationToken);
            }

            await allConsumed.Task.WaitAsync(TimeSpan.FromSeconds(30), cancellationToken);
            await Task.Delay(TimeSpan.FromSeconds(1), cancellationToken);

            Assert.Equal(messageCount, deliveries.Count);
            Assert.All(deliveries, static delivery => Assert.Equal(1, delivery.Value));
            Assert.True(firstConsumerCalls > 0, "The first consumer received no assigned queues.");
            Assert.True(secondConsumerCalls > 0, "The second consumer received no assigned queues.");
        }
        finally
        {
            await secondConsumer.StopAsync(CancellationToken.None);
            await firstConsumer.StopAsync(CancellationToken.None);
            await producer.StopAsync(CancellationToken.None);
        }
    }

    [Fact]
    [Trait("Category", "Integration")]
    public async Task LegacyPushConsumer_FreshGroupBacklog_ConsumesFromBeginning()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var suffix = Guid.NewGuid().ToString("N");
        var tag = $"legacy-backlog-{suffix}";
        var expectedBody = $"backlog-before-start-{suffix}";
        var consumed = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var services = new ServiceCollection();
        services
            .AddRocketMQRemoting(options =>
            {
                options.NamesrvAddr = _fixture.NameServerAddress;
                options.InstanceName = $"legacy-backlog-{suffix}";
                options.HeartbeatBrokerInterval = TimeSpan.FromMilliseconds(250);
                options.PollNameServerInterval = TimeSpan.FromMilliseconds(250);
            })
            .AddRemotingProducer(options => options.GroupName = $"legacy-backlog-producer-{suffix}")
            .AddRemotingPushConsumerWithTestHandler<RocketMQContainerTests>(options =>
            {
                options.GroupName = $"legacy-backlog-consumer-{suffix}";
                options.InitialPosition = ConsumeFromPosition.Beginning;
                options.LongPollingTimeout = TimeSpan.FromSeconds(1);
                options.Subscribe(RocketMQSingleBrokerContainerFixture.TestTopic, new FilterExpression(tag));
            }, (messages, _, _) =>
                {
                    var message = Assert.Single(messages);
                    if (Encoding.UTF8.GetString(message.Body) == expectedBody)
                    {
                        consumed.TrySetResult();
                    }

                    return ValueTask.FromResult(ConsumeResult.Success);
                });
        await using var provider = services.BuildServiceProvider(
            new ServiceProviderOptions { ValidateOnBuild = true });
        var producer = provider.GetRequiredService<IRemotingProducer>();
        var consumer = provider.GetRequiredService<IRemotingPushConsumer>();

        await producer.StartAsync(cancellationToken);
        try
        {
            await producer.SendAsync(new Message(
                RocketMQSingleBrokerContainerFixture.TestTopic,
                Encoding.UTF8.GetBytes(expectedBody))
            {
                Tag = tag
            }, cancellationToken);

            await consumer.StartAsync(cancellationToken);
            try
            {
                await consumed.Task.WaitAsync(TimeSpan.FromSeconds(30), cancellationToken);
            }
            finally
            {
                await consumer.StopAsync(CancellationToken.None);
            }
        }
        finally
        {
            await producer.StopAsync(CancellationToken.None);
        }
    }

    [Fact]
    [Trait("Category", "Integration")]
    public async Task LegacyBroadcastConsumers_SameMessage_EachReceive()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var suffix = Guid.NewGuid().ToString("N");
        var group = $"legacy-broadcast-{suffix}";
        var tag = $"legacy-broadcast-{suffix}";
        var expectedBody = $"broadcast-to-every-instance-{suffix}";
        var firstConsumed = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var secondConsumed = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var firstOffsetPath = System.IO.Path.Combine(
            System.IO.Path.GetTempPath(),
            $"rocketmq-broadcast-a-{suffix}.json");
        var secondOffsetPath = System.IO.Path.Combine(
            System.IO.Path.GetTempPath(),
            $"rocketmq-broadcast-b-{suffix}.json");

        ServiceProvider CreateConsumerProvider(
            string instanceName,
            string offsetPath,
            TaskCompletionSource completion)
        {
            var services = new ServiceCollection();
            services
                .AddRocketMQRemoting(options =>
                {
                    options.NamesrvAddr = _fixture.NameServerAddress;
                    options.InstanceName = instanceName;
                    options.HeartbeatBrokerInterval = TimeSpan.FromMilliseconds(250);
                    options.PollNameServerInterval = TimeSpan.FromMilliseconds(250);
                })
                .AddRemotingPushConsumerWithTestHandler<RocketMQContainerTests>(options =>
                {
                    options.GroupName = group;
                    options.ConsumerMode = ConsumerMode.Broadcasting;
                    options.LocalOffsetStorePath = offsetPath;
                    options.InitialPosition = ConsumeFromPosition.Beginning;
                    options.LongPollingTimeout = TimeSpan.FromSeconds(1);
                    options.Subscribe(RocketMQSingleBrokerContainerFixture.TestTopic, new FilterExpression(tag));
                }, (messages, _, _) =>
                    {
                        var message = Assert.Single(messages);
                        if (Encoding.UTF8.GetString(message.Body) == expectedBody)
                        {
                            completion.TrySetResult();
                        }

                        return ValueTask.FromResult(ConsumeResult.Success);
                    });
            return services.BuildServiceProvider(new ServiceProviderOptions { ValidateOnBuild = true });
        }

        await using var firstProvider = CreateConsumerProvider(
            $"legacy-broadcast-a-{suffix}",
            firstOffsetPath,
            firstConsumed);
        await using var secondProvider = CreateConsumerProvider(
            $"legacy-broadcast-b-{suffix}",
            secondOffsetPath,
            secondConsumed);
        var producerServices = new ServiceCollection();
        producerServices
            .AddRocketMQRemoting(options =>
            {
                options.NamesrvAddr = _fixture.NameServerAddress;
            })
            .AddRemotingProducer(options => options.GroupName = $"legacy-broadcast-producer-{suffix}");
        await using var producerProvider = producerServices.BuildServiceProvider(
            new ServiceProviderOptions { ValidateOnBuild = true });
        var firstConsumer = firstProvider.GetRequiredService<IRemotingPushConsumer>();
        var secondConsumer = secondProvider.GetRequiredService<IRemotingPushConsumer>();
        var producer = producerProvider.GetRequiredService<IRemotingProducer>();

        try
        {
            await producer.StartAsync(cancellationToken);
            await producer.SendAsync(new Message(
                RocketMQSingleBrokerContainerFixture.TestTopic,
                Encoding.UTF8.GetBytes(expectedBody))
            {
                Tag = tag
            }, cancellationToken);

            await firstConsumer.StartAsync(cancellationToken);
            await secondConsumer.StartAsync(cancellationToken);
            await Task.WhenAll(firstConsumed.Task, secondConsumed.Task)
                .WaitAsync(TimeSpan.FromSeconds(30), cancellationToken);
        }
        finally
        {
            await secondConsumer.StopAsync(CancellationToken.None);
            await firstConsumer.StopAsync(CancellationToken.None);
            await producer.StopAsync(CancellationToken.None);
            File.Delete(firstOffsetPath);
            File.Delete(secondOffsetPath);
        }
    }

    private static async Task AssertReachableAsync(string address, CancellationToken cancellationToken)
    {
        var separator = address.LastIndexOf(':');
        var host = address[..separator];
        var port = int.Parse(address[(separator + 1)..]);
        using var client = new TcpClient();
        await client.ConnectAsync(host, port, cancellationToken);
        Assert.True(client.Connected);
    }

    private sealed record OrdersPushConsumerMarker;

    private sealed record OrdersObserverPushConsumerMarker;

    private sealed record PaymentsPushConsumerMarker;

    private sealed record FirstSharedGroupPushConsumerMarker;

    private sealed record SecondSharedGroupPushConsumerMarker;

    private sealed class RecordingLoggerFactory : ILoggerFactory
    {
        private readonly ConcurrentQueue<string> _entries = new();

        public ILogger CreateLogger(string categoryName) => new RecordingLogger(categoryName, _entries);

        public void AddProvider(ILoggerProvider provider)
        {
        }

        public string Snapshot() => string.Join(" | ", _entries);

        public void Dispose()
        {
        }

        private sealed class RecordingLogger(string category, ConcurrentQueue<string> entries) : ILogger
        {
            public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

            public bool IsEnabled(LogLevel logLevel) => true;

            public void Log<TState>(
                LogLevel logLevel,
                EventId eventId,
                TState state,
                Exception? exception,
                Func<TState, Exception?, string> formatter)
            {
                entries.Enqueue($"{logLevel} {category}: {formatter(state, exception)}{(exception is null ? string.Empty : $" ({exception.GetType().Name}: {exception.Message})")}");
            }
        }
    }
}
