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
using EventHorizon.RocketMQ.Remoting.Consumer.Pull;
using EventHorizon.RocketMQ.Remoting.Consumer.Pull.Lite;
using EventHorizon.RocketMQ.Remoting.Consumer.Push;
using EventHorizon.RocketMQ.Remoting.Consumer.Push.Pop;
using EventHorizon.RocketMQ.Remoting.Producer;
using EventHorizon.RocketMQ.Remoting.Protocol;
using EventHorizon.RocketMQ.Remoting.Protocol.Route;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Xunit;

namespace EventHorizon.RocketMQ.Remoting.IntegrationTests;

[Collection(RocketMQCollection.Name)]
public sealed class RocketMQContainerTests
{
    private readonly RocketMQContainerFixture _fixture;

    public RocketMQContainerTests(RocketMQContainerFixture fixture)
    {
        _fixture = fixture;
    }

    [Fact]
    [Trait("Category", "Integration")]
    public async Task ProducerSendsMessagesToContainerizedBroker()
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

        var nameServer = provider.GetRequiredKeyedService<INameServer>(
            new RemotingRocketMQRoleKey(Options.DefaultName, RemotingRocketMQRole.Producer));
        var route = await nameServer.GetTopicRouteInfoAsync(
            RocketMQContainerFixture.TestTopic,
            TimeSpan.FromSeconds(10),
            cancellationToken: cancellationToken);
        Assert.NotEmpty(route.BrokerDatas);
        Assert.NotEmpty(route.QueueDatas);

        await producer.StartAsync(cancellationToken);
        try
        {
            var result = await producer.SendAsync(new Message(
                RocketMQContainerFixture.TestTopic,
                "hello from dotnet"u8.ToArray()), cancellationToken);
            Assert.Equal(RemotingSendStatus.SendOk, result.Status);
            Assert.NotEmpty(result.MessageId);
            Assert.NotEmpty(result.OffsetMessageId);
            Assert.Equal(RocketMQContainerFixture.TestTopic, result.MessageQueue.Topic);

            await producer.SendOnewayAsync(new Message(
                RocketMQContainerFixture.TestTopic,
                "one way"u8.ToArray()), cancellationToken);
        }
        finally
        {
            await producer.StopAsync(CancellationToken.None);
        }
    }

    [Fact]
    [Trait("Category", "Integration")]
    public async Task LegacyPullConsumerControlsOffsets()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var services = new ServiceCollection();
        services
            .AddRocketMQRemoting(options =>
            {
                options.NamesrvAddr = _fixture.NameServerAddress;
            })
            .AddRemotingProducer(options => options.GroupName = "legacy-pull-producer-it")
            .AddRemotingPullConsumer(options =>
            {
                options.GroupName = "remoting-pull-consumer-it";
                options.LongPollingTimeout = TimeSpan.FromSeconds(3);
                options.Subscribe(RocketMQContainerFixture.TestTopic, new FilterExpression("remoting-pull"));
            });

        await using var provider = services.BuildServiceProvider(new ServiceProviderOptions { ValidateOnBuild = true });
        var producer = provider.GetRequiredService<IRemotingProducer>();
        var consumer = provider.GetRequiredService<IRemotingPullConsumer>();
        await producer.StartAsync(cancellationToken);
        await consumer.StartAsync(cancellationToken);
        try
        {
            var queues = await consumer.GetMessageQueuesAsync(
                RocketMQContainerFixture.TestTopic,
                cancellationToken);
            var endOffsets = new Dictionary<(string Broker, int QueueId), long>();
            foreach (var candidate in queues)
            {
                endOffsets[(candidate.BrokerName, candidate.QueueId)] =
                    await consumer.QueryOffsetAsync(
                        candidate,
                        QueryOffsetPolicy.End,
                        cancellationToken: cancellationToken);
            }

            var body = $"remoting-pull-{Guid.NewGuid():N}";
            var sent = await producer.SendAsync(new Message(RocketMQContainerFixture.TestTopic, Encoding.UTF8.GetBytes(body))
            {
                Tag = "remoting-pull"
            }, cancellationToken);
            var queue = queues.Single(value => value.QueueId == sent.MessageQueue.QueueId && value.BrokerName == sent.MessageQueue.BrokerName);
            var offset = endOffsets[(queue.BrokerName, queue.QueueId)];

            RemotingPullResult? matching = null;
            for (var attempt = 0; attempt < 10 && matching is null; attempt++)
            {
                var result = await consumer.PullAsync(
                    queue,
                    offset,
                    cancellationToken: cancellationToken);
                offset = result.NextOffset;
                if (result.Messages.Any(message => Encoding.UTF8.GetString(message.Body) == body))
                {
                    matching = result;
                }
            }

            Assert.NotNull(matching);
            await consumer.UpdateOffsetAsync(queue, matching.NextOffset, cancellationToken);
            Assert.Equal(matching.NextOffset, await consumer.GetOffsetAsync(queue, cancellationToken));
        }
        finally
        {
            await consumer.StopAsync(CancellationToken.None);
            await producer.StopAsync(CancellationToken.None);
        }
    }

    [Fact]
    [Trait("Category", "Integration")]
    public async Task LegacyLitePullConsumerPollsAndExplicitlyCommitsMessage()
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
                options.LongPollingTimeout = TimeSpan.FromSeconds(3);
                options.Subscribe(RocketMQContainerFixture.TestTopic, new FilterExpression("remoting-lite-pull"));
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
            await producer.SendAsync(new Message(RocketMQContainerFixture.TestTopic, Encoding.UTF8.GetBytes(body))
            {
                Tag = "remoting-lite-pull"
            }, cancellationToken);

            RemotingPullResult? matching = null;
            for (var attempt = 0; attempt < 10 && matching is null; attempt++)
            {
                var result = await consumer.PollAsync(cancellationToken: cancellationToken);
                if (result.Messages.Any(message => Encoding.UTF8.GetString(message.Body) == body))
                {
                    matching = result;
                }
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
    public async Task LegacyPopConsumerRenewsAndAcknowledgesBrokerReceipt()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var suffix = Guid.NewGuid().ToString("N");
        var group = $"legacy-pop-consumer-{suffix}";
        var tag = $"legacy-pop-{suffix}";
        var body = $"legacy-pop-message-{suffix}";
        var services = new ServiceCollection();
        services
            .AddRocketMQRemoting(options =>
            {
                options.NamesrvAddr = _fixture.NameServerAddress;
            })
            .AddRemotingProducer(options => options.GroupName = $"legacy-pop-producer-{suffix}")
            .AddRemotingPopConsumer(options =>
            {
                options.GroupName = group;
                options.BatchSize = 1;
                options.InvisibleDuration = TimeSpan.FromSeconds(5);
                options.LongPollingTimeout = TimeSpan.FromSeconds(2);
            });

        await using var provider = services.BuildServiceProvider(new ServiceProviderOptions { ValidateOnBuild = true });
        var producer = provider.GetRequiredService<IRemotingProducer>();
        var consumer = provider.GetRequiredService<IRemotingPopConsumer>();
        await producer.StartAsync(cancellationToken);
        await consumer.StartAsync(cancellationToken);
        try
        {
            var queues = await consumer.GetMessageQueuesAsync(
                RocketMQContainerFixture.TestTopic,
                cancellationToken);
            var sent = await producer.SendAsync(
                new Message(RocketMQContainerFixture.TestTopic, Encoding.UTF8.GetBytes(body)) { Tag = tag },
                cancellationToken);
            var queue = queues.Single(value =>
                value.QueueId == sent.MessageQueue.QueueId && value.BrokerName == sent.MessageQueue.BrokerName);

            RemotingPopMessage? received = null;
            for (var attempt = 0; attempt < 10 && received is null; attempt++)
            {
                var result = await consumer.PopAsync(
                    queue,
                    maxMessages: 1,
                    filter: new FilterExpression(tag),
                    cancellationToken: cancellationToken);
                received = result.Messages.SingleOrDefault(message =>
                    Encoding.UTF8.GetString(message.Message.Body) == body);
            }

            var message = Assert.IsType<RemotingPopMessage>(received);
            var renewed = await consumer.ChangeInvisibleTimeAsync(
                message.Receipt,
                TimeSpan.FromSeconds(5),
                cancellationToken);
            await consumer.AcknowledgeAsync(renewed, cancellationToken);

            var afterAcknowledgement = await consumer.PopAsync(
                queue,
                maxMessages: 1,
                invisibleDuration: TimeSpan.FromSeconds(1),
                filter: new FilterExpression(tag),
                cancellationToken: cancellationToken);
            Assert.DoesNotContain(
                afterAcknowledgement.Messages,
                message => Encoding.UTF8.GetString(message.Message.Body) == body);
        }
        finally
        {
            await consumer.StopAsync(CancellationToken.None);
            await producer.StopAsync(CancellationToken.None);
        }
    }

    [Fact]
    [Trait("Category", "Integration")]
    public async Task LegacyPushConsumerDispatchesAndCommitsMessage()
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
            .AddRemotingPushConsumer(options =>
            {
                options.GroupName = "legacy-push-consumer-it";
                options.LongPollingTimeout = TimeSpan.FromSeconds(3);
                options.RetryDelay = TimeSpan.FromMilliseconds(100);
                options.Subscribe(RocketMQContainerFixture.TestTopic, new FilterExpression("legacy-push"));
                options.MessageHandler = (messages, _, _) =>
                {
                    var message = Assert.Single(messages);
                    var body = Encoding.UTF8.GetString(message.Body);
                    if (body == expected && Interlocked.Increment(ref handlerCalls) == 1)
                    {
                        firstAttempt.TrySetResult();
                        return ValueTask.FromResult(ConsumeResult.Retry);
                    }

                    if (body == expected) consumed.TrySetResult((message.MessageId, message.DeliveryAttempt));
                    return ValueTask.FromResult(ConsumeResult.Success);
                };
            });

        await using var provider = services.BuildServiceProvider(new ServiceProviderOptions { ValidateOnBuild = true });
        var producer = provider.GetRequiredService<IRemotingProducer>();
        var consumer = provider.GetRequiredService<IRemotingPushConsumer>();
        await producer.StartAsync(cancellationToken);
        await consumer.StartAsync(cancellationToken);
        try
        {
            var sent = await producer.SendAsync(new Message(RocketMQContainerFixture.TestTopic, Encoding.UTF8.GetBytes(expected))
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
                RocketMQContainerFixture.TestTopic,
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
    public async Task LegacyPushConsumerDispatchesConfiguredMessageBatch()
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
            .AddRemotingPushConsumer(options =>
            {
                options.GroupName = group;
                options.InitialPosition = ConsumeFromPosition.Beginning;
                options.BatchSize = messageCount;
                options.ConsumeMessageBatchSize = messageCount;
                options.LongPollingTimeout = TimeSpan.FromSeconds(1);
                options.Subscribe(RocketMQContainerFixture.TestTopic, new FilterExpression(tag));
                options.MessageHandler = (messages, _, _) =>
                {
                    var bodies = messages.Select(message => Encoding.UTF8.GetString(message.Body)).ToArray();
                    if (bodies.SequenceEqual(expectedBodies))
                    {
                        consumed.TrySetResult(bodies);
                    }

                    return ValueTask.FromResult(ConsumeResult.Success);
                };
            });

        await using var provider = services.BuildServiceProvider(new ServiceProviderOptions { ValidateOnBuild = true });
        var producer = provider.GetRequiredService<IRemotingProducer>();
        var consumer = provider.GetRequiredService<IRemotingPushConsumer>();
        await producer.StartAsync(cancellationToken);
        try
        {
            var queue = (await producer.GetPublishMessageQueuesAsync(
                RocketMQContainerFixture.TestTopic,
                cancellationToken)).First();
            var sent = await producer.SendAsync(
                expectedBodies.Select(body => new Message(
                RocketMQContainerFixture.TestTopic,
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
                RocketMQContainerFixture.TestTopic,
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
    public async Task LegacyPushConsumerAcknowledgesBatchPrefixAndRetriesRemainingMessages()
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
            .AddRemotingPushConsumer(options =>
            {
                options.GroupName = group;
                options.InitialPosition = ConsumeFromPosition.Beginning;
                options.BatchSize = messageCount;
                options.ConsumeMessageBatchSize = messageCount;
                options.MaxConcurrency = 1;
                options.MaxCachedMessages = messageCount;
                options.LongPollingTimeout = TimeSpan.FromSeconds(1);
                options.RetryDelay = TimeSpan.FromSeconds(1);
                options.Subscribe(RocketMQContainerFixture.TestTopic, new FilterExpression(tag));
                options.MessageHandler = (messages, context, _) =>
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
                };
            });

        await using var provider = services.BuildServiceProvider(new ServiceProviderOptions { ValidateOnBuild = true });
        var producer = provider.GetRequiredService<IRemotingProducer>();
        var consumer = provider.GetRequiredService<IRemotingPushConsumer>();
        await producer.StartAsync(cancellationToken);
        try
        {
            var queue = (await producer.GetPublishMessageQueuesAsync(
                RocketMQContainerFixture.TestTopic,
                cancellationToken)).First();
            var sent = await producer.SendAsync(
                expectedBodies.Select(body => new Message(
                    RocketMQContainerFixture.TestTopic,
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
                RocketMQContainerFixture.TestTopic,
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
    public async Task LegacyOrderlyPushConsumerWaitsForBrokerQueueLock()
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
            .AddRemotingPushConsumer(options =>
            {
                options.GroupName = group;
                options.ConsumeOrderly = true;
                options.InitialPosition = ConsumeFromPosition.Beginning;
                options.LongPollingTimeout = TimeSpan.FromSeconds(1);
                options.Subscribe(RocketMQContainerFixture.TestTopic, new FilterExpression(tag));
                options.MessageHandler = (messages, _, _) =>
                {
                    var message = Assert.Single(messages);
                    if (Encoding.UTF8.GetString(message.Body) == expected)
                    {
                        delivered.TrySetResult();
                    }

                    return ValueTask.FromResult(ConsumeResult.Success);
                };
            });

        await using var provider = services.BuildServiceProvider(
            new ServiceProviderOptions { ValidateOnBuild = true });
        var producer = provider.GetRequiredService<IRemotingProducer>();
        var consumer = provider.GetRequiredService<IRemotingPushConsumer>();
        var producerRoleKey = new RemotingRocketMQRoleKey(Options.DefaultName, RemotingRocketMQRole.Producer);
        var lockClient = provider.GetRequiredKeyedService<IRemotingClient>(producerRoleKey);
        var brokerEndpoint = EndpointParser.Parse(_fixture.BrokerAddress);
        byte[]? lockBody = null;
        var lockHeld = false;

        await producer.StartAsync(cancellationToken);
        try
        {
            var queue = (await producer.GetPublishMessageQueuesAsync(
                RocketMQContainerFixture.TestTopic,
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
            var lockRequest = new RemotingCommand(RequestCode.LockBatchMq, new LockBatchMqRequestHeader())
            {
                Body = lockBody
            };
            var unlockRequest = new RemotingCommand(RequestCode.UnlockBatchMq, new UnlockBatchMqRequestHeader())
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
            await producer.SendAsync(new Message(RocketMQContainerFixture.TestTopic, Encoding.UTF8.GetBytes(expected))
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
                        new RemotingCommand(RequestCode.UnlockBatchMq, new UnlockBatchMqRequestHeader())
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
    public async Task LegacyPushConsumersInSameGroupShareQueuesWithoutDuplicates()
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
                .AddRemotingPushConsumer(options =>
                {
                    options.GroupName = "legacy-push-cluster-it";
                    options.MaxConcurrency = 4;
                    options.LongPollingTimeout = TimeSpan.FromSeconds(1);
                    options.Subscribe(RocketMQContainerFixture.TestTopic, new FilterExpression(tag));
                    options.MessageHandler = (messages, _, _) =>
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
                    };
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
                    RocketMQContainerFixture.TestTopic,
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
    public async Task LegacyPushConsumerBeginningConsumesBacklogForFreshGroup()
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
            .AddRemotingPushConsumer(options =>
            {
                options.GroupName = $"legacy-backlog-consumer-{suffix}";
                options.InitialPosition = ConsumeFromPosition.Beginning;
                options.LongPollingTimeout = TimeSpan.FromSeconds(1);
                options.Subscribe(RocketMQContainerFixture.TestTopic, new FilterExpression(tag));
                options.MessageHandler = (messages, _, _) =>
                {
                    var message = Assert.Single(messages);
                    if (Encoding.UTF8.GetString(message.Body) == expectedBody)
                    {
                        consumed.TrySetResult();
                    }

                    return ValueTask.FromResult(ConsumeResult.Success);
                };
            });
        await using var provider = services.BuildServiceProvider(
            new ServiceProviderOptions { ValidateOnBuild = true });
        var producer = provider.GetRequiredService<IRemotingProducer>();
        var consumer = provider.GetRequiredService<IRemotingPushConsumer>();

        await producer.StartAsync(cancellationToken);
        try
        {
            await producer.SendAsync(new Message(
                RocketMQContainerFixture.TestTopic,
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
    public async Task LegacyBroadcastConsumersEachReceiveTheSameMessage()
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
                .AddRemotingPushConsumer(options =>
                {
                    options.GroupName = group;
                    options.ConsumerMode = ConsumerMode.Broadcasting;
                    options.LocalOffsetStorePath = offsetPath;
                    options.InitialPosition = ConsumeFromPosition.Beginning;
                    options.LongPollingTimeout = TimeSpan.FromSeconds(1);
                    options.Subscribe(RocketMQContainerFixture.TestTopic, new FilterExpression(tag));
                    options.MessageHandler = (messages, _, _) =>
                    {
                        var message = Assert.Single(messages);
                        if (Encoding.UTF8.GetString(message.Body) == expectedBody)
                        {
                            completion.TrySetResult();
                        }

                        return ValueTask.FromResult(ConsumeResult.Success);
                    };
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
                RocketMQContainerFixture.TestTopic,
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
