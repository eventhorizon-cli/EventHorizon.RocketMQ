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
using EventHorizon.RocketMQ.Remoting.Consumer;
using EventHorizon.RocketMQ.Remoting.Consumer.Push;
using EventHorizon.RocketMQ.Remoting.Protocol;
using EventHorizon.RocketMQ.Remoting.Protocol.Route;
using EventHorizon.RocketMQ.Remoting.Tests.Consumer.Push.Support;
using Moq;
using Xunit;

namespace EventHorizon.RocketMQ.Remoting.Tests.Consumer.Push.Pull.Settlement;

public sealed class RemotingPushConsumerSettlementTests : RemotingPushConsumerTestSupport
{
    [Fact]
    public async Task ConcurrentPushConsumer_RetryDelayIsConfigured_LeavesDelaySelectionToBroker()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var delivered = 0;
        var body = CreateMessageRecord("orders", "slow-retry", null, 0, 1_000);
        var remoting = new FakeRemotingClient("127.0.0.1@maximum-delay-level")
        {
            PullHandler = async (request, token) =>
            {
                if (Assert.IsType<string>(request.ExtFields["topic"]) == "orders" &&
                    Interlocked.Exchange(ref delivered, 1) == 0)
                {
                    return PullSuccess(body, 1);
                }

                await Task.Delay(Timeout.InfiniteTimeSpan, token);
                throw new InvalidOperationException("An infinite pull completed unexpectedly.");
            }
        };
        var options = WithMessageHandler(new RemotingPushConsumerOptions
        {
            GroupName = "legacy-group",
            InitialPosition = ConsumeFromPosition.Beginning,
            MaxConcurrency = 1,
            PullMaxCachedMessages = 1,
            LongPollingTimeout = TimeSpan.FromSeconds(1),
            RetryDelay = TimeSpan.FromHours(3),
        }, static (_, _, _) => ValueTask.FromResult(ConsumeResult.Retry));
        options.Subscribe("orders");
        await using var consumer = CreateRemotingPushConsumer(
            options,
            CreateRouteServiceMock().Object,
            remoting,
            "maximum-delay-level");

        await consumer.StartAsync(cancellationToken);
        await remoting.WaitForRequestCountAsync(RequestCode.ConsumerSendMsgBack, 1, cancellationToken);
        await consumer.StopAsync(cancellationToken);

        var sendBack = Assert.Single(
            remoting.Requests,
            static request => request.Code == RequestCode.ConsumerSendMsgBack);
        Assert.Equal(0, Convert.ToInt32(sendBack.ExtFields["delayLevel"]));
    }

    [Fact]
    public async Task ConcurrentPushConsumer_MissingRetryTopic_RecoversFromOffsetAndHandlerFailure()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var orderPulls = 0;
        var body = CreateMessageRecord("orders", "recoverable", null, 4, 1_004);
        var routes = new Mock<ITopicRouteService>(MockBehavior.Strict);
        routes
            .Setup(value => value.GetAsync(
                It.IsAny<string>(),
                It.IsAny<bool>(),
                It.IsAny<CancellationToken>()))
            .Returns<string, bool, CancellationToken>((topic, _, _) =>
                Task.FromResult(topic == "orders" ? Route(1) : new TopicRouteData()));
        var remoting = new FakeRemotingClient("127.0.0.1@offset-illegal-recovery")
        {
            PullHandler = async (request, token) =>
            {
                if (Assert.IsType<string>(request.ExtFields["topic"]) == "orders")
                {
                    return Interlocked.Increment(ref orderPulls) switch
                    {
                        1 => PullOffsetIllegal(4),
                        2 => PullSuccess(body, 5),
                        _ => await WaitForCanceledPullAsync(token)
                    };
                }

                return await WaitForCanceledPullAsync(token);
            },
            MaxOffsetHandler = (request, _) => Task.FromResult(
                Assert.IsType<string>(request.ExtFields["topic"]) == "%RETRY%legacy-group"
                    ? new RemotingCommand { Code = ResponseCodes.ResTopicNotExist }
                    : new RemotingCommand
                    {
                        Code = ResponseCodes.ResSuccess,
                        ExtFields = new Dictionary<string, object> { ["offset"] = "17" }
                    })
        };
        var options = WithMessageHandler(new RemotingPushConsumerOptions
        {
            GroupName = "legacy-group",
            InitialPosition = ConsumeFromPosition.Beginning,
            MaxConcurrency = 1,
            PullMaxCachedMessages = 1,
            LongPollingTimeout = TimeSpan.FromSeconds(1),
        }, static (_, _, _) => throw new InvalidOperationException("The application handler failed."));
        options.Subscribe("orders");
        await using var consumer = CreateRemotingPushConsumer(
            options,
            routes.Object,
            remoting,
            "offset-illegal-recovery");

        await consumer.StartAsync(cancellationToken);
        await remoting.WaitForRequestCountAsync(RequestCode.ConsumerSendMsgBack, 1, cancellationToken);
        while (!remoting.UpdatedOffsets.Any(static update =>
                   update.Topic == "orders" && update.Offset == 5) ||
               !remoting.UpdatedOffsets.Any(static update =>
                   update.Topic == "%RETRY%legacy-group" && update.Offset == 0))
        {
            await Task.Delay(10, cancellationToken);
        }

        Assert.Equal(
            [0L, 4L],
            remoting.PullOffsets
                .Where(static pull => pull.Topic == "orders")
                .Take(2)
                .Select(static pull => pull.Offset));
    }

    [Fact]
    public async Task ConcurrentPushConsumer_SendBackFailure_RetriesWithoutRedelivery()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var deliveries = 0;
        var handlerCalls = 0;
        var sendBackAttempts = 0;
        var body = CreateMessageRecord("orders", "send-back-retry", null, 0, 1_000);
        var remoting = new FakeRemotingClient("127.0.0.1@send-back-retry")
        {
            PullHandler = async (request, token) =>
            {
                if (Assert.IsType<string>(request.ExtFields["topic"]) == "orders" &&
                    Interlocked.Increment(ref deliveries) <= 2)
                {
                    return PullSuccess(body, 1);
                }

                return await WaitForCanceledPullAsync(token);
            },
            SendBackHandler = (_, _) => Interlocked.Increment(ref sendBackAttempts) == 1
                ? Task.FromException<RemotingCommand>(
                    new IOException("The broker did not accept the retry acknowledgement."))
                : Task.FromResult(new RemotingCommand { Code = ResponseCodes.ResSuccess })
        };
        var options = WithMessageHandler(new RemotingPushConsumerOptions
        {
            GroupName = "legacy-group",
            InitialPosition = ConsumeFromPosition.Beginning,
            MaxConcurrency = 1,
            PullMaxCachedMessages = 1,
            LongPollingTimeout = TimeSpan.FromSeconds(1),
            RetryDelay = TimeSpan.FromMilliseconds(10),
        }, (_, _, _) =>
            {
                Interlocked.Increment(ref handlerCalls);
                return ValueTask.FromResult(ConsumeResult.Retry);
            });
        options.Subscribe("orders");
        await using var consumer = CreateRemotingPushConsumer(
            options,
            CreateRouteServiceMock().Object,
            remoting,
            "send-back-retry");

        await consumer.StartAsync(cancellationToken);
        await remoting.WaitForRequestCountAsync(RequestCode.ConsumerSendMsgBack, 2, cancellationToken);
        while (!remoting.UpdatedOffsets.Any(static update =>
                   update.Topic == "orders" && update.Offset == 1))
        {
            await Task.Delay(10, cancellationToken);
        }

        Assert.Equal(2, sendBackAttempts);
        Assert.Equal(1, handlerCalls);
        Assert.Equal(
            [0L, 1L],
            remoting.PullOffsets
                .Where(static pull => pull.Topic == "orders")
                .Take(2)
                .Select(static pull => pull.Offset));
    }

    [Fact]
    public async Task SettlementFailure_HotQueue_LeavesHealthyQueueAdmitted()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var hotFollowerCached = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var settlementFailed = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var healthyHandled = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var hotPulls = 0;
        var healthyPulls = 0;
        var hotFollowerCalls = 0;
        var sendBackAttempts = 0;
        var remoting = new FakeRemotingClient("127.0.0.1@settlement-isolation")
        {
            PullHandler = async (request, token) =>
            {
                if (Assert.IsType<string>(request.ExtFields["topic"]) != "orders")
                {
                    return await WaitForCanceledPullAsync(token);
                }

                var queueId = Convert.ToInt32(request.ExtFields["queueId"]);
                if (queueId == 0)
                {
                    return Interlocked.Increment(ref hotPulls) switch
                    {
                        1 => PullSuccess(
                            CreateMessageRecord("orders", "hot-head", null, 0, 1_000, queueId: 0),
                            1),
                        2 => PullSuccess(
                            CreateMessageRecord("orders", "hot-follower", null, 1, 1_001, queueId: 0),
                            2),
                        3 => await SignalThenWaitForCanceledPullAsync(hotFollowerCached, token),
                        _ => await WaitForCanceledPullAsync(token)
                    };
                }

                await settlementFailed.Task.WaitAsync(token);
                if (Interlocked.Exchange(ref healthyPulls, 1) == 0)
                {
                    return PullSuccess(
                        CreateMessageRecord("orders", "healthy", null, 0, 2_000, queueId: 1),
                        1);
                }

                return await WaitForCanceledPullAsync(token);
            },
            SendBackHandler = (_, _) =>
            {
                Interlocked.Increment(ref sendBackAttempts);
                settlementFailed.TrySetResult();
                return Task.FromException<RemotingCommand>(
                    new IOException("The Broker did not accept the retry acknowledgement."));
            }
        };
        var options = WithMessageHandler(new RemotingPushConsumerOptions
        {
            GroupName = "legacy-group",
            InitialPosition = ConsumeFromPosition.Beginning,
            PullBatchSize = 1,
            ConsumeMessageBatchSize = 1,
            MaxConcurrency = 1,
            PullMaxCachedMessages = 1,
            RetryDelay = TimeSpan.FromHours(1),
            LongPollingTimeout = TimeSpan.FromSeconds(1),
        }, async (messages, _, token) =>
            {
                switch (Assert.Single(messages).MessageId)
                {
                    case "hot-head":
                        await hotFollowerCached.Task.WaitAsync(token);
                        return ConsumeResult.Retry;
                    case "hot-follower":
                        Interlocked.Increment(ref hotFollowerCalls);
                        return ConsumeResult.Success;
                    case "healthy":
                        healthyHandled.TrySetResult();
                        return ConsumeResult.Success;
                    default:
                        throw new InvalidOperationException("The test received an unexpected message.");
                }
            });
        options.Subscribe("orders");
        await using var consumer = CreateRemotingPushConsumer(
            options,
            CreateRouteServiceMock(queueCount: 2).Object,
            remoting,
            "settlement-isolation");

        await consumer.StartAsync(cancellationToken);
        await healthyHandled.Task.WaitAsync(TimeSpan.FromSeconds(3), cancellationToken);

        Assert.Equal(1, sendBackAttempts);
        Assert.Equal(0, hotFollowerCalls);
    }

    [Fact]
    public async Task BlockedFifoSuccessor_HealthyQueueAdmission_DoesNotConsume()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var predecessorStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var successorCached = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var settlementFailed = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var healthyHandled = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var queueZeroPulls = 0;
        var queueOnePulls = 0;
        var queueTwoPulls = 0;
        var successorCalls = 0;
        var sendBackAttempts = 0;
        var remoting = new FakeRemotingClient("127.0.0.1@cross-queue-fifo-admission")
        {
            PullHandler = async (request, token) =>
            {
                if (Assert.IsType<string>(request.ExtFields["topic"]) != "orders")
                {
                    return await WaitForCanceledPullAsync(token);
                }

                var queueId = Convert.ToInt32(request.ExtFields["queueId"]);
                if (queueId == 0)
                {
                    if (Interlocked.Increment(ref queueZeroPulls) == 1)
                    {
                        return PullSuccess(
                            CreateMessageRecord("orders", "fifo-head", "account-7", 0, 1_000, queueId: 0),
                            1);
                    }

                    return await WaitForCanceledPullAsync(token);
                }

                if (queueId == 1)
                {
                    if (Interlocked.Increment(ref queueOnePulls) == 1)
                    {
                        await predecessorStarted.Task.WaitAsync(token);
                        return PullSuccess(
                            CreateMessageRecord("orders", "fifo-successor", "account-7", 0, 1_001, queueId: 1),
                            1);
                    }

                    return await SignalThenWaitForCanceledPullAsync(successorCached, token);
                }

                await settlementFailed.Task.WaitAsync(token);
                if (Interlocked.Increment(ref queueTwoPulls) == 1)
                {
                    return PullSuccess(
                        CreateMessageRecord("orders", "healthy", null, 0, 2_000, queueId: 2),
                        1);
                }

                return await WaitForCanceledPullAsync(token);
            },
            SendBackHandler = (_, _) =>
            {
                Interlocked.Increment(ref sendBackAttempts);
                settlementFailed.TrySetResult();
                return Task.FromException<RemotingCommand>(
                    new IOException("The Broker did not accept the dead-letter settlement."));
            }
        };
        var options = WithMessageHandler(new RemotingPushConsumerOptions
        {
            GroupName = "legacy-group",
            InitialPosition = ConsumeFromPosition.Beginning,
            PullBatchSize = 1,
            ConsumeMessageBatchSize = 1,
            MaxConcurrency = 1,
            PullMaxCachedMessages = 1,
            RetryDelay = TimeSpan.FromHours(1),
            LongPollingTimeout = TimeSpan.FromSeconds(1),
        }, async (messages, _, token) =>
            {
                switch (Assert.Single(messages).MessageId)
                {
                    case "fifo-head":
                        predecessorStarted.TrySetResult();
                        await successorCached.Task.WaitAsync(token);
                        return ConsumeResult.DeadLetter;
                    case "fifo-successor":
                        Interlocked.Increment(ref successorCalls);
                        return ConsumeResult.Success;
                    case "healthy":
                        healthyHandled.TrySetResult();
                        return ConsumeResult.Success;
                    default:
                        throw new InvalidOperationException("The test received an unexpected message.");
                }
            });
        options.Subscribe("orders");
        await using var consumer = CreateRemotingPushConsumer(
            options,
            CreateRouteServiceMock(queueCount: 3).Object,
            remoting,
            "cross-queue-fifo-admission");

        await consumer.StartAsync(cancellationToken);
        await healthyHandled.Task.WaitAsync(TimeSpan.FromSeconds(3), cancellationToken);

        Assert.Equal(1, sendBackAttempts);
        Assert.Equal(0, successorCalls);
    }

    [Fact]
    public async Task ConcurrentPushConsumer_FirstRetryBoundaryPersistenceFails_RetriesOriginalBoundary()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var retryBoundaryPersisted = new TaskCompletionSource<long>(TaskCreationOptions.RunContinuationsAsynchronously);
        var routes = new Mock<ITopicRouteService>(MockBehavior.Strict);
        routes
            .Setup(value => value.GetAsync(
                It.IsAny<string>(),
                It.IsAny<bool>(),
                It.IsAny<CancellationToken>()))
            .Returns<string, bool, CancellationToken>((topic, _, _) =>
                Task.FromResult(topic == "orders" ? Route(1) : new TopicRouteData()));
        var delivered = 0;
        var retryEndOffset = 0;
        var retryOffsetUpdates = 0;
        var retryMaximumQueries = 0;
        var handlerCalls = 0;
        var body = CreateMessageRecord("orders", "first", null, 0, 1_000)
            .Concat(CreateMessageRecord("orders", "second", null, 1, 1_001))
            .ToArray();
        var remoting = new FakeRemotingClient("127.0.0.1@retry-boundary")
        {
            ConsumerOffsetHandler = static (_, _) =>
                Task.FromResult(new RemotingCommand { Code = ResponseCodes.ResQueryNotFound }),
            PullHandler = async (request, token) =>
            {
                if (Assert.IsType<string>(request.ExtFields["topic"]) == "orders" &&
                    Interlocked.Exchange(ref delivered, 1) == 0)
                {
                    return PullSuccess(body, 2);
                }

                return await WaitForCanceledPullAsync(token);
            },
            MaxOffsetHandler = (request, _) =>
            {
                Assert.Equal("%RETRY%legacy-group", Assert.IsType<string>(request.ExtFields["topic"]));
                Interlocked.Increment(ref retryMaximumQueries);
                return Task.FromResult(new RemotingCommand
                {
                    Code = ResponseCodes.ResSuccess,
                    ExtFields = new Dictionary<string, object>
                    {
                        ["offset"] = Volatile.Read(ref retryEndOffset).ToString()
                    }
                });
            },
            SendBackHandler = (_, _) =>
            {
                Interlocked.Increment(ref retryEndOffset);
                return Task.FromResult(new RemotingCommand { Code = ResponseCodes.ResSuccess });
            },
            UpdateOffsetHandler = (request, _) =>
            {
                if (Assert.IsType<string>(request.ExtFields["topic"]) == "%RETRY%legacy-group")
                {
                    var boundary = Convert.ToInt64(request.ExtFields["commitOffset"]);
                    if (Interlocked.Increment(ref retryOffsetUpdates) == 1)
                    {
                        return Task.FromException<RemotingCommand>(
                            new IOException("The first retry-boundary update failed."));
                    }

                    retryBoundaryPersisted.TrySetResult(boundary);
                }

                return Task.FromResult(new RemotingCommand { Code = ResponseCodes.ResSuccess });
            }
        };
        var options = WithMessageHandler(new RemotingPushConsumerOptions
        {
            GroupName = "legacy-group",
            InitialPosition = ConsumeFromPosition.Beginning,
            PullBatchSize = 2,
            ConsumeMessageBatchSize = 2,
            MaxConcurrency = 1,
            PullMaxCachedMessages = 2,
            LongPollingTimeout = TimeSpan.FromSeconds(1),
        }, (_, _, _) =>
            {
                Interlocked.Increment(ref handlerCalls);
                return ValueTask.FromResult(ConsumeResult.Retry);
            });
        options.Subscribe("orders");
        await using var consumer = CreateRemotingPushConsumer(
            options,
            routes.Object,
            remoting,
            "retry-boundary");

        await consumer.StartAsync(cancellationToken);
        var persistedBoundary = await retryBoundaryPersisted.Task.WaitAsync(
            TimeSpan.FromSeconds(3),
            cancellationToken);

        Assert.Equal(0, persistedBoundary);
        Assert.Equal(1, retryMaximumQueries);
        Assert.Equal(2, retryOffsetUpdates);
        Assert.Equal(2, retryEndOffset);
        Assert.Equal(1, handlerCalls);
    }

    [Fact]
    public async Task FifoSuccessor_FailedDeadLetterSettlement_WaitsForRetry()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var secondSettlementStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseSettlement = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var secondHandled = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var handlerCalls = new ConcurrentDictionary<string, int>(StringComparer.Ordinal);
        var sendBackAttempts = 0;
        var delivered = 0;
        var body = CreateMessageRecord("orders", "first", "account-7", 0, 1_000)
            .Concat(CreateMessageRecord("orders", "second", "account-7", 1, 1_001))
            .ToArray();
        var remoting = new FakeRemotingClient("127.0.0.1@fifo-settlement-retry")
        {
            PullHandler = async (request, token) =>
            {
                if (Assert.IsType<string>(request.ExtFields["topic"]) == "orders" &&
                    Interlocked.Exchange(ref delivered, 1) == 0)
                {
                    return PullSuccess(body, 2);
                }

                return await WaitForCanceledPullAsync(token);
            },
            SendBackHandler = async (_, token) =>
            {
                if (Interlocked.Increment(ref sendBackAttempts) == 1)
                {
                    throw new IOException("The Broker did not accept the dead-letter settlement.");
                }

                secondSettlementStarted.TrySetResult();
                await releaseSettlement.Task.WaitAsync(token);
                return new RemotingCommand { Code = ResponseCodes.ResSuccess };
            }
        };
        var options = WithMessageHandler(new RemotingPushConsumerOptions
        {
            GroupName = "legacy-group",
            InitialPosition = ConsumeFromPosition.Beginning,
            ConsumeMessageBatchSize = 2,
            MaxConcurrency = 2,
            PullMaxCachedMessages = 2,
            RetryDelay = TimeSpan.FromMilliseconds(10),
            LongPollingTimeout = TimeSpan.FromSeconds(1),
        }, (messages, _, _) =>
            {
                var messageId = Assert.Single(messages).MessageId;
                handlerCalls.AddOrUpdate(messageId, 1, static (_, count) => count + 1);
                if (messageId == "second")
                {
                    secondHandled.TrySetResult();
                    return ValueTask.FromResult(ConsumeResult.Success);
                }

                return ValueTask.FromResult(ConsumeResult.DeadLetter);
            });
        options.Subscribe("orders");
        await using var consumer = CreateRemotingPushConsumer(
            options,
            CreateRouteServiceMock().Object,
            remoting,
            "fifo-settlement-retry");

        await consumer.StartAsync(cancellationToken);
        await secondSettlementStarted.Task.WaitAsync(TimeSpan.FromSeconds(3), cancellationToken);
        Assert.False(secondHandled.Task.IsCompleted);
        Assert.Equal(1, handlerCalls["first"]);

        releaseSettlement.TrySetResult();
        await secondHandled.Task.WaitAsync(TimeSpan.FromSeconds(3), cancellationToken);
        while (!remoting.UpdatedOffsets.Any(static update => update.Topic == "orders" && update.Offset == 2))
        {
            await Task.Delay(10, cancellationToken);
        }

        Assert.Equal(2, sendBackAttempts);
        Assert.Equal(1, handlerCalls["first"]);
        Assert.Equal(1, handlerCalls["second"]);
    }
}
