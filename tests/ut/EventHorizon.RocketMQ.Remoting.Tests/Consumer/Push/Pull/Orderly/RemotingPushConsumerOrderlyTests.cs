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

using System.Text.Json;
using EventHorizon.RocketMQ.Remoting.Consumer;
using EventHorizon.RocketMQ.Remoting.Consumer.Push;
using EventHorizon.RocketMQ.Remoting.Instrumentation;
using EventHorizon.RocketMQ.Remoting.Protocol;
using EventHorizon.RocketMQ.Remoting.Tests.Consumer.Coordination.Rebalance;
using EventHorizon.RocketMQ.Remoting.Tests.Consumer.Push.Support;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Moq;
using Xunit;

namespace EventHorizon.RocketMQ.Remoting.Tests.Consumer.Push.Pull.Orderly;

public sealed class RemotingPushConsumerOrderlyTests : RemotingPushConsumerTestSupport
{
    [Fact]
    public async Task FifoRetry_SameGroupPredecessorAndBatchContext_KeepsSuccessorBlocked()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var firstSecondAttempt = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var firstCancellationRequested = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseFirst = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var secondHandled = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var firstCalls = 0;
        var delivered = 0;
        var body = CreateMessageRecord("orders", "first", "account-7", 17, 1_017)
            .Concat(CreateMessageRecord("orders", "second", "account-7", 18, 1_018))
            .ToArray();
        var remoting = new FakeRemotingClient("127.0.0.1@legacy-test")
        {
            PullHandler = async (request, token) =>
            {
                var topic = Assert.IsType<string>(request.ExtFields["topic"]);
                if (topic == "orders" && Interlocked.Exchange(ref delivered, 1) == 0)
                {
                    return PullSuccess(body, 19);
                }

                await Task.Delay(Timeout.InfiniteTimeSpan, token);
                throw new InvalidOperationException("An infinite pull completed unexpectedly.");
            }
        };
        var options = WithMessageHandler(new RemotingPushConsumerOptions
        {
            GroupName = "legacy-group",
            ConsumeMessageBatchSize = 2,
            ConsumeTimeout = TimeSpan.FromMilliseconds(50),
            MaxConcurrency = 2,
            PullMaxCachedMessages = 8,
            MaxDeliveryAttempts = 3,
            RetryDelay = TimeSpan.FromMilliseconds(10),
            LongPollingTimeout = TimeSpan.FromSeconds(1),
        }, async (messages, context, token) =>
            {
                var message = Assert.Single(messages);
                if (message.MessageId == "first")
                {
                    if (Interlocked.Increment(ref firstCalls) == 1)
                    {
                        return ConsumeResult.Retry;
                    }

                    firstSecondAttempt.TrySetResult();
                    using var registration = token.Register(() => firstCancellationRequested.TrySetResult());
                    await releaseFirst.Task.WaitAsync(token);
                    context.AckIndex = -1;
                    return ConsumeResult.Success;
                }

                secondHandled.TrySetResult();
                return ConsumeResult.Success;
            });
        options.Subscribe("orders");
        await using var consumer = CreateRemotingPushConsumer(
            Options.Create(options),
            Options.Create(new RemotingClientOptions
            {
                ClientIP = "127.0.0.1",
                InstanceName = "legacy-test",
                PollNameServerInterval = TimeSpan.FromHours(1),
                HeartbeatBrokerInterval = TimeSpan.FromHours(1)
            }),
            CreateRouteServiceMock().Object,
            remoting,
            TimeProvider.System,
            NullLogger<RemotingPushConsumer>.Instance);

        await consumer.StartAsync(cancellationToken);
        await firstSecondAttempt.Task.WaitAsync(TimeSpan.FromSeconds(3), cancellationToken);
        await Task.Delay(100, cancellationToken);
        Assert.False(firstCancellationRequested.Task.IsCompleted);
        Assert.False(secondHandled.Task.IsCompleted);

        releaseFirst.TrySetResult();
        await secondHandled.Task.WaitAsync(TimeSpan.FromSeconds(3), cancellationToken);
        await consumer.StopAsync(cancellationToken);

        Assert.Equal(2, firstCalls);
        Assert.DoesNotContain(remoting.Requests, static request => request.Code == RequestCode.ConsumerSendMsgBack);
    }

    [Fact]
    public async Task FifoCompletion_SameGroupSuccessor_WakesAcrossProcessQueue()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var firstStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var secondPullReturned = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseFirst = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var secondHandled = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var queueZeroDelivered = 0;
        var queueOneDelivered = 0;
        var remoting = new FakeRemotingClient("127.0.0.1@cross-queue-fifo")
        {
            PullHandler = async (request, token) =>
            {
                if (Assert.IsType<string>(request.ExtFields["topic"]) != "orders")
                {
                    return await WaitForCanceledPullAsync(token);
                }

                var queueId = Convert.ToInt32(request.ExtFields["queueId"]);
                if (queueId == 0 && Interlocked.Exchange(ref queueZeroDelivered, 1) == 0)
                {
                    return PullSuccess(
                        CreateMessageRecord("orders", "first", "account-7", 0, 1_000, queueId: 0),
                        1);
                }

                if (queueId == 1 && Interlocked.Exchange(ref queueOneDelivered, 1) == 0)
                {
                    await firstStarted.Task.WaitAsync(token);
                    secondPullReturned.TrySetResult();
                    return PullSuccess(
                        CreateMessageRecord("orders", "second", "account-7", 0, 1_001, queueId: 1),
                        1);
                }

                return await WaitForCanceledPullAsync(token);
            }
        };
        var options = WithMessageHandler(new RemotingPushConsumerOptions
        {
            GroupName = "legacy-group",
            InitialPosition = ConsumeFromPosition.Beginning,
            PullBatchSize = 1,
            ConsumeMessageBatchSize = 1,
            MaxConcurrency = 2,
            PullMaxCachedMessages = 2,
            LongPollingTimeout = TimeSpan.FromSeconds(1),
        }, async (messages, _, token) =>
            {
                if (Assert.Single(messages).MessageId == "first")
                {
                    firstStarted.TrySetResult();
                    await releaseFirst.Task.WaitAsync(token);
                }
                else
                {
                    secondHandled.TrySetResult();
                }

                return ConsumeResult.Success;
            });
        options.Subscribe("orders");
        await using var consumer = CreateRemotingPushConsumer(
            options,
            CreateRouteServiceMock(queueCount: 2).Object,
            remoting,
            "cross-queue-fifo");

        await consumer.StartAsync(cancellationToken);
        await secondPullReturned.Task.WaitAsync(TimeSpan.FromSeconds(3), cancellationToken);
        await Task.Delay(100, cancellationToken);
        Assert.False(secondHandled.Task.IsCompleted);

        releaseFirst.TrySetResult();
        await secondHandled.Task.WaitAsync(TimeSpan.FromSeconds(3), cancellationToken);
    }

    [Fact]
    public async Task Rebalance_DroppedInFlightHandler_KeepsCrossQueueSuccessorBlocked()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        const string clientId = "127.0.0.1@cross-queue-fifo-drop";
        var firstStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var firstCancellationRequested = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var secondPullReturned = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseFirst = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var secondHandled = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var queueZeroDelivered = 0;
        var queueOneDelivered = 0;
        var remoting = new FakeRemotingClient(clientId)
        {
            PullHandler = async (request, token) =>
            {
                if (Assert.IsType<string>(request.ExtFields["topic"]) != "orders")
                {
                    return await WaitForCanceledPullAsync(token);
                }

                var queueId = Convert.ToInt32(request.ExtFields["queueId"]);
                if (queueId == 0 && Interlocked.Exchange(ref queueZeroDelivered, 1) == 0)
                {
                    return PullSuccess(
                        CreateMessageRecord("orders", "first", "account-7", 0, 1_000, queueId: 0),
                        1);
                }

                if (queueId == 1 && Interlocked.Exchange(ref queueOneDelivered, 1) == 0)
                {
                    await firstStarted.Task.WaitAsync(token);
                    secondPullReturned.TrySetResult();
                    return PullSuccess(
                        CreateMessageRecord("orders", "second", "account-7", 0, 1_001, queueId: 1),
                        1);
                }

                return await WaitForCanceledPullAsync(token);
            }
        };
        var options = WithMessageHandler(new RemotingPushConsumerOptions
        {
            GroupName = "legacy-group",
            InitialPosition = ConsumeFromPosition.Beginning,
            PullBatchSize = 1,
            ConsumeMessageBatchSize = 1,
            MaxConcurrency = 2,
            PullMaxCachedMessages = 2,
            LongPollingTimeout = TimeSpan.FromSeconds(1),
        }, async (messages, _, token) =>
            {
                if (Assert.Single(messages).MessageId == "first")
                {
                    firstStarted.TrySetResult();
                    using var registration = token.Register(() => firstCancellationRequested.TrySetResult());
                    await releaseFirst.Task;
                }
                else
                {
                    secondHandled.TrySetResult();
                }

                return ConsumeResult.Success;
            });
        options.Subscribe("orders");
        var rebalance = new TestRemotingRebalanceService();
        await using var consumer = CreateRemotingPushConsumer(
            options,
            CreateRouteServiceMock(queueCount: 2).Object,
            remoting,
            "cross-queue-fifo-drop",
            rebalanceService: rebalance);

        await consumer.StartAsync(cancellationToken);
        try
        {
            await secondPullReturned.Task.WaitAsync(TimeSpan.FromSeconds(3), cancellationToken);
            Assert.False(secondHandled.Task.IsCompleted);

            remoting.SetConsumerIds("000-other-client", clientId);
            await rebalance.RebalanceAsync("legacy-group", cancellationToken);
            await firstCancellationRequested.Task.WaitAsync(TimeSpan.FromSeconds(3), cancellationToken);
            await Task.Delay(100, cancellationToken);
            Assert.False(secondHandled.Task.IsCompleted);

            releaseFirst.TrySetResult();
            await secondHandled.Task.WaitAsync(TimeSpan.FromSeconds(3), cancellationToken);

            Assert.DoesNotContain(remoting.Requests, static request =>
                request.Code == RequestCode.UpdateConsumerOffset &&
                Convert.ToInt32(request.ExtFields["queueId"]) == 0 &&
                Convert.ToInt64(request.ExtFields["commitOffset"]) == 1);
        }
        finally
        {
            releaseFirst.TrySetResult();
            await consumer.StopAsync(CancellationToken.None);
        }
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task StopAsync_NonCooperativeFifoOrOrderlyHandler_CancelsAndIgnoresLateResult(bool consumeOrderly)
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var handlerStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var handlerCancellationRequested = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseHandler = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var handlerFinished = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var processCompleted = new TaskCompletionSource<(Exception? Exception, bool? Success, string? Outcome)>(
            TaskCreationOptions.RunContinuationsAsynchronously);
        var processDisposed = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var processOperation = new Mock<IRemotingRocketMQTelemetryOperation>(MockBehavior.Strict);
        processOperation
            .Setup(value => value.Complete(It.IsAny<Exception>()))
            .Callback<Exception>(exception => { processCompleted.TrySetResult((exception, null, null)); });
        processOperation
            .Setup(value => value.Complete(It.IsAny<bool>(), It.IsAny<string?>()))
            .Callback<bool, string?>((success, outcome) => { processCompleted.TrySetResult((null, success, outcome)); });
        processOperation.Setup(value => value.Dispose()).Callback(() => { processDisposed.TrySetResult(); });
        var telemetry = new ProcessTelemetry(processOperation.Object);
        var delivered = 0;
        var body = CreateMessageRecord(
            "orders",
            "stuck",
            consumeOrderly ? null : "account-7",
            0,
            1_000);
        var remoting = new FakeRemotingClient($"127.0.0.1@stop-cancellation-{consumeOrderly}")
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
            ConsumeOrderly = consumeOrderly,
            InitialPosition = ConsumeFromPosition.Beginning,
            MaxConcurrency = 1,
            PullMaxCachedMessages = 1,
            LongPollingTimeout = TimeSpan.FromSeconds(1),
        }, async (_, _, token) =>
            {
                handlerStarted.TrySetResult();
                using var registration = token.Register(() => handlerCancellationRequested.TrySetResult());
                await releaseHandler.Task;
                handlerFinished.TrySetResult();
                return ConsumeResult.Success;
            });
        options.Subscribe("orders");
        var consumer = CreateRemotingPushConsumer(
            options,
            CreateRouteServiceMock().Object,
            remoting,
            $"stop-cancellation-{consumeOrderly}",
            telemetry);

        try
        {
            await consumer.StartAsync(cancellationToken);
            await handlerStarted.Task.WaitAsync(TimeSpan.FromSeconds(3), cancellationToken);

            using var stopCancellation = new CancellationTokenSource();
            var stop = consumer.StopAsync(stopCancellation.Token).AsTask();
            await handlerCancellationRequested.Task.WaitAsync(TimeSpan.FromSeconds(3), cancellationToken);
            stopCancellation.Cancel();
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() =>
                stop.WaitAsync(TimeSpan.FromSeconds(3), cancellationToken));

            releaseHandler.TrySetResult();
            await handlerFinished.Task.WaitAsync(TimeSpan.FromSeconds(3), cancellationToken);
            var completion = await processCompleted.Task.WaitAsync(TimeSpan.FromSeconds(3), cancellationToken);
            await processDisposed.Task.WaitAsync(TimeSpan.FromSeconds(3), cancellationToken);

            Assert.IsType<OperationCanceledException>(completion.Exception);
            Assert.DoesNotContain(remoting.Requests, static request => request.Code == RequestCode.ConsumerSendMsgBack);
            Assert.DoesNotContain(
                remoting.UpdatedOffsets,
                static update => update.Topic == "orders" && update.Offset == 1);
        }
        finally
        {
            releaseHandler.TrySetResult();
            await consumer.DisposeAsync();
        }
    }

    [Fact]
    public async Task OrderlyConsumer_DeadLetterResult_CommitsPastMessage()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var delivered = 0;
        var body = CreateMessageRecord("orders", "dead-letter", null, 0, 1_000);
        var remoting = new FakeRemotingClient("127.0.0.1@orderly-dead-letter")
        {
            PullHandler = async (request, token) =>
            {
                if (Assert.IsType<string>(request.ExtFields["topic"]) == "orders" &&
                    Interlocked.Exchange(ref delivered, 1) == 0)
                {
                    return PullSuccess(body, 1);
                }

                return await WaitForCanceledPullAsync(token);
            }
        };
        var options = WithMessageHandler(new RemotingPushConsumerOptions
        {
            GroupName = "legacy-group",
            ConsumeOrderly = true,
            InitialPosition = ConsumeFromPosition.Beginning,
            MaxConcurrency = 1,
            PullMaxCachedMessages = 1,
            LongPollingTimeout = TimeSpan.FromSeconds(1),
        }, static (_, _, _) => ValueTask.FromResult(ConsumeResult.DeadLetter));
        options.Subscribe("orders");
        await using var consumer = CreateRemotingPushConsumer(
            options,
            CreateRouteServiceMock().Object,
            remoting,
            "orderly-dead-letter");

        await consumer.StartAsync(cancellationToken);
        await remoting.WaitForRequestCountAsync(RequestCode.ConsumerSendMsgBack, 1, cancellationToken);
        while (!remoting.UpdatedOffsets.Any(static update =>
                   update.Topic == "orders" && update.Offset == 1))
        {
            await Task.Delay(10, cancellationToken);
        }

        var sendBack = Assert.Single(
            remoting.Requests,
            static request => request.Code == RequestCode.ConsumerSendMsgBack);
        Assert.Equal(-1, Convert.ToInt32(sendBack.ExtFields["delayLevel"]));
    }

    [Fact]
    public async Task OrderlyConsumer_BrokerQueueLock_UsesBeforePulling()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var remoting = new FakeRemotingClient("127.0.0.1@orderly-lock-test");
        var options = WithMessageHandler(new RemotingPushConsumerOptions
        {
            GroupName = "legacy-group",
            ConsumeOrderly = true,
            InitialPosition = ConsumeFromPosition.Beginning,
            MaxConcurrency = 2,
            PullMaxCachedMessages = 8,
            LongPollingTimeout = TimeSpan.FromSeconds(1),
        }, static (_, _, _) => ValueTask.FromResult(ConsumeResult.Success));
        options.Subscribe("orders");
        await using var consumer = CreateRemotingPushConsumer(
            Options.Create(options),
            Options.Create(new RemotingClientOptions
            {
                ClientIP = "127.0.0.1",
                InstanceName = "orderly-lock-test",
                Namespace = "tenant-a",
                PollNameServerInterval = TimeSpan.FromHours(1),
                HeartbeatBrokerInterval = TimeSpan.FromHours(1)
            }),
            CreateRouteServiceMock().Object,
            remoting,
            TimeProvider.System,
            NullLogger<RemotingPushConsumer>.Instance);

        await consumer.StartAsync(cancellationToken);
        await remoting.WaitForTopicPullsAsync("tenant-a%orders", 1, cancellationToken);

        var requests = remoting.Requests.ToArray();
        var lockRequest = requests.First(request =>
            request.Code == RequestCode.LockBatchMq &&
            RequestContainsQueueTopic(request, "tenant-a%orders"));
        using var body = JsonDocument.Parse(lockRequest.Body!);
        Assert.Equal("tenant-a%legacy-group", body.RootElement.GetProperty("consumerGroup").GetString());
        Assert.Equal("127.0.0.1@orderly-lock-test", body.RootElement.GetProperty("clientId").GetString());
        Assert.False(body.RootElement.GetProperty("onlyThisBroker").GetBoolean());
        var queue = Assert.Single(body.RootElement.GetProperty("mqSet").EnumerateArray());
        Assert.Equal("tenant-a%orders", queue.GetProperty("topic").GetString());
        Assert.Equal("broker-a", queue.GetProperty("brokerName").GetString());
        Assert.Equal(0, queue.GetProperty("queueId").GetInt32());

        var lockIndex = Array.IndexOf(requests, lockRequest);
        var pullIndex = Array.FindIndex(requests, request =>
            request.Code == RequestCode.PullMessage &&
            string.Equals(request.ExtFields["topic"] as string, "tenant-a%orders", StringComparison.Ordinal));
        Assert.True(lockIndex >= 0 && pullIndex > lockIndex);

        await consumer.StopAsync(cancellationToken);
        Assert.Contains(
            remoting.Requests,
            request => request.Code == RequestCode.UnlockBatchMq &&
                       RequestContainsQueueTopic(request, "tenant-a%orders"));
    }

    [Fact]
    public async Task OrderlyConsumer_BrokerQueueLock_DoesNotPullEarly()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var lockOrders = 0;
        var remoting = new FakeRemotingClient("127.0.0.1@orderly-lock-wait")
        {
            LockHandler = (request, _) =>
            {
                var grant = Volatile.Read(ref lockOrders) != 0 ||
                            !RequestContainsQueueTopic(request, "orders");
                return Task.FromResult(CreateLockResponse(request, grant));
            }
        };
        var options = WithMessageHandler(new RemotingPushConsumerOptions
        {
            GroupName = "legacy-group",
            ConsumeOrderly = true,
            InitialPosition = ConsumeFromPosition.Beginning,
            MaxConcurrency = 1,
            PullMaxCachedMessages = 8,
            LongPollingTimeout = TimeSpan.FromSeconds(1),
        }, static (_, _, _) => ValueTask.FromResult(ConsumeResult.Success));
        options.Subscribe("orders");
        var rebalance = new TestRemotingRebalanceService();
        await using var consumer = CreateRemotingPushConsumer(
            Options.Create(options),
            Options.Create(new RemotingClientOptions
            {
                ClientIP = "127.0.0.1",
                InstanceName = "orderly-lock-wait",
                PollNameServerInterval = TimeSpan.FromMilliseconds(20),
                HeartbeatBrokerInterval = TimeSpan.FromMilliseconds(20)
            }),
            CreateRouteServiceMock().Object,
            remoting,
            TimeProvider.System,
            NullLogger<RemotingPushConsumer>.Instance,
            rebalanceService: rebalance);

        await consumer.StartAsync(cancellationToken);
        await Task.Delay(100, cancellationToken);
        Assert.DoesNotContain(
            remoting.PullOffsets,
            static pull => string.Equals(pull.Topic, "orders", StringComparison.Ordinal));

        Assert.False(await rebalance.RebalanceAsync("legacy-group", cancellationToken));

        Volatile.Write(ref lockOrders, 1);
        Assert.True(await rebalance.RebalanceAsync("legacy-group", cancellationToken));
        await remoting.WaitForTopicPullsAsync("orders", 1, cancellationToken);
        await consumer.StopAsync(cancellationToken);
    }

    [Fact]
    public async Task OrderlyConsumer_LeaseAfterTransientLockRenewalFailure_PreservesLease()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var orderLockCalls = 0;
        var remoting = new FakeRemotingClient("127.0.0.1@orderly-renewal")
        {
            LockHandler = (request, _) =>
            {
                if (!RequestContainsQueueTopic(request, "orders"))
                {
                    return Task.FromResult(CreateLockResponse(request, grant: true));
                }

                return Interlocked.Increment(ref orderLockCalls) == 1
                    ? Task.FromResult(CreateLockResponse(request, grant: true))
                    : Task.FromException<RemotingCommand>(
                        new IOException("The broker did not respond to the lock renewal."));
            },
            PullHandler = async (request, token) =>
            {
                var topic = Assert.IsType<string>(request.ExtFields["topic"]);
                if (topic == "orders")
                {
                    await Task.Delay(100, token);
                    return PullEmpty(0);
                }

                await Task.Delay(Timeout.InfiniteTimeSpan, token);
                throw new InvalidOperationException("An infinite pull completed unexpectedly.");
            }
        };
        var options = WithMessageHandler(new RemotingPushConsumerOptions
        {
            GroupName = "legacy-group",
            ConsumeOrderly = true,
            InitialPosition = ConsumeFromPosition.Beginning,
            MaxConcurrency = 1,
            PullMaxCachedMessages = 8,
            LongPollingTimeout = TimeSpan.FromSeconds(1),
        }, static (_, _, _) => ValueTask.FromResult(ConsumeResult.Success));
        options.Subscribe("orders");
        var rebalance = new TestRemotingRebalanceService();
        await using var consumer = CreateRemotingPushConsumer(
            Options.Create(options),
            Options.Create(new RemotingClientOptions
            {
                ClientIP = "127.0.0.1",
                InstanceName = "orderly-renewal",
                PollNameServerInterval = TimeSpan.FromMilliseconds(20),
                HeartbeatBrokerInterval = TimeSpan.FromMilliseconds(20)
            }),
            CreateRouteServiceMock().Object,
            remoting,
            TimeProvider.System,
            NullLogger<RemotingPushConsumer>.Instance,
            rebalanceService: rebalance);

        await consumer.StartAsync(cancellationToken);
        Assert.False(await rebalance.RebalanceAsync("legacy-group", cancellationToken));
        await remoting.WaitForTopicPullsAsync("orders", 2, cancellationToken);
        await consumer.StopAsync(cancellationToken);

        Assert.True(Volatile.Read(ref orderLockCalls) > 1);
    }

    [Fact]
    public async Task OrderlyConsumer_QueueMessagesWithoutMessageGroup_SerializesQueueMessages()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var firstStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var firstCancellationRequested = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseFirst = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var secondStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var delivered = 0;
        var body = CreateMessageRecord("orders", "first", null, 0, 1_000)
            .Concat(CreateMessageRecord("orders", "second", null, 1, 1_001))
            .ToArray();
        var remoting = new FakeRemotingClient("127.0.0.1@orderly-serial")
        {
            PullHandler = async (request, token) =>
            {
                var topic = Assert.IsType<string>(request.ExtFields["topic"]);
                if (topic == "orders" && Interlocked.Exchange(ref delivered, 1) == 0)
                {
                    return PullSuccess(body, 2);
                }

                await Task.Delay(Timeout.InfiniteTimeSpan, token);
                throw new InvalidOperationException("An infinite pull completed unexpectedly.");
            }
        };
        var options = WithMessageHandler(new RemotingPushConsumerOptions
        {
            GroupName = "legacy-group",
            ConsumeOrderly = true,
            InitialPosition = ConsumeFromPosition.Beginning,
            ConsumeTimeout = TimeSpan.FromMilliseconds(50),
            MaxConcurrency = 2,
            PullMaxCachedMessages = 8,
            LongPollingTimeout = TimeSpan.FromSeconds(1),
        }, async (messages, _, token) =>
            {
                var message = Assert.Single(messages);
                if (message.MessageId == "first")
                {
                    firstStarted.TrySetResult();
                    using var registration = token.Register(() => firstCancellationRequested.TrySetResult());
                    await releaseFirst.Task.WaitAsync(token);
                }
                else
                {
                    secondStarted.TrySetResult();
                }

                return ConsumeResult.Success;
            });
        options.Subscribe("orders");
        await using var consumer = CreateRemotingPushConsumer(
            options,
            CreateRouteServiceMock().Object,
            remoting,
            "orderly-serial");

        await consumer.StartAsync(cancellationToken);
        await firstStarted.Task.WaitAsync(TimeSpan.FromSeconds(3), cancellationToken);
        await Task.Delay(100, cancellationToken);
        Assert.False(firstCancellationRequested.Task.IsCompleted);
        Assert.False(secondStarted.Task.IsCompleted);

        releaseFirst.TrySetResult();
        await secondStarted.Task.WaitAsync(TimeSpan.FromSeconds(3), cancellationToken);
        await consumer.StopAsync(cancellationToken);

        Assert.Contains(
            remoting.UpdatedOffsets,
            static update => update.Topic == "orders" && update.Offset == 1);
        Assert.Contains(
            remoting.UpdatedOffsets,
            static update => update.Topic == "orders" && update.Offset == 2);
    }

    [Fact]
    public async Task OrderlyConsumer_CommitFailure_RepullsFromSameOffset()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var releaseFirstPull = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var orderPulls = 0;
        var body = CreateMessageRecord("orders", "orderly-commit-retry", null, 0, 1_000);
        var remoting = new FakeRemotingClient("127.0.0.1@orderly-commit-retry")
        {
            PullHandler = async (request, token) =>
            {
                if (Assert.IsType<string>(request.ExtFields["topic"]) == "orders" &&
                    Interlocked.Increment(ref orderPulls) == 1)
                {
                    await releaseFirstPull.Task.WaitAsync(token);
                    return PullSuccess(body, 1);
                }

                await Task.Delay(Timeout.InfiniteTimeSpan, token);
                throw new InvalidOperationException("An infinite pull completed unexpectedly.");
            }
        };
        var options = WithMessageHandler(new RemotingPushConsumerOptions
        {
            GroupName = "legacy-group",
            ConsumeOrderly = true,
            InitialPosition = ConsumeFromPosition.Beginning,
            MaxConcurrency = 1,
            PullMaxCachedMessages = 1,
            RetryDelay = TimeSpan.FromMilliseconds(10),
            LongPollingTimeout = TimeSpan.FromSeconds(1),
        }, static (_, _, _) => ValueTask.FromResult(ConsumeResult.Success));
        options.Subscribe("orders");
        await using var consumer = CreateRemotingPushConsumer(
            options,
            CreateRouteServiceMock().Object,
            remoting,
            "orderly-commit-retry");

        await consumer.StartAsync(cancellationToken);
        await remoting.WaitForTopicPullsAsync("orders", 1, cancellationToken);
        remoting.FailNextOffsetUpdate();
        releaseFirstPull.TrySetResult();
        await remoting.WaitForTopicPullsAsync("orders", 2, cancellationToken);

        Assert.Equal(
            [0L, 0L],
            remoting.PullOffsets
                .Where(static pull => pull.Topic == "orders")
                .Take(2)
                .Select(static pull => pull.Offset));
    }

    [Fact]
    public async Task OrderlyConsumer_BatchAcknowledgementContext_IgnoresBatchAcknowledgementContext()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var handled = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var delivered = 0;
        var body = CreateMessageRecord("orders", "order", null, 0, 1_000);
        var remoting = new FakeRemotingClient("127.0.0.1@orderly-context")
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
            ConsumeOrderly = true,
            InitialPosition = ConsumeFromPosition.Beginning,
            MaxConcurrency = 1,
            PullMaxCachedMessages = 1,
            MaxDeliveryAttempts = 1,
            RetryDelay = TimeSpan.FromMilliseconds(1),
            LongPollingTimeout = TimeSpan.FromSeconds(1),
        }, (messages, context, _) =>
            {
                Assert.Single(messages);
                context.AckIndex = -1;
                handled.TrySetResult();
                return ValueTask.FromResult(ConsumeResult.Success);
            });
        options.Subscribe("orders");
        await using var consumer = CreateRemotingPushConsumer(
            options,
            CreateRouteServiceMock().Object,
            remoting,
            "orderly-context");

        await consumer.StartAsync(cancellationToken);
        await handled.Task.WaitAsync(TimeSpan.FromSeconds(3), cancellationToken);
        while (!remoting.UpdatedOffsets.Any(static update => update.Topic == "orders" && update.Offset == 1))
        {
            await Task.Delay(10, cancellationToken);
        }

        await consumer.StopAsync(cancellationToken);

        Assert.DoesNotContain(remoting.Requests, static request => request.Code == RequestCode.ConsumerSendMsgBack);
    }

    [Fact]
    public async Task OrderlyConsumer_QueueLockAfterUnsubscribeDrainsHandler_ReleasesQueueLockAfterDrain()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var firstStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseFirst = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var delivered = 0;
        var body = CreateMessageRecord("orders", "first", null, 0, 1_000);
        var remoting = new FakeRemotingClient("127.0.0.1@orderly-unsubscribe")
        {
            PullHandler = async (request, token) =>
            {
                var topic = Assert.IsType<string>(request.ExtFields["topic"]);
                if (topic == "orders" && Interlocked.Exchange(ref delivered, 1) == 0)
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
            ConsumeOrderly = true,
            InitialPosition = ConsumeFromPosition.Beginning,
            MaxConcurrency = 1,
            PullMaxCachedMessages = 8,
            LongPollingTimeout = TimeSpan.FromSeconds(1),
        }, async (_, _, _) =>
            {
                firstStarted.TrySetResult();
                await releaseFirst.Task;
                return ConsumeResult.Success;
            });
        options.Subscribe("orders");
        await using var consumer = CreateRemotingPushConsumer(
            options,
            CreateRouteServiceMock().Object,
            remoting,
            "orderly-unsubscribe");

        await consumer.StartAsync(cancellationToken);
        await firstStarted.Task.WaitAsync(TimeSpan.FromSeconds(3), cancellationToken);
        var unsubscribe = consumer.UnsubscribeAsync("orders", cancellationToken);
        await Task.Delay(100, cancellationToken);
        Assert.False(unsubscribe.IsCompleted);
        Assert.DoesNotContain(
            remoting.OnewayRequests,
            static request => request.Code == RequestCode.UnlockBatchMq &&
                              RequestContainsQueueTopic(request, "orders"));

        releaseFirst.TrySetResult();
        await unsubscribe.WaitAsync(TimeSpan.FromSeconds(3), cancellationToken);
        await WaitForOnewayRequestAsync(
            remoting,
            static request => request.Code == RequestCode.UnlockBatchMq &&
                              RequestContainsQueueTopic(request, "orders"),
            cancellationToken);
    }
}
