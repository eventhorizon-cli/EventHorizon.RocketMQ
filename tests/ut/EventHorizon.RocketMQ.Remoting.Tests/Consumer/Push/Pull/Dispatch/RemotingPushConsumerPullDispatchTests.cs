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
using System.Diagnostics;
using EventHorizon.RocketMQ.Remoting.Consumer;
using EventHorizon.RocketMQ.Remoting.Consumer.Push;
using EventHorizon.RocketMQ.Remoting.Instrumentation;
using EventHorizon.RocketMQ.Remoting.Protocol;
using EventHorizon.RocketMQ.Remoting.Tests.Consumer.Push.Support;
using Moq;
using Xunit;

namespace EventHorizon.RocketMQ.Remoting.Tests.Consumer.Push.Pull.Dispatch;

public sealed class RemotingPushConsumerPullDispatchTests : RemotingPushConsumerTestSupport
{
    [Fact]
    public async Task BroadcastingConsumer_ConsumeTimeout_DoesNotApply()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var offsetPath = System.IO.Path.Combine(
            System.IO.Path.GetTempPath(),
            $"rocketmq-broadcast-timeout-{Guid.NewGuid():N}.json");
        var handlerStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var handlerCancellationRequested = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var handlerFinished = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseHandler = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var delivered = 0;
        var remoting = new FakeRemotingClient("127.0.0.1@broadcast-timeout")
        {
            PullHandler = async (request, token) =>
            {
                if (Assert.IsType<string>(request.ExtFields["topic"]) == "orders" &&
                    Interlocked.Exchange(ref delivered, 1) == 0)
                {
                    return PullSuccess(CreateMessageRecord("orders", "broadcast", null, 0, 1_000), 1);
                }

                await Task.Delay(Timeout.InfiniteTimeSpan, token);
                throw new InvalidOperationException("An infinite pull completed unexpectedly.");
            }
        };
        var options = CreateBroadcastOptions(
            offsetPath,
            async (_, _, token) =>
            {
                handlerStarted.TrySetResult();
                using var registration = token.Register(() => handlerCancellationRequested.TrySetResult());
                await releaseHandler.Task.WaitAsync(token);
                handlerFinished.TrySetResult();
                return ConsumeResult.Success;
            });
        options.ConsumeTimeout = TimeSpan.FromMilliseconds(50);
        options.MaxConcurrency = 1;
        options.PullMaxCachedMessages = 1;
        try
        {
            await using var consumer = CreateRemotingPushConsumer(
                options,
                CreateRouteServiceMock().Object,
                remoting,
                "broadcast-timeout");

            await consumer.StartAsync(cancellationToken);
            await handlerStarted.Task.WaitAsync(TimeSpan.FromSeconds(3), cancellationToken);
            await Task.Delay(100, cancellationToken);
            Assert.False(handlerCancellationRequested.Task.IsCompleted);

            releaseHandler.TrySetResult();
            await handlerFinished.Task.WaitAsync(TimeSpan.FromSeconds(3), cancellationToken);
            await consumer.StopAsync(cancellationToken);

            Assert.DoesNotContain(remoting.Requests, static request => request.Code == RequestCode.ConsumerSendMsgBack);
        }
        finally
        {
            File.Delete(offsetPath);
        }
    }

    [Theory]
    [InlineData(ConsumeResult.Retry)]
    [InlineData(ConsumeResult.DeadLetter)]
    public async Task UnsuccessfulHandlerResult_FailedProcessTelemetry_RecordsFailure(ConsumeResult result)
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var offsetPath = System.IO.Path.Combine(
            System.IO.Path.GetTempPath(),
            $"rocketmq-broadcast-telemetry-{Guid.NewGuid():N}.json");
        var receiveOperation = new Mock<IRemotingRocketMQTelemetryOperation>(MockBehavior.Strict);
        receiveOperation.SetupGet(value => value.Activity).Returns((Activity?)null);
        receiveOperation.Setup(value => value.Complete());
        receiveOperation.Setup(value => value.Dispose());
        var processCompleted = new TaskCompletionSource<(bool Success, string? Outcome)>(
            TaskCreationOptions.RunContinuationsAsynchronously);
        var processOperation = new Mock<IRemotingRocketMQTelemetryOperation>(MockBehavior.Strict);
        processOperation
            .Setup(value => value.Complete(It.IsAny<bool>(), It.IsAny<string?>()))
            .Callback<bool, string?>((success, outcome) => processCompleted.TrySetResult((success, outcome)));
        processOperation.Setup(value => value.Dispose());
        var telemetry = new Mock<IRemotingRocketMQTelemetry>(MockBehavior.Strict);
        telemetry
            .Setup(value => value.StartReceive(
                It.IsAny<string>(),
                It.IsAny<string>(),
                It.IsAny<int>(),
                It.IsAny<ActivityContext?>(),
                It.IsAny<DateTimeOffset>(),
                It.IsAny<long>(),
                It.IsAny<int?>(),
                It.IsAny<bool>(),
                It.IsAny<IEnumerable<IReadOnlyDictionary<string, string>>?>()))
            .Returns(receiveOperation.Object);
        telemetry
            .Setup(value => value.StartProcessBatch(
                It.IsAny<string>(),
                It.IsAny<string>(),
                It.Is<IReadOnlyList<RemotingMessageView>>(messages => messages.Count == 1)))
            .Returns(processOperation.Object);
        var delivered = 0;
        var remoting = new FakeRemotingClient("127.0.0.1@telemetry-test")
        {
            PullHandler = async (request, token) =>
            {
                if (Assert.IsType<string>(request.ExtFields["topic"]) == "orders" &&
                    Interlocked.Exchange(ref delivered, 1) == 0)
                {
                    return PullSuccess(CreateMessageRecord("orders", "telemetry-message", null, 0, 1_000), 1);
                }

                await Task.Delay(Timeout.InfiniteTimeSpan, token);
                throw new InvalidOperationException("An infinite pull completed unexpectedly.");
            }
        };
        var options = CreateBroadcastOptions(
            offsetPath,
            (_, _, _) => ValueTask.FromResult(result));
        try
        {
            await using var consumer = CreateRemotingPushConsumer(
                options,
                CreateRouteServiceMock().Object,
                remoting,
                "telemetry-test",
                telemetry.Object);
            await consumer.StartAsync(cancellationToken);
            var processOutcome = await processCompleted.Task.WaitAsync(TimeSpan.FromSeconds(3), cancellationToken);
            Assert.False(processOutcome.Success);
            Assert.Equal(result.ToString(), processOutcome.Outcome);
            await consumer.StopAsync(cancellationToken);
        }
        finally
        {
            File.Delete(offsetPath);
        }

        telemetry.VerifyAll();
        receiveOperation.VerifyAll();
        processOperation.VerifyAll();
    }

    [Fact]
    public async Task ConcurrentPushConsumer_MessagesInConfiguredBatches_DispatchesConfiguredBatches()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var completed = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var batches = new ConcurrentQueue<string[]>();
        var delivered = 0;
        var body = CreateMessageRecord("orders", "one", null, 0, 1_000)
            .Concat(CreateMessageRecord("orders", "two", null, 1, 1_001))
            .Concat(CreateMessageRecord("orders", "three", null, 2, 1_002))
            .Concat(CreateMessageRecord("orders", "four", null, 3, 1_003))
            .Concat(CreateMessageRecord("orders", "five", null, 4, 1_004))
            .ToArray();
        var remoting = new FakeRemotingClient("127.0.0.1@batch-dispatch")
        {
            PullHandler = async (request, token) =>
            {
                if (Assert.IsType<string>(request.ExtFields["topic"]) == "orders" &&
                    Interlocked.Exchange(ref delivered, 1) == 0)
                {
                    return PullSuccess(body, 5);
                }

                await Task.Delay(Timeout.InfiniteTimeSpan, token);
                throw new InvalidOperationException("An infinite pull completed unexpectedly.");
            }
        };
        var options = WithMessageHandler(new RemotingPushConsumerOptions
        {
            GroupName = "legacy-group",
            InitialPosition = ConsumeFromPosition.Beginning,
            PullBatchSize = 5,
            ConsumeMessageBatchSize = 2,
            MaxConcurrency = 1,
            PullMaxCachedMessages = 5,
            LongPollingTimeout = TimeSpan.FromSeconds(1),
        }, (messages, _, _) =>
            {
                batches.Enqueue(messages.Select(static message => message.MessageId).ToArray());
                if (batches.Count == 3)
                {
                    completed.TrySetResult();
                }

                return ValueTask.FromResult(ConsumeResult.Success);
            });
        options.Subscribe("orders");
        await using var consumer = CreateRemotingPushConsumer(
            options,
            CreateRouteServiceMock().Object,
            remoting,
            "batch-dispatch");

        await consumer.StartAsync(cancellationToken);
        await completed.Task.WaitAsync(TimeSpan.FromSeconds(3), cancellationToken);
        while (!remoting.UpdatedOffsets.Any(static update => update.Topic == "orders" && update.Offset == 5))
        {
            await Task.Delay(10, cancellationToken);
        }

        await consumer.StopAsync(cancellationToken);

        Assert.Collection(
            batches.ToArray(),
            batch => Assert.Equal(["one", "two"], batch),
            batch => Assert.Equal(["three", "four"], batch),
            batch => Assert.Equal(["five"], batch));
        var pull = remoting.Requests.First(request =>
            request.Code == RequestCode.PullMessage &&
            string.Equals(request.ExtFields["topic"] as string, "orders", StringComparison.Ordinal));
        Assert.Equal(5, Convert.ToInt32(pull.ExtFields["maxMsgNums"]));
    }

    [Fact]
    public async Task ConcurrentPushConsumer_BatchRetryResult_SendsBackEveryMessage()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var delivered = 0;
        var body = CreateMessageRecord("orders", "one", null, 0, 1_000)
            .Concat(CreateMessageRecord("orders", "two", null, 1, 1_001))
            .ToArray();
        var remoting = new FakeRemotingClient("127.0.0.1@batch-retry")
        {
            PullHandler = async (request, token) =>
            {
                if (Assert.IsType<string>(request.ExtFields["topic"]) == "orders" &&
                    Interlocked.Exchange(ref delivered, 1) == 0)
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
            InitialPosition = ConsumeFromPosition.Beginning,
            PullBatchSize = 2,
            ConsumeMessageBatchSize = 2,
            MaxConcurrency = 1,
            PullMaxCachedMessages = 2,
            LongPollingTimeout = TimeSpan.FromSeconds(1),
        }, static (_, context, _) =>
            {
                context.AckIndex = 0;
                return ValueTask.FromResult(ConsumeResult.Retry);
            });
        options.Subscribe("orders");
        await using var consumer = CreateRemotingPushConsumer(
            options,
            CreateRouteServiceMock().Object,
            remoting,
            "batch-retry");

        await consumer.StartAsync(cancellationToken);
        await remoting.WaitForRequestCountAsync(RequestCode.ConsumerSendMsgBack, 2, cancellationToken);
        await consumer.StopAsync(cancellationToken);

        Assert.Equal(
            2,
            remoting.Requests.Count(static request => request.Code == RequestCode.ConsumerSendMsgBack));
    }

    [Fact]
    public async Task ConcurrentPushConsumer_BatchPrefixAndRemainingMessages_AcknowledgesAndRetries()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var delivered = 0;
        var body = CreateMessageRecord("orders", "one", null, 0, 1_000)
            .Concat(CreateMessageRecord("orders", "two", null, 1, 1_001))
            .Concat(CreateMessageRecord("orders", "three", null, 2, 1_002))
            .ToArray();
        var remoting = new FakeRemotingClient("127.0.0.1@batch-partial-ack")
        {
            PullHandler = async (request, token) =>
            {
                if (Assert.IsType<string>(request.ExtFields["topic"]) == "orders" &&
                    Interlocked.Exchange(ref delivered, 1) == 0)
                {
                    return PullSuccess(body, 3);
                }

                await Task.Delay(Timeout.InfiniteTimeSpan, token);
                throw new InvalidOperationException("An infinite pull completed unexpectedly.");
            }
        };
        var options = WithMessageHandler(new RemotingPushConsumerOptions
        {
            GroupName = "legacy-group",
            InitialPosition = ConsumeFromPosition.Beginning,
            PullBatchSize = 3,
            ConsumeMessageBatchSize = 3,
            MaxConcurrency = 1,
            PullMaxCachedMessages = 3,
            LongPollingTimeout = TimeSpan.FromSeconds(1),
        }, (messages, context, _) =>
            {
                Assert.Equal(["one", "two", "three"], messages.Select(static message => message.MessageId));
                context.AckIndex = 0;
                context.DelayLevelWhenNextConsume = 7;
                return ValueTask.FromResult(ConsumeResult.Success);
            });
        options.Subscribe("orders");
        await using var consumer = CreateRemotingPushConsumer(
            options,
            CreateRouteServiceMock().Object,
            remoting,
            "batch-partial-ack");

        await consumer.StartAsync(cancellationToken);
        await remoting.WaitForRequestCountAsync(RequestCode.ConsumerSendMsgBack, 2, cancellationToken);
        while (!remoting.UpdatedOffsets.Any(static update => update.Topic == "orders" && update.Offset == 3))
        {
            await Task.Delay(10, cancellationToken);
        }

        await consumer.StopAsync(cancellationToken);

        var sendBackRequests = remoting.Requests
            .Where(static request => request.Code == RequestCode.ConsumerSendMsgBack)
            .ToArray();
        Assert.Equal(2, sendBackRequests.Length);
        Assert.Equal(
            [1_001L, 1_002L],
            sendBackRequests.Select(static request => Convert.ToInt64(request.ExtFields["offset"])).Order());
        Assert.All(
            sendBackRequests,
            static request => Assert.Equal(7, Convert.ToInt32(request.ExtFields["delayLevel"])));
    }

    [Fact]
    public async Task ConcurrentPushConsumer_PartialAcknowledgement_RecordsPartialSuccessTelemetry()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var processCompleted = new TaskCompletionSource<(bool Success, string? Outcome)>(
            TaskCreationOptions.RunContinuationsAsynchronously);
        var processOperation = new Mock<IRemotingRocketMQTelemetryOperation>(MockBehavior.Strict);
        processOperation
            .Setup(value => value.Complete(false, "partial_success"))
            .Callback(() => processCompleted.TrySetResult((false, "partial_success")));
        processOperation.Setup(value => value.Dispose());
        var telemetry = new ProcessTelemetry(processOperation.Object);
        var delivered = 0;
        var body = CreateMessageRecord("orders", "one", null, 0, 1_000)
            .Concat(CreateMessageRecord("orders", "two", null, 1, 1_001))
            .ToArray();
        var remoting = new FakeRemotingClient("127.0.0.1@batch-partial-telemetry")
        {
            PullHandler = async (request, token) =>
            {
                if (Assert.IsType<string>(request.ExtFields["topic"]) == "orders" &&
                    Interlocked.Exchange(ref delivered, 1) == 0)
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
            InitialPosition = ConsumeFromPosition.Beginning,
            PullBatchSize = 2,
            ConsumeMessageBatchSize = 2,
            MaxConcurrency = 1,
            PullMaxCachedMessages = 2,
            LongPollingTimeout = TimeSpan.FromSeconds(1),
        }, static (_, context, _) =>
            {
                context.AckIndex = 0;
                return ValueTask.FromResult(ConsumeResult.Success);
            });
        options.Subscribe("orders");
        await using var consumer = CreateRemotingPushConsumer(
            options,
            CreateRouteServiceMock().Object,
            remoting,
            "batch-partial-telemetry",
            telemetry);

        await consumer.StartAsync(cancellationToken);
        var outcome = await processCompleted.Task.WaitAsync(TimeSpan.FromSeconds(3), cancellationToken);
        await remoting.WaitForRequestCountAsync(RequestCode.ConsumerSendMsgBack, 1, cancellationToken);
        await consumer.StopAsync(cancellationToken);

        Assert.False(outcome.Success);
        Assert.Equal("partial_success", outcome.Outcome);
        Assert.Equal(1, telemetry.ProcessBatchCalls);
        processOperation.VerifyAll();
    }

    [Fact]
    public async Task ConcurrentPushConsumer_CacheMatchesBatchSize_AdmitsFullBatches()
    {
        const int queueCount = 4;
        const int messagesPerBatch = 2;
        var cancellationToken = TestContext.Current.CancellationToken;
        var readyPulls = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releasePulls = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var allBatchesHandled = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var deliveredQueues = new ConcurrentDictionary<int, byte>();
        var handledBatches = new ConcurrentQueue<string[]>();
        var waitingPulls = 0;
        var handledBatchCount = 0;
        var remoting = new FakeRemotingClient("127.0.0.1@batch-admission")
        {
            PullHandler = async (request, token) =>
            {
                if (!string.Equals(request.ExtFields["topic"] as string, "orders", StringComparison.Ordinal))
                {
                    await Task.Delay(Timeout.InfiniteTimeSpan, token);
                    throw new InvalidOperationException("An infinite pull completed unexpectedly.");
                }

                var queueId = Convert.ToInt32(request.ExtFields["queueId"]);
                if (!deliveredQueues.TryAdd(queueId, 0))
                {
                    await Task.Delay(Timeout.InfiniteTimeSpan, token);
                    throw new InvalidOperationException("An infinite pull completed unexpectedly.");
                }

                if (Interlocked.Increment(ref waitingPulls) == queueCount)
                {
                    readyPulls.TrySetResult();
                }

                await releasePulls.Task.WaitAsync(token);
                return PullSuccess(
                    CreateMessageRecord("orders", $"queue-{queueId}-first", null, 0, 1_000 + queueId)
                        .Concat(CreateMessageRecord("orders", $"queue-{queueId}-second", null, 1, 2_000 + queueId))
                        .ToArray(),
                    messagesPerBatch);
            }
        };
        var options = WithMessageHandler(new RemotingPushConsumerOptions
        {
            GroupName = "legacy-group",
            InitialPosition = ConsumeFromPosition.Beginning,
            PullBatchSize = messagesPerBatch,
            ConsumeMessageBatchSize = messagesPerBatch,
            MaxConcurrency = queueCount,
            PullMaxCachedMessages = messagesPerBatch,
            LongPollingTimeout = TimeSpan.FromSeconds(1),
        }, (messages, _, _) =>
            {
                Assert.Equal(messagesPerBatch, messages.Count);
                handledBatches.Enqueue(messages.Select(static message => message.MessageId).ToArray());
                if (Interlocked.Increment(ref handledBatchCount) == queueCount)
                {
                    allBatchesHandled.TrySetResult();
                }

                return ValueTask.FromResult(ConsumeResult.Success);
            });
        options.Subscribe("orders");
        await using var consumer = CreateRemotingPushConsumer(
            options,
            CreateRouteServiceMock(queueCount).Object,
            remoting,
            "batch-admission");

        await consumer.StartAsync(cancellationToken);
        await readyPulls.Task.WaitAsync(TimeSpan.FromSeconds(3), cancellationToken);
        releasePulls.TrySetResult();
        await allBatchesHandled.Task.WaitAsync(TimeSpan.FromSeconds(3), cancellationToken);
        await consumer.StopAsync(cancellationToken);

        Assert.Equal(queueCount, deliveredQueues.Count);
        Assert.Equal(
            Enumerable.Range(0, queueCount).Select(static queueId => $"queue-{queueId}-first").Order(),
            handledBatches.Select(static batch => batch[0]).Order());
        Assert.All(
            handledBatches,
            batch =>
            {
                var prefix = batch[0][..^"-first".Length];
                Assert.Equal(new[] { $"{prefix}-first", $"{prefix}-second" }, batch);
            });
    }

    [Fact]
    public async Task ConcurrentPushConsumer_PullCachedBodyBytesReachLimit_DelaysFurtherAdmission()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var firstHandlerStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseFirstHandler = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var thirdPullReturned = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var fourthPullStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var remoting = new FakeRemotingClient("127.0.0.1@byte-admission")
        {
            PullHandler = async (request, token) =>
            {
                if (!string.Equals(request.ExtFields["topic"] as string, "orders", StringComparison.Ordinal))
                {
                    return await WaitForCanceledPullAsync(token);
                }

                var offset = Convert.ToInt64(request.ExtFields["queueOffset"]);
                switch (offset)
                {
                    case 0:
                        return PullSuccess(CreateMessageRecord("orders", "first", null, 0, 1_000), 1);
                    case 1:
                        return PullSuccess(CreateMessageRecord("orders", "second", null, 1, 1_001), 2);
                    case 2:
                        thirdPullReturned.TrySetResult();
                        return PullSuccess(CreateMessageRecord("orders", "third", null, 2, 1_002), 3);
                    default:
                        fourthPullStarted.TrySetResult();
                        return await WaitForCanceledPullAsync(token);
                }
            }
        };
        var options = WithMessageHandler(new RemotingPushConsumerOptions
        {
            GroupName = "legacy-group",
            InitialPosition = ConsumeFromPosition.Beginning,
            PullBatchSize = 1,
            ConsumeMessageBatchSize = 1,
            MaxConcurrency = 1,
            PullMaxCachedMessages = 8,
            PullMaxCachedMessageBytes = 6,
            LongPollingTimeout = TimeSpan.FromSeconds(1),
        }, async (messages, _, token) =>
            {
                if (Assert.Single(messages).MessageId == "first")
                {
                    firstHandlerStarted.TrySetResult();
                    await releaseFirstHandler.Task.WaitAsync(token);
                }

                return ConsumeResult.Success;
            });
        options.Subscribe("orders");
        await using var consumer = CreateRemotingPushConsumer(
            options,
            CreateRouteServiceMock().Object,
            remoting,
            "byte-admission");

        try
        {
            await consumer.StartAsync(cancellationToken);
            await firstHandlerStarted.Task.WaitAsync(TimeSpan.FromSeconds(3), cancellationToken);
            await thirdPullReturned.Task.WaitAsync(TimeSpan.FromSeconds(3), cancellationToken);
            await Task.Delay(100, cancellationToken);
            Assert.False(fourthPullStarted.Task.IsCompleted);

            releaseFirstHandler.TrySetResult();
            await fourthPullStarted.Task.WaitAsync(TimeSpan.FromSeconds(3), cancellationToken);
        }
        finally
        {
            releaseFirstHandler.TrySetResult();
            await consumer.StopAsync(CancellationToken.None);
        }
    }

    [Fact]
    public async Task ConcurrentPushConsumer_TimedOutHandlerAndWithQueuedMessages_CancelsTimedOutHandlerAndContinues()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var firstHandlerCanceled = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var secondHandled = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var delivered = 0;
        var body = CreateMessageRecord("orders", "first", null, 0, 1_000)
            .Concat(CreateMessageRecord("orders", "second", null, 1, 1_001))
            .ToArray();
        var remoting = new FakeRemotingClient("127.0.0.1@consume-timeout")
        {
            PullHandler = async (request, token) =>
            {
                if (Assert.IsType<string>(request.ExtFields["topic"]) == "orders" &&
                    Interlocked.Exchange(ref delivered, 1) == 0)
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
            InitialPosition = ConsumeFromPosition.Beginning,
            PullBatchSize = 2,
            ConsumeMessageBatchSize = 1,
            ConsumeTimeout = TimeSpan.FromMilliseconds(50),
            MaxConcurrency = 1,
            PullMaxCachedMessages = 2,
            LongPollingTimeout = TimeSpan.FromSeconds(1),
        }, async (messages, _, token) =>
            {
                var message = Assert.Single(messages);
                if (message.MessageId == "first")
                {
                    using var failingRegistration = token.Register(
                        static () => throw new InvalidOperationException("The test cancellation callback failed."));
                    try
                    {
                        await Task.Delay(Timeout.InfiniteTimeSpan, token);
                    }
                    catch (OperationCanceledException) when (token.IsCancellationRequested)
                    {
                        firstHandlerCanceled.TrySetResult();
                        throw;
                    }
                }

                secondHandled.TrySetResult();
                return ConsumeResult.Success;
            });
        options.Subscribe("orders");
        await using var consumer = CreateRemotingPushConsumer(
            options,
            CreateRouteServiceMock().Object,
            remoting,
            "consume-timeout");

        await consumer.StartAsync(cancellationToken);
        await firstHandlerCanceled.Task.WaitAsync(TimeSpan.FromSeconds(3), cancellationToken);
        await secondHandled.Task.WaitAsync(TimeSpan.FromSeconds(3), cancellationToken);
        await remoting.WaitForRequestCountAsync(RequestCode.ConsumerSendMsgBack, 1, cancellationToken);
        while (!remoting.UpdatedOffsets.Any(static update => update.Topic == "orders" && update.Offset == 2))
        {
            await Task.Delay(10, cancellationToken);
        }

        await consumer.StopAsync(cancellationToken);

        Assert.Single(remoting.Requests, static request => request.Code == RequestCode.ConsumerSendMsgBack);
    }

    [Fact]
    public async Task ConcurrentPushConsumer_NonCooperativeTimedOutHandler_RetriesAndContinues()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var handlerStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var handlerFinished = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseHandler = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var processCompleted = new TaskCompletionSource<(bool Success, string? Outcome)>(
            TaskCreationOptions.RunContinuationsAsynchronously);
        var processOperation = new Mock<IRemotingRocketMQTelemetryOperation>(MockBehavior.Strict);
        processOperation
            .Setup(value => value.Complete(false, "timeout"))
            .Callback(() => processCompleted.TrySetResult((false, "timeout")));
        processOperation.Setup(value => value.Dispose());
        var telemetry = new ProcessTelemetry(processOperation.Object);
        var delivered = 0;
        var body = CreateMessageRecord("orders", "stuck", null, 0, 1_000);
        var remoting = new FakeRemotingClient("127.0.0.1@consume-timeout-uncooperative")
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
            PullBatchSize = 1,
            ConsumeTimeout = TimeSpan.FromMilliseconds(50),
            MaxConcurrency = 1,
            PullMaxCachedMessages = 1,
            LongPollingTimeout = TimeSpan.FromSeconds(1),
        }, async (_, _, _) =>
            {
                handlerStarted.TrySetResult();
                await releaseHandler.Task;
                handlerFinished.TrySetResult();
                return ConsumeResult.Success;
            });
        options.Subscribe("orders");
        await using var consumer = CreateRemotingPushConsumer(
            options,
            CreateRouteServiceMock().Object,
            remoting,
            "consume-timeout-uncooperative",
            telemetry);

        await consumer.StartAsync(cancellationToken);
        await handlerStarted.Task.WaitAsync(TimeSpan.FromSeconds(3), cancellationToken);
        var processOutcome = await processCompleted.Task.WaitAsync(TimeSpan.FromSeconds(3), cancellationToken);
        Assert.False(processOutcome.Success);
        Assert.Equal("timeout", processOutcome.Outcome);
        await remoting.WaitForRequestCountAsync(RequestCode.ConsumerSendMsgBack, 1, cancellationToken);
        while (!remoting.UpdatedOffsets.Any(static update => update.Topic == "orders" && update.Offset == 1))
        {
            await Task.Delay(10, cancellationToken);
        }

        await consumer.StopAsync(cancellationToken).AsTask().WaitAsync(TimeSpan.FromSeconds(3), cancellationToken);
        releaseHandler.TrySetResult();
        await handlerFinished.Task.WaitAsync(TimeSpan.FromSeconds(3), cancellationToken);

        Assert.Single(remoting.Requests, static request => request.Code == RequestCode.ConsumerSendMsgBack);
        Assert.Equal(1, telemetry.ProcessBatchCalls);
        processOperation.VerifyAll();
    }

    [Fact]
    public async Task ConcurrentPushConsumer_EveryMessageInTimedOutBatch_RetriesEveryTimedOutMessage()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var handlerStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var handlerFinished = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseHandler = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var delivered = 0;
        var body = CreateMessageRecord("orders", "first", null, 0, 1_000)
            .Concat(CreateMessageRecord("orders", "second", null, 1, 1_001))
            .ToArray();
        var remoting = new FakeRemotingClient("127.0.0.1@consume-timeout-batch")
        {
            PullHandler = async (request, token) =>
            {
                if (Assert.IsType<string>(request.ExtFields["topic"]) == "orders" &&
                    Interlocked.Exchange(ref delivered, 1) == 0)
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
            InitialPosition = ConsumeFromPosition.Beginning,
            PullBatchSize = 2,
            ConsumeMessageBatchSize = 2,
            ConsumeTimeout = TimeSpan.FromMilliseconds(50),
            RetryDelay = TimeSpan.FromSeconds(5),
            MaxConcurrency = 1,
            PullMaxCachedMessages = 2,
            LongPollingTimeout = TimeSpan.FromSeconds(1),
        }, async (messages, context, _) =>
            {
                Assert.Equal(2, messages.Count);
                context.DelayLevelWhenNextConsume = -1;
                handlerStarted.TrySetResult();
                await releaseHandler.Task;
                handlerFinished.TrySetResult();
                return ConsumeResult.Success;
            });
        options.Subscribe("orders");
        await using var consumer = CreateRemotingPushConsumer(
            options,
            CreateRouteServiceMock().Object,
            remoting,
            "consume-timeout-batch");

        await consumer.StartAsync(cancellationToken);
        await handlerStarted.Task.WaitAsync(TimeSpan.FromSeconds(3), cancellationToken);
        await remoting.WaitForRequestCountAsync(RequestCode.ConsumerSendMsgBack, 2, cancellationToken);
        while (!remoting.UpdatedOffsets.Any(static update => update.Topic == "orders" && update.Offset == 2))
        {
            await Task.Delay(10, cancellationToken);
        }

        await consumer.StopAsync(cancellationToken).AsTask().WaitAsync(TimeSpan.FromSeconds(3), cancellationToken);
        releaseHandler.TrySetResult();
        await handlerFinished.Task.WaitAsync(TimeSpan.FromSeconds(3), cancellationToken);

        Assert.Equal(
            2,
            remoting.Requests.Count(static request => request.Code == RequestCode.ConsumerSendMsgBack));
        Assert.All(
            remoting.Requests.Where(static request => request.Code == RequestCode.ConsumerSendMsgBack),
            static request => Assert.Equal(0, Convert.ToInt32(request.ExtFields["delayLevel"])));
    }

    [Fact]
    public async Task ConcurrentPushConsumer_IncompleteMessage_PullsAheadWithoutCommitting()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var releaseFirst = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var secondCompleted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var handlerCalls = new ConcurrentDictionary<string, int>(StringComparer.Ordinal);
        var remoting = new FakeRemotingClient("127.0.0.1@pull-ahead-watermark")
        {
            PullHandler = async (request, token) =>
            {
                if (Assert.IsType<string>(request.ExtFields["topic"]) == "orders")
                {
                    return Convert.ToInt64(request.ExtFields["queueOffset"]) switch
                    {
                        0 => PullSuccess(CreateMessageRecord("orders", "first", null, 0, 1_000), 1),
                        1 => PullSuccess(CreateMessageRecord("orders", "second", null, 1, 1_001), 2),
                        _ => await WaitForCanceledPullAsync(token)
                    };
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
                var messageId = Assert.Single(messages).MessageId;
                handlerCalls.AddOrUpdate(messageId, 1, static (_, count) => count + 1);
                if (messageId == "first")
                {
                    await releaseFirst.Task.WaitAsync(token);
                }
                else
                {
                    secondCompleted.TrySetResult();
                }

                return ConsumeResult.Success;
            });
        options.Subscribe("orders");
        await using var consumer = CreateRemotingPushConsumer(
            options,
            CreateRouteServiceMock().Object,
            remoting,
            "pull-ahead-watermark");

        await consumer.StartAsync(cancellationToken);
        await secondCompleted.Task.WaitAsync(TimeSpan.FromSeconds(3), cancellationToken);
        Assert.Contains(
            remoting.PullOffsets,
            static pull => pull.Topic == "orders" && pull.Offset == 1);
        Assert.DoesNotContain(
            remoting.UpdatedOffsets,
            static update => update.Topic == "orders" && update.Offset == 2);

        releaseFirst.TrySetResult();
        while (!remoting.UpdatedOffsets.Any(static update => update.Topic == "orders" && update.Offset == 2))
        {
            await Task.Delay(10, cancellationToken);
        }

        Assert.Equal(1, handlerCalls["first"]);
        Assert.Equal(1, handlerCalls["second"]);
    }

    [Fact]
    public async Task ConcurrentPushConsumer_AllBatchesBeforeCommit_Waits()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var allBatchesStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseBatches = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var delivered = 0;
        var handlerCalls = 0;
        var body = CreateMessageRecord("orders", "one", null, 0, 1_000)
            .Concat(CreateMessageRecord("orders", "two", null, 1, 1_001))
            .Concat(CreateMessageRecord("orders", "three", null, 2, 1_002))
            .Concat(CreateMessageRecord("orders", "four", null, 3, 1_003))
            .Concat(CreateMessageRecord("orders", "five", null, 4, 1_004))
            .ToArray();
        var remoting = new FakeRemotingClient("127.0.0.1@batch-completion")
        {
            PullHandler = async (request, token) =>
            {
                if (Assert.IsType<string>(request.ExtFields["topic"]) == "orders" &&
                    Interlocked.Exchange(ref delivered, 1) == 0)
                {
                    return PullSuccess(body, 5);
                }

                await Task.Delay(Timeout.InfiniteTimeSpan, token);
                throw new InvalidOperationException("An infinite pull completed unexpectedly.");
            }
        };
        var options = WithMessageHandler(new RemotingPushConsumerOptions
        {
            GroupName = "legacy-group",
            InitialPosition = ConsumeFromPosition.Beginning,
            PullBatchSize = 5,
            ConsumeMessageBatchSize = 2,
            MaxConcurrency = 3,
            PullMaxCachedMessages = 2,
            LongPollingTimeout = TimeSpan.FromSeconds(1),
        }, async (_, _, token) =>
            {
                if (Interlocked.Increment(ref handlerCalls) == 3)
                {
                    allBatchesStarted.TrySetResult();
                }

                await releaseBatches.Task.WaitAsync(token);
                return ConsumeResult.Success;
            });
        options.Subscribe("orders");
        await using var consumer = CreateRemotingPushConsumer(
            options,
            CreateRouteServiceMock().Object,
            remoting,
            "batch-completion");

        await consumer.StartAsync(cancellationToken);
        await allBatchesStarted.Task.WaitAsync(TimeSpan.FromSeconds(3), cancellationToken);
        Assert.DoesNotContain(
            remoting.UpdatedOffsets,
            static update => update.Topic == "orders" && update.Offset == 5);

        releaseBatches.TrySetResult();
        while (!remoting.UpdatedOffsets.Any(static update => update.Topic == "orders" && update.Offset == 5))
        {
            await Task.Delay(10, cancellationToken);
        }

        await consumer.StopAsync(cancellationToken);

        Assert.Equal(3, handlerCalls);
    }
}
