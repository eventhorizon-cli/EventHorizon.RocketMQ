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
using System.Text;
using System.Text.Json;
using EventHorizon.RocketMQ.Remoting.Consumer;
using EventHorizon.RocketMQ.Remoting.Consumer.Coordination.Rebalance;
using EventHorizon.RocketMQ.Remoting.Consumer.LitePull;
using EventHorizon.RocketMQ.Remoting.Consumer.Pull;
using EventHorizon.RocketMQ.Remoting.Consumer.Route;
using EventHorizon.RocketMQ.Remoting.Protocol;
using EventHorizon.RocketMQ.Remoting.Tests.Consumer.Coordination.Rebalance;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Moq;
using Xunit;

namespace EventHorizon.RocketMQ.Remoting.Tests.Consumer.LitePull;

public sealed class RemotingLitePullConsumerTests
{
    private const string ClientId = "127.0.0.1@lite-client";

    [Fact]
    public void RemotingConsumerQueue_EquivalentLogicalValues_UsesValueEquality()
    {
        var first = new RemotingConsumerQueue("orders", "broker-a", 1);
        var equivalent = new RemotingConsumerQueue("orders", "broker-a", 1);
        var otherQueue = new RemotingConsumerQueue("orders", "broker-a", 2);

        Assert.Equal(first, equivalent);
        Assert.True(first == equivalent);
        Assert.Equal(first.GetHashCode(), equivalent.GetHashCode());
        Assert.NotEqual(first, otherQueue);
        Assert.Single(new HashSet<RemotingConsumerQueue> { first, equivalent });
    }

    [Fact]
    public async Task StartAsync_ActiveConsumer_HeartbeatsAndAllocatesQueues()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var queues = new[]
        {
            ResolvedQueue("orders", 0),
            ResolvedQueue("orders", 1)
        };
        var heartbeats = new ConcurrentQueue<byte[]>();
        var engine = new ScriptedConsumerEngine
        {
            GetMessageQueuesHandler = (_, _) => Task.FromResult<IReadOnlyList<RemotingConsumerQueue>>(queues)
        };
        var remoting = CreateRemotingClient(request => request.Code switch
        {
            RequestCode.HeartBeat => CaptureHeartbeat(request, heartbeats),
            RequestCode.GetConsumerListByGroup => ConsumerListResponse(ClientId, "peer-client"),
            RequestCode.UnregisterClient => SuccessResponse(),
            _ => throw new InvalidOperationException($"Unexpected request code {request.Code}.")
        });
        var options = NewOptions();
        options.Subscribe("orders");

        await using (var consumer = CreateConsumer(options, engine, remoting.Object))
        {
            await consumer.StartAsync(cancellationToken);

            var assigned = Assert.Single(consumer.Assignment);
            Assert.Equal(0, assigned.QueueId);
            var exception = await Assert.ThrowsAsync<InvalidOperationException>(() =>
                consumer.AssignAsync([queues[0]], cancellationToken: cancellationToken));
            Assert.Contains("subscriptions", exception.Message, StringComparison.OrdinalIgnoreCase);
        }

        var heartbeat = Assert.Single(heartbeats);
        using var document = JsonDocument.Parse(heartbeat);
        var consumerData = Assert.Single(document.RootElement.GetProperty("consumerDataSet").EnumerateArray());
        Assert.Equal("CONSUME_ACTIVELY", consumerData.GetProperty("consumeType").GetString());
        Assert.Equal("CLUSTERING", consumerData.GetProperty("messageModel").GetString());
        Assert.Equal("CONSUME_FROM_LAST_OFFSET", consumerData.GetProperty("consumeFromWhere").GetString());
        Assert.Equal(1, engine.StopCount);
        Assert.Equal(1, engine.DisposeCount);
    }

    [Fact]
    public async Task BackgroundReceivers_EmptyLongPollIsInFlight_DoesNotBlockReadyQueue()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var emptyQueue = Queue(0);
        var readyQueue = Queue(1);
        var emptyPullStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var readyReturned = 0;
        var engine = new ScriptedConsumerEngine
        {
            PullHandler = async (queue, offset, _, token) =>
            {
                if (queue == emptyQueue)
                {
                    emptyPullStarted.TrySetResult();
                    return await WaitForPullCancellationAsync(offset, token).ConfigureAwait(false);
                }

                await emptyPullStarted.Task.WaitAsync(token).ConfigureAwait(false);
                if (Interlocked.Exchange(ref readyReturned, 1) != 0)
                {
                    return await WaitForPullCancellationAsync(offset, token).ConfigureAwait(false);
                }

                return Found(Message(readyQueue, offset, "ready"), offset + 1);
            }
        };

        await using var consumer = CreateConsumer(NewOptions(), engine);
        await consumer.StartAsync(cancellationToken);
        await consumer.AssignAsync([emptyQueue, readyQueue], cancellationToken: cancellationToken);
        await emptyPullStarted.Task.WaitAsync(TimeSpan.FromSeconds(1), cancellationToken);

        var messages = await consumer.PollAsync(TimeSpan.FromMilliseconds(500), cancellationToken);

        Assert.Equal("ready", Assert.Single(messages).MessageId);
        Assert.True(engine.CountPulls(emptyQueue) >= 1);
        Assert.True(engine.CountPulls(readyQueue) >= 1);
    }

    [Fact]
    public async Task PollAsync_TimeoutIsZero_DrainsWithoutWaiting()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var engine = new ScriptedConsumerEngine();
        await using var consumer = CreateConsumer(NewOptions(), engine);
        await consumer.StartAsync(cancellationToken);
        await consumer.AssignAsync([Queue()], cancellationToken: cancellationToken);

        var started = Stopwatch.GetTimestamp();
        var messages = await consumer.PollAsync(TimeSpan.Zero, cancellationToken);

        Assert.Empty(messages);
        Assert.True(
            Stopwatch.GetElapsedTime(started) < TimeSpan.FromSeconds(1),
            "A zero-timeout poll must be a non-blocking local-buffer drain.");
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() =>
            consumer.PollAsync(TimeSpan.FromMilliseconds(-1), cancellationToken));
    }

    [Fact]
    public async Task PollAsync_LocalTimeoutExpires_ReturnsEmptyWithoutCancellingBackgroundReceive()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var receiveCancelled = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var engine = new ScriptedConsumerEngine
        {
            PullHandler = async (_, offset, _, token) =>
            {
                try
                {
                    return await WaitForPullCancellationAsync(offset, token).ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                    receiveCancelled.TrySetResult();
                    throw;
                }
            }
        };

        await using var consumer = CreateConsumer(NewOptions(), engine);
        await consumer.StartAsync(cancellationToken);
        await consumer.AssignAsync([Queue()], cancellationToken: cancellationToken);

        var messages = await consumer.PollAsync(TimeSpan.FromMilliseconds(50), cancellationToken);
        await Task.Delay(TimeSpan.FromMilliseconds(50), cancellationToken);

        Assert.Empty(messages);
        Assert.False(
            receiveCancelled.Task.IsCompleted,
            "A local poll timeout must not cancel the per-assignment Broker receive.");
    }

    [Fact]
    public async Task BackgroundReceive_MessageCapacityIsReached_StopsFetching()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var queue = Queue();
        var options = NewOptions();
        options.PullBatchSize = 1;
        options.MaxCachedMessages = 2;
        options.MaxCachedMessageBytes = 1024;
        var engine = new ScriptedConsumerEngine
        {
            PullHandler = (value, offset, _, token) => offset < 2
                ? Task.FromResult(Found(Message(value, offset), offset + 1))
                : WaitForPullCancellationAsync(offset, token)
        };

        await using var consumer = CreateConsumer(options, engine);
        await consumer.StartAsync(cancellationToken);
        await consumer.AssignAsync([queue], cancellationToken: cancellationToken);
        await WaitUntilAsync(() => engine.CountPulls(queue) >= 2, "two background pulls", cancellationToken);
        await Task.Delay(TimeSpan.FromMilliseconds(100), cancellationToken);

        Assert.Equal(2, engine.CountPulls(queue));
    }

    [Fact]
    public async Task BackgroundReceive_BodyCapacityIsReached_StopsFetching()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var queue = Queue();
        var options = NewOptions();
        options.PullBatchSize = 1;
        options.MaxCachedMessages = 10;
        options.MaxCachedMessageBytes = 4;
        var engine = new ScriptedConsumerEngine
        {
            PullHandler = (value, offset, _, token) => offset == 0
                ? Task.FromResult(Found(Message(value, offset, bodySize: 4), 1))
                : WaitForPullCancellationAsync(offset, token)
        };

        await using var consumer = CreateConsumer(options, engine);
        await consumer.StartAsync(cancellationToken);
        await consumer.AssignAsync([queue], cancellationToken: cancellationToken);
        await WaitUntilAsync(() => engine.CountPulls(queue) >= 1, "the first background pull", cancellationToken);
        await Task.Delay(TimeSpan.FromMilliseconds(100), cancellationToken);

        Assert.Equal(1, engine.CountPulls(queue));
    }

    [Fact]
    public async Task BackgroundReceive_MultipleQueuesShareMessageCapacity_RespectsAggregateLimit()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var firstQueue = Queue(0);
        var secondQueue = Queue(1);
        var firstPullStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var secondPullStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var options = NewOptions();
        options.PullBatchSize = 2;
        options.MaxCachedMessages = 2;
        options.MaxCachedMessageBytes = 1024;
        var engine = new ScriptedConsumerEngine
        {
            PullHandler = (queue, offset, _, token) =>
            {
                if (offset != 0)
                {
                    return WaitForPullCancellationAsync(offset, token);
                }

                if (queue == firstQueue)
                {
                    firstPullStarted.TrySetResult();
                    return Task.FromResult(Found(Message(firstQueue, 0, "first"), 1));
                }

                secondPullStarted.TrySetResult();
                return Task.FromResult(new RemotingPullResult(
                    [
                        Message(secondQueue, 0, "second-first"),
                        Message(secondQueue, 1, "second-pending")
                    ],
                    2,
                    RemotingPullStatus.Found));
            }
        };

        await using var consumer = CreateConsumer(options, engine);
        await consumer.StartAsync(cancellationToken);
        await consumer.AssignAsync([firstQueue, secondQueue], cancellationToken: cancellationToken);
        await Task.WhenAll(firstPullStarted.Task, secondPullStarted.Task)
            .WaitAsync(TimeSpan.FromSeconds(1), cancellationToken);

        var batches = await PollUntilMessagesAsync(consumer, expectedMessageCount: 3, cancellationToken);

        Assert.All(batches, batch => Assert.InRange(batch.Count, 1, options.MaxCachedMessages));
        Assert.Equal(
            ["first", "second-first", "second-pending"],
            batches.SelectMany(static batch => batch).Select(static message => message.MessageId).Order());
    }

    [Fact]
    public async Task BackgroundReceive_MultipleQueuesShareBodyCapacity_RespectsAggregateLimit()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var firstQueue = Queue(0);
        var secondQueue = Queue(1);
        var firstPullStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var secondPullStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var options = NewOptions();
        options.PullBatchSize = 1;
        options.MaxCachedMessages = 10;
        options.MaxCachedMessageBytes = 4;
        var engine = new ScriptedConsumerEngine
        {
            PullHandler = (queue, offset, _, token) =>
            {
                if (offset != 0)
                {
                    return WaitForPullCancellationAsync(offset, token);
                }

                if (queue == firstQueue)
                {
                    firstPullStarted.TrySetResult();
                }
                else
                {
                    secondPullStarted.TrySetResult();
                }

                return Task.FromResult(Found(Message(queue, 0, queue == firstQueue ? "first" : "second", 3), 1));
            }
        };

        await using var consumer = CreateConsumer(options, engine);
        await consumer.StartAsync(cancellationToken);
        await consumer.AssignAsync([firstQueue, secondQueue], cancellationToken: cancellationToken);
        await Task.WhenAll(firstPullStarted.Task, secondPullStarted.Task)
            .WaitAsync(TimeSpan.FromSeconds(1), cancellationToken);

        var batches = await PollUntilMessagesAsync(consumer, expectedMessageCount: 2, cancellationToken);

        Assert.All(
            batches,
            batch => Assert.True(
                batch.Sum(static message => message.Body.Length) <= options.MaxCachedMessageBytes,
                "A normal batch must not exceed the shared cache byte limit."));
        Assert.Equal(
            ["first", "second"],
            batches.SelectMany(static batch => batch).Select(static message => message.MessageId).Order());
    }

    [Fact]
    public async Task BackgroundReceive_OversizedSingleMessageWhenBufferIsEmpty_IsAdmitted()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var queue = Queue();
        var pullStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var options = NewOptions();
        options.PullBatchSize = 1;
        options.MaxCachedMessages = 2;
        options.MaxCachedMessageBytes = 4;
        var engine = new ScriptedConsumerEngine
        {
            PullHandler = (value, offset, _, token) =>
            {
                if (offset != 0)
                {
                    return WaitForPullCancellationAsync(offset, token);
                }

                pullStarted.TrySetResult();
                return Task.FromResult(Found(Message(value, 0, "oversized", bodySize: 8), 1));
            }
        };

        await using var consumer = CreateConsumer(options, engine);
        await consumer.StartAsync(cancellationToken);
        await consumer.AssignAsync([queue], cancellationToken: cancellationToken);
        await pullStarted.Task.WaitAsync(TimeSpan.FromSeconds(1), cancellationToken);

        var messages = await consumer.PollAsync(TimeSpan.FromSeconds(1), cancellationToken);

        var message = Assert.Single(messages);
        Assert.Equal("oversized", message.MessageId);
        Assert.Equal(8, message.Body.Length);
    }

    [Fact]
    public async Task CommitAsync_MessagesArePrefetched_PersistsDeliveredPosition()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var queue = Queue();
        var engine = new ScriptedConsumerEngine
        {
            PullHandler = (value, offset, _, token) => offset == 0
                ? Task.FromResult(Found(Message(value, offset), 1))
                : WaitForPullCancellationAsync(offset, token)
        };

        await using var consumer = CreateConsumer(NewOptions(), engine);
        await consumer.StartAsync(cancellationToken);
        await consumer.AssignAsync([queue], cancellationToken: cancellationToken);
        await WaitUntilAsync(
            () => engine.CountPulls(queue, 1) >= 1,
            "a second fetch after the first message was buffered",
            cancellationToken);

        Assert.Equal(0, consumer.Position(queue));
        await consumer.CommitAsync(queue, cancellationToken);
        Assert.Equal(0, engine.OffsetUpdates.Last().Offset);

        var message = Assert.Single(await consumer.PollAsync(TimeSpan.Zero, cancellationToken));
        Assert.Equal(0, message.QueueOffset);
        Assert.Equal(1, consumer.Position(queue));
        await consumer.CommitAsync(queue, cancellationToken);

        Assert.Equal(new long[] { 0, 1 }, engine.OffsetUpdates.Select(static update => update.Offset).ToArray());
    }

    [Fact]
    public async Task StopAsync_AutoCommitEnabled_CommitsFinalDeliveredPositionBeforeUnregister()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var queue = Queue();
        var events = new ConcurrentQueue<string>();
        var options = NewOptions();
        options.EnableAutoCommit = true;
        options.AutoCommitInterval = TimeSpan.FromDays(1);
        var engine = new ScriptedConsumerEngine
        {
            PullHandler = (value, offset, _, token) => offset == 0
                ? Task.FromResult(Found(Message(value, offset), 1))
                : WaitForPullCancellationAsync(offset, token),
            UpdateOffsetHandler = (_, offset, _) =>
            {
                events.Enqueue($"commit:{offset}");
                return Task.CompletedTask;
            }
        };
        var remoting = CreateRemotingClient(request =>
        {
            if (request.Code == RequestCode.UnregisterClient)
            {
                events.Enqueue("unregister");
            }

            return DefaultMaintenanceResponse(request);
        });

        await using var consumer = CreateConsumer(options, engine, remoting.Object);
        await consumer.StartAsync(cancellationToken);
        await consumer.AssignAsync([queue], cancellationToken: cancellationToken);
        Assert.Single(await consumer.PollAsync(TimeSpan.FromSeconds(1), cancellationToken));

        await consumer.StopAsync(cancellationToken);

        Assert.Equal(["commit:1", "unregister"], events);
        Assert.Equal(1, engine.StopCount);
    }

    [Fact]
    public async Task StopAsync_ManualCommitMode_DoesNotCommitDeliveredPosition()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var queue = Queue();
        var engine = new ScriptedConsumerEngine
        {
            PullHandler = (value, offset, _, token) => offset == 0
                ? Task.FromResult(Found(Message(value, offset), 1))
                : WaitForPullCancellationAsync(offset, token)
        };

        await using var consumer = CreateConsumer(NewOptions(), engine);
        await consumer.StartAsync(cancellationToken);
        await consumer.AssignAsync([queue], cancellationToken: cancellationToken);
        Assert.Single(await consumer.PollAsync(TimeSpan.FromSeconds(1), cancellationToken));

        await consumer.StopAsync(cancellationToken);

        Assert.Empty(engine.OffsetUpdates);
    }

    [Fact]
    public async Task StopAsync_FinalAutoCommitFails_StillUnregistersAndStopsEngine()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var queue = Queue();
        var unregisterCount = 0;
        var options = NewOptions();
        options.EnableAutoCommit = true;
        options.AutoCommitInterval = TimeSpan.FromDays(1);
        var engine = new ScriptedConsumerEngine
        {
            PullHandler = (value, offset, _, token) => offset == 0
                ? Task.FromResult(Found(Message(value, offset), 1))
                : WaitForPullCancellationAsync(offset, token),
            UpdateOffsetHandler = static (_, _, _) =>
                Task.FromException(new InvalidOperationException("commit failed"))
        };
        var remoting = CreateRemotingClient(request =>
        {
            if (request.Code == RequestCode.UnregisterClient)
            {
                Interlocked.Increment(ref unregisterCount);
            }

            return DefaultMaintenanceResponse(request);
        });

        await using var consumer = CreateConsumer(options, engine, remoting.Object);
        await consumer.StartAsync(cancellationToken);
        await consumer.AssignAsync([queue], cancellationToken: cancellationToken);
        Assert.Single(await consumer.PollAsync(TimeSpan.FromSeconds(1), cancellationToken));

        await consumer.StopAsync(cancellationToken);

        Assert.Single(engine.OffsetUpdates);
        Assert.Equal(1, unregisterCount);
        Assert.Equal(1, engine.StopCount);
    }

    [Fact]
    public async Task GetCommittedOffsetAsync_StopBegins_CancelsQueryBeforeEngineStops()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var queue = Queue();
        var queryStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var queryCanceled = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseQuery = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var getOffsetCalls = 0;
        var engine = new ScriptedConsumerEngine
        {
            GetOffsetHandler = async (_, token) =>
            {
                if (Interlocked.Increment(ref getOffsetCalls) == 1)
                {
                    return 0;
                }

                queryStarted.TrySetResult();
                try
                {
                    await releaseQuery.Task.WaitAsync(token).ConfigureAwait(false);
                    return 5;
                }
                catch (OperationCanceledException)
                {
                    queryCanceled.TrySetResult();
                    throw;
                }
            }
        };

        await using var consumer = CreateConsumer(NewOptions(), engine);
        await consumer.StartAsync(cancellationToken);
        await consumer.AssignAsync([queue], cancellationToken: cancellationToken);

        var getCommittedOffset = consumer.GetCommittedOffsetAsync(queue, cancellationToken);
        await queryStarted.Task.WaitAsync(TimeSpan.FromSeconds(1), cancellationToken);
        var stop = consumer.StopAsync(cancellationToken).AsTask();
        var cancellationObserved = await Task.WhenAny(
            queryCanceled.Task,
            Task.Delay(TimeSpan.FromMilliseconds(500), cancellationToken));
        releaseQuery.TrySetResult();
        await stop;

        Assert.Same(queryCanceled.Task, cancellationObserved);
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => getCommittedOffset);
        Assert.Equal(1, engine.StopCount);
    }

    [Fact]
    public async Task PollAsync_StopBeginsAfterRunLookup_DoesNotAccessDisposedRun()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var timeProvider = new BlockingTimerCreationTimeProvider();
        var engine = new ScriptedConsumerEngine();
        await using var consumer = CreateConsumer(NewOptions(), engine, timeProvider: timeProvider);
        await consumer.StartAsync(cancellationToken);

        var poll = Task.Run(
            () => consumer.PollAsync(TimeSpan.FromMinutes(1), cancellationToken),
            cancellationToken);
        await timeProvider.TimerCreationStarted.WaitAsync(TimeSpan.FromSeconds(1), cancellationToken);

        var stop = consumer.StopAsync(cancellationToken);
        var stopCompletedBeforeAdmission = stop.IsCompleted;
        timeProvider.AllowTimerCreation();

        var pollException = await Record.ExceptionAsync(() => poll);
        await stop;

        Assert.IsAssignableFrom<OperationCanceledException>(pollException);
        Assert.False(
            stopCompletedBeforeAdmission,
            "Stop must wait until the operation that already selected the current run is admitted or rejected.");
        Assert.Equal(1, engine.StopCount);
    }

    [Fact]
    public async Task Seek_PreviousReceiveCompletes_DiscardsPreviousGenerationResult()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var queue = Queue();
        var filter = new FilterExpression("priority");
        var oldReceiveStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var completeOldReceive = new TaskCompletionSource<RemotingPullResult>(
            TaskCreationOptions.RunContinuationsAsynchronously);
        var freshReturned = 0;
        var engine = new ScriptedConsumerEngine
        {
            PullHandler = (value, offset, _, token) => offset switch
            {
                0 => ObserveAndReturnAsync(oldReceiveStarted, completeOldReceive.Task),
                10 when Interlocked.Exchange(ref freshReturned, 1) == 0 =>
                    Task.FromResult(Found(Message(value, offset, "fresh"), 11)),
                _ => WaitForPullCancellationAsync(offset, token)
            }
        };

        await using var consumer = CreateConsumer(NewOptions(), engine);
        await consumer.StartAsync(cancellationToken);
        await consumer.AssignAsync(
            [queue],
            new Dictionary<string, FilterExpression> { [queue.Topic] = filter },
            cancellationToken);
        await oldReceiveStarted.Task.WaitAsync(TimeSpan.FromSeconds(1), cancellationToken);

        consumer.Seek(queue, 10);
        completeOldReceive.SetResult(Found(Message(queue, 0, "stale"), 1));
        await WaitUntilAsync(() => engine.CountPulls(queue, 10) >= 1, "the seek generation", cancellationToken);
        await Task.Delay(TimeSpan.FromMilliseconds(50), cancellationToken);

        var messages = await consumer.PollAsync(TimeSpan.FromMilliseconds(500), cancellationToken);
        Assert.Equal("fresh", Assert.Single(messages).MessageId);
        Assert.Equal(11, consumer.Position(queue));
        Assert.All(engine.PullCalls, call => Assert.Equal(filter, call.Filter));
    }

    [Fact]
    public async Task StopAndSeekToBeginning_BlockedOldReceive_DoesNotStartOffsetTenGeneration()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var queue = Queue();
        var oldReceiveStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var oldReceiveCanceled = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseOldReceive = new TaskCompletionSource<RemotingPullResult>(
            TaskCreationOptions.RunContinuationsAsynchronously);
        var seekQueryStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseSeekQuery = new TaskCompletionSource<int>(TaskCreationOptions.RunContinuationsAsynchronously);
        var options = NewOptions();
        var engine = new ScriptedConsumerEngine
        {
            PullHandler = async (value, offset, _, token) =>
            {
                if (offset == 0)
                {
                    oldReceiveStarted.TrySetResult();
                    using var registration = token.Register(() => oldReceiveCanceled.TrySetResult());
                    return await releaseOldReceive.Task.ConfigureAwait(false);
                }

                if (offset == 10)
                {
                    return Found(Message(value, offset, "offset-ten"), offset + 1);
                }

                return await WaitForPullCancellationAsync(offset, token).ConfigureAwait(false);
            },
            QueryOffsetHandler = async (_, position, _, _) =>
            {
                Assert.Equal(ConsumeFromPosition.Beginning, position);
                seekQueryStarted.TrySetResult();
                var offset = await releaseSeekQuery.Task.ConfigureAwait(false);
                return offset;
            }
        };

        await using var consumer = CreateConsumer(options, engine);
        await consumer.StartAsync(cancellationToken);
        await consumer.AssignAsync([queue], cancellationToken: cancellationToken);
        await oldReceiveStarted.Task.WaitAsync(TimeSpan.FromSeconds(1), cancellationToken);

        var seek = consumer.SeekToBeginningAsync(queue, cancellationToken);
        await seekQueryStarted.Task.WaitAsync(TimeSpan.FromSeconds(1), cancellationToken);
        var stop = consumer.StopAsync(cancellationToken).AsTask();
        await oldReceiveCanceled.Task.WaitAsync(TimeSpan.FromSeconds(1), cancellationToken);
        releaseOldReceive.SetResult(new RemotingPullResult([], 1, RemotingPullStatus.NoNewMessage));
        Assert.False(stop.IsCompleted, "Stop must wait for the in-flight seek operation to release its gate.");

        releaseSeekQuery.SetResult(10);
        await seek;
        await stop;

        Assert.Equal(0, engine.CountPulls(queue, 10));
    }

    [Fact]
    public async Task AssignAsync_QueueIsRevoked_DiscardsPreviousGenerationResult()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var revokedQueue = Queue(0);
        var replacementQueue = Queue(1);
        var revokedReceiveStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var revokedReceiveCancelled = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var completeRevokedReceive = new TaskCompletionSource<RemotingPullResult>(
            TaskCreationOptions.RunContinuationsAsynchronously);
        var replacementReturned = 0;
        var engine = new ScriptedConsumerEngine
        {
            PullHandler = async (queue, offset, _, token) =>
            {
                if (queue == revokedQueue)
                {
                    revokedReceiveStarted.TrySetResult();
                    using var registration = token.Register(() => revokedReceiveCancelled.TrySetResult());
                    return await completeRevokedReceive.Task.ConfigureAwait(false);
                }

                if (Interlocked.Exchange(ref replacementReturned, 1) == 0)
                {
                    return Found(Message(replacementQueue, offset, "replacement"), offset + 1);
                }

                return await WaitForPullCancellationAsync(offset, token).ConfigureAwait(false);
            }
        };

        await using var consumer = CreateConsumer(NewOptions(), engine);
        await consumer.StartAsync(cancellationToken);
        await consumer.AssignAsync([revokedQueue], cancellationToken: cancellationToken);
        await revokedReceiveStarted.Task.WaitAsync(TimeSpan.FromSeconds(1), cancellationToken);

        var replacement = consumer.AssignAsync([replacementQueue], cancellationToken: cancellationToken);
        await revokedReceiveCancelled.Task.WaitAsync(TimeSpan.FromSeconds(1), cancellationToken);
        completeRevokedReceive.SetResult(Found(Message(revokedQueue, 0, "revoked"), 1));
        await replacement;

        var messages = await consumer.PollAsync(TimeSpan.FromMilliseconds(500), cancellationToken);
        var message = Assert.Single(messages);
        Assert.Equal("replacement", message.MessageId);
        Assert.Equal(replacementQueue.QueueId, message.QueueId);
    }

    [Fact]
    public async Task Pause_BufferContainsMessages_DrainsBufferWithoutFetchingUntilResume()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var queue = Queue();
        var secondReceiveStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var secondMessageAvailable = new TaskCompletionSource<RemotingPullResult>(
            TaskCreationOptions.RunContinuationsAsynchronously);
        var engine = new ScriptedConsumerEngine
        {
            PullHandler = async (value, offset, _, token) =>
            {
                if (offset == 0)
                {
                    return Found(Message(value, offset, "first"), 1);
                }

                secondReceiveStarted.TrySetResult();
                return await secondMessageAvailable.Task.WaitAsync(token).ConfigureAwait(false);
            }
        };

        await using var consumer = CreateConsumer(NewOptions(), engine);
        await consumer.StartAsync(cancellationToken);
        await consumer.AssignAsync([queue], cancellationToken: cancellationToken);
        await secondReceiveStarted.Task.WaitAsync(TimeSpan.FromSeconds(1), cancellationToken);

        consumer.Pause([queue]);
        var pullsAtPause = engine.CountPulls(queue);
        await Task.Delay(TimeSpan.FromMilliseconds(100), cancellationToken);
        Assert.Equal(pullsAtPause, engine.CountPulls(queue));
        Assert.Equal("first", Assert.Single(await consumer.PollAsync(TimeSpan.Zero, cancellationToken)).MessageId);
        Assert.Empty(await consumer.PollAsync(TimeSpan.Zero, cancellationToken));

        consumer.Resume([queue]);
        secondMessageAvailable.TrySetResult(Found(Message(queue, 1, "second"), 2));
        Assert.Equal(
            "second",
            Assert.Single(await consumer.PollAsync(TimeSpan.FromMilliseconds(500), cancellationToken)).MessageId);
        Assert.Equal(2, consumer.Position(queue));
    }

    [Fact]
    public async Task AutoCommit_MessageIsBuffered_PersistsOnlyAfterDelivery()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var queue = Queue();
        var options = NewOptions();
        options.EnableAutoCommit = true;
        options.AutoCommitInterval = TimeSpan.FromMilliseconds(25);
        var engine = new ScriptedConsumerEngine
        {
            PullHandler = (value, offset, _, token) => offset == 0
                ? Task.FromResult(Found(Message(value, offset), 1))
                : WaitForPullCancellationAsync(offset, token)
        };

        await using var consumer = CreateConsumer(options, engine);
        await consumer.StartAsync(cancellationToken);
        await consumer.AssignAsync([queue], cancellationToken: cancellationToken);
        await WaitUntilAsync(() => engine.CountPulls(queue, 1) >= 1, "a buffered message", cancellationToken);
        await Task.Delay(TimeSpan.FromMilliseconds(100), cancellationToken);
        Assert.DoesNotContain(engine.OffsetUpdates, static update => update.Offset == 1);

        Assert.Single(await consumer.PollAsync(TimeSpan.Zero, cancellationToken));
        await WaitUntilAsync(
            () => engine.OffsetUpdates.Any(static update => update.Offset == 1),
            "auto-commit of the delivered position",
            cancellationToken);
    }

    [Fact]
    public async Task RouteRefresh_RouteLookupFails_DoesNotSuppressHeartbeatToKnownBroker()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var routeQueries = 0;
        var heartbeatCount = 0;
        var queue = ResolvedQueue("orders", 0);
        var engine = new ScriptedConsumerEngine
        {
            GetMessageQueuesHandler = (_, _) => Interlocked.Increment(ref routeQueries) == 1
                ? Task.FromResult<IReadOnlyList<RemotingConsumerQueue>>([queue])
                : Task.FromException<IReadOnlyList<RemotingConsumerQueue>>(
                    new InvalidOperationException("route refresh failed"))
        };
        var remoting = CreateRemotingClient(request => request.Code switch
        {
            RequestCode.HeartBeat => CountHeartbeat(ref heartbeatCount),
            RequestCode.GetConsumerListByGroup => ConsumerListResponse(ClientId),
            RequestCode.UnregisterClient => SuccessResponse(),
            _ => throw new InvalidOperationException($"Unexpected request code {request.Code}.")
        });
        var rebalance = new TestRemotingRebalanceService();
        var options = NewOptions();
        options.Subscribe("orders");

        await using var consumer = CreateConsumer(options, engine, remoting.Object, rebalance);
        await consumer.StartAsync(cancellationToken);
        Assert.Equal(1, Volatile.Read(ref heartbeatCount));

        var balanced = await rebalance.RebalanceAsync("orders-consumer", cancellationToken);

        Assert.False(balanced);
        Assert.Equal(2, Volatile.Read(ref heartbeatCount));
        Assert.Single(consumer.Assignment);
    }

    [Fact]
    public async Task Heartbeat_BrokerCallFails_DoesNotSuppressAssignmentReconciliation()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var heartbeatAttempts = 0;
        var queue = ResolvedQueue("orders", 0);
        var engine = new ScriptedConsumerEngine
        {
            GetMessageQueuesHandler = (_, _) =>
                Task.FromResult<IReadOnlyList<RemotingConsumerQueue>>([queue])
        };
        var remoting = CreateRemotingClient(request => request.Code switch
        {
            RequestCode.HeartBeat => ThrowHeartbeatFailure(ref heartbeatAttempts),
            RequestCode.GetConsumerListByGroup => ConsumerListResponse(ClientId),
            RequestCode.UnregisterClient => SuccessResponse(),
            _ => throw new InvalidOperationException($"Unexpected request code {request.Code}.")
        });
        var options = NewOptions();
        options.Subscribe("orders");

        await using var consumer = CreateConsumer(options, engine, remoting.Object);
        await consumer.StartAsync(cancellationToken);

        Assert.Equal(1, Volatile.Read(ref heartbeatAttempts));
        Assert.Equal(queue, Assert.Single(consumer.Assignment));
    }

    [Fact]
    public async Task AssignAsync_PublicQueueIdentity_ResolvesRouteAndSendsHeartbeat()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var publicQueue = new RemotingConsumerQueue("orders", "broker-a", 0);
        var resolvedQueue = ResolvedQueue("orders", 0);
        var routeQueries = 0;
        var heartbeatCount = 0;
        var engine = new ScriptedConsumerEngine
        {
            GetMessageQueuesHandler = (topic, _) =>
            {
                Assert.Equal("orders", topic);
                Interlocked.Increment(ref routeQueries);
                return Task.FromResult<IReadOnlyList<RemotingConsumerQueue>>([resolvedQueue]);
            }
        };
        var remoting = CreateRemotingClient(request => request.Code switch
        {
            RequestCode.HeartBeat => CountHeartbeat(ref heartbeatCount),
            RequestCode.UnregisterClient => SuccessResponse(),
            _ => throw new InvalidOperationException($"Unexpected request code {request.Code}.")
        });

        await using var consumer = CreateConsumer(NewOptions(), engine, remoting.Object);
        await consumer.StartAsync(cancellationToken);
        await consumer.AssignAsync([publicQueue], cancellationToken: cancellationToken);

        Assert.Equal(1, Volatile.Read(ref routeQueries));
        Assert.Equal(1, Volatile.Read(ref heartbeatCount));
        Assert.Equal(publicQueue, Assert.Single(consumer.Assignment));
    }

    [Fact]
    public async Task AssignAsync_ManualTagFilter_SendsHeartbeatSubscription()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var heartbeats = new ConcurrentQueue<byte[]>();
        var queue = Queue();
        var filter = new FilterExpression("priority");
        var remoting = CreateRemotingClient(request => request.Code switch
        {
            RequestCode.HeartBeat => CaptureHeartbeat(request, heartbeats),
            RequestCode.UnregisterClient => SuccessResponse(),
            _ => throw new InvalidOperationException($"Unexpected request code {request.Code}.")
        });

        await using (var consumer = CreateConsumer(NewOptions(), new ScriptedConsumerEngine(), remoting.Object))
        {
            await consumer.StartAsync(cancellationToken);
            await consumer.AssignAsync(
                [queue],
                new Dictionary<string, FilterExpression> { [queue.Topic] = filter },
                cancellationToken);
        }

        using var document = JsonDocument.Parse(Assert.Single(heartbeats));
        var consumerData = Assert.Single(document.RootElement.GetProperty("consumerDataSet").EnumerateArray());
        var subscription = Assert.Single(consumerData.GetProperty("subscriptionDataSet").EnumerateArray());
        Assert.Equal(queue.Topic, subscription.GetProperty("topic").GetString());
        Assert.Equal(filter.Expression, subscription.GetProperty("subString").GetString());
        Assert.Equal("TAG", subscription.GetProperty("expressionType").GetString());
        Assert.True(subscription.GetProperty("subVersion").GetInt64() > 0);
    }

    [Fact]
    public async Task SubscribeAsync_PeriodicRebalanceHasOldFilter_NeverPairsOldFilterWithNewVersion()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var queue = ResolvedQueue("orders", 0);
        var oldFilter = new FilterExpression("old");
        var newFilter = new FilterExpression("new");
        var routeCallCount = 0;
        var blockedRouteStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseBlockedRoute = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var heartbeats = new ConcurrentQueue<byte[]>();
        var engine = new ScriptedConsumerEngine
        {
            GetMessageQueuesHandler = async (_, token) =>
            {
                if (Interlocked.Increment(ref routeCallCount) == 2)
                {
                    blockedRouteStarted.TrySetResult();
                    await releaseBlockedRoute.Task.WaitAsync(token).ConfigureAwait(false);
                }

                return [queue];
            }
        };
        var remoting = CreateRemotingClient(request => request.Code switch
        {
            RequestCode.HeartBeat => CaptureHeartbeat(request, heartbeats),
            RequestCode.GetConsumerListByGroup => ConsumerListResponse(ClientId),
            RequestCode.UnregisterClient => SuccessResponse(),
            _ => throw new InvalidOperationException($"Unexpected request code {request.Code}.")
        });
        var options = NewOptions();
        options.Subscribe("orders", oldFilter);
        var rebalance = new TestRemotingRebalanceService();

        await using var consumer = CreateConsumer(options, engine, remoting.Object, rebalance);
        await consumer.StartAsync(cancellationToken);
        await WaitUntilAsync(
            () => engine.PullCalls.Any(call => call.Queue == queue && call.Filter == oldFilter),
            "the original filtered receive",
            cancellationToken);
        while (heartbeats.TryDequeue(out _))
        {
        }

        var periodic = rebalance.RebalanceAsync(options.GroupName, cancellationToken);
        await blockedRouteStarted.Task.WaitAsync(TimeSpan.FromSeconds(1), cancellationToken);
        var replacement = consumer.SubscribeAsync("orders", newFilter, cancellationToken);
        Assert.False(replacement.IsCompleted);

        releaseBlockedRoute.TrySetResult();
        await periodic.WaitAsync(TimeSpan.FromSeconds(1), cancellationToken);
        await replacement.WaitAsync(TimeSpan.FromSeconds(1), cancellationToken);
        await WaitUntilAsync(
            () => engine.PullCalls.Any(call => call.Queue == queue && call.Filter == newFilter),
            "the replacement filtered receive",
            cancellationToken);

        var observed = heartbeats.SelectMany(ReadHeartbeatSubscriptions).ToArray();
        var replacementRecord = Assert.Single(observed, value => value.Filter == newFilter.Expression);
        Assert.DoesNotContain(observed, value =>
            value.Version == replacementRecord.Version && value.Filter == oldFilter.Expression);
        Assert.Contains(engine.PullCalls, call => call.Queue == queue && call.Filter == oldFilter);
        Assert.Contains(engine.PullCalls, call => call.Queue == queue && call.Filter == newFilter);
        Assert.DoesNotContain(engine.PullCalls, call => call.Queue == queue && call.Filter == FilterExpression.All);
    }

    [Fact]
    public async Task AssignAsync_InitialOffsetFails_PeriodicReconciliationStartsDesiredReceiver()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var queue = Queue();
        var offsetAttempts = 0;
        var engine = new ScriptedConsumerEngine
        {
            GetOffsetHandler = (_, _) => Interlocked.Increment(ref offsetAttempts) == 1
                ? Task.FromException<long>(new InvalidOperationException("offset unavailable"))
                : Task.FromResult(0L)
        };
        var rebalance = new TestRemotingRebalanceService();

        await using var consumer = CreateConsumer(NewOptions(), engine, rebalanceService: rebalance);
        var options = NewOptions();
        await consumer.StartAsync(cancellationToken);
        await consumer.AssignAsync([queue], cancellationToken: cancellationToken);

        await rebalance.RebalanceAsync(options.GroupName, cancellationToken);
        await WaitUntilAsync(
            () => engine.CountPulls(queue) > 0,
            "manual assignment recovery after offset initialization failure",
            cancellationToken);

        Assert.True(Volatile.Read(ref offsetAttempts) >= 2);
        Assert.Equal(queue, Assert.Single(consumer.Assignment));
    }

    [Fact]
    public async Task ReceiverFault_CurrentManualAssignment_ReconcilesReplacementGeneration()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var queue = Queue();
        var filter = new FilterExpression("priority");
        var pullCount = 0;
        var engine = new ScriptedConsumerEngine
        {
            PullHandler = (current, offset, _, token) => Interlocked.Increment(ref pullCount) switch
            {
                1 => Task.FromResult(Found(Message(current, long.MaxValue, "invalid-offset"), long.MaxValue)),
                2 => Task.FromResult(Found(Message(current, offset, "recovered"), offset + 1)),
                _ => WaitForPullCancellationAsync(offset, token)
            }
        };
        var rebalance = new TestRemotingRebalanceService();
        var options = NewOptions();

        await using var consumer = CreateConsumer(options, engine, rebalanceService: rebalance);
        await consumer.StartAsync(cancellationToken);
        await consumer.AssignAsync(
            [queue],
            new Dictionary<string, FilterExpression> { [queue.Topic] = filter },
            cancellationToken);
        await WaitUntilAsync(() => Volatile.Read(ref pullCount) >= 1, "the failed receiver", cancellationToken);

        await rebalance.RebalanceAsync(options.GroupName, cancellationToken);
        await WaitUntilAsync(
            () => Volatile.Read(ref pullCount) >= 2,
            "a replacement receiver generation",
            cancellationToken);
        var messages = await consumer.PollAsync(TimeSpan.FromSeconds(1), cancellationToken);

        Assert.Equal("recovered", Assert.Single(messages).MessageId);
        Assert.All(engine.PullCalls, call => Assert.Equal(filter, call.Filter));
    }

    [Fact]
    public async Task AssignAsync_ManualFiltersAreReplaced_PullsWithCurrentFiltersAndRejectsSubscriptionsUntilCleared()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var orders = new RemotingConsumerQueue("orders", "broker-a", 0);
        var payments = new RemotingConsumerQueue("payments", "broker-a", 0);
        var ordersFilter = new FilterExpression("priority");
        var replacementFilter = new FilterExpression("urgent");
        var engine = new ScriptedConsumerEngine();

        await using var consumer = CreateConsumer(NewOptions(), engine);
        await consumer.StartAsync(cancellationToken);
        await consumer.AssignAsync(
            [orders, payments],
            new Dictionary<string, FilterExpression> { ["orders"] = ordersFilter },
            cancellationToken);

        await WaitUntilAsync(
            () => engine.PullCalls.Any(call => call.Queue == orders && call.Filter == ordersFilter) &&
                engine.PullCalls.Any(call => call.Queue == payments && call.Filter == FilterExpression.All),
            "the initial manual-assignment receives",
            cancellationToken);
        var ordersPullCount = engine.PullCalls.Count(call => call.Queue == orders);

        await consumer.AssignAsync(
            [orders, payments],
            new Dictionary<string, FilterExpression> { ["orders"] = replacementFilter },
            cancellationToken);
        await WaitUntilAsync(
            () => engine.PullCalls
                .Where(call => call.Queue == orders)
                .Skip(ordersPullCount)
                .Any(call => call.Filter == replacementFilter),
            "the replacement manual-assignment receive",
            cancellationToken);

        var replacementCalls = engine.PullCalls
            .Where(call => call.Queue == orders)
            .Skip(ordersPullCount)
            .ToArray();
        Assert.NotEmpty(replacementCalls);
        Assert.All(replacementCalls, call => Assert.Equal(replacementFilter, call.Filter));
        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            consumer.SubscribeAsync("orders", cancellationToken: cancellationToken));

        await consumer.AssignAsync([], cancellationToken: cancellationToken);
        await consumer.SubscribeAsync("orders", cancellationToken: cancellationToken);
        Assert.Empty(consumer.Assignment);
    }

    [Fact]
    public async Task RebalanceRegistration_ConsumerRestarts_RecreatesThenRemovesOnDispose()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var engine = new ScriptedConsumerEngine();
        var rebalance = new TestRemotingRebalanceService();

        await using var consumer = CreateConsumer(NewOptions(), engine, rebalanceService: rebalance);
        await consumer.StartAsync(cancellationToken);
        Assert.Equal(1, rebalance.ActiveRegistrations);
        await consumer.StopAsync(cancellationToken);
        Assert.Equal(0, rebalance.ActiveRegistrations);
        await consumer.StartAsync(cancellationToken);

        Assert.Equal(1, rebalance.ActiveRegistrations);
    }

    private static RemotingLitePullConsumerOptions NewOptions() => new()
    {
        GroupName = "orders-consumer",
        EnableAutoCommit = false,
        PollTimeout = TimeSpan.FromMilliseconds(250),
        LongPollingTimeout = TimeSpan.FromSeconds(1)
    };

    private static RemotingConsumerQueue Queue(int queueId = 0) =>
        new("orders", "broker-a", queueId);

    private static RemotingConsumerQueue ResolvedQueue(string topic, int queueId) =>
        new(topic, "broker-a", queueId);

    private static RemotingPullResult Found(RemotingMessageView message, long nextOffset) =>
        new([message], nextOffset, RemotingPullStatus.Found);

    private static RemotingMessageView Message(
        RemotingConsumerQueue queue,
        long offset,
        string? messageId = null,
        int bodySize = 1) =>
        new(
            queue.Topic,
            new byte[bodySize],
            messageId ?? $"message-{queue.QueueId}-{offset}",
            $"offset-{queue.QueueId}-{offset}",
            null,
            [],
            new Dictionary<string, string>(),
            1,
            null,
            queue.QueueId,
            queue.BrokerName,
            offset,
            offset,
            DateTimeOffset.UnixEpoch,
            DateTimeOffset.UnixEpoch);

    private static async Task<RemotingPullResult> WaitForPullCancellationAsync(
        long offset,
        CancellationToken cancellationToken)
    {
        await Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken).ConfigureAwait(false);
        return new RemotingPullResult([], offset, RemotingPullStatus.NoNewMessage);
    }

    private static async Task<RemotingPullResult> ObserveAndReturnAsync(
        TaskCompletionSource observed,
        Task<RemotingPullResult> result)
    {
        observed.TrySetResult();
        return await result.ConfigureAwait(false);
    }

    private static async Task WaitUntilAsync(
        Func<bool> condition,
        string description,
        CancellationToken cancellationToken)
    {
        var started = Stopwatch.GetTimestamp();
        while (!condition())
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (Stopwatch.GetElapsedTime(started) >= TimeSpan.FromSeconds(1))
            {
                throw new TimeoutException($"Timed out waiting for {description}.");
            }

            await Task.Delay(TimeSpan.FromMilliseconds(10), cancellationToken).ConfigureAwait(false);
        }
    }

    private static IReadOnlyList<HeartbeatSubscription> ReadHeartbeatSubscriptions(byte[] heartbeat)
    {
        using var document = JsonDocument.Parse(heartbeat);
        var consumerData = Assert.Single(document.RootElement.GetProperty("consumerDataSet").EnumerateArray());
        return consumerData.GetProperty("subscriptionDataSet")
            .EnumerateArray()
            .Select(static subscription => new HeartbeatSubscription(
                subscription.GetProperty("subVersion").GetInt64(),
                subscription.GetProperty("subString").GetString() ?? string.Empty))
            .ToArray();
    }

    private static async Task<IReadOnlyList<IReadOnlyList<RemotingMessageView>>> PollUntilMessagesAsync(
        RemotingLitePullConsumer consumer,
        int expectedMessageCount,
        CancellationToken cancellationToken)
    {
        var batches = new List<IReadOnlyList<RemotingMessageView>>();
        var messageCount = 0;
        while (messageCount < expectedMessageCount)
        {
            var batch = await consumer.PollAsync(TimeSpan.FromSeconds(1), cancellationToken);
            Assert.NotEmpty(batch);
            batches.Add(batch);
            messageCount += batch.Count;
        }

        return batches;
    }

    private static RemotingLitePullConsumer CreateConsumer(
        RemotingLitePullConsumerOptions options,
        IRemotingConsumerEngine engine,
        IRemotingClient? remoting = null,
        IRemotingRebalanceService? rebalanceService = null,
        TimeProvider? timeProvider = null) =>
        new(
            Options.Create(options),
            Options.Create(new RemotingClientOptions
            {
                ClientIP = "127.0.0.1",
                InstanceName = "lite-client"
            }),
            engine,
            remoting ?? CreateRemotingClient(DefaultMaintenanceResponse).Object,
            rebalanceService ?? new TestRemotingRebalanceService(),
            timeProvider ?? TimeProvider.System,
            NullLogger<RemotingLitePullConsumer>.Instance);

    private static Mock<IRemotingClient> CreateRemotingClient(Func<RemotingCommand, RemotingCommand> invoke)
    {
        var remoting = new Mock<IRemotingClient>(MockBehavior.Strict);
        remoting
            .Setup(value => value.InvokeAsync(
                It.IsAny<System.Net.EndPoint>(),
                It.IsAny<RemotingCommand>(),
                It.IsAny<TimeSpan>(),
                It.IsAny<CancellationToken>()))
            .Returns<System.Net.EndPoint, RemotingCommand, TimeSpan, CancellationToken>(
                (_, request, _, _) => Task.FromResult(invoke(request)));
        return remoting;
    }

    private static RemotingCommand DefaultMaintenanceResponse(RemotingCommand request) => request.Code switch
    {
        RequestCode.HeartBeat or RequestCode.UnregisterClient => SuccessResponse(),
        RequestCode.GetConsumerListByGroup => ConsumerListResponse(ClientId),
        _ => throw new InvalidOperationException($"Unexpected request code {request.Code}.")
    };

    private static RemotingCommand CaptureHeartbeat(
        RemotingCommand request,
        ConcurrentQueue<byte[]> heartbeats)
    {
        heartbeats.Enqueue(Assert.IsType<byte[]>(request.Body));
        return SuccessResponse();
    }

    private static RemotingCommand CountHeartbeat(ref int heartbeatCount)
    {
        Interlocked.Increment(ref heartbeatCount);
        return SuccessResponse();
    }

    private static RemotingCommand ThrowHeartbeatFailure(ref int heartbeatAttempts)
    {
        Interlocked.Increment(ref heartbeatAttempts);
        throw new InvalidOperationException("heartbeat failed");
    }

    private static RemotingCommand ConsumerListResponse(params string[] consumerIds) =>
        new()
        {
            Code = ResponseCodes.ResSuccess,
            Body = Encoding.UTF8.GetBytes($$"""{"consumerIdList":{{JsonSerializer.Serialize(consumerIds)}}}""")
        };

    private static RemotingCommand SuccessResponse() => new() { Code = ResponseCodes.ResSuccess };

    private sealed class ScriptedConsumerEngine : IRemotingConsumerEngine
    {
        public Func<string, CancellationToken, Task<IReadOnlyList<RemotingConsumerQueue>>> GetMessageQueuesHandler
        {
            get;
            set;
        } = static (_, _) => Task.FromResult<IReadOnlyList<RemotingConsumerQueue>>([]);

        public Func<RemotingConsumerQueue, long, int?, CancellationToken, Task<RemotingPullResult>> PullHandler
        {
            get;
            set;
        } = static (_, offset, _, token) => WaitForPullCancellationAsync(offset, token);

        public Func<RemotingConsumerQueue, bool, CancellationToken, Task<RemotingBrokerEndpoint>> ResolveBrokerHandler
        {
            get;
            set;
        } = static (queue, _, _) => Task.FromResult(
            new RemotingBrokerEndpoint(queue.BrokerName, "127.0.0.1:10911"));

        public Func<RemotingConsumerQueue, CancellationToken, Task<long>> GetOffsetHandler { get; set; }

        public Func<RemotingConsumerQueue, ConsumeFromPosition, DateTimeOffset?, CancellationToken, Task<long>>
            QueryOffsetHandler
        { get; set; } = static (_, _, _, _) => Task.FromResult(0L);

        public Func<RemotingConsumerQueue, long, CancellationToken, Task> UpdateOffsetHandler { get; set; } =
            static (_, _, _) => Task.CompletedTask;

        public ConcurrentDictionary<RemotingConsumerQueue, long> CommittedOffsets { get; } = new();

        public ConcurrentQueue<PullCall> PullCalls { get; } = new();

        public ConcurrentQueue<OffsetUpdate> OffsetUpdates { get; } = new();

        public ConcurrentQueue<QueryCall> QueryCalls { get; } = new();

        public int StopCount => Volatile.Read(ref _stopCount);

        public int DisposeCount => Volatile.Read(ref _disposeCount);

        private int _stopCount;
        private int _disposeCount;

        public ScriptedConsumerEngine()
        {
            GetOffsetHandler = (queue, _) => Task.FromResult(
                CommittedOffsets.TryGetValue(queue, out var offset) ? offset : 0L);
        }

        public ValueTask StartAsync(CancellationToken cancellationToken = default) => ValueTask.CompletedTask;

        public ValueTask StopAsync(CancellationToken cancellationToken = default)
        {
            Interlocked.Increment(ref _stopCount);
            return ValueTask.CompletedTask;
        }

        public Task<IReadOnlyList<RemotingConsumerQueue>> GetMessageQueuesAsync(
            string topic,
            CancellationToken cancellationToken = default) =>
            GetMessageQueuesHandler(topic, cancellationToken);

        public Task<RemotingBrokerEndpoint> ResolveBrokerAsync(
            RemotingConsumerQueue queue,
            bool useSuggestedBroker,
            CancellationToken cancellationToken = default) =>
            ResolveBrokerHandler(queue, useSuggestedBroker, cancellationToken);

        public Task<RemotingPullResult> PullAsync(
            RemotingConsumerQueue queue,
            long offset,
            FilterExpression filter,
            int? maxMessages = null,
            int? maxBytes = null,
            CancellationToken cancellationToken = default)
        {
            PullCalls.Enqueue(new PullCall(queue, offset, filter, maxMessages, maxBytes));
            return PullHandler(queue, offset, maxMessages, cancellationToken);
        }

        public Task<long> GetOffsetAsync(
            RemotingConsumerQueue queue,
            CancellationToken cancellationToken = default) =>
            GetOffsetHandler(queue, cancellationToken);

        public async Task UpdateOffsetAsync(
            RemotingConsumerQueue queue,
            long offset,
            CancellationToken cancellationToken = default)
        {
            OffsetUpdates.Enqueue(new OffsetUpdate(queue, offset));
            await UpdateOffsetHandler(queue, offset, cancellationToken).ConfigureAwait(false);
            CommittedOffsets[queue] = offset;
        }

        public Task<long> QueryOffsetAsync(
            RemotingConsumerQueue queue,
            ConsumeFromPosition position,
            DateTimeOffset? timestamp = null,
            CancellationToken cancellationToken = default)
        {
            QueryCalls.Enqueue(new QueryCall(queue, position, timestamp));
            return QueryOffsetHandler(queue, position, timestamp, cancellationToken);
        }

        public Task SendBackAsync(
            RemotingConsumerQueue queue,
            RemotingMessageView message,
            int delayLevel,
            int maxReconsumeTimes,
            CancellationToken cancellationToken) =>
            Task.FromException(new NotSupportedException("LitePull tests do not send messages back."));

        public ValueTask DisposeAsync()
        {
            Interlocked.Increment(ref _disposeCount);
            return ValueTask.CompletedTask;
        }

        public int CountPulls(RemotingConsumerQueue queue) =>
            PullCalls.Count(call => call.Queue == queue);

        public int CountPulls(RemotingConsumerQueue queue, long offset) =>
            PullCalls.Count(call => call.Queue == queue && call.Offset == offset);

        public sealed record PullCall(
            RemotingConsumerQueue Queue,
            long Offset,
            FilterExpression Filter,
            int? MaxMessages,
            int? MaxBytes);

        public sealed record OffsetUpdate(RemotingConsumerQueue Queue, long Offset);

        public sealed record QueryCall(
            RemotingConsumerQueue Queue,
            ConsumeFromPosition Position,
            DateTimeOffset? Timestamp);
    }

    private sealed class BlockingTimerCreationTimeProvider : TimeProvider
    {
        private readonly TaskCompletionSource _timerCreationStarted =
            new(TaskCreationOptions.RunContinuationsAsynchronously);
        private readonly TaskCompletionSource _allowTimerCreation =
            new(TaskCreationOptions.RunContinuationsAsynchronously);

        public Task TimerCreationStarted => _timerCreationStarted.Task;

        public void AllowTimerCreation() => _allowTimerCreation.TrySetResult();

        public override ITimer CreateTimer(
            TimerCallback callback,
            object? state,
            TimeSpan dueTime,
            TimeSpan period)
        {
            _timerCreationStarted.TrySetResult();
            _allowTimerCreation.Task.GetAwaiter().GetResult();
            return TimeProvider.System.CreateTimer(callback, state, dueTime, period);
        }
    }

    private sealed record HeartbeatSubscription(long Version, string Filter);
}
