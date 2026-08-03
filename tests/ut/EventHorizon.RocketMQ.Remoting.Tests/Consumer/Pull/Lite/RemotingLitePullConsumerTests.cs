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
using EventHorizon.RocketMQ.Remoting.Consumer.Pull;
using EventHorizon.RocketMQ.Remoting.Consumer.Pull.Lite;
using EventHorizon.RocketMQ.Remoting.Protocol;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Moq;
using Xunit;

namespace EventHorizon.RocketMQ.Remoting.Tests.Consumer.Pull.Lite;

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
    public async Task BackgroundReceivers_OneQueueIsEmpty_DoesNotBlockReadyQueue()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var emptyQueue = Queue(0);
        var readyQueue = Queue(1);
        var readyReturned = 0;
        var engine = new ScriptedConsumerEngine
        {
            PullHandler = async (queue, offset, _, token) =>
            {
                if (queue == emptyQueue || Interlocked.Exchange(ref readyReturned, 1) != 0)
                {
                    return await WaitForPullCancellationAsync(offset, token).ConfigureAwait(false);
                }

                return Found(Message(readyQueue, offset, "ready"), offset + 1);
            }
        };

        await using var consumer = CreateConsumer(NewOptions(), engine);
        await consumer.StartAsync(cancellationToken);
        await consumer.AssignAsync([emptyQueue, readyQueue], cancellationToken: cancellationToken);

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
    public async Task Seek_PreviousReceiveCompletes_DiscardsPreviousGenerationResult()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var queue = Queue();
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
        await consumer.AssignAsync([queue], cancellationToken: cancellationToken);
        await oldReceiveStarted.Task.WaitAsync(TimeSpan.FromSeconds(1), cancellationToken);

        consumer.Seek(queue, 10);
        completeOldReceive.SetResult(Found(Message(queue, 0, "stale"), 1));
        await WaitUntilAsync(() => engine.CountPulls(queue, 10) >= 1, "the seek generation", cancellationToken);
        await Task.Delay(TimeSpan.FromMilliseconds(50), cancellationToken);

        var messages = await consumer.PollAsync(TimeSpan.FromMilliseconds(500), cancellationToken);
        Assert.Equal("fresh", Assert.Single(messages).MessageId);
        Assert.Equal(11, consumer.Position(queue));
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
    public async Task ManualAssignment_TopicFiltersAreConfigured_AppliesFiltersAndRejectsSubscriptionsUntilCleared()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var orders = new RemotingConsumerQueue("orders", "broker-a", 0);
        var payments = new RemotingConsumerQueue("payments", "broker-a", 0);
        var ordersFilter = new FilterExpression("priority");
        var engine = new ScriptedConsumerEngine();

        await using var consumer = CreateConsumer(NewOptions(), engine);
        await consumer.StartAsync(cancellationToken);
        await consumer.AssignAsync(
            [orders, payments],
            new Dictionary<string, FilterExpression> { ["orders"] = ordersFilter },
            cancellationToken);

        Assert.Equal(ordersFilter, engine.Subscriptions["orders"]);
        Assert.Equal(FilterExpression.All, engine.Subscriptions["payments"]);
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
        new(topic, queueId, "broker-a", "127.0.0.1:10911");

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

    private static RemotingLitePullConsumer CreateConsumer(
        RemotingLitePullConsumerOptions options,
        IRemotingConsumerEngine engine,
        IRemotingClient? remoting = null,
        IRemotingRebalanceService? rebalanceService = null) =>
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
            TimeProvider.System,
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

        public Func<RemotingConsumerQueue, CancellationToken, Task<long>> GetOffsetHandler { get; set; }

        public Func<RemotingConsumerQueue, ConsumeFromPosition, DateTimeOffset?, CancellationToken, Task<long>>
            QueryOffsetHandler
        { get; set; } = static (_, _, _, _) => Task.FromResult(0L);

        public ConcurrentDictionary<RemotingConsumerQueue, long> CommittedOffsets { get; } = new();

        public ConcurrentDictionary<string, FilterExpression> Subscriptions { get; } =
            new(StringComparer.Ordinal);

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

        public Task<RemotingPullResult> PullAsync(
            RemotingConsumerQueue queue,
            long offset,
            int? maxMessages = null,
            CancellationToken cancellationToken = default)
        {
            PullCalls.Enqueue(new PullCall(queue, offset, maxMessages));
            return PullHandler(queue, offset, maxMessages, cancellationToken);
        }

        public Task<long> GetOffsetAsync(
            RemotingConsumerQueue queue,
            CancellationToken cancellationToken = default) =>
            GetOffsetHandler(queue, cancellationToken);

        public Task UpdateOffsetAsync(
            RemotingConsumerQueue queue,
            long offset,
            CancellationToken cancellationToken = default)
        {
            OffsetUpdates.Enqueue(new OffsetUpdate(queue, offset));
            CommittedOffsets[queue] = offset;
            return Task.CompletedTask;
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

        public void SetSubscription(string topic, FilterExpression expression) =>
            Subscriptions[topic] = expression;

        public void RemoveSubscription(string topic) => Subscriptions.TryRemove(topic, out _);

        public ValueTask DisposeAsync()
        {
            Interlocked.Increment(ref _disposeCount);
            return ValueTask.CompletedTask;
        }

        public int CountPulls(RemotingConsumerQueue queue) =>
            PullCalls.Count(call => call.Queue == queue);

        public int CountPulls(RemotingConsumerQueue queue, long offset) =>
            PullCalls.Count(call => call.Queue == queue && call.Offset == offset);

        public sealed record PullCall(RemotingConsumerQueue Queue, long Offset, int? MaxMessages);

        public sealed record OffsetUpdate(RemotingConsumerQueue Queue, long Offset);

        public sealed record QueryCall(
            RemotingConsumerQueue Queue,
            ConsumeFromPosition Position,
            DateTimeOffset? Timestamp);
    }
}
