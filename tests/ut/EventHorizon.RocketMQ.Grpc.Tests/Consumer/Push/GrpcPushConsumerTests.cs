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
using System.Reflection;
using System.Threading.Channels;
using EventHorizon.RocketMQ.Grpc.Consumer;
using EventHorizon.RocketMQ.Grpc.Consumer.Push;
using EventHorizon.RocketMQ.Grpc.Consumer.Simple;
using EventHorizon.RocketMQ.Grpc.Exceptions;
using EventHorizon.RocketMQ.Grpc.Instrumentation;
using EventHorizon.RocketMQ.Grpc.Protocol;
using EventHorizon.RocketMQ.Grpc.Protocol.Route;
using EventHorizon.RocketMQ.Grpc.Protocol.Telemetry;
using Google.Protobuf;
using Google.Protobuf.WellKnownTypes;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Moq;
using Xunit;
using Proto = Apache.Rocketmq.V2;

namespace EventHorizon.RocketMQ.Grpc.Tests.Consumer.Push;

public sealed class GrpcPushConsumerTests
{
    [Fact]
    public async Task FifoMessagesWithSameGroupAreProcessedInArrivalOrder()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var firstStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var firstCancellationRequested = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseFirst = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var secondStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var processingOrder = new ConcurrentQueue<string>();
        var client = new FakeGrpcClient();
        await using var consumer = CreateConsumer(
            client,
            async (message, handlerCancellationToken) =>
            {
                processingOrder.Enqueue(message.MessageId);
                if (message.MessageId == "first")
                {
                    using var registration = handlerCancellationToken.Register(
                        () => firstCancellationRequested.TrySetResult());
                    firstStarted.TrySetResult();
                    await releaseFirst.Task.WaitAsync(handlerCancellationToken);
                }
                else
                {
                    secondStarted.TrySetResult();
                }

                return ConsumeResult.Success;
            },
            out var engine,
            options =>
            {
                options.MaxConcurrency = 2;
                options.ConsumeTimeout = TimeSpan.FromMilliseconds(50);
            });

        var channelField = typeof(GrpcPushConsumer).GetField("_messages", BindingFlags.Instance | BindingFlags.NonPublic);
        var processMethod = typeof(GrpcPushConsumer).GetMethod("ProcessMessagesAsync", BindingFlags.Instance | BindingFlags.NonPublic);
        var enqueueMethod = typeof(GrpcPushConsumer).GetMethod("EnqueueAsync", BindingFlags.Instance | BindingFlags.NonPublic);
        var channel = Assert.IsAssignableFrom<Channel<GrpcMessageView>>(channelField?.GetValue(consumer));
        var workers = Enumerable.Range(0, 2)
            .Select(_ => Assert.IsAssignableFrom<Task>(processMethod?.Invoke(consumer, [cancellationToken])))
            .ToArray();

        var first = Message("first", "account-7");
        var second = Message("second", "account-7");
        engine.BindMessage(first);
        engine.BindMessage(second);
        await Assert.IsType<ValueTask>(enqueueMethod?.Invoke(consumer, [first, cancellationToken]));
        await Assert.IsType<ValueTask>(enqueueMethod?.Invoke(consumer, [second, cancellationToken]));
        channel.Writer.Complete();

        await firstStarted.Task.WaitAsync(TimeSpan.FromSeconds(3), cancellationToken);
        await Task.Delay(100, cancellationToken);
        Assert.False(firstCancellationRequested.Task.IsCompleted);
        Assert.False(secondStarted.Task.IsCompleted);
        releaseFirst.TrySetResult();
        await Task.WhenAll(workers).WaitAsync(TimeSpan.FromSeconds(5), cancellationToken);

        Assert.Equal(new[] { "first", "second" }, processingOrder);
    }

    [Fact]
    public async Task FifoRetryKeepsSuccessorBlockedUntilPredecessorSucceeds()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var firstSecondAttempt = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseFirst = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var secondStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var processingOrder = new ConcurrentQueue<string>();
        var firstCalls = 0;
        var client = new FakeGrpcClient();
        await using var consumer = CreateConsumer(
            client,
            async (message, handlerCancellationToken) =>
            {
                processingOrder.Enqueue($"{message.MessageId}:{message.DeliveryAttempt}");
                if (message.MessageId == "first")
                {
                    if (Interlocked.Increment(ref firstCalls) == 1)
                    {
                        return ConsumeResult.Retry;
                    }

                    firstSecondAttempt.TrySetResult();
                    await releaseFirst.Task.WaitAsync(handlerCancellationToken);
                    return ConsumeResult.Success;
                }

                secondStarted.TrySetResult();
                return ConsumeResult.Success;
            },
            out var engine,
            options =>
            {
                options.MaxConcurrency = 2;
                options.MaxDeliveryAttempts = 3;
                options.RetryDelay = TimeSpan.FromMilliseconds(10);
            });

        var processing = RunOrderedWorkersAsync(
            consumer,
            engine,
            2,
            cancellationToken,
            Message("first", "account-7"),
            Message("second", "account-7"));
        await firstSecondAttempt.Task.WaitAsync(TimeSpan.FromSeconds(3), cancellationToken);
        try
        {
            await Task.Delay(100, cancellationToken);
            Assert.False(secondStarted.Task.IsCompleted);
            Assert.Empty(client.AckRequests);
            Assert.Empty(client.ChangeInvisibleRequests);
            Assert.Empty(client.DeadLetterRequests);
        }
        finally
        {
            releaseFirst.TrySetResult();
        }

        await processing;

        Assert.Equal(new[] { "first:1", "first:2", "second:1" }, processingOrder);
        Assert.Equal(new[] { "first", "second" },
            client.AckRequests.SelectMany(static request => request.Entries).Select(static entry => entry.MessageId));
        Assert.Empty(client.ChangeInvisibleRequests);
        Assert.Empty(client.DeadLetterRequests);
    }

    [Fact]
    public async Task FifoRetryExhaustionForwardsOnceThenReleasesSuccessor()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var processingOrder = new ConcurrentQueue<string>();
        var client = new FakeGrpcClient();
        await using var consumer = CreateConsumer(
            client,
            (message, _) =>
            {
                processingOrder.Enqueue($"{message.MessageId}:{message.DeliveryAttempt}");
                return ValueTask.FromResult(
                    message.MessageId == "first" ? ConsumeResult.Retry : ConsumeResult.Success);
            },
            out var engine,
            options =>
            {
                options.MaxConcurrency = 2;
                options.MaxDeliveryAttempts = 3;
                options.RetryDelay = TimeSpan.FromMilliseconds(1);
            });

        await RunOrderedWorkersAsync(
            consumer,
            engine,
            2,
            cancellationToken,
            Message("first", "account-7"),
            Message("second", "account-7"));

        Assert.Equal(new[] { "first:1", "first:2", "first:3", "second:1" }, processingOrder);
        var deadLetter = Assert.Single(client.DeadLetterRequests);
        Assert.Equal("first", deadLetter.MessageId);
        Assert.Equal(3, deadLetter.DeliveryAttempt);
        Assert.Equal(3, deadLetter.MaxDeliveryAttempts);
        var ack = Assert.Single(client.AckRequests);
        Assert.Equal("second", Assert.Single(ack.Entries).MessageId);
        Assert.Empty(client.ChangeInvisibleRequests);
    }

    [Fact]
    public async Task ReceiveAsync_KeepsCorruptedMessageAndLaterValidMessageInBatch()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var queue = Queue();
        var client = new FakeGrpcClient();
        client.ReceiveResponses.Add(new Proto.ReceiveMessageResponse
        {
            Message = ProtoMessage("corrupted", corrupted: true)
        });
        client.ReceiveResponses.Add(new Proto.ReceiveMessageResponse
        {
            Message = ProtoMessage("valid")
        });
        client.ReceiveResponses.Add(ReceiveStatus(Proto.Code.Ok));
        await using var engine = CreateEngine(client);
        await engine.StartAsync(cancellationToken);

        var messages = await engine.ReceiveAsync(
            queue,
            FilterExpression.All,
            2,
            TimeSpan.FromSeconds(30),
            TimeSpan.FromSeconds(5),
            false,
            cancellationToken);

        Assert.Equal(2, messages.Count);
        Assert.True(messages[0].IsCorrupted);
        Assert.Equal("corrupted", messages[0].MessageId);
        Assert.False(messages[1].IsCorrupted);
        Assert.Equal("valid", messages[1].MessageId);
        await engine.StopAsync(cancellationToken);
    }

    [Fact]
    public async Task CorruptedStandardMessageSkipsHandlerAndRequestsRedelivery()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var handlerCalls = 0;
        var client = new FakeGrpcClient();
        await using var consumer = CreateConsumer(
            client,
            (_, _) =>
            {
                Interlocked.Increment(ref handlerCalls);
                return ValueTask.FromResult(ConsumeResult.Success);
            },
            out var engine);

        await RunWorkerAsync(consumer, engine, cancellationToken, Message("corrupted", corrupted: true));

        Assert.Equal(0, Volatile.Read(ref handlerCalls));
        Assert.Empty(client.AckRequests);
        Assert.Empty(client.DeadLetterRequests);
        var retry = Assert.Single(client.ChangeInvisibleRequests);
        Assert.Equal("corrupted", retry.MessageId);
    }

    [Fact]
    public async Task CorruptedFifoMessageGoesToDeadLetterBeforeSuccessorRuns()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var handled = new ConcurrentQueue<string>();
        var client = new FakeGrpcClient();
        await using var consumer = CreateConsumer(
            client,
            (message, _) =>
            {
                handled.Enqueue(message.MessageId);
                return ValueTask.FromResult(ConsumeResult.Success);
            },
            out var engine,
            options => options.MaxConcurrency = 2);

        await RunOrderedWorkersAsync(
            consumer,
            engine,
            2,
            cancellationToken,
            Message("corrupted", "account-7", corrupted: true),
            Message("successor", "account-7"));

        Assert.Equal(["successor"], handled);
        var deadLetter = Assert.Single(client.DeadLetterRequests);
        Assert.Equal("corrupted", deadLetter.MessageId);
        var ack = Assert.Single(client.AckRequests);
        Assert.Equal("successor", Assert.Single(ack.Entries).MessageId);
        Assert.Empty(client.ChangeInvisibleRequests);
    }

    [Fact]
    public async Task RuntimeSubscriptionsResyncSettingsAndReconcileTopicReceivers()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var ordersStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var ordersCancelled = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var paymentsStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var client = new FakeGrpcClient
        {
            QueryAssignmentHandler = (request, _) => Task.FromResult(
                AssignmentResponse(Queue(topic: request.Topic.Name))),
            ReceiveHandler = async (request, _, receiverCancellationToken) =>
            {
                var topic = request.MessageQueue.Topic.Name;
                if (topic == "orders")
                {
                    ordersStarted.TrySetResult();
                }
                else if (topic == "payments")
                {
                    paymentsStarted.TrySetResult();
                }

                using var registration = receiverCancellationToken.Register(() =>
                {
                    if (topic == "orders")
                    {
                        ordersCancelled.TrySetResult();
                    }
                });
                await Task.Delay(Timeout.InfiniteTimeSpan, receiverCancellationToken);
                return [];
            }
        };
        await using var consumer = CreateConsumer(
            client,
            static (_, _) => ValueTask.FromResult(ConsumeResult.Success),
            routes: CreateRouteService(Queue()));

        await consumer.StartAsync(cancellationToken);
        await ordersStarted.Task.WaitAsync(TimeSpan.FromSeconds(3), cancellationToken);
        await consumer.SubscribeAsync(
            "payments",
            new FilterExpression("paid", FilterExpressionType.Sql),
            cancellationToken);
        await paymentsStarted.Task.WaitAsync(TimeSpan.FromSeconds(3), cancellationToken);
        await consumer.UnsubscribeAsync("orders", cancellationToken);
        await ordersCancelled.Task.WaitAsync(TimeSpan.FromSeconds(3), cancellationToken);

        var settings = client.SyncedSettings.Last();
        var subscription = Assert.Single(settings.Subscription.Subscriptions);
        Assert.Equal("payments", subscription.Topic.Name);
        Assert.Equal("paid", subscription.Expression.Expression);
        Assert.Equal(Proto.FilterType.Sql, subscription.Expression.Type);
        await consumer.StopAsync(cancellationToken);
    }

    [Fact]
    public async Task GrpcSimpleConsumerRuntimeSubscriptionsResyncActiveSession()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var client = new FakeGrpcClient();
        var options = new GrpcSimpleConsumerOptions
        {
            GroupName = "simple-tests",
            AwaitDuration = TimeSpan.FromSeconds(1),
            InvisibleDuration = TimeSpan.FromSeconds(30)
        };
        options.Subscribe("orders", new FilterExpression("created"));
        var engine = new GrpcReceiveConsumerEngine(
            client,
            CreateRouteService(Queue()),
            Options.Create(new GrpcClientOptions()),
            options.GroupName,
            options.Subscriptions,
            Proto.ClientType.SimpleConsumer,
            options.AwaitDuration,
            NullLogger<GrpcReceiveConsumerEngine>.Instance,
            NullLogger<GrpcSessionManager>.Instance);
        await using var consumer = new GrpcSimpleConsumer(
            Options.Create(options),
            engine);
        await consumer.StartAsync(cancellationToken);
        await consumer.ReceiveAsync(1, cancellationToken: cancellationToken);

        await consumer.SubscribeAsync("payments", new FilterExpression("paid"), cancellationToken);
        await consumer.UnsubscribeAsync("orders", cancellationToken);

        var settings = client.SyncedSettings.Last();
        var subscription = Assert.Single(settings.Subscription.Subscriptions);
        Assert.Equal("payments", subscription.Topic.Name);
        Assert.Equal("paid", subscription.Expression.Expression);
        await consumer.StopAsync(cancellationToken);
    }

    [Fact]
    public async Task AssignmentsReceiveIndependentlyAndRebalanceCancelsRemovedReceiver()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var queues = new[] { Queue(0), Queue(1), Queue(2) };
        var started = Enumerable.Range(0, 3)
            .Select(_ => new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously))
            .ToArray();
        var cancelled = Enumerable.Range(0, 3)
            .Select(_ => new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously))
            .ToArray();
        var receiveCalls = new int[3];
        var phase = 0;
        var client = new FakeGrpcClient
        {
            QueryAssignmentHandler = (_, _) => Task.FromResult(
                AssignmentResponse(Volatile.Read(ref phase) == 0 ? [queues[0], queues[1]] : [queues[1], queues[2]])),
            ReceiveHandler = async (request, _, receiverCancellationToken) =>
            {
                var queueId = request.MessageQueue.Id;
                Interlocked.Increment(ref receiveCalls[queueId]);
                started[queueId].TrySetResult();
                using var registration = receiverCancellationToken.Register(
                    () => cancelled[queueId].TrySetResult());
                await Task.Delay(Timeout.InfiniteTimeSpan, receiverCancellationToken);
                return [];
            }
        };
        await using var consumer = CreateConsumer(
            client,
            static (_, _) => ValueTask.FromResult(ConsumeResult.Success),
            routes: CreateRouteService(queues[0]));

        await consumer.StartAsync(cancellationToken);
        await Task.WhenAll(started[0].Task, started[1].Task)
            .WaitAsync(TimeSpan.FromSeconds(3), cancellationToken);
        Assert.Equal(1, Volatile.Read(ref receiveCalls[0]));
        Assert.Equal(1, Volatile.Read(ref receiveCalls[1]));

        Volatile.Write(ref phase, 1);
        await Task.WhenAll(cancelled[0].Task, started[2].Task)
            .WaitAsync(TimeSpan.FromSeconds(4), cancellationToken);
        Assert.False(cancelled[1].Task.IsCompleted);
        Assert.Equal(1, Volatile.Read(ref receiveCalls[1]));

        await consumer.StopAsync(cancellationToken).AsTask()
            .WaitAsync(TimeSpan.FromSeconds(3), cancellationToken);
    }

    [Fact]
    public async Task RebalanceCancellationDuringReceiveRetryIsNotARefreshFailure()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var queues = new[] { Queue(0), Queue(1) };
        var firstReceiveStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var replacementStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var phase = 0;
        var logger = new RecordingLogger();
        var client = new FakeGrpcClient
        {
            QueryAssignmentHandler = (_, _) => Task.FromResult(
                AssignmentResponse(queues[Volatile.Read(ref phase)])),
            ReceiveHandler = async (request, _, receiverCancellationToken) =>
            {
                if (request.MessageQueue.Id == 0)
                {
                    firstReceiveStarted.TrySetResult();
                    await Task.Delay(500, receiverCancellationToken);
                    throw new IOException("receive unavailable");
                }

                replacementStarted.TrySetResult();
                await Task.Delay(Timeout.InfiniteTimeSpan, receiverCancellationToken);
                return [];
            }
        };
        await using var consumer = CreateConsumer(
            client,
            static (_, _) => ValueTask.FromResult(ConsumeResult.Success),
            routes: CreateRouteService(queues[0]),
            engineLogger: logger);

        await consumer.StartAsync(cancellationToken);
        await firstReceiveStarted.Task.WaitAsync(TimeSpan.FromSeconds(3), cancellationToken);
        Volatile.Write(ref phase, 1);
        await replacementStarted.Task.WaitAsync(TimeSpan.FromSeconds(3), cancellationToken);
        await Task.Delay(100, cancellationToken);

        Assert.DoesNotContain(
            logger.Messages,
            static message => message.Contains("Failed to refresh assignments", StringComparison.Ordinal));
        await consumer.StopAsync(cancellationToken).AsTask()
            .WaitAsync(TimeSpan.FromSeconds(3), cancellationToken);
    }

    [Fact]
    public async Task OversizedMessageExclusivelyReservesByteCapacityAndReleasesAfterConsumption()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var firstStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseFirst = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var client = new FakeGrpcClient();
        await using var consumer = CreateConsumer(
            client,
            async (message, handlerCancellationToken) =>
            {
                if (message.MessageId == "12345678")
                {
                    firstStarted.TrySetResult();
                    await releaseFirst.Task.WaitAsync(handlerCancellationToken);
                }

                return ConsumeResult.Success;
            },
            out var engine,
            options => options.MaxCachedMessageBytes = 5);

        var channelField = typeof(GrpcPushConsumer).GetField("_messages", BindingFlags.Instance | BindingFlags.NonPublic);
        var processMethod = typeof(GrpcPushConsumer).GetMethod("ProcessMessagesAsync", BindingFlags.Instance | BindingFlags.NonPublic);
        var channel = Assert.IsAssignableFrom<Channel<GrpcMessageView>>(channelField?.GetValue(consumer));
        var worker = Assert.IsAssignableFrom<Task>(processMethod?.Invoke(consumer, [cancellationToken]));
        var first = Message("12345678");
        var second = Message("5678");
        engine.BindMessage(first);
        engine.BindMessage(second);

        await InvokeEnqueueAsync(consumer, first, cancellationToken);
        await firstStarted.Task.WaitAsync(TimeSpan.FromSeconds(3), cancellationToken);
        Assert.Equal(8, consumer.CachedMessageBytes);

        var secondEnqueue = InvokeEnqueueAsync(consumer, second, cancellationToken).AsTask();
        await Task.Delay(100, cancellationToken);
        Assert.False(secondEnqueue.IsCompleted);

        releaseFirst.TrySetResult();
        await secondEnqueue.WaitAsync(TimeSpan.FromSeconds(3), cancellationToken);
        channel.Writer.Complete();
        await worker.WaitAsync(TimeSpan.FromSeconds(5), cancellationToken);
        Assert.Equal(0, consumer.CachedMessageBytes);
    }

    [Fact]
    public async Task FifoCompletionFailureBlocksSuccessorAndStopCancelsBlockedGroup()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var queue = Queue();
        var finalCompletionAttemptStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseFinalCompletionFailure = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var secondHandled = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var handled = new ConcurrentQueue<string>();
        var receiveCalls = 0;
        var ackCalls = 0;
        var client = new FakeGrpcClient
        {
            QueryAssignmentHandler = (_, _) => Task.FromResult(AssignmentResponse(queue)),
            ReceiveHandler = async (_, _, receiverCancellationToken) =>
            {
                if (Interlocked.Increment(ref receiveCalls) == 1)
                {
                    return
                    [
                        new Proto.ReceiveMessageResponse { Message = ProtoMessage("first", "account-7") },
                        new Proto.ReceiveMessageResponse { Message = ProtoMessage("second", "account-7") },
                        new Proto.ReceiveMessageResponse { Message = ProtoMessage("third", "account-7") },
                        ReceiveStatus(Proto.Code.Ok)
                    ];
                }

                await Task.Delay(Timeout.InfiniteTimeSpan, receiverCancellationToken);
                return [];
            },
            AckHandler = async _ =>
            {
                if (Interlocked.Increment(ref ackCalls) == 3)
                {
                    finalCompletionAttemptStarted.TrySetResult();
                    await releaseFinalCompletionFailure.Task;
                }

                throw new IOException("ack unavailable");
            }
        };
        await using var consumer = CreateConsumer(
            client,
            (message, _) =>
            {
                handled.Enqueue(message.MessageId);
                if (message.MessageId == "second")
                {
                    secondHandled.TrySetResult();
                }

                return ValueTask.FromResult(ConsumeResult.Success);
            },
            options => options.MaxConcurrency = 1,
            CreateRouteService(queue));

        await consumer.StartAsync(cancellationToken);
        await finalCompletionAttemptStarted.Task.WaitAsync(TimeSpan.FromSeconds(3), cancellationToken);
        var stop = consumer.StopAsync(cancellationToken).AsTask();
        try
        {
            await Task.Delay(100, cancellationToken);
            Assert.False(stop.IsCompleted);
            Assert.False(secondHandled.Task.IsCompleted);
        }
        finally
        {
            releaseFinalCompletionFailure.TrySetResult();
        }

        await stop.WaitAsync(TimeSpan.FromSeconds(3), cancellationToken);

        Assert.Equal(new[] { "first" }, handled);
        Assert.Equal(3, ackCalls);
        Assert.Equal(0, consumer.CachedMessageBytes);
    }

    [Fact]
    public async Task StopInterruptsFifoRetryDelayWithoutReleasingSuccessor()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var queue = Queue();
        var firstHandled = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var secondHandled = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var receiveCalls = 0;
        var client = new FakeGrpcClient
        {
            QueryAssignmentHandler = (_, _) => Task.FromResult(AssignmentResponse(queue)),
            ReceiveHandler = async (_, _, receiverCancellationToken) =>
            {
                if (Interlocked.Increment(ref receiveCalls) == 1)
                {
                    return
                    [
                        new Proto.ReceiveMessageResponse { Message = ProtoMessage("first", "account-7") },
                        new Proto.ReceiveMessageResponse { Message = ProtoMessage("second", "account-7") },
                        ReceiveStatus(Proto.Code.Ok)
                    ];
                }

                await Task.Delay(Timeout.InfiniteTimeSpan, receiverCancellationToken);
                return [];
            }
        };
        await using var consumer = CreateConsumer(
            client,
            (message, _) =>
            {
                if (message.MessageId == "first")
                {
                    firstHandled.TrySetResult();
                    return ValueTask.FromResult(ConsumeResult.Retry);
                }

                secondHandled.TrySetResult();
                return ValueTask.FromResult(ConsumeResult.Success);
            },
            options =>
            {
                options.MaxConcurrency = 2;
                options.MaxDeliveryAttempts = 3;
                options.RetryDelay = TimeSpan.FromHours(1);
            },
            CreateRouteService(queue));

        await consumer.StartAsync(cancellationToken);
        await firstHandled.Task.WaitAsync(TimeSpan.FromSeconds(3), cancellationToken);
        await consumer.StopAsync(cancellationToken).AsTask()
            .WaitAsync(TimeSpan.FromSeconds(3), cancellationToken);

        Assert.False(secondHandled.Task.IsCompleted);
        Assert.Empty(client.AckRequests);
        Assert.Empty(client.ChangeInvisibleRequests);
        Assert.Empty(client.DeadLetterRequests);
        Assert.Equal(0, consumer.CachedMessageBytes);
    }

    [Fact]
    public async Task StopAsync_CancelsNonCooperativeFifoHandlerAndIgnoresLateSuccess()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var queue = Queue();
        var handlerStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseHandler = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var receiveCalls = 0;
        var client = new FakeGrpcClient
        {
            QueryAssignmentHandler = (_, _) => Task.FromResult(AssignmentResponse(queue)),
            ReceiveHandler = async (_, _, receiverCancellationToken) =>
            {
                if (Interlocked.Increment(ref receiveCalls) == 1)
                {
                    return
                    [
                        new Proto.ReceiveMessageResponse { Message = ProtoMessage("first", "account-7") },
                        ReceiveStatus(Proto.Code.Ok)
                    ];
                }

                await Task.Delay(Timeout.InfiniteTimeSpan, receiverCancellationToken);
                return [];
            }
        };
        var consumer = CreateConsumer(
            client,
            async (_, handlerCancellationToken) =>
            {
                handlerStarted.TrySetResult();
                await releaseHandler.Task.ConfigureAwait(false);
                using var registration = handlerCancellationToken.Register(static () => { });
                return ConsumeResult.Success;
            },
            options => options.MaxConcurrency = 1,
            CreateRouteService(queue));
        Task? worker = null;

        try
        {
            await consumer.StartAsync(cancellationToken);
            await handlerStarted.Task.WaitAsync(TimeSpan.FromSeconds(3), cancellationToken);
            var workersField = typeof(GrpcPushConsumer).GetField("_workers", BindingFlags.Instance | BindingFlags.NonPublic);
            worker = Assert.Single(Assert.IsType<Task[]>(workersField?.GetValue(consumer)));

            using var stopCancellation = new CancellationTokenSource(TimeSpan.FromMilliseconds(100));
            await Assert.ThrowsAnyAsync<OperationCanceledException>(
                () => consumer.StopAsync(stopCancellation.Token).AsTask()
                    .WaitAsync(TimeSpan.FromSeconds(3), cancellationToken));

            await consumer.StartAsync(cancellationToken);
            releaseHandler.TrySetResult();
            await Assert.ThrowsAnyAsync<OperationCanceledException>(
                () => worker.WaitAsync(TimeSpan.FromSeconds(3), cancellationToken));

            Assert.Empty(client.AckRequests);
            Assert.Empty(client.ChangeInvisibleRequests);
            Assert.Empty(client.DeadLetterRequests);

            await consumer.StopAsync(cancellationToken);
        }
        finally
        {
            releaseHandler.TrySetResult();
            await consumer.DisposeAsync();
        }
    }

    [Fact]
    public async Task ConcurrentMessageTimeout_CancelsHandlerRequestsRetryAndIgnoresLateSuccess()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var handlerStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var handlerCancellationRequested = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var handlerFinished = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseHandler = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var operation = new Mock<IGrpcRocketMQTelemetryOperation>(MockBehavior.Strict);
        operation.Setup(value => value.Complete(false, "timeout"));
        operation.Setup(value => value.Dispose());
        var telemetry = new Mock<IGrpcRocketMQTelemetry>(MockBehavior.Strict);
        telemetry
            .Setup(value => value.StartProcess(
                It.IsAny<string>(),
                It.IsAny<string>(),
                It.IsAny<string>(),
                It.IsAny<int>(),
                It.IsAny<long>(),
                It.IsAny<IReadOnlyDictionary<string, string>>(),
                It.IsAny<ActivityContext?>()))
            .Returns(operation.Object);
        var client = new FakeGrpcClient();
        await using var consumer = CreateConsumer(
            client,
            async (_, handlerCancellationToken) =>
            {
                handlerStarted.TrySetResult();
                using var registration = handlerCancellationToken.Register(
                    () => handlerCancellationRequested.TrySetResult());
                using var failingRegistration = handlerCancellationToken.Register(
                    static () => throw new InvalidOperationException("The test cancellation callback failed."));
                await releaseHandler.Task.ConfigureAwait(false);
                handlerFinished.TrySetResult();
                return ConsumeResult.Success;
            },
            out var engine,
            options =>
            {
                options.ConsumeTimeout = TimeSpan.FromMilliseconds(100);
                options.RetryDelay = TimeSpan.FromSeconds(3);
            },
            telemetry: telemetry.Object);

        var channelField = typeof(GrpcPushConsumer).GetField("_messages", BindingFlags.Instance | BindingFlags.NonPublic);
        var processMethod = typeof(GrpcPushConsumer).GetMethod("ProcessMessagesAsync", BindingFlags.Instance | BindingFlags.NonPublic);
        var channel = Assert.IsAssignableFrom<Channel<GrpcMessageView>>(channelField?.GetValue(consumer));
        var worker = Assert.IsAssignableFrom<Task>(processMethod?.Invoke(consumer, [cancellationToken]));
        var message = Message("timeout");
        engine.BindMessage(message);
        await channel.Writer.WriteAsync(message, cancellationToken);
        channel.Writer.Complete();

        try
        {
            await handlerStarted.Task.WaitAsync(TimeSpan.FromSeconds(3), cancellationToken);
            await handlerCancellationRequested.Task.WaitAsync(TimeSpan.FromSeconds(3), cancellationToken);
            await worker.WaitAsync(TimeSpan.FromSeconds(3), cancellationToken);

            Assert.Empty(client.AckRequests);
            var retry = Assert.Single(client.ChangeInvisibleRequests);
            Assert.Equal(TimeSpan.FromSeconds(3), retry.InvisibleDuration.ToTimeSpan());
        }
        finally
        {
            releaseHandler.TrySetResult();
        }

        await handlerFinished.Task.WaitAsync(TimeSpan.FromSeconds(3), cancellationToken);
        Assert.Empty(client.AckRequests);
        operation.Verify(value => value.Complete(false, "timeout"), Times.Once);
        operation.Verify(value => value.Dispose(), Times.Once);
        telemetry.VerifyAll();
    }

    [Fact]
    public async Task Worker_RetriesAckAfterTransientFailureAndContinues()
    {
        var client = new FakeGrpcClient();
        var calls = 0;
        client.AckHandler = _ => Interlocked.Increment(ref calls) == 1
            ? Task.FromException<Proto.AckMessageResponse>(new IOException("ack failed"))
            : Task.FromResult(AckSuccess());
        await using var consumer = CreateConsumer(
            client,
            static (_, _) => ValueTask.FromResult(ConsumeResult.Success),
            out var engine);

        await RunWorkerAsync(
            consumer,
            engine,
            TestContext.Current.CancellationToken,
            Message("first"),
            Message("second"));

        Assert.Equal(3, calls);
    }

    [Fact]
    public async Task Worker_RetriesChangeInvisibleAfterTransientFailureAndContinues()
    {
        var client = new FakeGrpcClient();
        var calls = 0;
        var handlerCalls = 0;
        client.ChangeInvisibleHandler = _ => Interlocked.Increment(ref calls) == 1
            ? Task.FromException<Proto.ChangeInvisibleDurationResponse>(new IOException("change invisible failed"))
            : Task.FromResult(ChangeInvisibleSuccess());
        await using var consumer = CreateConsumer(client, (_, _) =>
        {
            Interlocked.Increment(ref handlerCalls);
            return ValueTask.FromResult(ConsumeResult.Retry);
        }, out var engine);

        await RunWorkerAsync(
            consumer,
            engine,
            TestContext.Current.CancellationToken,
            Message("first"),
            Message("second"));

        Assert.Equal(3, calls);
        Assert.Equal(2, handlerCalls);
        Assert.Equal(3, client.ChangeInvisibleRequests.Count);
        Assert.Empty(client.AckRequests);
        Assert.Empty(client.DeadLetterRequests);
    }

    [Fact]
    public async Task Worker_RecordsRetryAsFailedProcessTelemetry()
    {
        var operation = new Mock<IGrpcRocketMQTelemetryOperation>(MockBehavior.Strict);
        operation.Setup(value => value.Complete(false, ConsumeResult.Retry.ToString()));
        operation.Setup(value => value.Dispose());
        var telemetry = new Mock<IGrpcRocketMQTelemetry>(MockBehavior.Strict);
        telemetry
            .Setup(value => value.StartProcess(
                It.IsAny<string>(),
                It.IsAny<string>(),
                It.IsAny<string>(),
                It.IsAny<int>(),
                It.IsAny<long>(),
                It.IsAny<IReadOnlyDictionary<string, string>>(),
                It.IsAny<ActivityContext?>()))
            .Returns(operation.Object);
        var client = new FakeGrpcClient();
        await using var consumer = CreateConsumer(
            client,
            static (_, _) => ValueTask.FromResult(ConsumeResult.Retry),
            out var engine,
            telemetry: telemetry.Object);

        await RunWorkerAsync(
            consumer,
            engine,
            TestContext.Current.CancellationToken,
            Message("message-1"));

        operation.Verify(value => value.Complete(false, ConsumeResult.Retry.ToString()), Times.Once);
        operation.Verify(value => value.Dispose(), Times.Once);
        telemetry.VerifyAll();
    }

    [Fact]
    public async Task Worker_RetriesDeadLetterAfterTransientFailureAndContinues()
    {
        var client = new FakeGrpcClient();
        var calls = 0;
        client.DeadLetterHandler = _ => Interlocked.Increment(ref calls) == 1
            ? Task.FromException<Proto.ForwardMessageToDeadLetterQueueResponse>(new IOException("dead letter failed"))
            : Task.FromResult(DeadLetterSuccess());
        await using var consumer = CreateConsumer(
            client,
            static (_, _) => ValueTask.FromResult(ConsumeResult.DeadLetter),
            out var engine);

        await RunWorkerAsync(
            consumer,
            engine,
            TestContext.Current.CancellationToken,
            Message("first"),
            Message("second"));

        Assert.Equal(3, calls);
    }

    [Fact]
    public async Task Worker_AcknowledgesAndContinuesAfterRenewalFailure()
    {
        var renewalAttempted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var client = new FakeGrpcClient
        {
            ChangeInvisibleHandler = _ =>
            {
                renewalAttempted.TrySetResult();
                return Task.FromException<Proto.ChangeInvisibleDurationResponse>(new IOException("renewal failed"));
            }
        };
        var ackCalls = 0;
        client.AckHandler = _ =>
        {
            Interlocked.Increment(ref ackCalls);
            return Task.FromResult(AckSuccess());
        };
        await using var consumer = CreateConsumer(
            client,
            async (message, cancellationToken) =>
            {
                if (message.MessageId == "first")
                {
                    await renewalAttempted.Task.WaitAsync(TimeSpan.FromSeconds(3), cancellationToken);
                }

                return ConsumeResult.Success;
            },
            out var engine,
            options => options.InvisibleDuration = TimeSpan.FromSeconds(2));

        await RunWorkerAsync(
            consumer,
            engine,
            TestContext.Current.CancellationToken,
            Message("first"),
            Message("second"));

        Assert.Equal(2, ackCalls);
    }

    [Fact]
    public async Task BrokerSettingsControlReceptionAndRetryPolicy()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var queue = Queue();
        var customizedBackoff = new Proto.CustomizedBackoff();
        customizedBackoff.Next.Add(Duration.FromTimeSpan(TimeSpan.FromSeconds(1)));
        customizedBackoff.Next.Add(Duration.FromTimeSpan(TimeSpan.FromSeconds(5)));
        var client = new FakeGrpcClient
        {
            ServerSettings = new Proto.Settings
            {
                RequestTimeout = Duration.FromTimeSpan(TimeSpan.FromSeconds(2)),
                Subscription = new Proto.Subscription
                {
                    ReceiveBatchSize = 7,
                    LongPollingTimeout = Duration.FromTimeSpan(TimeSpan.FromSeconds(6))
                },
                BackoffPolicy = new Proto.RetryPolicy
                {
                    MaxAttempts = 4,
                    CustomizedBackoff = customizedBackoff
                }
            }
        };
        client.Assignments.Add(new Proto.Assignment { MessageQueue = queue });
        client.ReceiveResponses.Add(new Proto.ReceiveMessageResponse
        {
            Message = ProtoMessage(
                "broker-settings",
                deliveryAttempt: 2,
                invisibleDuration: TimeSpan.FromSeconds(9),
                liteTopic: "lite-orders")
        });
        client.ReceiveResponses.Add(ReceiveStatus(Proto.Code.Ok));

        var options = PushOptions(static (_, _) => ValueTask.FromResult(ConsumeResult.Success));
        await using var engine = new GrpcReceiveConsumerEngine(
            client,
            CreateRouteService(queue),
            Options.Create(new GrpcClientOptions()),
            options.GroupName,
            options.Subscriptions,
            Proto.ClientType.PushConsumer,
            options.LongPollingTimeout,
            NullLogger<GrpcReceiveConsumerEngine>.Instance,
            NullLogger<GrpcSessionManager>.Instance);

        await engine.StartAsync(cancellationToken);
        var assignments = await engine.GetAssignmentsAsync("orders", cancellationToken);
        var messages = await engine.ReceiveAsync(
            Assert.Single(assignments).MessageQueue,
            FilterExpression.All,
            32,
            TimeSpan.FromSeconds(30),
            TimeSpan.FromSeconds(15),
            true,
            cancellationToken);

        var request = Assert.IsType<Proto.ReceiveMessageRequest>(client.LastReceiveRequest);
        Assert.Equal(client.AccessPoint, client.LastQueryAssignmentEndpoint);
        Assert.NotEqual(new Uri("http://127.0.0.1:8081"), client.LastQueryAssignmentEndpoint);
        Assert.Equal(7, request.BatchSize);
        Assert.Equal(TimeSpan.FromSeconds(6), request.LongPollingTimeout.ToTimeSpan());
        Assert.True(request.HasAttemptId);
        Assert.Equal(TimeSpan.FromSeconds(9), client.LastReceiveTimeout);
        Assert.Equal(TimeSpan.FromSeconds(15), Assert.Single(client.OpenSettings).Subscription.LongPollingTimeout.ToTimeSpan());
        var message = Assert.Single(messages);
        Assert.Equal(TimeSpan.FromSeconds(9), message.InvisibleDuration);
        Assert.Equal("lite-orders", message.LiteTopic);
        Assert.Equal(4, engine.GetMaxDeliveryAttempts(message, 16));
        Assert.Equal(TimeSpan.FromSeconds(5), engine.GetRetryDelay(message, TimeSpan.FromSeconds(10)));

        await engine.StopAsync(cancellationToken);
    }

    [Fact]
    public async Task CompletionRequestsPreserveLiteTopicAndUpdatedInvisibility()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var client = new FakeGrpcClient
        {
            ChangeInvisibleHandler = static _ => Task.FromResult(new Proto.ChangeInvisibleDurationResponse
            {
                Status = new Proto.Status { Code = Proto.Code.Ok },
                ReceiptHandle = "receipt-updated"
            })
        };
        var message = Message("lite-message", liteTopic: "lite-orders");
        await using var engine = CreateEngine(client);
        engine.BindMessage(message);

        await engine.AckAsync(message, cancellationToken);
        await engine.ChangeInvisibleDurationAsync(message, TimeSpan.FromSeconds(12), cancellationToken);
        await engine.ForwardToDeadLetterQueueAsync(message, 8, cancellationToken);

        Assert.Equal("lite-orders", Assert.Single(client.AckRequests).Entries[0].LiteTopic);
        Assert.Equal("lite-orders", Assert.Single(client.ChangeInvisibleRequests).LiteTopic);
        Assert.Equal("lite-orders", Assert.Single(client.DeadLetterRequests).LiteTopic);
        Assert.Equal("receipt-updated", message.ReceiptHandle);
        Assert.Equal(TimeSpan.FromSeconds(12), message.InvisibleDuration);
    }

    [Fact]
    public async Task CompletionRetriesAreBoundedAndCancellationAware()
    {
        var client = new FakeGrpcClient
        {
            AckHandler = static _ => Task.FromException<Proto.AckMessageResponse>(new IOException("unavailable"))
        };
        await using var engine = CreateEngine(client);
        var bounded = Message("bounded");
        engine.BindMessage(bounded);

        await Assert.ThrowsAsync<IOException>(() =>
            engine.AckAsync(bounded, TestContext.Current.CancellationToken));
        Assert.Equal(3, client.AckRequests.Count);

        using var cancellation = new CancellationTokenSource(TimeSpan.FromMilliseconds(50));
        var cancelled = Message("cancelled");
        engine.BindMessage(cancelled);
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() =>
            engine.AckAsync(cancelled, cancellation.Token));
        Assert.Equal(4, client.AckRequests.Count);
    }

    [Fact]
    public async Task CompletionDoesNotRetryPermanentBrokerFailure()
    {
        var client = new FakeGrpcClient
        {
            AckHandler = static _ => Task.FromResult(new Proto.AckMessageResponse
            {
                Status = new Proto.Status { Code = Proto.Code.InvalidReceiptHandle }
            })
        };
        await using var engine = CreateEngine(client);
        var message = Message("permanent");
        engine.BindMessage(message);

        await Assert.ThrowsAsync<GrpcServiceException>(() =>
            engine.AckAsync(message, TestContext.Current.CancellationToken));
        Assert.Single(client.AckRequests);
    }

    [Fact]
    public async Task ConsumerEngineCanRestartAfterStop()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var queue = Queue();
        var client = new FakeGrpcClient();
        var options = PushOptions(static (_, _) => ValueTask.FromResult(ConsumeResult.Success));
        await using var engine = new GrpcReceiveConsumerEngine(
            client,
            CreateRouteService(queue),
            Options.Create(new GrpcClientOptions()),
            options.GroupName,
            options.Subscriptions,
            Proto.ClientType.PushConsumer,
            options.LongPollingTimeout,
            NullLogger<GrpcReceiveConsumerEngine>.Instance,
            NullLogger<GrpcSessionManager>.Instance);

        await engine.StartAsync(cancellationToken);
        await engine.GetAssignmentsAsync("orders", cancellationToken);
        await engine.StopAsync(cancellationToken);
        await engine.StartAsync(cancellationToken);
        await engine.GetAssignmentsAsync("orders", cancellationToken);
        await engine.StopAsync(cancellationToken);

        Assert.Equal(2, client.OpenTelemetryCalls);
    }

    [Fact]
    public async Task PushConsumerCanRestartAfterStop()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var client = new FakeGrpcClient();
        await using var consumer = CreateConsumer(
            client,
            static (_, _) => ValueTask.FromResult(ConsumeResult.Success),
            routes: CreateRouteService(Queue()));

        await consumer.StartAsync(cancellationToken);
        await consumer.StopAsync(cancellationToken);
        await consumer.StartAsync(cancellationToken);
        await consumer.StopAsync(cancellationToken);

        Assert.Equal(2, client.OpenTelemetryCalls);
    }

    [Fact]
    public async Task GrpcSimpleConsumerReceiveOmitsLongPollingTimeoutFromRequest()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var queue = Queue();
        var client = new FakeGrpcClient
        {
            ReceiveHandler = static (_, _, _) =>
                Task.FromResult<IReadOnlyList<Proto.ReceiveMessageResponse>>(
                    [ReceiveStatus(Proto.Code.MessageNotFound, "no new message")])
        };
        var options = new GrpcSimpleConsumerOptions
        {
            GroupName = "tests",
            AwaitDuration = TimeSpan.FromSeconds(5),
            InvisibleDuration = TimeSpan.FromSeconds(30)
        };
        options.Subscribe("orders");
        await using var engine = new GrpcReceiveConsumerEngine(
            client,
            CreateUnusedRouteService(),
            Options.Create(new GrpcClientOptions { RequestTimeout = TimeSpan.FromSeconds(2) }),
            options.GroupName,
            options.Subscriptions,
            Proto.ClientType.SimpleConsumer,
            options.AwaitDuration,
            NullLogger<GrpcReceiveConsumerEngine>.Instance,
            NullLogger<GrpcSessionManager>.Instance);

        await engine.StartAsync(cancellationToken);
        var messages = await engine.ReceiveAsync(
            queue,
            FilterExpression.All,
            32,
            options.InvisibleDuration,
            options.AwaitDuration,
            false,
            cancellationToken);

        var request = Assert.IsType<Proto.ReceiveMessageRequest>(client.LastReceiveRequest);
        Assert.Empty(messages);
        Assert.Null(request.LongPollingTimeout);
        Assert.False(request.HasAttemptId);
        Assert.Equal(TimeSpan.FromSeconds(7), client.LastReceiveTimeout);
        await engine.StopAsync(cancellationToken);
    }

    private static GrpcPushConsumer CreateConsumer(
        FakeGrpcClient client,
        Func<GrpcMessageView, CancellationToken, ValueTask<ConsumeResult>> handler,
        Action<GrpcPushConsumerOptions>? configure = null,
        IGrpcRouteService? routes = null,
        ILogger<GrpcReceiveConsumerEngine>? engineLogger = null,
        IGrpcRocketMQTelemetry? telemetry = null)
    {
        return CreateConsumer(client, handler, out _, configure, routes, engineLogger, telemetry);
    }

    private static GrpcPushConsumer CreateConsumer(
        FakeGrpcClient client,
        Func<GrpcMessageView, CancellationToken, ValueTask<ConsumeResult>> handler,
        out GrpcReceiveConsumerEngine engine,
        Action<GrpcPushConsumerOptions>? configure = null,
        IGrpcRouteService? routes = null,
        ILogger<GrpcReceiveConsumerEngine>? engineLogger = null,
        IGrpcRocketMQTelemetry? telemetry = null)
    {
        var options = PushOptions(handler);
        configure?.Invoke(options);
        engine = new GrpcReceiveConsumerEngine(
            client,
            routes ?? CreateUnusedRouteService(),
            Options.Create(new GrpcClientOptions()),
            options.GroupName,
            options.Subscriptions,
            Proto.ClientType.PushConsumer,
            options.LongPollingTimeout,
            engineLogger ?? NullLogger<GrpcReceiveConsumerEngine>.Instance,
            NullLogger<GrpcSessionManager>.Instance);
        return new GrpcPushConsumer(
            Options.Create(options),
            engine,
            NullLogger<GrpcPushConsumer>.Instance,
            telemetry: telemetry);
    }

    private static GrpcReceiveConsumerEngine CreateEngine(FakeGrpcClient client)
    {
        var options = PushOptions(static (_, _) => ValueTask.FromResult(ConsumeResult.Success));
        return new GrpcReceiveConsumerEngine(
            client,
            CreateUnusedRouteService(),
            Options.Create(new GrpcClientOptions()),
            options.GroupName,
            options.Subscriptions,
            Proto.ClientType.PushConsumer,
            options.LongPollingTimeout,
            NullLogger<GrpcReceiveConsumerEngine>.Instance,
            NullLogger<GrpcSessionManager>.Instance);
    }

    private static IGrpcRouteService CreateRouteService(Proto.MessageQueue queue)
    {
        var routeService = new Mock<IGrpcRouteService>(MockBehavior.Strict);
        routeService
            .Setup(service => service.GetAsync(
                It.IsAny<string>(),
                It.IsAny<bool>(),
                It.IsAny<CancellationToken>()))
            .Returns(() => Task.FromResult<IReadOnlyList<Proto.MessageQueue>>([queue.Clone()]));
        return routeService.Object;
    }

    private static IGrpcRouteService CreateUnusedRouteService() =>
        new Mock<IGrpcRouteService>(MockBehavior.Strict).Object;

    private static GrpcPushConsumerOptions PushOptions(
        Func<GrpcMessageView, CancellationToken, ValueTask<ConsumeResult>> handler)
    {
        var options = new GrpcPushConsumerOptions
        {
            GroupName = "tests",
            MaxConcurrency = 1,
            MaxCachedMessages = 8,
            InvisibleDuration = TimeSpan.FromHours(1),
            MessageHandler = handler
        };
        options.Subscribe("orders");
        return options;
    }

    private static async Task RunWorkerAsync(
        GrpcPushConsumer consumer,
        GrpcReceiveConsumerEngine engine,
        CancellationToken cancellationToken,
        params GrpcMessageView[] messages)
    {
        var channelField = typeof(GrpcPushConsumer).GetField("_messages", BindingFlags.Instance | BindingFlags.NonPublic);
        var processMethod = typeof(GrpcPushConsumer).GetMethod("ProcessMessagesAsync", BindingFlags.Instance | BindingFlags.NonPublic);
        var channel = Assert.IsAssignableFrom<Channel<GrpcMessageView>>(channelField?.GetValue(consumer));
        var worker = Assert.IsAssignableFrom<Task>(processMethod?.Invoke(consumer, [cancellationToken]));
        foreach (var message in messages)
        {
            engine.BindMessage(message);
            await channel.Writer.WriteAsync(message, cancellationToken);
        }

        channel.Writer.Complete();
        await worker.WaitAsync(TimeSpan.FromSeconds(5), cancellationToken);
    }

    private static async Task RunOrderedWorkersAsync(
        GrpcPushConsumer consumer,
        GrpcReceiveConsumerEngine engine,
        int workerCount,
        CancellationToken cancellationToken,
        params GrpcMessageView[] messages)
    {
        var channelField = typeof(GrpcPushConsumer).GetField("_messages", BindingFlags.Instance | BindingFlags.NonPublic);
        var processMethod = typeof(GrpcPushConsumer).GetMethod("ProcessMessagesAsync", BindingFlags.Instance | BindingFlags.NonPublic);
        var channel = Assert.IsAssignableFrom<Channel<GrpcMessageView>>(channelField?.GetValue(consumer));
        var workers = Enumerable.Range(0, workerCount)
            .Select(_ => Assert.IsAssignableFrom<Task>(processMethod?.Invoke(consumer, [cancellationToken])))
            .ToArray();
        foreach (var message in messages)
        {
            engine.BindMessage(message);
            await InvokeEnqueueAsync(consumer, message, cancellationToken);
        }

        channel.Writer.Complete();
        await Task.WhenAll(workers).WaitAsync(TimeSpan.FromSeconds(5), cancellationToken);
    }

    private static ValueTask InvokeEnqueueAsync(
        GrpcPushConsumer consumer,
        GrpcMessageView message,
        CancellationToken cancellationToken)
    {
        var enqueueMethod = typeof(GrpcPushConsumer).GetMethod("EnqueueAsync", BindingFlags.Instance | BindingFlags.NonPublic);
        return Assert.IsType<ValueTask>(enqueueMethod?.Invoke(consumer, [message, cancellationToken]));
    }

    private static GrpcMessageView Message(
        string id,
        string? messageGroup = null,
        int deliveryAttempt = 1,
        TimeSpan? invisibleDuration = null,
        string? liteTopic = null,
        bool corrupted = false) =>
        GrpcMessageViewFactory.Create(
            ProtoMessage(id, messageGroup, deliveryAttempt, invisibleDuration, liteTopic, corrupted),
            Queue(),
            new Uri("http://127.0.0.1:8081"));

    private static Proto.MessageQueue Queue(int id = 0, string topic = "orders")
    {
        var endpoints = new Proto.Endpoints
        {
            Scheme = Proto.AddressScheme.Ipv4,
            Addresses = { new Proto.Address { Host = "127.0.0.1", Port = 8081 } }
        };
        return new Proto.MessageQueue
        {
            Topic = new Proto.Resource { Name = topic },
            Id = id,
            Permission = Proto.Permission.ReadWrite,
            Broker = new Proto.Broker { Name = "broker-a", Endpoints = endpoints }
        };
    }

    private static Proto.Message ProtoMessage(
        string id,
        string? messageGroup = null,
        int deliveryAttempt = 1,
        TimeSpan? invisibleDuration = null,
        string? liteTopic = null,
        bool corrupted = false)
    {
        var systemProperties = new Proto.SystemProperties
        {
            MessageId = id,
            ReceiptHandle = $"receipt-{id}",
            DeliveryAttempt = deliveryAttempt
        };
        if (messageGroup is not null)
        {
            systemProperties.MessageGroup = messageGroup;
        }

        if (invisibleDuration is { } duration)
        {
            systemProperties.InvisibleDuration = Duration.FromTimeSpan(duration);
        }

        if (liteTopic is not null)
        {
            systemProperties.LiteTopic = liteTopic;
        }

        if (corrupted)
        {
            systemProperties.BodyDigest = new Proto.Digest
            {
                Type = Proto.DigestType.Crc32,
                Checksum = "00000000"
            };
        }

        return new Proto.Message
        {
            Topic = new Proto.Resource { Name = "orders" },
            Body = ByteString.CopyFromUtf8(id),
            SystemProperties = systemProperties
        };
    }

    private static Proto.AckMessageResponse AckSuccess() => new()
    {
        Status = new Proto.Status { Code = Proto.Code.Ok }
    };

    private static Proto.ChangeInvisibleDurationResponse ChangeInvisibleSuccess() => new()
    {
        Status = new Proto.Status { Code = Proto.Code.Ok }
    };

    private static Proto.ForwardMessageToDeadLetterQueueResponse DeadLetterSuccess() => new()
    {
        Status = new Proto.Status { Code = Proto.Code.Ok }
    };

    private static Proto.ReceiveMessageResponse ReceiveStatus(Proto.Code code, string message = "") => new()
    {
        Status = new Proto.Status { Code = code, Message = message }
    };

    private static Proto.QueryAssignmentResponse AssignmentResponse(params Proto.MessageQueue[] queues)
    {
        var response = new Proto.QueryAssignmentResponse
        {
            Status = new Proto.Status { Code = Proto.Code.Ok }
        };
        response.Assignments.AddRange(queues.Select(static queue => new Proto.Assignment
        {
            MessageQueue = queue.Clone()
        }));
        return response;
    }

    private sealed class FakeGrpcClient : IRocketMQGrpcClient
    {
        private int _openTelemetryCalls;

        public Func<Proto.AckMessageRequest, Task<Proto.AckMessageResponse>> AckHandler { get; set; } =
            static _ => Task.FromResult(AckSuccess());

        public Func<Proto.ChangeInvisibleDurationRequest, Task<Proto.ChangeInvisibleDurationResponse>> ChangeInvisibleHandler { get; set; } =
            static _ => Task.FromResult(ChangeInvisibleSuccess());

        public Func<Proto.ForwardMessageToDeadLetterQueueRequest, Task<Proto.ForwardMessageToDeadLetterQueueResponse>> DeadLetterHandler { get; set; } =
            static _ => Task.FromResult(DeadLetterSuccess());

        public Func<Proto.QueryAssignmentRequest, CancellationToken, Task<Proto.QueryAssignmentResponse>>? QueryAssignmentHandler { get; set; }

        public Func<Proto.ReceiveMessageRequest, TimeSpan, CancellationToken, Task<IReadOnlyList<Proto.ReceiveMessageResponse>>>? ReceiveHandler { get; set; }

        public Uri AccessPoint { get; } = new("http://127.0.0.1:8080");
        public Proto.Endpoints AccessPointProtobuf { get; } = new();
        public Proto.Settings? ServerSettings { get; set; }
        public List<Proto.Assignment> Assignments { get; } = [];
        public List<Proto.ReceiveMessageResponse> ReceiveResponses { get; } = [];
        public List<Proto.Settings> OpenSettings { get; } = [];
        public ConcurrentQueue<Proto.Settings> SyncedSettings { get; } = [];
        public ConcurrentQueue<Proto.AckMessageRequest> AckRequests { get; } = [];
        public ConcurrentQueue<Proto.ChangeInvisibleDurationRequest> ChangeInvisibleRequests { get; } = [];
        public ConcurrentQueue<Proto.ForwardMessageToDeadLetterQueueRequest> DeadLetterRequests { get; } = [];
        public Proto.ReceiveMessageRequest? LastReceiveRequest { get; private set; }
        public Uri? LastQueryAssignmentEndpoint { get; private set; }
        public TimeSpan LastReceiveTimeout { get; private set; }
        public int OpenTelemetryCalls => Volatile.Read(ref _openTelemetryCalls);

        public Task<Proto.AckMessageResponse> AckMessageAsync(Uri endpoint, Proto.AckMessageRequest request, CancellationToken cancellationToken)
        {
            AckRequests.Enqueue(request.Clone());
            return AckHandler(request);
        }

        public Task<Proto.ChangeInvisibleDurationResponse> ChangeInvisibleDurationAsync(Uri endpoint, Proto.ChangeInvisibleDurationRequest request, CancellationToken cancellationToken)
        {
            ChangeInvisibleRequests.Enqueue(request.Clone());
            return ChangeInvisibleHandler(request);
        }

        public Task<Proto.ForwardMessageToDeadLetterQueueResponse> ForwardToDeadLetterQueueAsync(Uri endpoint, Proto.ForwardMessageToDeadLetterQueueRequest request, CancellationToken cancellationToken)
        {
            DeadLetterRequests.Enqueue(request.Clone());
            return DeadLetterHandler(request);
        }

        public Task<Proto.QueryRouteResponse> QueryRouteAsync(Proto.QueryRouteRequest request, CancellationToken cancellationToken) => Throw<Proto.QueryRouteResponse>();
        public Task<Proto.SendMessageResponse> SendMessageAsync(Uri endpoint, Proto.SendMessageRequest request, TimeSpan timeout, CancellationToken cancellationToken) => Throw<Proto.SendMessageResponse>();
        public Task<Proto.QueryAssignmentResponse> QueryAssignmentAsync(Uri endpoint, Proto.QueryAssignmentRequest request, CancellationToken cancellationToken)
        {
            LastQueryAssignmentEndpoint = endpoint;
            if (QueryAssignmentHandler is not null)
            {
                return QueryAssignmentHandler(request, cancellationToken);
            }

            var response = new Proto.QueryAssignmentResponse { Status = new Proto.Status { Code = Proto.Code.Ok } };
            response.Assignments.AddRange(Assignments.Select(static assignment => assignment.Clone()));
            return Task.FromResult(response);
        }

        public Task<IReadOnlyList<Proto.ReceiveMessageResponse>> ReceiveMessageAsync(Uri endpoint, Proto.ReceiveMessageRequest request, TimeSpan timeout, CancellationToken cancellationToken)
        {
            LastReceiveRequest = request.Clone();
            LastReceiveTimeout = timeout;
            if (ReceiveHandler is not null)
            {
                return ReceiveHandler(request, timeout, cancellationToken);
            }

            return Task.FromResult<IReadOnlyList<Proto.ReceiveMessageResponse>>(
                ReceiveResponses.Select(static response => response.Clone()).ToArray());
        }
        public Task<Proto.EndTransactionResponse> EndTransactionAsync(Uri endpoint, Proto.EndTransactionRequest request, CancellationToken cancellationToken) => Throw<Proto.EndTransactionResponse>();
        public Task<Proto.HeartbeatResponse> HeartbeatAsync(Uri endpoint, Proto.HeartbeatRequest request, CancellationToken cancellationToken) =>
            Task.FromResult(new Proto.HeartbeatResponse { Status = new Proto.Status { Code = Proto.Code.Ok } });

        public Task<Proto.NotifyClientTerminationResponse> NotifyTerminationAsync(Uri endpoint, Proto.NotifyClientTerminationRequest request, CancellationToken cancellationToken) =>
            Task.FromResult(new Proto.NotifyClientTerminationResponse { Status = new Proto.Status { Code = Proto.Code.Ok } });
        public Task<Proto.RecallMessageResponse> RecallMessageAsync(Uri endpoint, Proto.RecallMessageRequest request, CancellationToken cancellationToken) => Throw<Proto.RecallMessageResponse>();
        public Task<Proto.SyncLiteSubscriptionResponse> SyncLiteSubscriptionAsync(Uri endpoint, Proto.SyncLiteSubscriptionRequest request, CancellationToken cancellationToken) => Throw<Proto.SyncLiteSubscriptionResponse>();
        public Task<IGrpcTelemetrySession> OpenTelemetryAsync(
            Uri endpoint,
            Proto.Settings settings,
            Func<Proto.TelemetryCommand, CancellationToken, Task>? commandHandler,
            CancellationToken cancellationToken)
        {
            Interlocked.Increment(ref _openTelemetryCalls);
            OpenSettings.Add(settings.Clone());
            return Task.FromResult(CreateTelemetrySession(
                ServerSettings?.Clone(),
                SyncedSettings));
        }
        public ValueTask DisposeAsync() => ValueTask.CompletedTask;

        private static Task<T> Throw<T>() => Task.FromException<T>(new NotSupportedException());
    }

    private static IGrpcTelemetrySession CreateTelemetrySession(
        Proto.Settings? serverSettings,
        ConcurrentQueue<Proto.Settings> syncedSettings)
    {
        var completion = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var session = new Mock<IGrpcTelemetrySession>(MockBehavior.Strict);
        session.SetupGet(value => value.ServerSettings).Returns(serverSettings);
        session.SetupGet(value => value.Completion).Returns(completion.Task);
        session
            .Setup(value => value.WriteSettingsAsync(
                It.IsAny<Proto.Settings>(),
                It.IsAny<CancellationToken>()))
            .Callback<Proto.Settings, CancellationToken>((settings, _) =>
                syncedSettings.Enqueue(settings.Clone()))
            .Returns(Task.CompletedTask);
        session
            .Setup(value => value.WriteCommandAsync(
                It.IsAny<Proto.TelemetryCommand>(),
                It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask);
        session
            .Setup(value => value.DisposeAsync())
            .Callback(() => completion.TrySetResult())
            .Returns(ValueTask.CompletedTask);
        return session.Object;
    }

    private sealed class RecordingLogger : ILogger<GrpcReceiveConsumerEngine>
    {
        public ConcurrentQueue<string> Messages { get; } = [];
        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;
        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(
            LogLevel logLevel,
            EventId eventId,
            TState state,
            Exception? exception,
            Func<TState, Exception?, string> formatter) =>
            Messages.Enqueue(formatter(state, exception));
    }
}
