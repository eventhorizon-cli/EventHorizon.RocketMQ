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
using EventHorizon.RocketMQ.Remoting.Consumer.Pull;
using EventHorizon.RocketMQ.Remoting.Consumer.Push;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace EventHorizon.RocketMQ.Remoting.Tests.Consumer.Push;

public sealed class ConcurrentConsumeDispatcherTests
{
    [Fact]
    public async Task RotatesActiveProcessQueuesWithOneConsumeLoop()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        using var stopping = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        var first = CreateProcessQueue(queueId: 0, messageCount: 3, stopping.Token);
        var second = CreateProcessQueue(queueId: 1, messageCount: 1, stopping.Token);
        var order = new ConcurrentQueue<int>();
        var allConsumed = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        using var firstDequeued = new ManualResetEventSlim();
        using var allowFirstRequeue = new ManualResetEventSlim();
        var releasedRequests = 0;
        var consumed = 0;
        var dispatcher = new ConcurrentConsumeDispatcher(
            maxConcurrency: 1,
            consumeMessageBatchSize: 1,
            (request, _) =>
            {
                order.Enqueue(request.ProcessQueue.MessageQueue.QueueId);
                request.ProcessQueue.CompleteConsumeRequest(request, [true]);
                if (Interlocked.Increment(ref consumed) == 4)
                {
                    allConsumed.TrySetResult();
                }

                return ValueTask.CompletedTask;
            },
            _ =>
            {
                if (Interlocked.Increment(ref releasedRequests) == 1)
                {
                    firstDequeued.Set();
                    Assert.True(allowFirstRequeue.Wait(TimeSpan.FromSeconds(3), cancellationToken));
                }
            },
            stopping.Token,
            NullLogger.Instance);

        dispatcher.NotifyReady(first);
        Assert.True(firstDequeued.Wait(TimeSpan.FromSeconds(3), cancellationToken));
        dispatcher.NotifyReady(second);
        allowFirstRequeue.Set();
        await allConsumed.Task.WaitAsync(TimeSpan.FromSeconds(3), cancellationToken);
        dispatcher.Complete();
        await Task.WhenAll(dispatcher.ConsumeLoopTasks).WaitAsync(cancellationToken);

        Assert.Equal([0, 1, 0], order.Take(3));
    }

    [Fact]
    public async Task DoesNotInvokeConsumeCallbackForARequestDroppedBeforeItStarts()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        using var stopping = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        using var queueStopping = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        using var requestDequeued = new ManualResetEventSlim();
        using var allowRequestStart = new ManualResetEventSlim();
        var processQueue = CreateProcessQueue(queueId: 0, messageCount: 1, queueStopping.Token);
        var callbackCalls = 0;
        var dispatcher = new ConcurrentConsumeDispatcher(
            maxConcurrency: 1,
            consumeMessageBatchSize: 1,
            (_, _) =>
            {
                Interlocked.Increment(ref callbackCalls);
                return ValueTask.CompletedTask;
            },
            _ =>
            {
                requestDequeued.Set();
                Assert.True(allowRequestStart.Wait(TimeSpan.FromSeconds(3), cancellationToken));
            },
            stopping.Token,
            NullLogger.Instance);

        dispatcher.NotifyReady(processQueue);
        Assert.True(requestDequeued.Wait(TimeSpan.FromSeconds(3), cancellationToken));
        processQueue.MarkDropped();
        queueStopping.Cancel();
        dispatcher.Complete();
        allowRequestStart.Set();
        await Task.WhenAll(dispatcher.ConsumeLoopTasks).WaitAsync(cancellationToken);

        Assert.Equal(0, callbackCalls);
    }

    private static ProcessQueue CreateProcessQueue(
        int queueId,
        int messageCount,
        CancellationToken cancellationToken)
    {
        var queue = new RemotingPullMessageQueue("orders", queueId, "broker-a", "127.0.0.1:10911");
        var processQueue = new ProcessQueue(queue, cancellationToken);
        processQueue.Initialize(0, forcePersist: false);
        processQueue.PutMessages(Enumerable.Range(0, messageCount).Select(offset => new RemotingMessageView(
            "orders",
            [],
            $"message-{queueId}-{offset}",
            $"offset-{queueId}-{offset}",
            null,
            [],
            new Dictionary<string, string>(),
            1,
            null,
            queueId,
            "broker-a",
            offset,
            offset,
            DateTimeOffset.UnixEpoch,
            DateTimeOffset.UnixEpoch)).ToArray());
        processQueue.AdvanceNextPullOffset(messageCount);
        return processQueue;
    }
}
