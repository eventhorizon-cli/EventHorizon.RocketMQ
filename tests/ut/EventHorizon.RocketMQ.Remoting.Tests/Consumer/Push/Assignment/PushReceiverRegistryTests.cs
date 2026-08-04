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

using EventHorizon.RocketMQ.Remoting.Consumer;
using EventHorizon.RocketMQ.Remoting.Consumer.Push.Assignment;
using EventHorizon.RocketMQ.Remoting.Consumer.Push.Pull.Dispatch;
using EventHorizon.RocketMQ.Remoting.Consumer.Push.Pull.Receive;
using EventHorizon.RocketMQ.Remoting.Consumer.Route;
using Xunit;

namespace EventHorizon.RocketMQ.Remoting.Tests.Consumer.Push.Assignment;

public sealed class PushReceiverRegistryTests
{
    [Fact]
    public void TryAdd_NewKey_RegistersReceiverAndExposesEntry()
    {
        var registry = new PushReceiverRegistry();
        var receiver = CreateReceiver();
        var key = PushReceiverKey.Create(receiver.Target);

        Assert.True(registry.TryAdd(key, receiver));
        Assert.True(registry.Contains(key));

        var snapshot = Assert.Single(registry.Snapshot());
        Assert.Same(receiver, snapshot);

        var entry = Assert.Single(registry.SnapshotEntries());
        Assert.Equal(key, entry.Key);
        Assert.Same(receiver, entry.Value);
    }

    [Fact]
    public void TryAdd_DuplicateKey_PreservesExistingReceiver()
    {
        var registry = new PushReceiverRegistry();
        var first = CreateReceiver();
        var replacement = CreateReceiver();
        var key = PushReceiverKey.Create(first.Target);

        Assert.True(registry.TryAdd(key, first));
        Assert.False(registry.TryAdd(key, replacement));

        var registered = Assert.Single(registry.Snapshot());
        Assert.Same(first, registered);
        Assert.Equal(0, replacement.StopCount);
        Assert.Equal(0, replacement.DisposeCount);
    }

    [Fact]
    public async Task TryAdd_ConcurrentRegistrationsOfSameKey_StoresOneReceiver()
    {
        var registry = new PushReceiverRegistry();
        var receivers = Enumerable.Range(0, 16)
            .Select(index => CreateReceiver(queueId: index))
            .ToArray();
        var key = new PushReceiverKey("orders", "broker-a", 0, "127.0.0.1:10911", RemotingPushReceiveMode.Pull);

        var attempts = receivers
            .Select(receiver => Task.Run(() => registry.TryAdd(key, receiver)))
            .ToArray();
        var results = await Task.WhenAll(attempts)
            .WaitAsync(TimeSpan.FromSeconds(1), TestContext.Current.CancellationToken);

        Assert.Equal(1, results.Count(static added => added));
        Assert.Single(registry.Snapshot());
    }

    [Fact]
    public void TryRemove_ExistingAndMissingKeys_ReturnsStoredReceiverAndClearsEntry()
    {
        var registry = new PushReceiverRegistry();
        var receiver = CreateReceiver();
        var key = PushReceiverKey.Create(receiver.Target);
        registry.TryAdd(key, receiver);

        Assert.True(registry.TryRemove(key, out var removed));
        Assert.Same(receiver, removed);
        Assert.False(registry.Contains(key));

        Assert.False(registry.TryRemove(key, out var missing));
        Assert.Null(missing);
    }

    [Fact]
    public void TryRemove_StaleReceiverAfterReplacement_DoesNotRemoveReplacement()
    {
        var registry = new PushReceiverRegistry();
        var first = CreateReceiver();
        var replacement = CreateReceiver();
        var key = PushReceiverKey.Create(first.Target);
        Assert.True(registry.TryAdd(key, first));
        Assert.True(registry.TryRemove(key, first));
        Assert.True(registry.TryAdd(key, replacement));

        Assert.False(registry.TryRemove(key, first));

        var registered = Assert.Single(registry.Snapshot());
        Assert.Same(replacement, registered);
    }

    [Fact]
    public void Snapshot_RegistryMutates_CapturesStableEntriesAcrossMutations()
    {
        var registry = new PushReceiverRegistry();
        var first = CreateReceiver(queueId: 0);
        var second = CreateReceiver(queueId: 1);
        var replacement = CreateReceiver(queueId: 2);
        var firstKey = PushReceiverKey.Create(first.Target);
        var replacementKey = PushReceiverKey.Create(replacement.Target);
        registry.TryAdd(firstKey, first);
        registry.TryAdd(PushReceiverKey.Create(second.Target), second);

        var receivers = registry.Snapshot();
        var entries = registry.SnapshotEntries();

        Assert.True(registry.TryRemove(firstKey, out _));
        Assert.True(registry.TryAdd(replacementKey, replacement));

        Assert.Equal(2, receivers.Count);
        Assert.Contains(first, receivers);
        Assert.Contains(second, receivers);
        Assert.Equal(2, entries.Count);
        Assert.Contains(entries, entry => entry.Key == firstKey && ReferenceEquals(entry.Value, first));
        Assert.Contains(entries, entry => ReferenceEquals(entry.Value, second));
    }

    [Fact]
    public void StopAll_ReceiverStopMutatesRegistry_ReturnsOriginalSnapshot()
    {
        var registry = new PushReceiverRegistry();
        var first = CreateReceiver(queueId: 0);
        var second = CreateReceiver(queueId: 1);
        var addedDuringStop = CreateReceiver(queueId: 2);
        var addedKey = PushReceiverKey.Create(addedDuringStop.Target);
        first.OnStop = () => Assert.True(registry.TryAdd(addedKey, addedDuringStop));
        registry.TryAdd(PushReceiverKey.Create(first.Target), first);
        registry.TryAdd(PushReceiverKey.Create(second.Target), second);

        var stopped = registry.StopAll(dispatchScheduler: null);

        Assert.Equal(2, stopped.Count);
        Assert.Contains(first, stopped);
        Assert.Contains(second, stopped);
        Assert.DoesNotContain(addedDuringStop, stopped);
        Assert.True(registry.Contains(addedKey));
        Assert.Equal(1, first.StopCount);
        Assert.Equal(1, second.StopCount);
        Assert.Equal(0, addedDuringStop.StopCount);
        Assert.False(registry.Contains(PushReceiverKey.Create(first.Target)));
        Assert.False(registry.Contains(PushReceiverKey.Create(second.Target)));
    }

    [Fact]
    public void HasPullQueue_RegisteredPullReceiver_MatchesEquivalentQueueOnly()
    {
        var registry = new PushReceiverRegistry();
        var receiver = new PullAssignmentReceiver(
            CreateTarget(queueId: 3),
            CancellationToken.None,
            createProcessQueue: false);
        var key = PushReceiverKey.Create(receiver.Target);

        try
        {
            Assert.True(registry.TryAdd(key, receiver));
            Assert.True(registry.HasPullQueue(new RemotingConsumerQueue("orders", "broker-a", 3)));
            Assert.False(registry.HasPullQueue(new RemotingConsumerQueue("orders", "broker-a", 4)));

            Assert.True(registry.TryRemove(key, out _));
            Assert.False(registry.HasPullQueue(new RemotingConsumerQueue("orders", "broker-a", 3)));
        }
        finally
        {
            receiver.Dispose();
        }
    }

    [Fact]
    public void Drop_PullReceiverWithProcessQueue_RequiresDispatchScheduler()
    {
        var registry = new PushReceiverRegistry();
        var receiver = new PullAssignmentReceiver(
            CreateTarget(queueId: 3),
            CancellationToken.None,
            createProcessQueue: true);

        try
        {
            var exception = Assert.Throws<InvalidOperationException>(() =>
                registry.Drop(receiver, dispatchScheduler: null));

            Assert.Contains("dispatch scheduler", exception.Message, StringComparison.OrdinalIgnoreCase);
            Assert.False(receiver.PullProcessQueue!.IsDropped);
            Assert.False(receiver.Cancellation.IsCancellationRequested);
        }
        finally
        {
            receiver.Dispose();
        }
    }

    [Fact]
    public void Drop_PullReceiverWithProcessQueue_DropsQueueBeforeStoppingReceiver()
    {
        var registry = new PushReceiverRegistry();
        var receiver = new PullAssignmentReceiver(
            CreateTarget(queueId: 3),
            CancellationToken.None,
            createProcessQueue: true);
        using var scheduler = new PullDeliveryCoordinator(maxCachedMessages: 1, maxCachedMessageBytes: 1);

        try
        {
            registry.Drop(receiver, scheduler);

            Assert.True(receiver.PullProcessQueue!.IsDropped);
            Assert.True(receiver.Cancellation.IsCancellationRequested);
        }
        finally
        {
            receiver.Dispose();
        }
    }

    [Fact]
    public async Task AwaitAsync_CanceledAndPendingReceivers_CompletesAfterAllTasksFinish()
    {
        using var cancellation = new CancellationTokenSource();
        cancellation.Cancel();
        var canceled = CreateReceiver();
        canceled.Completion = Task.FromCanceled(cancellation.Token);

        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var pending = CreateReceiver();
        pending.Completion = release.Task;

        var awaiting = PushReceiverRegistry.AwaitAsync([canceled, pending]);

        Assert.False(awaiting.IsCompleted);
        release.TrySetResult();
        await awaiting.WaitAsync(TimeSpan.FromSeconds(1), TestContext.Current.CancellationToken);
    }

    [Fact]
    public async Task AwaitAsync_DroppedPullHasActiveConsumeRequest_WaitsForRequestProtocolWorkToExit()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var registry = new PushReceiverRegistry();
        var receiver = new PullAssignmentReceiver(
            CreateTarget(queueId: 3),
            CancellationToken.None,
            createProcessQueue: true);
        using var scheduler = new PullDeliveryCoordinator(maxCachedMessages: 1, maxCachedMessageBytes: 3);
        try
        {
            var processQueue = receiver.PullProcessQueue!;
            processQueue.Initialize(0, forcePersist: false);
            processQueue.PutMessages([CreateMessage()]);
            processQueue.AdvanceNextPullOffset(1);
            Assert.True(processQueue.TryCreateConsumeRequest(1, out var request));
            Assert.True(processQueue.TryStartConsumeRequest(request));
            Assert.True(processQueue.TryBeginSettlement(
                request.Entries[0],
                ConsumeResult.Retry,
                delayLevel: 3,
                out _));
            receiver.Start(static _ => Task.CompletedTask);

            registry.Drop(receiver, scheduler);
            var awaiting = PushReceiverRegistry.AwaitAsync([receiver]);
            var waitedForActiveRequest = !awaiting.IsCompleted;

            Assert.False(processQueue.CompleteConsumeRequest(request, [true]));
            await awaiting.WaitAsync(TimeSpan.FromSeconds(1), cancellationToken);

            Assert.True(
                waitedForActiveRequest,
                "A removed PULL receiver must retain ownership until its active consume request exits protocol work.");
        }
        finally
        {
            receiver.Dispose();
        }
    }

    [Fact]
    public async Task AwaitAsync_FailedReceiverTask_PropagatesFailure()
    {
        var receiver = CreateReceiver();
        receiver.Completion = Task.FromException(new InvalidOperationException("receiver failed"));

        var exception = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            PushReceiverRegistry.AwaitAsync([receiver]));

        Assert.Equal("receiver failed", exception.Message);
    }

    [Fact]
    public void DisposeAll_Receivers_InvokesDisposeOnEachReceiver()
    {
        var first = CreateReceiver();
        var second = CreateReceiver(queueId: 1);

        PushReceiverRegistry.DisposeAll([first, second]);

        Assert.Equal(1, first.DisposeCount);
        Assert.Equal(1, second.DisposeCount);
    }

    [Fact]
    public void TryAdd_NullReceiver_ThrowsArgumentNullException()
    {
        var registry = new PushReceiverRegistry();
        var key = new PushReceiverKey("orders", "broker-a", 0, "127.0.0.1:10911", RemotingPushReceiveMode.Pull);

        Assert.Throws<ArgumentNullException>(() => registry.TryAdd(key, null!));
    }

    [Fact]
    public void HasPullQueue_NullQueue_ThrowsArgumentNullException()
    {
        var registry = new PushReceiverRegistry();

        Assert.Throws<ArgumentNullException>(() => registry.HasPullQueue(null!));
    }

    [Fact]
    public void Drop_NullReceiver_ThrowsArgumentNullException()
    {
        var registry = new PushReceiverRegistry();

        Assert.Throws<ArgumentNullException>(() => registry.Drop(null!, dispatchScheduler: null));
    }

    [Fact]
    public async Task AwaitAsync_NullReceivers_ThrowsArgumentNullException()
    {
        await Assert.ThrowsAsync<ArgumentNullException>(() => PushReceiverRegistry.AwaitAsync(null!));
    }

    [Fact]
    public void DisposeAll_NullReceivers_ThrowsArgumentNullException()
    {
        Assert.Throws<ArgumentNullException>(() => PushReceiverRegistry.DisposeAll(null!));
    }

    private static TestReceiver CreateReceiver(
        string topic = "orders",
        int queueId = 0,
        RemotingPushReceiveMode mode = RemotingPushReceiveMode.Pull) =>
        new(CreateTarget(topic, queueId, mode));

    private static PushReceiveTarget CreateTarget(
        string topic = "orders",
        int queueId = 0,
        RemotingPushReceiveMode mode = RemotingPushReceiveMode.Pull) =>
        new(
            new RemotingPushAssignment(
                topic,
                "broker-a",
                queueId,
                mode,
                new Dictionary<string, string>()),
            new RemotingBrokerEndpoint("broker-a", "127.0.0.1:10911"),
            FilterExpression.All);

    private static RemotingMessageView CreateMessage() => new(
        "orders",
        new byte[] { 1, 2, 3 },
        "message-0",
        "offset-0",
        null,
        [],
        new Dictionary<string, string>(),
        1,
        null,
        0,
        "broker-a",
        0,
        0,
        DateTimeOffset.UnixEpoch,
        DateTimeOffset.UnixEpoch);

    private sealed class TestReceiver : IRemotingPushReceiver
    {
        public TestReceiver(PushReceiveTarget target)
        {
            Target = target;
        }

        public RemotingPushAssignment Assignment => Target.Assignment;

        public PushReceiveTarget Target { get; }

        public CancellationToken Cancellation => CancellationToken.None;

        public Task Completion { get; set; } = System.Threading.Tasks.Task.CompletedTask;

        public Action? OnStop { get; set; }

        public int StopCount => Volatile.Read(ref _stopCount);

        public int DisposeCount => Volatile.Read(ref _disposeCount);

        private int _stopCount;
        private int _disposeCount;

        public void Stop()
        {
            Interlocked.Increment(ref _stopCount);
            OnStop?.Invoke();
        }

        public void Dispose() => Interlocked.Increment(ref _disposeCount);
    }
}
