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
using EventHorizon.RocketMQ.Remoting.Consumer.LitePull.Assignment;
using EventHorizon.RocketMQ.Remoting.Consumer.LitePull.Receive;
using Xunit;

namespace EventHorizon.RocketMQ.Remoting.Tests.Consumer.LitePull.Receive;

public sealed class LitePullReceiverRegistryTests
{
    [Fact]
    public async Task Start_AlreadyRegisteredState_ThrowsWhileReceiverIsRunning()
    {
        var registry = new LitePullReceiverRegistry();
        var state = CreateState();
        var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        registry.Start(
            state,
            async _ =>
            {
                started.TrySetResult();
                await release.Task.ConfigureAwait(false);
            });
        await started.Task.WaitAsync(TimeSpan.FromSeconds(1), TestContext.Current.CancellationToken);

        var exception = Assert.Throws<InvalidOperationException>(() => registry.Start(state, _ => Task.CompletedTask));
        Assert.Contains("already registered", exception.Message, StringComparison.OrdinalIgnoreCase);

        release.TrySetResult();
        await state.ReceiverTask.WaitAsync(TimeSpan.FromSeconds(1), TestContext.Current.CancellationToken);
    }

    [Fact]
    public async Task CancelAll_ActiveReceivers_ReturnsSnapshotAndCancelsReceivers()
    {
        var registry = new LitePullReceiverRegistry();
        var first = CreateState(queueId: 0);
        var second = CreateState(queueId: 1);
        var firstStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var secondStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var unexpectedCompletion = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var firstToken = first.ReceiveCancellation.Token;
        var secondToken = second.ReceiveCancellation.Token;

        registry.Start(
            first,
            state => WaitForCancellationAsync(state, firstStarted),
            (_, _) => unexpectedCompletion.TrySetResult());
        registry.Start(
            second,
            state => WaitForCancellationAsync(state, secondStarted),
            (_, _) => unexpectedCompletion.TrySetResult());
        await Task.WhenAll(firstStarted.Task, secondStarted.Task)
            .WaitAsync(TimeSpan.FromSeconds(1), TestContext.Current.CancellationToken);

        var snapshot = registry.CancelAll();

        Assert.Equal(2, snapshot.Count);
        Assert.Contains(first, snapshot);
        Assert.Contains(second, snapshot);
        Assert.True(firstToken.IsCancellationRequested);
        Assert.True(secondToken.IsCancellationRequested);

        await first.ReceiverTask.WaitAsync(TimeSpan.FromSeconds(1), TestContext.Current.CancellationToken);
        await second.ReceiverTask.WaitAsync(TimeSpan.FromSeconds(1), TestContext.Current.CancellationToken);
        Assert.False(unexpectedCompletion.Task.IsCompleted);
        Assert.Empty(registry.CancelAll());
    }

    [Fact]
    public async Task ReceiverCompletion_RegisteredState_RemovesAndDisposesState()
    {
        var registry = new LitePullReceiverRegistry();
        var state = CreateState();

        registry.Start(state, _ => Task.CompletedTask);
        await state.ReceiverTask.WaitAsync(TimeSpan.FromSeconds(1), TestContext.Current.CancellationToken);

        Assert.Empty(registry.CancelAll());
        Assert.Throws<ObjectDisposedException>(() => _ = state.ReceiveCancellation.Token);
    }

    [Fact]
    public async Task ReceiverCompletion_UnexpectedReturn_NotifiesCallbackWithoutException()
    {
        var registry = new LitePullReceiverRegistry();
        var state = CreateState();
        var callback = new TaskCompletionSource<Exception?>(TaskCreationOptions.RunContinuationsAsynchronously);

        registry.Start(state, _ => Task.CompletedTask, (_, exception) => callback.TrySetResult(exception));

        await state.ReceiverTask.WaitAsync(TimeSpan.FromSeconds(1), TestContext.Current.CancellationToken);

        Assert.Null(await callback.Task.WaitAsync(TimeSpan.FromSeconds(1), TestContext.Current.CancellationToken));
        Assert.Empty(registry.CancelAll());
    }

    [Fact]
    public async Task ReceiverFailure_RegisteredState_CompletesAndNotifiesCallback()
    {
        var registry = new LitePullReceiverRegistry();
        var state = CreateState();
        var callback = new TaskCompletionSource<Exception?>(TaskCreationOptions.RunContinuationsAsynchronously);

        registry.Start(
            state,
            _ => Task.FromException(new InvalidOperationException("receiver failed")),
            (_, exception) => callback.TrySetResult(exception));

        await state.ReceiverTask.WaitAsync(TimeSpan.FromSeconds(1), TestContext.Current.CancellationToken);

        var exception = await callback.Task.WaitAsync(TimeSpan.FromSeconds(1), TestContext.Current.CancellationToken);
        var failure = Assert.IsType<InvalidOperationException>(exception);
        Assert.Equal("receiver failed", failure.Message);
        Assert.Empty(registry.CancelAll());
        Assert.Throws<ObjectDisposedException>(() => _ = state.ReceiveCancellation.Token);
    }

    private static async Task WaitForCancellationAsync(
        LitePullQueueState state,
        TaskCompletionSource started)
    {
        started.TrySetResult();
        await Task.Delay(Timeout.InfiniteTimeSpan, state.ReceiveCancellation.Token).ConfigureAwait(false);
    }

    private static LitePullQueueState CreateState(int queueId = 0) =>
        new(
            Target(new RemotingConsumerQueue("orders", "broker-a", queueId)),
            offset: 0);

    private static LitePullReceiveTarget Target(
        RemotingConsumerQueue queue,
        FilterExpression? filter = null) =>
        new(queue, filter ?? FilterExpression.All);
}
