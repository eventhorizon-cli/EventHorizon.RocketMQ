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
using EventHorizon.RocketMQ.Remoting.Consumer.Route;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace EventHorizon.RocketMQ.Remoting.Tests.Consumer.Push.Assignment;

public sealed class PushReceiverSupervisorTests
{
    [Fact]
    public async Task Observe_ReceiverFails_RemovesDisposesAndWakesReplacement()
    {
        var registry = new PushReceiverRegistry();
        using var stopping = new CancellationTokenSource();
        var wakeup = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var supervisor = new PushReceiverSupervisor(
            registry,
            dispatchScheduler: null,
            stopping.Token,
            wakeup.SetResult,
            NullLogger.Instance);
        var completion = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var receiver = new TestReceiver(CreateTarget(), completion.Task);
        var key = PushReceiverKey.Create(receiver.Target);
        Assert.True(registry.TryAdd(key, receiver));

        var observation = supervisor.Observe(key, receiver);
        completion.TrySetException(new IOException("receiver failed"));

        await wakeup.Task.WaitAsync(TimeSpan.FromSeconds(1), TestContext.Current.CancellationToken);
        await observation;
        Assert.False(registry.Contains(key));
        Assert.Equal(1, receiver.StopCount);
        Assert.Equal(1, receiver.DisposeCount);
    }

    [Fact]
    public async Task Observe_StaleReceiverCompletesAfterReplacement_PreservesReplacement()
    {
        var registry = new PushReceiverRegistry();
        using var stopping = new CancellationTokenSource();
        var wakeupCount = 0;
        var supervisor = new PushReceiverSupervisor(
            registry,
            dispatchScheduler: null,
            stopping.Token,
            () => Interlocked.Increment(ref wakeupCount),
            NullLogger.Instance);
        var firstCompletion = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var first = new TestReceiver(CreateTarget(), firstCompletion.Task);
        var replacement = new TestReceiver(CreateTarget(), Task.CompletedTask);
        var key = PushReceiverKey.Create(first.Target);
        Assert.True(registry.TryAdd(key, first));
        var observation = supervisor.Observe(key, first);
        Assert.True(registry.TryRemove(key, first));
        Assert.True(registry.TryAdd(key, replacement));

        firstCompletion.TrySetResult();
        await observation;

        Assert.Same(replacement, Assert.Single(registry.Snapshot()));
        Assert.Equal(0, first.StopCount);
        Assert.Equal(0, first.DisposeCount);
        Assert.Equal(0, Volatile.Read(ref wakeupCount));
    }

    [Fact]
    public async Task Observe_StopAllOwnsReceiver_DoesNotDisposeOrWakeFromSupervisor()
    {
        var registry = new PushReceiverRegistry();
        using var stopping = new CancellationTokenSource();
        var wakeupCount = 0;
        var supervisor = new PushReceiverSupervisor(
            registry,
            dispatchScheduler: null,
            stopping.Token,
            () => Interlocked.Increment(ref wakeupCount),
            NullLogger.Instance);
        var completion = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var receiver = new TestReceiver(CreateTarget(), completion.Task)
        {
            OnStop = completion.SetResult
        };
        var key = PushReceiverKey.Create(receiver.Target);
        Assert.True(registry.TryAdd(key, receiver));
        var observation = supervisor.Observe(key, receiver);

        var stopped = registry.StopAll(dispatchScheduler: null);
        await observation;
        PushReceiverRegistry.DisposeAll(stopped);

        Assert.False(registry.Contains(key));
        Assert.Equal(1, receiver.StopCount);
        Assert.Equal(1, receiver.DisposeCount);
        Assert.Equal(0, Volatile.Read(ref wakeupCount));
    }

    private static PushReceiveTarget CreateTarget() => new(
        new RemotingPushAssignment(
            "orders",
            "broker-a",
            0,
            RemotingPushReceiveMode.Pull,
            new Dictionary<string, string>()),
        new RemotingBrokerEndpoint("broker-a", "127.0.0.1:10911"),
        FilterExpression.All);

    private sealed class TestReceiver(PushReceiveTarget target, Task completion) : IRemotingPushReceiver
    {
        private int _stopCount;
        private int _disposeCount;

        public RemotingPushAssignment Assignment => target.Assignment;

        public PushReceiveTarget Target => target;

        public CancellationToken Cancellation => CancellationToken.None;

        public Task Completion => completion;

        public Action? OnStop { get; init; }

        public int StopCount => Volatile.Read(ref _stopCount);

        public int DisposeCount => Volatile.Read(ref _disposeCount);

        public void Stop()
        {
            Interlocked.Increment(ref _stopCount);
            OnStop?.Invoke();
        }

        public void Dispose() => Interlocked.Increment(ref _disposeCount);
    }
}
