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

using System.Runtime.ExceptionServices;
using EventHorizon.RocketMQ.Remoting.Consumer.Push.Assignment;
using EventHorizon.RocketMQ.Remoting.Consumer.Push.Pull.Dispatch;

namespace EventHorizon.RocketMQ.Remoting.Consumer.Push.Pull.Receive;

internal sealed class PullAssignmentReceiver : IRemotingPushReceiver
{
    private readonly CancellationTokenSource _stopping;
    private readonly SemaphoreSlim _brokerLockAvailable = new(0, 1);
    private long _brokerLockTimestamp;
    private int _brokerLockHeld;

    public PullAssignmentReceiver(
        PushReceiveTarget target,
        CancellationToken stopping,
        bool createProcessQueue)
    {
        ArgumentNullException.ThrowIfNull(target);
        Target = target;
        Queue = target.Queue ?? throw new ArgumentException(
            "A PULL receive target must identify a physical queue.",
            nameof(target));
        _stopping = CancellationTokenSource.CreateLinkedTokenSource(stopping);
        PullProcessQueue = createProcessQueue ? new PullProcessQueue(Queue, _stopping.Token) : null;
    }

    public RemotingConsumerQueue Queue { get; }

    public RemotingPushAssignment Assignment => Target.Assignment;

    public PushReceiveTarget Target { get; }

    public PullProcessQueue? PullProcessQueue { get; }

    public CancellationToken Cancellation => _stopping.Token;

    public Task Completion { get; private set; } = Task.CompletedTask;

    public void Start(Func<CancellationToken, Task> receiveLoop)
    {
        ArgumentNullException.ThrowIfNull(receiveLoop);
        if (!Completion.IsCompleted)
        {
            throw new InvalidOperationException("The PULL receiver has already been started.");
        }

        Completion = AwaitProtocolCompletionAsync(receiveLoop(_stopping.Token));
    }

    public void Stop() => _stopping.Cancel();

    private async Task AwaitProtocolCompletionAsync(Task receiveLoop)
    {
        ExceptionDispatchInfo? failure = null;
        try
        {
            await receiveLoop.ConfigureAwait(false);
        }
        catch (Exception exception)
        {
            failure = ExceptionDispatchInfo.Capture(exception);
        }

        if (PullProcessQueue is { } processQueue)
        {
            await processQueue.WaitForActiveSettlementsAsync().ConfigureAwait(false);
        }

        failure?.Throw();
    }

    public void MarkBrokerLockAcquired(DateTimeOffset timestamp)
    {
        Interlocked.Exchange(ref _brokerLockTimestamp, timestamp.ToUnixTimeMilliseconds());
        Interlocked.Exchange(ref _brokerLockHeld, 1);
        try
        {
            _brokerLockAvailable.Release();
        }
        catch (SemaphoreFullException)
        {
        }
    }

    public void MarkBrokerLockLost() => Interlocked.Exchange(ref _brokerLockHeld, 0);

    public bool HasValidBrokerLock(TimeProvider timeProvider, TimeSpan maximumLifetime)
    {
        if (Volatile.Read(ref _brokerLockHeld) == 0)
        {
            return false;
        }

        var lockTimestamp = Interlocked.Read(ref _brokerLockTimestamp);
        return timeProvider.GetUtcNow().ToUnixTimeMilliseconds() - lockTimestamp <= maximumLifetime.TotalMilliseconds;
    }

    public async Task WaitForBrokerLockAsync(
        TimeProvider timeProvider,
        TimeSpan maximumLifetime,
        CancellationToken cancellationToken)
    {
        while (!HasValidBrokerLock(timeProvider, maximumLifetime))
        {
            await _brokerLockAvailable.WaitAsync(cancellationToken).ConfigureAwait(false);
        }
    }

    public void Dispose()
    {
        _brokerLockAvailable.Dispose();
        _stopping.Dispose();
    }
}
