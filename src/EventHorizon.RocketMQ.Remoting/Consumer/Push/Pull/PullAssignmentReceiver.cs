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

using EventHorizon.RocketMQ.Remoting.Consumer.Push.Assignment;
using EventHorizon.RocketMQ.Remoting.Consumer.Push.Dispatch;

namespace EventHorizon.RocketMQ.Remoting.Consumer.Push.Pull;

internal sealed class PullAssignmentReceiver : IRemotingPushReceiver
{
    private readonly CancellationTokenSource _stopping;
    private readonly SemaphoreSlim _brokerLockAvailable = new(0, 1);
    private long _brokerLockTimestamp;
    private int _brokerLockHeld;

    public PullAssignmentReceiver(
        RemotingConsumerQueue queue,
        RemotingPushAssignment assignment,
        CancellationToken stopping,
        bool createProcessQueue)
    {
        Queue = queue;
        Assignment = assignment;
        _stopping = CancellationTokenSource.CreateLinkedTokenSource(stopping);
        ProcessQueue = createProcessQueue ? new ProcessQueue(queue, _stopping.Token) : null;
    }

    public RemotingConsumerQueue Queue { get; }

    public RemotingPushAssignment Assignment { get; }

    public ProcessQueue? ProcessQueue { get; }

    public CancellationToken Cancellation => _stopping.Token;

    public Task Task { get; private set; } = Task.CompletedTask;

    public void Start(Func<CancellationToken, Task> receiveLoop)
    {
        ArgumentNullException.ThrowIfNull(receiveLoop);
        if (!Task.IsCompleted)
        {
            throw new InvalidOperationException("The PULL receiver has already been started.");
        }

        Task = receiveLoop(_stopping.Token);
    }

    public void Stop() => _stopping.Cancel();

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
