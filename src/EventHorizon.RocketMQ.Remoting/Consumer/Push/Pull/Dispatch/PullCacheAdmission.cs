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

namespace EventHorizon.RocketMQ.Remoting.Consumer.Push.Pull.Dispatch;

internal sealed class PullCacheAdmission
{
    private const int MaximumWaiterBypasses = 1;

    private readonly int _maxCachedMessages;
    private readonly int _maxCachedMessageBytes;
    private readonly object _gate = new();
    private readonly LinkedList<AdmissionWaiter> _waiters = [];
    private int _reservedMessageCount;
    private int _reservedBodyBytes;

    public PullCacheAdmission(int maxCachedMessages, int maxCachedMessageBytes)
    {
        if (maxCachedMessages <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(maxCachedMessages));
        }

        if (maxCachedMessageBytes <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(maxCachedMessageBytes));
        }

        _maxCachedMessages = maxCachedMessages;
        _maxCachedMessageBytes = maxCachedMessageBytes;
    }

    public ValueTask ReserveAsync(int count, int bodyBytes, CancellationToken cancellationToken)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(count);
        ArgumentOutOfRangeException.ThrowIfNegative(bodyBytes);
        cancellationToken.ThrowIfCancellationRequested();

        AdmissionWaiter? waiter = null;
        List<AdmissionWaiter> granted;
        lock (_gate)
        {
            if (_waiters.Count == 0 && CanReserveCore(count, bodyBytes))
            {
                ReserveCore(count, bodyBytes);
                return ValueTask.CompletedTask;
            }

            waiter = new AdmissionWaiter(count, bodyBytes);
            waiter.Node = _waiters.AddLast(waiter);
            granted = GrantWaitersCore();
        }

        CompleteGranted(granted);
        return AwaitReservationAsync(waiter, cancellationToken);
    }

    public void Release(PullCacheReservation reservation)
    {
        if (reservation.IsEmpty)
        {
            return;
        }

        List<AdmissionWaiter> granted;
        lock (_gate)
        {
            ValidateReleaseCore(reservation);
            _reservedMessageCount -= reservation.MessageCount;
            _reservedBodyBytes -= reservation.BodyBytes;
            granted = GrantWaitersCore();
        }

        CompleteGranted(granted);
    }

    private async ValueTask AwaitReservationAsync(
        AdmissionWaiter waiter,
        CancellationToken cancellationToken)
    {
        try
        {
            await waiter.Completion.Task.WaitAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            List<AdmissionWaiter> granted;
            lock (_gate)
            {
                if (waiter.Node?.List is not null)
                {
                    _waiters.Remove(waiter.Node);
                }
                else if (waiter.Granted)
                {
                    waiter.Granted = false;
                    _reservedMessageCount -= waiter.MessageCount;
                    _reservedBodyBytes -= waiter.BodyBytes;
                }

                granted = GrantWaitersCore();
            }

            CompleteGranted(granted);
            throw;
        }
    }

    private List<AdmissionWaiter> GrantWaitersCore()
    {
        List<AdmissionWaiter> granted = [];
        while (_waiters.First is { } oldestNode)
        {
            var oldest = oldestNode.Value;
            if (CanReserveCore(oldest.MessageCount, oldest.BodyBytes))
            {
                GrantCore(oldestNode, granted);
                continue;
            }

            if (oldest.BypassCount >= MaximumWaiterBypasses)
            {
                break;
            }

            var fittingNode = oldestNode.Next;
            while (fittingNode is not null &&
                   !CanReserveCore(fittingNode.Value.MessageCount, fittingNode.Value.BodyBytes))
            {
                fittingNode = fittingNode.Next;
            }

            if (fittingNode is null)
            {
                break;
            }

            oldest.BypassCount++;
            GrantCore(fittingNode, granted);
        }

        return granted;
    }

    private void GrantCore(LinkedListNode<AdmissionWaiter> node, ICollection<AdmissionWaiter> granted)
    {
        var waiter = node.Value;
        _waiters.Remove(node);
        waiter.Node = null;
        waiter.Granted = true;
        ReserveCore(waiter.MessageCount, waiter.BodyBytes);
        granted.Add(waiter);
    }

    private void ReserveCore(int messageCount, int bodyBytes)
    {
        _reservedMessageCount = checked(_reservedMessageCount + messageCount);
        _reservedBodyBytes = checked(_reservedBodyBytes + bodyBytes);
    }

    private bool CanReserveCore(int messageCount, int bodyBytes) =>
        _reservedMessageCount == 0 ||
        (long)_reservedMessageCount + messageCount <= _maxCachedMessages &&
        (long)_reservedBodyBytes + bodyBytes <= _maxCachedMessageBytes;

    private void ValidateReleaseCore(PullCacheReservation reservation)
    {
        if (reservation.MessageCount < 0 || reservation.MessageCount > _reservedMessageCount ||
            reservation.BodyBytes < 0 || reservation.BodyBytes > _reservedBodyBytes)
        {
            throw new InvalidOperationException("The PULL cache reservation release exceeds reserved capacity.");
        }
    }

    private static void CompleteGranted(IEnumerable<AdmissionWaiter> granted)
    {
        foreach (var waiter in granted)
        {
            waiter.Completion.TrySetResult();
        }
    }

    private sealed class AdmissionWaiter(int messageCount, int bodyBytes)
    {
        public int MessageCount { get; } = messageCount;

        public int BodyBytes { get; } = bodyBytes;

        public TaskCompletionSource Completion { get; } =
            new(TaskCreationOptions.RunContinuationsAsynchronously);

        public LinkedListNode<AdmissionWaiter>? Node { get; set; }

        public int BypassCount { get; set; }

        public bool Granted { get; set; }
    }
}
