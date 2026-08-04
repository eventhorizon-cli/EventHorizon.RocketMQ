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

using Microsoft.Extensions.Logging;

namespace EventHorizon.RocketMQ.Remoting.Consumer.Push.Pull.Dispatch;

internal sealed class PullDeliveryCoordinator : IDisposable
{
    private readonly PullCacheAdmission _cacheAdmission;
    private readonly PullMessageGroupFifoCoordinator _fifoCoordinator = new();
    private ConcurrentPullConsumeDispatcher? _dispatcher;

    public PullDeliveryCoordinator(int maxCachedMessages, int maxCachedMessageBytes)
    {
        if (maxCachedMessages <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(maxCachedMessages));
        }

        if (maxCachedMessageBytes <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(maxCachedMessageBytes));
        }

        _cacheAdmission = new PullCacheAdmission(maxCachedMessages, maxCachedMessageBytes);
    }

    public IReadOnlyList<Task> Tasks => _dispatcher?.ConsumeLoopTasks ?? [];

    public void Start(
        int maxConcurrency,
        int consumeMessageBatchSize,
        Func<PullConsumeRequest, CancellationToken, ValueTask> consumeRequest,
        CancellationToken stopping,
        ILogger logger)
    {
        if (_dispatcher is not null)
        {
            throw new InvalidOperationException("The PULL dispatch scheduler has already been started.");
        }

        _dispatcher = new ConcurrentPullConsumeDispatcher(
            maxConcurrency,
            consumeMessageBatchSize,
            consumeRequest,
            ReleaseDeliverySlots,
            stopping,
            logger);
    }

    public void NotifyReady(PullProcessQueue processQueue) => GetDispatcher().NotifyReady(processQueue);

    public void Complete() => _dispatcher?.Complete();

    public ValueTask ReserveDeliverySlotsAsync(
        int count,
        int bodyBytes,
        CancellationToken cancellationToken) =>
        _cacheAdmission.ReserveAsync(count, bodyBytes, cancellationToken);

    public void ReleaseDeliverySlots(int count, int bodyBytes)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(count);
        ArgumentOutOfRangeException.ThrowIfNegative(bodyBytes);
        ReleaseDeliverySlots(new PullCacheReservation(count, bodyBytes));
    }

    public void ReleaseDeliverySlots(PullCacheReservation reservation)
    {
        _cacheAdmission.Release(reservation);
    }

    public void ReserveFifoOrder(PullProcessQueue processQueue, PullProcessQueue.MessageEntry entry)
    {
        _fifoCoordinator.Reserve(processQueue, entry, NotifyReady);
    }

    public void CompleteFifoOrder(PullProcessQueue.MessageEntry entry)
    {
        _fifoCoordinator.Complete(entry);
    }

    public PullCacheReservation DropProcessQueue(PullProcessQueue processQueue)
    {
        ArgumentNullException.ThrowIfNull(processQueue);
        var reservation = processQueue.MarkDropped(_fifoCoordinator.Drop);
        ReleaseDeliverySlots(reservation);
        return reservation;
    }

    public void Dispose()
    {
        _fifoCoordinator.Clear();
    }

    private ConcurrentPullConsumeDispatcher GetDispatcher() =>
        _dispatcher ?? throw new InvalidOperationException("The PULL dispatch scheduler has not been started.");

}
