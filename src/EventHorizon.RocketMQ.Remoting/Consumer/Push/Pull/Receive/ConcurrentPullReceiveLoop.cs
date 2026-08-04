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

using EventHorizon.RocketMQ.Remoting.Consumer.Pull;
using EventHorizon.RocketMQ.Remoting.Consumer.Push.Pull.Dispatch;
using EventHorizon.RocketMQ.Remoting.Consumer.Push.Pull.Offset;
using Microsoft.Extensions.Logging;

namespace EventHorizon.RocketMQ.Remoting.Consumer.Push.Pull.Receive;

internal sealed class ConcurrentPullReceiveLoop
{
    private readonly RemotingPushConsumerOptions _options;
    private readonly IRemotingPullWireClient _pullClient;
    private readonly PullOffsetManager _offsetManager;
    private readonly PullDeliveryCoordinator _dispatchScheduler;
    private readonly ILogger _logger;

    public ConcurrentPullReceiveLoop(
        RemotingPushConsumerOptions options,
        IRemotingPullWireClient pullClient,
        PullOffsetManager offsetManager,
        PullDeliveryCoordinator dispatchScheduler,
        ILogger logger)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(pullClient);
        ArgumentNullException.ThrowIfNull(offsetManager);
        ArgumentNullException.ThrowIfNull(dispatchScheduler);
        ArgumentNullException.ThrowIfNull(logger);

        _options = options;
        _pullClient = pullClient;
        _offsetManager = offsetManager;
        _dispatchScheduler = dispatchScheduler;
        _logger = logger;
    }

    public async Task RunAsync(PullAssignmentReceiver receiver)
    {
        ArgumentNullException.ThrowIfNull(receiver);
        var processQueue = receiver.PullProcessQueue ?? throw new ArgumentException(
            "A concurrent PULL receiver requires a process queue.",
            nameof(receiver));
        var cancellationToken = receiver.Cancellation;
        var initialOffset = await _offsetManager.InitializeAsync(receiver.Queue, cancellationToken)
            .ConfigureAwait(false);
        var persistInitialOffset = _offsetManager.HasResetBoundary(receiver.Queue);
        processQueue.Initialize(initialOffset, persistInitialOffset);

        await Task.WhenAll(
            PollIntoProcessQueueAsync(
                processQueue,
                receiver.Target.Filter,
                initialOffset,
                persistInitialOffset,
                cancellationToken),
            PersistProcessQueueOffsetsAsync(processQueue, cancellationToken)).ConfigureAwait(false);
    }

    private async Task PollIntoProcessQueueAsync(
        PullProcessQueue processQueue,
        FilterExpression filter,
        long initialOffset,
        bool persistInitialOffset,
        CancellationToken cancellationToken)
    {
        var queue = processQueue.MessageQueue;
        var offset = initialOffset;
        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                var result = await _pullClient.PullAsync(
                        queue,
                        offset,
                        filter,
                        cancellationToken: cancellationToken)
                    .ConfigureAwait(false);
                if (result.Status == RemotingPullStatus.OffsetIllegal)
                {
                    await processQueue.WaitUntilEmptyAsync(cancellationToken).ConfigureAwait(false);
                    processQueue.RequestOffsetReset(result.NextOffset);
                    await processQueue.WaitUntilOffsetPersistedAsync(result.NextOffset, cancellationToken)
                        .ConfigureAwait(false);
                    persistInitialOffset = false;
                    offset = result.NextOffset;
                    continue;
                }

                if (result.Messages.Count > 0)
                {
                    foreach (var messages in result.Messages
                                 .OrderBy(static message => message.QueueOffset)
                                 .Chunk(_options.ConsumeMessageBatchSize))
                    {
                        var reservation = new PullCacheReservation(
                            messages.Length,
                            messages.Sum(static message => message.Body.Length));
                        var slotsReserved = false;
                        try
                        {
                            await processQueue.WaitUntilAdmissionAllowedAsync(cancellationToken)
                                .ConfigureAwait(false);
                            await _dispatchScheduler.ReserveDeliverySlotsAsync(
                                    reservation.MessageCount,
                                    reservation.BodyBytes,
                                    cancellationToken)
                                .ConfigureAwait(false);
                            slotsReserved = true;
                            var added = processQueue.PutMessages(
                                messages,
                                entry => _dispatchScheduler.ReserveFifoOrder(processQueue, entry));
                            _dispatchScheduler.ReleaseDeliverySlots(new PullCacheReservation(
                                reservation.MessageCount - added.Count,
                                reservation.BodyBytes - added.Sum(static entry => entry.Message.Body.Length)));

                            slotsReserved = false;
                            _dispatchScheduler.ReleaseDeliverySlots(
                                processQueue.ReleaseAdmissionBlockedDeliverySlots());
                            _dispatchScheduler.NotifyReady(processQueue);
                        }
                        finally
                        {
                            if (slotsReserved)
                            {
                                _dispatchScheduler.ReleaseDeliverySlots(reservation);
                            }
                        }
                    }
                }

                processQueue.AdvanceNextPullOffset(result.NextOffset);
                if (persistInitialOffset)
                {
                    await processQueue.WaitUntilOffsetPersistedAsync(initialOffset, cancellationToken)
                        .ConfigureAwait(false);
                    persistInitialOffset = false;
                }

                offset = result.NextOffset;
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                return;
            }
            catch (Exception exception)
            {
                _logger.LogWarning(
                    exception,
                    "Legacy pull failed for {Topic}/{BrokerName}/{QueueId}; retrying",
                    queue.Topic,
                    queue.BrokerName,
                    queue.QueueId);
                await Task.Delay(_options.RetryDelay, cancellationToken).ConfigureAwait(false);
            }
        }
    }

    private async Task PersistProcessQueueOffsetsAsync(
        PullProcessQueue processQueue,
        CancellationToken cancellationToken)
    {
        var queue = processQueue.MessageQueue;
        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                await processQueue.WaitForCommitAsync(cancellationToken).ConfigureAwait(false);
                while (processQueue.TryGetOffsetCommit(out var commit))
                {
                    try
                    {
                        await _offsetManager.PersistAsync(queue, commit.Offset, cancellationToken)
                            .ConfigureAwait(false);
                        processQueue.MarkOffsetPersisted(commit);
                    }
                    catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                    {
                        return;
                    }
                    catch (Exception exception)
                    {
                        _logger.LogWarning(
                            exception,
                            "Unable to persist legacy process-queue offset for {Topic}/{BrokerName}/{QueueId}; retrying",
                            queue.Topic,
                            queue.BrokerName,
                            queue.QueueId);
                        await Task.Delay(_options.RetryDelay, cancellationToken).ConfigureAwait(false);
                    }
                }
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                return;
            }
        }
    }
}
