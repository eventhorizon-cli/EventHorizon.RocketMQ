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

using EventHorizon.RocketMQ.Remoting.Consumer.Push.Processing;
using EventHorizon.RocketMQ.Remoting.Consumer.Push.Pull.Settlement;
using Microsoft.Extensions.Logging;

namespace EventHorizon.RocketMQ.Remoting.Consumer.Push.Pull.Dispatch;

internal sealed class PullConsumeRequestProcessor
{
    private readonly RemotingPushConsumerOptions _options;
    private readonly PullSendBackSettlement _sendBackSettlement;
    private readonly PushMessageHandlerInvoker _messageHandlerInvoker;
    private readonly PullDeliveryCoordinator _dispatchScheduler;
    private readonly ILogger _logger;

    public PullConsumeRequestProcessor(
        RemotingPushConsumerOptions options,
        PullSendBackSettlement sendBackSettlement,
        PushMessageHandlerInvoker messageHandlerInvoker,
        PullDeliveryCoordinator dispatchScheduler,
        ILogger logger)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(sendBackSettlement);
        ArgumentNullException.ThrowIfNull(messageHandlerInvoker);
        ArgumentNullException.ThrowIfNull(dispatchScheduler);
        ArgumentNullException.ThrowIfNull(logger);

        _options = options;
        _sendBackSettlement = sendBackSettlement;
        _messageHandlerInvoker = messageHandlerInvoker;
        _dispatchScheduler = dispatchScheduler;
        _logger = logger;
    }

    public async ValueTask ProcessAsync(PullConsumeRequest request, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(request);
        var completed = new bool[request.Entries.Count];
        var fifoReleased = new bool[request.Entries.Count];
        var retryLocally = false;
        try
        {
            cancellationToken.ThrowIfCancellationRequested();
            foreach (var entry in request.Entries)
            {
                if (entry.FifoOrder is { } order)
                {
                    await order.Predecessor.WaitAsync(cancellationToken).ConfigureAwait(false);
                }
            }

            cancellationToken.ThrowIfCancellationRequested();

            var batchResult = default(PushHandlerBatchResult);
            if (!request.IsSettlementRetry)
            {
                try
                {
                    batchResult = UsesConcurrentTimeoutRecovery(request)
                        ? await _messageHandlerInvoker.InvokeAsync(
                            request.Messages,
                            enableTimeoutRecovery: true,
                            cancellationToken).ConfigureAwait(false)
                        : await HandleConsumeRequestAsync(request, cancellationToken).ConfigureAwait(false);
                }
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                {
                    throw;
                }
                catch (Exception exception)
                {
                    _logger.LogError(
                        exception,
                        "Legacy message handler failed for a consume request containing {MessageCount} messages",
                        request.Messages.Count);
                    batchResult = PushHandlerBatchResult.Retry(
                        request.Messages.Count,
                        delayLevelWhenNextConsume: 0);
                }

                if (_messageHandlerInvoker.ShouldAbandonResults)
                {
                    cancellationToken.ThrowIfCancellationRequested();
                }
            }

            for (var index = 0; index < request.Entries.Count; index++)
            {
                var entry = request.Entries[index];
                var outcome = request.IsSettlementRetry
                    ? new PushHandlerMessageOutcome(
                        entry.PendingSettlementResult ?? throw new InvalidOperationException(
                            "A settlement retry requires a stored consume result."),
                        entry.PendingSettlementDelayLevel)
                    : batchResult.GetOutcome(index);
                if (_options.ConsumerMode == ConsumerMode.Broadcasting)
                {
                    if (outcome.Result != ConsumeResult.Success)
                    {
                        _logger.LogWarning(
                            "Broadcast consumer drops message {MessageId} after handler outcome {ConsumeResult}; broadcast retry and dead-letter queues are unavailable",
                            entry.Message.MessageId,
                            outcome.Result);
                    }

                    completed[index] = true;
                    continue;
                }

                if (outcome.Result == ConsumeResult.Success)
                {
                    completed[index] = true;
                    continue;
                }

                var message = entry.Message;
                var deadLetter = outcome.Result == ConsumeResult.DeadLetter ||
                                 outcome.DelayLevelWhenNextConsume < 0 ||
                                 message.DeliveryAttempt >= _options.MaxDeliveryAttempts;
                if (!request.PullProcessQueue.TryBeginSettlement(
                        entry,
                        deadLetter ? ConsumeResult.DeadLetter : ConsumeResult.Retry,
                        deadLetter ? -1 : outcome.DelayLevelWhenNextConsume,
                        out var releasedReservation))
                {
                    continue;
                }

                _dispatchScheduler.ReleaseDeliverySlots(releasedReservation);
                try
                {
                    await _sendBackSettlement.SendAsync(
                        request.PullProcessQueue.MessageQueue,
                        message,
                        deadLetter,
                        outcome.DelayLevelWhenNextConsume,
                        cancellationToken).ConfigureAwait(false);
                    completed[index] = true;
                }
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                {
                    throw;
                }
                catch (Exception exception)
                {
                    retryLocally = true;
                    request.PullProcessQueue.DelaySettlementRetry(entry);
                    _logger.LogWarning(
                        exception,
                        "Unable to settle message {MessageId} from {Topic}/{BrokerName}/{QueueId}; retrying locally",
                        entry.Message.MessageId,
                        request.PullProcessQueue.MessageQueue.Topic,
                        request.PullProcessQueue.MessageQueue.BrokerName,
                        request.PullProcessQueue.MessageQueue.QueueId);
                }
            }

            for (var index = 0; index < request.Entries.Count; index++)
            {
                if (completed[index])
                {
                    _dispatchScheduler.CompleteFifoOrder(request.Entries[index]);
                    fifoReleased[index] = true;
                }
            }

            var hasPending = request.PullProcessQueue.CompleteConsumeRequest(request, completed);
            if (retryLocally)
            {
                _ = NotifyProcessQueueAfterDelayAsync(request.PullProcessQueue, request.Entries);
            }
            else if (hasPending)
            {
                _dispatchScheduler.NotifyReady(request.PullProcessQueue);
            }
        }
        finally
        {
            for (var index = 0; index < request.Entries.Count; index++)
            {
                if (!fifoReleased[index] && (completed[index] || request.PullProcessQueue.IsDropped))
                {
                    _dispatchScheduler.CompleteFifoOrder(request.Entries[index]);
                }
            }
        }
    }

    private async Task NotifyProcessQueueAfterDelayAsync(
        PullProcessQueue processQueue,
        IReadOnlyList<PullProcessQueue.MessageEntry> entries)
    {
        try
        {
            await Task.Delay(_options.RetryDelay, processQueue.QueueCancellation).ConfigureAwait(false);
            if (processQueue.ActivateSettlementRetries(entries))
            {
                _dispatchScheduler.NotifyReady(processQueue);
            }
        }
        catch (OperationCanceledException) when (processQueue.QueueCancellation.IsCancellationRequested)
        {
        }
    }

    private async ValueTask<PushHandlerBatchResult> HandleConsumeRequestAsync(
        PullConsumeRequest request,
        CancellationToken cancellationToken)
    {
        if (request.Entries.Count == 1)
        {
            var outcome = await HandleMessageAsync(request.Entries[0], cancellationToken).ConfigureAwait(false);
            return PushHandlerBatchResult.All(outcome.Result, 1, outcome.DelayLevelWhenNextConsume);
        }

        return await _messageHandlerInvoker.InvokeAsync(
            request.Messages,
            enableTimeoutRecovery: false,
            cancellationToken).ConfigureAwait(false);
    }

    private async ValueTask<PushHandlerMessageOutcome> HandleMessageAsync(
        PullProcessQueue.MessageEntry entry,
        CancellationToken cancellationToken)
    {
        var attempt = entry.Message.DeliveryAttempt;
        while (true)
        {
            PushHandlerMessageOutcome outcome;
            try
            {
                var batchResult = await _messageHandlerInvoker.InvokeAsync(
                    new[] { entry.Message },
                    enableTimeoutRecovery: false,
                    cancellationToken).ConfigureAwait(false);
                outcome = entry.FifoOrder is null
                    ? batchResult.GetOutcome(0)
                    : new PushHandlerMessageOutcome(batchResult.Result, batchResult.DelayLevelWhenNextConsume);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception exception)
            {
                _logger.LogError(
                    exception,
                    "Legacy message handler failed for message {MessageId}",
                    entry.Message.MessageId);
                outcome = PushHandlerMessageOutcome.Retry(delayLevelWhenNextConsume: 0);
            }

            if (_options.ConsumerMode == ConsumerMode.Broadcasting ||
                entry.FifoOrder is null || outcome.Result != ConsumeResult.Retry)
            {
                return outcome;
            }

            if (attempt >= _options.MaxDeliveryAttempts)
            {
                return new PushHandlerMessageOutcome(ConsumeResult.DeadLetter, outcome.DelayLevelWhenNextConsume);
            }

            attempt++;
            await Task.Delay(_options.RetryDelay, cancellationToken).ConfigureAwait(false);
        }
    }

    private bool UsesConcurrentTimeoutRecovery(PullConsumeRequest request) =>
        _options.ConsumerMode == ConsumerMode.Clustering &&
        request.Entries.All(static entry => entry.FifoOrder is null);
}
