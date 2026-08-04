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
using EventHorizon.RocketMQ.Remoting.Consumer.Push.Processing;
using EventHorizon.RocketMQ.Remoting.Consumer.Push.Pull.Offset;
using EventHorizon.RocketMQ.Remoting.Consumer.Push.Pull.Settlement;
using Microsoft.Extensions.Logging;

namespace EventHorizon.RocketMQ.Remoting.Consumer.Push.Pull.Receive;

internal sealed class OrderlyPullReceiveLoop
{
    private static readonly TimeSpan MaximumBrokerLockLiveTime = TimeSpan.FromSeconds(30);

    private readonly RemotingPushConsumerOptions _options;
    private readonly IRemotingPullWireClient _pullClient;
    private readonly PullOffsetManager _offsetManager;
    private readonly PullSendBackSettlement _sendBackSettlement;
    private readonly PushMessageHandlerInvoker _messageHandlerInvoker;
    private readonly TimeProvider _timeProvider;
    private readonly ILogger _logger;

    public OrderlyPullReceiveLoop(
        RemotingPushConsumerOptions options,
        IRemotingPullWireClient pullClient,
        PullOffsetManager offsetManager,
        PullSendBackSettlement sendBackSettlement,
        PushMessageHandlerInvoker messageHandlerInvoker,
        TimeProvider timeProvider,
        ILogger logger)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(pullClient);
        ArgumentNullException.ThrowIfNull(offsetManager);
        ArgumentNullException.ThrowIfNull(sendBackSettlement);
        ArgumentNullException.ThrowIfNull(messageHandlerInvoker);
        ArgumentNullException.ThrowIfNull(timeProvider);
        ArgumentNullException.ThrowIfNull(logger);

        _options = options;
        _pullClient = pullClient;
        _offsetManager = offsetManager;
        _sendBackSettlement = sendBackSettlement;
        _messageHandlerInvoker = messageHandlerInvoker;
        _timeProvider = timeProvider;
        _logger = logger;
    }

    public async Task RunAsync(PullAssignmentReceiver receiver)
    {
        ArgumentNullException.ThrowIfNull(receiver);
        var queue = receiver.Queue;
        var cancellationToken = receiver.Cancellation;
        var offset = await _offsetManager.InitializeAsync(queue, cancellationToken).ConfigureAwait(false);
        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                if (UsesBrokerQueueLocks)
                {
                    await receiver.WaitForBrokerLockAsync(
                        _timeProvider,
                        MaximumBrokerLockLiveTime,
                        cancellationToken).ConfigureAwait(false);
                }

                var result = await _pullClient.PullAsync(
                        queue,
                        offset,
                        receiver.Target.Filter,
                        cancellationToken: cancellationToken)
                    .ConfigureAwait(false);
                if (!HasValidBrokerQueueLock(receiver))
                {
                    continue;
                }

                if (result.Status == RemotingPullStatus.OffsetIllegal)
                {
                    await _offsetManager.PersistAsync(queue, result.NextOffset, cancellationToken)
                        .ConfigureAwait(false);
                    offset = result.NextOffset;
                    continue;
                }

                if (result.Messages.Count == 0)
                {
                    if (_offsetManager.HasResetBoundary(queue) ||
                        (_options.ConsumerMode == ConsumerMode.Broadcasting && result.NextOffset != offset))
                    {
                        await _offsetManager.PersistAsync(queue, result.NextOffset, cancellationToken)
                            .ConfigureAwait(false);
                    }

                    offset = result.NextOffset;
                    continue;
                }

                var completed = true;
                foreach (var message in result.Messages.OrderBy(static message => message.QueueOffset))
                {
                    var outcome = await HandleMessageAsync(receiver, message, cancellationToken)
                        .ConfigureAwait(false);
                    if (_messageHandlerInvoker.ShouldAbandonResults)
                    {
                        cancellationToken.ThrowIfCancellationRequested();
                    }

                    if (outcome == OrderlyDeliveryOutcome.LeaseLost)
                    {
                        completed = false;
                        break;
                    }

                    if (_options.ConsumerMode == ConsumerMode.Broadcasting)
                    {
                        if (outcome != OrderlyDeliveryOutcome.Success)
                        {
                            _logger.LogWarning(
                                "Broadcast orderly consumer drops message {MessageId} after handler outcome {ConsumeResult}; broadcast retry and dead-letter queues are unavailable",
                                message.MessageId,
                                outcome);
                        }
                    }
                    else if (outcome == OrderlyDeliveryOutcome.DeadLetter)
                    {
                        await _sendBackSettlement.SendAsync(
                            queue,
                            message,
                            deadLetter: true,
                            delayLevel: -1,
                            cancellationToken).ConfigureAwait(false);
                    }

                    if (!HasValidBrokerQueueLock(receiver))
                    {
                        completed = false;
                        break;
                    }

                    var nextOffset = checked(message.QueueOffset + 1);
                    await _offsetManager.PersistAsync(queue, nextOffset, cancellationToken).ConfigureAwait(false);
                    offset = nextOffset;
                }

                if (!completed)
                {
                    continue;
                }

                if (result.NextOffset > offset)
                {
                    await _offsetManager.PersistAsync(queue, result.NextOffset, cancellationToken)
                        .ConfigureAwait(false);
                    offset = result.NextOffset;
                }
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                return;
            }
            catch (Exception exception)
            {
                _logger.LogWarning(
                    exception,
                    "Legacy orderly pull failed for {Topic}/{BrokerName}/{QueueId}; retrying",
                    queue.Topic,
                    queue.BrokerName,
                    queue.QueueId);
                await Task.Delay(_options.RetryDelay, cancellationToken).ConfigureAwait(false);
            }
        }
    }

    private async ValueTask<OrderlyDeliveryOutcome> HandleMessageAsync(
        PullAssignmentReceiver receiver,
        RemotingMessageView message,
        CancellationToken cancellationToken)
    {
        var attempt = message.DeliveryAttempt;
        while (true)
        {
            if (!HasValidBrokerQueueLock(receiver))
            {
                return OrderlyDeliveryOutcome.LeaseLost;
            }

            ConsumeResult result;
            try
            {
                var batchResult = await _messageHandlerInvoker.InvokeAsync(
                    new[] { message },
                    enableTimeoutRecovery: false,
                    cancellationToken).ConfigureAwait(false);
                if (_messageHandlerInvoker.ShouldAbandonResults)
                {
                    cancellationToken.ThrowIfCancellationRequested();
                }

                result = batchResult.Result;
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception exception)
            {
                _logger.LogError(
                    exception,
                    "Legacy orderly message handler failed for message {MessageId}",
                    message.MessageId);
                result = ConsumeResult.Retry;
            }

            if (!HasValidBrokerQueueLock(receiver))
            {
                return OrderlyDeliveryOutcome.LeaseLost;
            }

            switch (result)
            {
                case ConsumeResult.Success:
                    return OrderlyDeliveryOutcome.Success;
                case ConsumeResult.DeadLetter:
                    return OrderlyDeliveryOutcome.DeadLetter;
                case ConsumeResult.Retry when _options.ConsumerMode == ConsumerMode.Broadcasting:
                    return OrderlyDeliveryOutcome.Retry;
                case ConsumeResult.Retry when attempt >= _options.MaxDeliveryAttempts:
                    return OrderlyDeliveryOutcome.DeadLetter;
                case ConsumeResult.Retry:
                    attempt++;
                    await Task.Delay(_options.RetryDelay, cancellationToken).ConfigureAwait(false);
                    break;
                default:
                    throw new InvalidOperationException($"Unsupported ordered consume result '{result}'.");
            }
        }
    }

    private bool UsesBrokerQueueLocks =>
        _options.ConsumeOrderly && _options.ConsumerMode == ConsumerMode.Clustering;

    private bool HasValidBrokerQueueLock(PullAssignmentReceiver receiver) =>
        !UsesBrokerQueueLocks || receiver.HasValidBrokerLock(_timeProvider, MaximumBrokerLockLiveTime);

    private enum OrderlyDeliveryOutcome
    {
        Success,
        Retry,
        DeadLetter,
        LeaseLost
    }
}
