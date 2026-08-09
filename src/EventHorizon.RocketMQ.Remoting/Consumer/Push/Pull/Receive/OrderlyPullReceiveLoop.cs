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
    private static readonly TimeSpan MinimumSuspendDuration = TimeSpan.FromMilliseconds(10);
    private static readonly TimeSpan MaximumSuspendDuration = TimeSpan.FromSeconds(30);

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
                await Task.Delay(_options.RetryDelay, _timeProvider, cancellationToken).ConfigureAwait(false);
            }
        }
    }

    private async ValueTask<OrderlyDeliveryOutcome> HandleMessageAsync(
        PullAssignmentReceiver receiver,
        RemotingMessageView message,
        CancellationToken cancellationToken)
    {
        var maxReconsumeTimes = _options.OrderlyMaxReconsumeTimes < 0
            ? int.MaxValue
            : _options.OrderlyMaxReconsumeTimes;
        while (true)
        {
            if (!HasValidBrokerQueueLock(receiver))
            {
                return OrderlyDeliveryOutcome.LeaseLost;
            }

            var consumeContext = new RemotingPushConsumeContext();
            PushHandlerBatchResult batchResult;
            try
            {
                batchResult = await _messageHandlerInvoker.InvokeOrderlyAsync(
                    new[] { message },
                    consumeContext,
                    cancellationToken).ConfigureAwait(false);
                if (_messageHandlerInvoker.ShouldAbandonResults)
                {
                    cancellationToken.ThrowIfCancellationRequested();
                }
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
                // Released Java converts the exception to SUSPEND while retaining the same orderly context, including
                // an override assigned before the handler threw.
                // https://github.com/apache/rocketmq/blob/rocketmq-all-5.5.0/client/src/main/java/org/apache/rocketmq/client/impl/consumer/ConsumeMessageOrderlyService.java
                batchResult = PushHandlerBatchResult.Retry(
                    messageCount: 1,
                    delayLevelWhenNextConsume: 0,
                    consumeContext.SuspendCurrentQueueDuration);
            }

            if (!HasValidBrokerQueueLock(receiver))
            {
                return OrderlyDeliveryOutcome.LeaseLost;
            }

            switch (batchResult.Result)
            {
                case ConsumeResult.Success:
                    return OrderlyDeliveryOutcome.Success;
                case ConsumeResult.Retry when message.ReconsumeTimes >= maxReconsumeTimes:
                    try
                    {
                        await _sendBackSettlement.SendOrderlyRetryAsync(
                            message,
                            maxReconsumeTimes,
                            cancellationToken).ConfigureAwait(false);
                        return OrderlyDeliveryOutcome.Success;
                    }
                    catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                    {
                        throw;
                    }
                    catch (Exception exception)
                    {
                        _logger.LogWarning(
                            exception,
                            "Unable to publish exhausted orderly message {MessageId} to the retry topic; retaining the current message for another local attempt",
                            message.MessageId);
                        IncrementReconsumeTimes(message);
                        var terminalSuspendDuration = ResolveSuspendDuration(
                            batchResult.SuspendCurrentQueueDuration,
                            _options.OrderlySuspendDuration);
                        await Task.Delay(terminalSuspendDuration, _timeProvider, cancellationToken)
                            .ConfigureAwait(false);
                        break;
                    }
                case ConsumeResult.Retry:
                    IncrementReconsumeTimes(message);
                    var suspendDuration = ResolveSuspendDuration(
                        batchResult.SuspendCurrentQueueDuration,
                        _options.OrderlySuspendDuration);
                    await Task.Delay(suspendDuration, _timeProvider, cancellationToken).ConfigureAwait(false);
                    break;
                default:
                    throw new InvalidOperationException($"Unsupported ordered consume result '{batchResult.Result}'.");
            }
        }
    }

    private static void IncrementReconsumeTimes(RemotingMessageView message)
    {
        if (message.ReconsumeTimes < int.MaxValue)
        {
            message.ReconsumeTimes++;
        }
    }

    internal static TimeSpan ResolveSuspendDuration(TimeSpan? requested, TimeSpan configured)
    {
        var duration = requested ?? configured;
        if (duration < MinimumSuspendDuration)
        {
            return MinimumSuspendDuration;
        }

        return duration > MaximumSuspendDuration ? MaximumSuspendDuration : duration;
    }

    private bool UsesBrokerQueueLocks =>
        _options.ConsumeOrderly && _options.ConsumerMode == ConsumerMode.Clustering;

    private bool HasValidBrokerQueueLock(PullAssignmentReceiver receiver) =>
        !UsesBrokerQueueLocks || receiver.HasValidBrokerLock(_timeProvider, MaximumBrokerLockLiveTime);

    private enum OrderlyDeliveryOutcome
    {
        Success,
        LeaseLost
    }
}
