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
using EventHorizon.RocketMQ.Remoting.Consumer.Settlement;
using Microsoft.Extensions.Logging;

namespace EventHorizon.RocketMQ.Remoting.Consumer.Push.Pop;

// Classic Remoting POP has a fixed receipt deadline. Unlike gRPC Push auto-renewal, this processor never extends a
// receipt while the handler is active. CHANGE_MESSAGE_INVISIBLETIME is issued once only after a Retry result.
internal sealed class PopDeliveryProcessor
{
    private static readonly TimeSpan MinimumOperationRetryDelay = TimeSpan.FromMilliseconds(100);

    private readonly RemotingPushConsumerOptions _options;
    private readonly PopWireClient _popClient;
    private readonly IRemotingSettlementClient _settlementClient;
    private readonly TimeProvider _timeProvider;
    private readonly ILogger _logger;
    private readonly Func<
        IReadOnlyList<RemotingMessageView>,
        Func<bool>,
        CancellationToken,
        ValueTask<IReadOnlyList<PushHandlerMessageOutcome>?>> _messageHandler;

    public PopDeliveryProcessor(
        RemotingPushConsumerOptions options,
        PopWireClient popClient,
        IRemotingSettlementClient settlementClient,
        TimeProvider timeProvider,
        ILogger logger,
        Func<
            IReadOnlyList<RemotingMessageView>,
            Func<bool>,
            CancellationToken,
            ValueTask<IReadOnlyList<PushHandlerMessageOutcome>?>> messageHandler)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(popClient);
        ArgumentNullException.ThrowIfNull(settlementClient);
        ArgumentNullException.ThrowIfNull(timeProvider);
        ArgumentNullException.ThrowIfNull(logger);
        ArgumentNullException.ThrowIfNull(messageHandler);

        _options = options;
        _popClient = popClient;
        _settlementClient = settlementClient;
        _timeProvider = timeProvider;
        _logger = logger;
        _messageHandler = messageHandler;
    }

    public async Task ProcessBatchAsync(
        IReadOnlyList<RemotingPopMessage> messages,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(messages);
        if (messages.Count == 0)
        {
            return;
        }

        var states = messages
            .Select(static message => new PopDeliveryState(message))
            .Where(state => CheckInitialLease(state))
            .ToArray();
        if (states.Length == 0)
        {
            return;
        }

        var outcomes = await _messageHandler(
            states.Select(static state => state.Message.Message).ToArray(),
            () => states.All(HasValidLease),
            cancellationToken).ConfigureAwait(false);
        if (outcomes is null)
        {
            foreach (var state in states)
            {
                LogLeaseExpired(state, "the receipt expired while waiting for handler admission");
            }

            return;
        }

        if (outcomes.Count != states.Length)
        {
            throw new InvalidOperationException(
                $"The POP handler returned {outcomes.Count} outcomes for {states.Length} messages.");
        }

        var settlements = states
            .Select((state, index) => SettleAsync(state, outcomes[index], cancellationToken))
            .ToArray();
        await Task.WhenAll(settlements).ConfigureAwait(false);
    }

    private async Task SettleAsync(
        PopDeliveryState state,
        PushHandlerMessageOutcome handlerOutcome,
        CancellationToken cancellationToken)
    {
        if (!HasValidLease(state))
        {
            LogLeaseExpired(state, "the handler result arrived after the receipt expired");
            return;
        }

        var outcome = handlerOutcome.Result;
        if (outcome == ConsumeResult.Retry &&
            (handlerOutcome.DelayLevelWhenNextConsume < 0 ||
             state.Message.Message.DeliveryAttempt >= _options.MaxDeliveryAttempts))
        {
            outcome = ConsumeResult.DeadLetter;
        }

        switch (outcome)
        {
            case ConsumeResult.Success:
                await RetrySettlementAsync(
                    state,
                    "acknowledge",
                    token => _popClient.AcknowledgeAsync(
                        state.Receipt,
                        state.Message.Message,
                        token),
                    cancellationToken).ConfigureAwait(false);
                break;
            case ConsumeResult.Retry:
                await ChangeInvisibleTimeOnceAsync(
                    state,
                    handlerOutcome.DelayLevelWhenNextConsume,
                    cancellationToken).ConfigureAwait(false);
                break;
            case ConsumeResult.DeadLetter:
                await SendToDeadLetterQueueAsync(state, cancellationToken).ConfigureAwait(false);
                break;
            default:
                throw new InvalidOperationException($"Unsupported POP consume result '{outcome}'.");
        }
    }

    private async Task ChangeInvisibleTimeOnceAsync(
        PopDeliveryState state,
        int delayLevelWhenNextConsume,
        CancellationToken cancellationToken)
    {
        try
        {
            await _popClient.ChangeInvisibleTimeAsync(
                state.Receipt,
                PopRetryDelaySchedule.Resolve(
                    delayLevelWhenNextConsume,
                    state.Message.Message.DeliveryAttempt),
                state.Message.Message,
                cancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception exception)
        {
            _logger.LogWarning(
                exception,
                "Unable to defer POP receipt for {Topic}/{BrokerName}/{QueueId}/{QueueOffset}; the old receipt will not be retried because the Broker outcome is indeterminate",
                state.Receipt.Topic,
                state.Receipt.BrokerName,
                state.Receipt.QueueId,
                state.Receipt.QueueOffset);
        }
    }

    private async Task SendToDeadLetterQueueAsync(
        PopDeliveryState state,
        CancellationToken cancellationToken)
    {
        var receipt = state.Receipt;
        var physicalQueue = new RemotingConsumerQueue(
            receipt.Topic,
            receipt.BrokerName,
            receipt.QueueId);
        if (!await RetrySettlementAsync(
                state,
                "send to the dead-letter queue",
                token => _settlementClient.SendBackAsync(
                    physicalQueue,
                    state.Message.Message,
                    delayLevel: -1,
                    _options.MaxDeliveryAttempts,
                    token),
                cancellationToken).ConfigureAwait(false))
        {
            return;
        }

        await RetrySettlementAsync(
            state,
            "acknowledge after dead-letter forwarding",
            token => _popClient.AcknowledgeAsync(
                state.Receipt,
                state.Message.Message,
                token),
            cancellationToken).ConfigureAwait(false);
    }

    private async Task<bool> RetrySettlementAsync(
        PopDeliveryState state,
        string operation,
        Func<CancellationToken, Task> settle,
        CancellationToken cancellationToken)
    {
        var failedAttempts = 0;
        while (HasValidLease(state))
        {
            try
            {
                await settle(cancellationToken).ConfigureAwait(false);
                return true;
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception exception)
            {
                failedAttempts++;
                LogOperationFailure(state, operation, failedAttempts, exception);
                var retryDelay = GetBoundedRetryDelay(state);
                if (retryDelay <= TimeSpan.Zero)
                {
                    break;
                }

                await Task.Delay(retryDelay, _timeProvider, cancellationToken).ConfigureAwait(false);
            }
        }

        LogLeaseExpired(state, $"the client could not {operation} before the receipt expired");
        return false;
    }

    private bool HasValidLease(PopDeliveryState state) => GetRemainingLease(state) > TimeSpan.Zero;

    private bool CheckInitialLease(PopDeliveryState state)
    {
        if (HasValidLease(state))
        {
            return true;
        }

        LogLeaseExpired(state, "the receipt expired before handler admission");
        return false;
    }

    private TimeSpan GetRemainingLease(PopDeliveryState state) =>
        state.Receipt.PopTime + state.Receipt.InvisibleTime - _timeProvider.GetUtcNow();

    private TimeSpan GetBoundedRetryDelay(PopDeliveryState state)
    {
        var remaining = GetRemainingLease(state);
        if (remaining <= TimeSpan.Zero)
        {
            return TimeSpan.Zero;
        }

        var configured = _options.RetryDelay < MinimumOperationRetryDelay
            ? MinimumOperationRetryDelay
            : _options.RetryDelay;
        return configured < remaining ? configured : remaining;
    }

    private void LogOperationFailure(
        PopDeliveryState state,
        string operation,
        int failedAttempts,
        Exception exception)
    {
        if (failedAttempts == 1)
        {
            _logger.LogWarning(
                exception,
                "Unable to {PopOperation} POP receipt for {Topic}/{BrokerName}/{QueueId}/{QueueOffset}; retrying while the receipt is valid",
                operation,
                state.Receipt.Topic,
                state.Receipt.BrokerName,
                state.Receipt.QueueId,
                state.Receipt.QueueOffset);
            return;
        }

        _logger.LogDebug(
            exception,
            "Unable to {PopOperation} POP receipt for {Topic}/{BrokerName}/{QueueId}/{QueueOffset} on attempt {Attempt}",
            operation,
            state.Receipt.Topic,
            state.Receipt.BrokerName,
            state.Receipt.QueueId,
            state.Receipt.QueueOffset,
            failedAttempts);
    }

    private void LogLeaseExpired(PopDeliveryState state, string reason)
    {
        if (state.LeaseExpirationLogged)
        {
            return;
        }

        state.LeaseExpirationLogged = true;
        _logger.LogWarning(
            "POP receipt expired for {Topic}/{BrokerName}/{QueueId}/{QueueOffset}; {LeaseExpirationReason}. The Broker may redeliver the message",
            state.Receipt.Topic,
            state.Receipt.BrokerName,
            state.Receipt.QueueId,
            state.Receipt.QueueOffset,
            reason);
    }

    private sealed class PopDeliveryState(RemotingPopMessage message)
    {
        public RemotingPopMessage Message { get; } = message;

        public RemotingPopReceipt Receipt { get; set; } = message.Receipt;

        public bool LeaseExpirationLogged { get; set; }
    }
}
