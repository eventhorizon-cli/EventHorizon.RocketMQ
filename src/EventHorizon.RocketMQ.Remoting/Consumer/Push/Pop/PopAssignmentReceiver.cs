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
using Microsoft.Extensions.Logging;

namespace EventHorizon.RocketMQ.Remoting.Consumer.Push.Pop;

internal sealed class PopAssignmentReceiver : IRemotingPushReceiver
{
    private readonly RemotingPushConsumerOptions _options;
    private readonly PopWireClient _popClient;
    private readonly IRemotingConsumerEngine _consumerEngine;
    private readonly TimeProvider _timeProvider;
    private readonly ILogger _logger;
    private readonly FilterExpression _filter;
    private readonly Func<
        IReadOnlyList<RemotingMessageView>,
        CancellationToken,
        ValueTask<IReadOnlyList<ConsumeResult>>> _messageHandler;
    private readonly CancellationTokenSource _stopping;

    public PopAssignmentReceiver(
        RemotingConsumerQueue queue,
        RemotingPushAssignment assignment,
        RemotingPushConsumerOptions options,
        PopWireClient popClient,
        IRemotingConsumerEngine consumerEngine,
        TimeProvider timeProvider,
        ILogger logger,
        FilterExpression filter,
        Func<
            IReadOnlyList<RemotingMessageView>,
            CancellationToken,
            ValueTask<IReadOnlyList<ConsumeResult>>> messageHandler,
        CancellationToken stopping)
    {
        Queue = queue;
        Assignment = assignment;
        _options = options;
        _popClient = popClient;
        _consumerEngine = consumerEngine;
        _timeProvider = timeProvider;
        _logger = logger;
        _filter = filter;
        _messageHandler = messageHandler;
        _stopping = CancellationTokenSource.CreateLinkedTokenSource(stopping);
    }

    public RemotingConsumerQueue Queue { get; }

    public RemotingPushAssignment Assignment { get; }

    public CancellationToken Cancellation => _stopping.Token;

    public Task Task { get; private set; } = Task.CompletedTask;

    public void Start()
    {
        if (!Task.IsCompleted)
        {
            throw new InvalidOperationException("The POP receiver has already been started.");
        }

        Task = RunAsync();
    }

    public void Stop() => _stopping.Cancel();

    public void Dispose() => _stopping.Dispose();

    private async Task RunAsync()
    {
        while (!_stopping.IsCancellationRequested)
        {
            try
            {
                var result = await _popClient.PopAsync(
                    Assignment,
                    Queue.BrokerAddress,
                    _filter,
                    _stopping.Token).ConfigureAwait(false);
                foreach (var messages in result.Messages.Chunk(_options.ConsumeMessageBatchSize))
                {
                    await ProcessBatchAsync(messages, _stopping.Token).ConfigureAwait(false);
                }
            }
            catch (OperationCanceledException) when (_stopping.IsCancellationRequested)
            {
                return;
            }
            catch (Exception exception)
            {
                _logger.LogWarning(
                    exception,
                    "Legacy POP failed for {Topic}/{BrokerName}/{QueueId}; retrying",
                    Assignment.Topic,
                    Assignment.BrokerName,
                    Assignment.QueueId);
                await Task.Delay(_options.RetryDelay, _timeProvider, _stopping.Token).ConfigureAwait(false);
            }
        }
    }

    private async Task ProcessBatchAsync(
        IReadOnlyList<RemotingPopMessage> messages,
        CancellationToken cancellationToken)
    {
        var states = messages.Select(static message => new PopDeliveryState(message)).ToArray();
        var handlerCompleted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var renewals = states
            .Select(state => RenewWhileHandlingAsync(state, handlerCompleted.Task, cancellationToken))
            .ToArray();
        IReadOnlyList<ConsumeResult> outcomes;
        try
        {
            outcomes = await _messageHandler(
                states.Select(static state => state.Message.Message).ToArray(),
                cancellationToken).ConfigureAwait(false);
            if (outcomes.Count != states.Length)
            {
                throw new InvalidOperationException(
                    $"The POP handler returned {outcomes.Count} outcomes for {states.Length} messages.");
            }
        }
        finally
        {
            handlerCompleted.TrySetResult();
        }

        await Task.WhenAll(renewals).ConfigureAwait(false);
        for (var index = 0; index < states.Length; index++)
        {
            var state = states[index];
            var outcome = outcomes[index];
            if (outcome == ConsumeResult.Retry &&
                state.Message.Message.DeliveryAttempt >= _options.MaxDeliveryAttempts)
            {
                outcome = ConsumeResult.DeadLetter;
            }

            switch (outcome)
            {
                case ConsumeResult.Success:
                    await _popClient.AcknowledgeAsync(
                        state.Receipt,
                        state.Message.Message,
                        cancellationToken).ConfigureAwait(false);
                    break;
                case ConsumeResult.Retry:
                    state.Receipt = await _popClient.ChangeInvisibleTimeAsync(
                        state.Receipt,
                        GetRetryInvisibleDuration(),
                        suspend: false,
                        state.Message.Message,
                        cancellationToken).ConfigureAwait(false);
                    break;
                case ConsumeResult.DeadLetter:
                    await SendToDeadLetterQueueAsync(state, cancellationToken).ConfigureAwait(false);
                    break;
                default:
                    throw new InvalidOperationException($"Unsupported POP consume result '{outcome}'.");
            }
        }
    }

    private async Task RenewWhileHandlingAsync(
        PopDeliveryState state,
        Task handlerCompleted,
        CancellationToken cancellationToken)
    {
        while (!handlerCompleted.IsCompleted)
        {
            var delay = TimeSpan.FromTicks(Math.Max(1, state.Receipt.InvisibleTime.Ticks * 4 / 5));
            var renewalDue = Task.Delay(delay, _timeProvider, cancellationToken);
            if (await Task.WhenAny(renewalDue, handlerCompleted).ConfigureAwait(false) == handlerCompleted)
            {
                return;
            }

            await renewalDue.ConfigureAwait(false);
            try
            {
                state.Receipt = await _popClient.ChangeInvisibleTimeAsync(
                    state.Receipt,
                    _options.PopInvisibleDuration,
                    suspend: true,
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
                    "Unable to renew POP receipt for {Topic}/{BrokerName}/{QueueId}/{QueueOffset}",
                    state.Receipt.Topic,
                    state.Receipt.BrokerName,
                    state.Receipt.QueueId,
                    state.Receipt.QueueOffset);
            }
        }
    }

    private async Task SendToDeadLetterQueueAsync(
        PopDeliveryState state,
        CancellationToken cancellationToken)
    {
        var receipt = state.Receipt;
        var physicalQueue = new RemotingConsumerQueue(
            receipt.Topic,
            receipt.QueueId,
            receipt.BrokerName,
            receipt.BrokerAddress);
        await _consumerEngine.SendBackAsync(
            physicalQueue,
            state.Message.Message,
            delayLevel: -1,
            _options.MaxDeliveryAttempts,
            cancellationToken).ConfigureAwait(false);
        await _popClient.AcknowledgeAsync(
            receipt,
            state.Message.Message,
            cancellationToken).ConfigureAwait(false);
    }

    private TimeSpan GetRetryInvisibleDuration()
    {
        if (_options.RetryDelay < RemotingPushConsumerOptions.MinimumPopInvisibleDuration)
        {
            return RemotingPushConsumerOptions.MinimumPopInvisibleDuration;
        }

        return _options.RetryDelay > RemotingPushConsumerOptions.MaximumPopInvisibleDuration
            ? RemotingPushConsumerOptions.MaximumPopInvisibleDuration
            : _options.RetryDelay;
    }

    private sealed class PopDeliveryState(RemotingPopMessage message)
    {
        public RemotingPopMessage Message { get; } = message;

        public RemotingPopReceipt Receipt { get; set; } = message.Receipt;
    }
}
