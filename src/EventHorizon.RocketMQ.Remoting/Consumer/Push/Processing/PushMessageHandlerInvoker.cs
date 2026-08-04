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

using EventHorizon.RocketMQ.Remoting.Instrumentation;
using Microsoft.Extensions.Logging;

namespace EventHorizon.RocketMQ.Remoting.Consumer.Push.Processing;

internal sealed class PushMessageHandlerInvoker : IDisposable
{
    private readonly RemotingPushConsumerOptions _options;
    private readonly Func<
        IReadOnlyList<RemotingMessageView>,
        RemotingPushConsumeContext,
        CancellationToken,
        ValueTask<ConsumeResult>> _messageHandler;
    private readonly TimeProvider _timeProvider;
    private readonly IRemotingRocketMQTelemetry _telemetry;
    private readonly ILogger _logger;
    private readonly SemaphoreSlim _concurrency;
    private int _abandonResults;

    public PushMessageHandlerInvoker(
        RemotingPushConsumerOptions options,
        Func<
            IReadOnlyList<RemotingMessageView>,
            RemotingPushConsumeContext,
            CancellationToken,
            ValueTask<ConsumeResult>> messageHandler,
        TimeProvider timeProvider,
        IRemotingRocketMQTelemetry telemetry,
        ILogger logger)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(messageHandler);
        ArgumentNullException.ThrowIfNull(timeProvider);
        ArgumentNullException.ThrowIfNull(telemetry);
        ArgumentNullException.ThrowIfNull(logger);

        _options = options;
        _messageHandler = messageHandler;
        _timeProvider = timeProvider;
        _telemetry = telemetry;
        _logger = logger;
        _concurrency = new SemaphoreSlim(options.MaxConcurrency, options.MaxConcurrency);
    }

    public bool ShouldAbandonResults => Volatile.Read(ref _abandonResults) != 0;

    public void AbandonResults() => Interlocked.Exchange(ref _abandonResults, 1);

    public async ValueTask<PushHandlerBatchResult> InvokeAsync(
        IReadOnlyList<RemotingMessageView> messages,
        bool enableTimeoutRecovery,
        CancellationToken cancellationToken)
    {
        var result = await InvokeCoreAsync(
            messages,
            enableTimeoutRecovery,
            canInvoke: null,
            cancellationToken).ConfigureAwait(false);
        return result!.Value;
    }

    public ValueTask<PushHandlerBatchResult?> InvokeIfAsync(
        IReadOnlyList<RemotingMessageView> messages,
        bool enableTimeoutRecovery,
        Func<bool> canInvoke,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(canInvoke);
        return InvokeCoreAsync(messages, enableTimeoutRecovery, canInvoke, cancellationToken);
    }

    private async ValueTask<PushHandlerBatchResult?> InvokeCoreAsync(
        IReadOnlyList<RemotingMessageView> messages,
        bool enableTimeoutRecovery,
        Func<bool>? canInvoke,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(messages);
        if (messages.Count == 0)
        {
            throw new ArgumentException("At least one message is required.", nameof(messages));
        }

        await _concurrency.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (canInvoke is not null && !canInvoke())
            {
                return null;
            }

            return enableTimeoutRecovery
                ? await InvokeWithTimeoutAsync(messages, cancellationToken).ConfigureAwait(false)
                : await InvokeTrackedAsync(
                    messages,
                    CreateConsumeContext(),
                    cancellationToken,
                    StartTelemetry(messages)).ConfigureAwait(false);
        }
        finally
        {
            _concurrency.Release();
        }
    }

    public void Dispose() => _concurrency.Dispose();

    private async ValueTask<PushHandlerBatchResult> InvokeWithTimeoutAsync(
        IReadOnlyList<RemotingMessageView> messages,
        CancellationToken cancellationToken)
    {
        var handlerCancellation = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        using var timeoutCancellation = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        try
        {
            var activeHandlerCancellation = handlerCancellation;
            var telemetry = StartTelemetry(messages);
            var handler = InvokeTrackedAsync(
                messages,
                CreateConsumeContext(),
                activeHandlerCancellation.Token,
                telemetry);
            if (handler.IsCompletedSuccessfully)
            {
                cancellationToken.ThrowIfCancellationRequested();
                return handler.Result;
            }

            var handlerTask = handler.AsTask();
            var timeout = Task.Delay(_options.ConsumeTimeout, _timeProvider, timeoutCancellation.Token);
            await Task.WhenAny(handlerTask, timeout).ConfigureAwait(false);
            timeoutCancellation.Cancel();
            if (handlerTask.IsCompleted)
            {
                return await handlerTask.ConfigureAwait(false);
            }

            if (cancellationToken.IsCancellationRequested)
            {
                telemetry.Complete(new OperationCanceledException(cancellationToken));
                _ = ObserveDetachedHandlerAsync(handlerTask, activeHandlerCancellation, messages.Count);
                handlerCancellation = null;
                cancellationToken.ThrowIfCancellationRequested();
            }

            telemetry.Complete(false, "timeout");
            try
            {
                activeHandlerCancellation.Cancel();
            }
            catch (AggregateException exception)
            {
                _logger.LogWarning(
                    exception,
                    "A cancellation callback failed after a legacy message handler exceeded the consume timeout");
            }

            _ = ObserveDetachedHandlerAsync(handlerTask, activeHandlerCancellation, messages.Count);
            handlerCancellation = null;
            _logger.LogWarning(
                "Legacy message handler exceeded the configured consume timeout of {ConsumeTimeout} for a batch containing {MessageCount} messages; cancellation was requested and the batch will be retried",
                _options.ConsumeTimeout,
                messages.Count);
            return PushHandlerBatchResult.Retry(
                messages.Count,
                delayLevelWhenNextConsume: 0);
        }
        finally
        {
            handlerCancellation?.Dispose();
        }
    }

    private async ValueTask<PushHandlerBatchResult> InvokeTrackedAsync(
        IReadOnlyList<RemotingMessageView> messages,
        RemotingPushConsumeContext context,
        CancellationToken cancellationToken,
        HandlerTelemetry telemetry)
    {
        try
        {
            var result = await _messageHandler(messages, context, cancellationToken).ConfigureAwait(false);
            if (ShouldAbandonResults)
            {
                cancellationToken.ThrowIfCancellationRequested();
            }

            var handlerResult = PushHandlerBatchResult.Create(
                result,
                context.AckIndex,
                context.DelayLevelWhenNextConsume,
                messages.Count);
            telemetry.Complete(handlerResult.IsFullySuccessful, handlerResult.TelemetryOutcome);
            return handlerResult;
        }
        catch (Exception exception)
        {
            telemetry.Complete(exception);
            throw;
        }
    }

    private async Task ObserveDetachedHandlerAsync(
        Task<PushHandlerBatchResult> handlerTask,
        CancellationTokenSource handlerCancellation,
        int messageCount)
    {
        try
        {
            await handlerTask.ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
        }
        catch (Exception exception)
        {
            _logger.LogDebug(
                exception,
                "Legacy message handler for a detached batch containing {MessageCount} messages completed with an exception",
                messageCount);
        }
        finally
        {
            handlerCancellation.Dispose();
        }
    }

    private static RemotingPushConsumeContext CreateConsumeContext() => new();

    private HandlerTelemetry StartTelemetry(IReadOnlyList<RemotingMessageView> messages) =>
        new(_telemetry.StartProcessBatch(
            messages[0].Topic,
            _options.GroupName,
            messages));

    private sealed class HandlerTelemetry(IRemotingRocketMQTelemetryOperation operation)
    {
        private IRemotingRocketMQTelemetryOperation? _operation = operation;

        public void Complete(bool success, string? outcome = null)
        {
            var current = Interlocked.Exchange(ref _operation, null);
            if (current is null)
            {
                return;
            }

            try
            {
                current.Complete(success, outcome);
            }
            finally
            {
                current.Dispose();
            }
        }

        public void Complete(Exception exception)
        {
            ArgumentNullException.ThrowIfNull(exception);

            var current = Interlocked.Exchange(ref _operation, null);
            if (current is null)
            {
                return;
            }

            try
            {
                current.Complete(exception);
            }
            finally
            {
                current.Dispose();
            }
        }
    }
}
