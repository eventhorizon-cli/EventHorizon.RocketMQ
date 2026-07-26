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

using System.Collections.Concurrent;
using System.Threading.Channels;
using EventHorizon.RocketMQ.Grpc.Consumer;
using EventHorizon.RocketMQ.Grpc.Instrumentation;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Proto = Apache.Rocketmq.V2;

namespace EventHorizon.RocketMQ.Grpc.Consumer.Push;

internal sealed class GrpcPushConsumer : IGrpcPushConsumer
{
    private readonly GrpcPushConsumerOptions _options;
    private readonly Func<GrpcMessageView, CancellationToken, ValueTask<ConsumeResult>> _messageHandler;
    private readonly IGrpcReceiveConsumerEngine _engine;
    private readonly IGrpcRocketMQTelemetry _telemetry;
    private readonly ILogger _logger;
    private readonly SemaphoreSlim _lifecycleGate = new(1, 1);
    private readonly object _fifoGate = new();
    private readonly Dictionary<string, Task> _fifoTails = new(StringComparer.Ordinal);
    private readonly HashSet<string> _blockedFifoGroups = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<GrpcMessageView, FifoOrder> _fifoOrders = new();
    private readonly ConcurrentDictionary<GrpcMessageView, ByteReservation> _cachedMessageBytes = new();
    private readonly SemaphoreSlim _subscriptionSignal = new(0, 1);
    private Channel<GrpcMessageView> _messages;
    private ByteCapacityGate _messageByteCapacity;
    private TaskCompletionSource _fifoBlockedSignal = NewFifoBlockedSignal();
    private CancellationTokenSource? _receivingCts;
    private CancellationTokenSource? _processingCts;
    private CancellationTokenSource? _fifoRetryCts;
    private Task[] _receivers = Array.Empty<Task>();
    private Task[] _workers = Array.Empty<Task>();
    private int _started;
    private int _disposed;

    public GrpcPushConsumer(
        IOptions<GrpcPushConsumerOptions> options,
        IGrpcReceiveConsumerEngine engine,
        ILogger<GrpcPushConsumer> logger,
        Func<GrpcMessageView, CancellationToken, ValueTask<ConsumeResult>>? messageHandler = null,
        IGrpcRocketMQTelemetry? telemetry = null)
    {
        _options = options.Value;
        _messageHandler = messageHandler ?? _options.MessageHandler ??
            throw new ArgumentException("A message handler is required.", nameof(options));
        _engine = engine;
        _telemetry = telemetry ?? GrpcRocketMQTelemetry.Disabled;
        _logger = logger;
        _messages = CreateMessageChannel();
        _messageByteCapacity = new ByteCapacityGate(_options.MaxCachedMessageBytes);
    }

    public async ValueTask StartAsync(CancellationToken cancellationToken = default)
    {
        await _lifecycleGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);
            if (Volatile.Read(ref _started) != 0)
            {
                return;
            }

            DrainMessageChannel();
            ReleaseAllCachedMessageBytes();
            _messages = CreateMessageChannel();
            _messageByteCapacity = new ByteCapacityGate(_options.MaxCachedMessageBytes);
            _fifoBlockedSignal = NewFifoBlockedSignal();
            lock (_fifoGate)
            {
                _fifoTails.Clear();
                _blockedFifoGroups.Clear();
            }

            _fifoOrders.Clear();
            DrainSubscriptionSignal();
            _receivingCts = new CancellationTokenSource();
            _processingCts = new CancellationTokenSource();
            _fifoRetryCts = new CancellationTokenSource();
            try
            {
                await _engine.StartAsync(cancellationToken).ConfigureAwait(false);
                _workers = Enumerable.Range(0, _options.MaxConcurrency)
                    .Select(_ => ProcessMessagesAsync(_processingCts.Token))
                    .ToArray();
                _receivers = [CoordinateSubscriptionLoopsAsync(_receivingCts.Token)];
                Volatile.Write(ref _started, 1);
            }
            catch
            {
                _messages.Writer.TryComplete();
                _receivingCts.Cancel();
                _processingCts.Cancel();
                _receivingCts.Dispose();
                _processingCts.Dispose();
                _fifoRetryCts.Dispose();
                _receivingCts = null;
                _processingCts = null;
                _fifoRetryCts = null;
                await _engine.StopAsync().ConfigureAwait(false);
                throw;
            }
        }
        finally
        {
            _lifecycleGate.Release();
        }
    }

    public async ValueTask StopAsync(CancellationToken cancellationToken = default)
    {
        await _lifecycleGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (Volatile.Read(ref _started) == 0)
            {
                return;
            }

            Volatile.Write(ref _started, 0);
            await StopCoreAsync(cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            _lifecycleGate.Release();
        }
    }

    public async Task SubscribeAsync(
        string topic,
        FilterExpression? expression = null,
        CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);
        await _engine.SubscribeAsync(topic, expression, cancellationToken).ConfigureAwait(false);
        SignalSubscriptionChange();
    }

    public async Task UnsubscribeAsync(string topic, CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);
        await _engine.UnsubscribeAsync(topic, cancellationToken).ConfigureAwait(false);
        SignalSubscriptionChange();
    }

    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
        {
            return;
        }

        await _lifecycleGate.WaitAsync(CancellationToken.None).ConfigureAwait(false);
        try
        {
            if (Volatile.Read(ref _started) != 0)
            {
                Volatile.Write(ref _started, 0);
                await StopCoreAsync(CancellationToken.None).ConfigureAwait(false);
            }
            else
            {
                _messages.Writer.TryComplete();
                DrainMessageChannel();
                ReleaseAllCachedMessageBytes();
                lock (_fifoGate)
                {
                    _fifoTails.Clear();
                    _blockedFifoGroups.Clear();
                }

                _fifoOrders.Clear();
                await _engine.DisposeAsync().ConfigureAwait(false);
            }
        }
        finally
        {
            _lifecycleGate.Release();
        }
    }

    private async Task StopCoreAsync(CancellationToken cancellationToken)
    {
        var receivingCts = _receivingCts!;
        var processingCts = _processingCts!;
        var fifoRetryCts = _fifoRetryCts!;
        receivingCts.Cancel();
        fifoRetryCts.Cancel();
        try
        {
            try
            {
                await Task.WhenAll(_receivers).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (receivingCts.IsCancellationRequested)
            {
            }

            _messages.Writer.TryComplete();
            var workers = Task.WhenAll(_workers);
            try
            {
                if (!workers.IsCompleted)
                {
                    var completed = await Task.WhenAny(workers, _fifoBlockedSignal.Task)
                        .WaitAsync(cancellationToken).ConfigureAwait(false);
                    if (!ReferenceEquals(completed, workers))
                    {
                        processingCts.Cancel();
                    }
                }

                await workers.WaitAsync(cancellationToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                processingCts.Cancel();
                try
                {
                    await Task.WhenAll(_workers).ConfigureAwait(false);
                }
                catch (OperationCanceledException) when (processingCts.IsCancellationRequested)
                {
                }

                throw;
            }
            catch (OperationCanceledException) when (processingCts.IsCancellationRequested)
            {
            }
        }
        finally
        {
            processingCts.Cancel();
            _messages.Writer.TryComplete();
            try
            {
                try
                {
                    await Task.WhenAll(_workers).ConfigureAwait(false);
                }
                catch (OperationCanceledException) when (processingCts.IsCancellationRequested)
                {
                }
            }
            finally
            {
                try
                {
                    await _engine.StopAsync().ConfigureAwait(false);
                }
                finally
                {
                    receivingCts.Dispose();
                    processingCts.Dispose();
                    fifoRetryCts.Dispose();
                    _receivingCts = null;
                    _processingCts = null;
                    _fifoRetryCts = null;
                    _receivers = Array.Empty<Task>();
                    _workers = Array.Empty<Task>();
                    DrainMessageChannel();
                    lock (_fifoGate)
                    {
                        _fifoTails.Clear();
                        _blockedFifoGroups.Clear();
                    }

                    _fifoOrders.Clear();
                    ReleaseAllCachedMessageBytes();
                }
            }
        }
    }

    private async Task CoordinateSubscriptionLoopsAsync(CancellationToken cancellationToken)
    {
        var active = new Dictionary<string, TopicReceiver>(StringComparer.Ordinal);
        try
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                var desired = _engine.GetSubscriptions().ToDictionary(
                    static subscription => subscription.Key,
                    static subscription => subscription.Value,
                    StringComparer.Ordinal);
                var removed = active.Values
                    .Where(receiver => !desired.TryGetValue(receiver.Topic, out var filter) ||
                                       filter != receiver.Filter)
                    .ToArray();
                foreach (var receiver in removed)
                {
                    active.Remove(receiver.Topic);
                    receiver.Cancel();
                }

                await AwaitTopicReceiversAsync(removed).ConfigureAwait(false);
                foreach (var receiver in removed)
                {
                    receiver.Dispose();
                }

                foreach (var subscription in desired)
                {
                    if (active.ContainsKey(subscription.Key))
                    {
                        continue;
                    }

                    var receiver = new TopicReceiver(
                        subscription.Key,
                        subscription.Value,
                        cancellationToken);
                    receiver.Task = ReceiveLoopAsync(
                        receiver.Topic,
                        receiver.Filter,
                        receiver.Stopping.Token);
                    active.Add(receiver.Topic, receiver);
                }

                await _subscriptionSignal.WaitAsync(cancellationToken).ConfigureAwait(false);
            }
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
        }
        finally
        {
            foreach (var receiver in active.Values)
            {
                receiver.Cancel();
            }

            await AwaitTopicReceiversAsync(active.Values).ConfigureAwait(false);
            foreach (var receiver in active.Values)
            {
                receiver.Dispose();
            }
        }
    }

    private async Task AwaitTopicReceiversAsync(IEnumerable<TopicReceiver> receivers)
    {
        foreach (var receiver in receivers)
        {
            try
            {
                await receiver.Task.ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (receiver.IsCancellationRequested)
            {
            }
            catch (Exception exception)
            {
                _logger.LogWarning(exception, "Subscription receiver for topic {Topic} stopped unexpectedly", receiver.Topic);
            }
        }
    }

    private void SignalSubscriptionChange()
    {
        try
        {
            _subscriptionSignal.Release();
        }
        catch (SemaphoreFullException)
        {
        }
    }

    private void DrainSubscriptionSignal()
    {
        while (_subscriptionSignal.Wait(0))
        {
        }
    }

    private async Task ReceiveLoopAsync(string topic, FilterExpression filter, CancellationToken cancellationToken)
    {
        var receivers = new Dictionary<string, AssignmentReceiver>(StringComparer.Ordinal);
        try
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                try
                {
                    var assignments = await _engine.GetAssignmentsAsync(topic, cancellationToken).ConfigureAwait(false);
                    var desired = assignments
                        .GroupBy(static assignment => GetAssignmentKey(assignment.MessageQueue), StringComparer.Ordinal)
                        .ToDictionary(static group => group.Key, static group => group.First(), StringComparer.Ordinal);
                    var removed = receivers
                        .Where(receiver => !desired.ContainsKey(receiver.Key))
                        .Select(static receiver => receiver.Value)
                        .ToArray();

                    foreach (var receiver in removed)
                    {
                        receivers.Remove(receiver.Key);
                        receiver.Cancel();
                    }

                    foreach (var assignment in desired)
                    {
                        if (receivers.ContainsKey(assignment.Key))
                        {
                            continue;
                        }

                        var receiverCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
                        var task = ReceiveAssignmentLoopAsync(
                            topic,
                            assignment.Value.MessageQueue.Clone(),
                            filter,
                            receiverCts.Token);
                        receivers.Add(assignment.Key, new AssignmentReceiver(assignment.Key, receiverCts, task));
                    }

                    if (removed.Length > 0)
                    {
                        try
                        {
                            await Task.WhenAll(removed.Select(static receiver => receiver.Task)).ConfigureAwait(false);
                        }
                        catch (OperationCanceledException) when (
                            removed.All(static receiver => receiver.IsCancellationRequested))
                        {
                        }
                        finally
                        {
                            foreach (var receiver in removed)
                            {
                                receiver.Dispose();
                            }
                        }
                    }

                    await Task.Delay(TimeSpan.FromSeconds(1), cancellationToken).ConfigureAwait(false);
                }
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                {
                    break;
                }
                catch (Exception exception)
                {
                    _logger.LogWarning(exception, "Failed to refresh assignments for topic {Topic}; retrying", topic);
                    await Task.Delay(TimeSpan.FromSeconds(1), cancellationToken).ConfigureAwait(false);
                }
            }
        }
        finally
        {
            foreach (var receiver in receivers.Values)
            {
                receiver.Cancel();
            }

            try
            {
                await Task.WhenAll(receivers.Values.Select(static receiver => receiver.Task)).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (
                receivers.Values.All(static receiver => receiver.IsCancellationRequested))
            {
            }
            finally
            {
                foreach (var receiver in receivers.Values)
                {
                    receiver.Dispose();
                }
            }
        }
    }

    private async Task ReceiveAssignmentLoopAsync(
        string topic,
        Proto.MessageQueue queue,
        FilterExpression filter,
        CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                var messages = await _engine.ReceiveAsync(
                    queue,
                    filter,
                    _options.BatchSize,
                    _options.InvisibleDuration,
                    _options.LongPollingTimeout,
                    true,
                    cancellationToken).ConfigureAwait(false);
                foreach (var message in messages)
                {
                    await EnqueueAsync(message, cancellationToken).ConfigureAwait(false);
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
                    "Failed to receive messages for topic {Topic}, broker {Broker}, queue {QueueId}; retrying",
                    topic,
                    queue.Broker.Name,
                    queue.Id);
                try
                {
                    await Task.Delay(TimeSpan.FromSeconds(1), cancellationToken).ConfigureAwait(false);
                }
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                {
                    return;
                }
            }
        }
    }

    private async Task ProcessMessagesAsync(CancellationToken cancellationToken)
    {
        await foreach (var message in _messages.Reader.ReadAllAsync(cancellationToken).ConfigureAwait(false))
        {
            try
            {
                await ProcessOrderedMessageAsync(message, cancellationToken).ConfigureAwait(false);
            }
            finally
            {
                ReleaseCachedMessageBytes(message);
            }
        }
    }

    private async ValueTask EnqueueAsync(GrpcMessageView message, CancellationToken cancellationToken)
    {
        var byteCapacity = _messageByteCapacity;
        var reservedBytes = await byteCapacity.ReserveAsync(message.Body.Length, cancellationToken).ConfigureAwait(false);
        var reservation = new ByteReservation(byteCapacity, reservedBytes, message.Body.Length);
        FifoOrder? order = null;
        var tracked = false;
        try
        {
            if (!_cachedMessageBytes.TryAdd(message, reservation))
            {
                throw new InvalidOperationException($"Message '{message.MessageId}' was enqueued more than once.");
            }

            tracked = true;
            order = ReserveFifoOrder(message);
            await _messages.Writer.WriteAsync(message, cancellationToken).ConfigureAwait(false);
        }
        catch
        {
            CompleteFifoOrder(message, order);
            if (tracked)
            {
                ReleaseCachedMessageBytes(message);
            }
            else
            {
                byteCapacity.Release(reservedBytes);
            }

            throw;
        }
    }

    private async Task ProcessOrderedMessageAsync(GrpcMessageView message, CancellationToken cancellationToken)
    {
        if (!_fifoOrders.TryGetValue(message, out var order))
        {
            await ProcessMessageAsync(message, false, cancellationToken).ConfigureAwait(false);
            return;
        }

        await order.Predecessor.WaitAsync(cancellationToken).ConfigureAwait(false);
        if (await ProcessMessageAsync(message, true, cancellationToken).ConfigureAwait(false))
        {
            CompleteFifoOrder(message, order);
        }
        else
        {
            BlockFifoOrder(message, order);
        }
    }

    private async Task<bool> ProcessMessageAsync(
        GrpcMessageView message,
        bool fifo,
        CancellationToken cancellationToken)
    {
        var maxDeliveryAttempts = _engine.GetMaxDeliveryAttempts(message, _options.MaxDeliveryAttempts);
        if (message.IsCorrupted)
        {
            return await DiscardCorruptedMessageAsync(
                message,
                fifo,
                maxDeliveryAttempts,
                cancellationToken).ConfigureAwait(false);
        }

        var fifoRetryCancellationToken = fifo
            ? Volatile.Read(ref _fifoRetryCts)?.Token ?? CancellationToken.None
            : CancellationToken.None;
        using var renewalCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        var renewal = RenewInvisibleDurationAsync(message, renewalCts.Token);
        ConsumeResult result;
        var fifoRetryStopped = false;
        try
        {
            result = await HandleMessageAsync(
                message,
                fifo,
                maxDeliveryAttempts,
                fifoRetryCancellationToken,
                cancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (OperationCanceledException) when (fifo && fifoRetryCancellationToken.IsCancellationRequested)
        {
            fifoRetryStopped = true;
            result = ConsumeResult.Retry;
        }
        finally
        {
            renewalCts.Cancel();
            try
            {
                await renewal.ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
            }
            catch (Exception exception)
            {
                _logger.LogWarning(exception, "Failed to renew invisible duration for message {MessageId}; processing will continue", message.MessageId);
            }
        }

        if (fifoRetryStopped)
        {
            return false;
        }

        try
        {
            if (result == ConsumeResult.Success)
            {
                await _engine.AckAsync(message, cancellationToken).ConfigureAwait(false);
            }
            else if (result == ConsumeResult.DeadLetter || message.DeliveryAttempt >= maxDeliveryAttempts)
            {
                await _engine.ForwardToDeadLetterQueueAsync(message, maxDeliveryAttempts, cancellationToken).ConfigureAwait(false);
            }
            else
            {
                var retryDelay = _engine.GetRetryDelay(message, _options.RetryDelay);
                await _engine.ChangeInvisibleDurationAsync(message, retryDelay, cancellationToken).ConfigureAwait(false);
            }
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception exception)
        {
            _logger.LogWarning(
                exception,
                "Failed to complete {ConsumeResult} processing for message {MessageId}; the message may be redelivered",
                result,
                message.MessageId);
            return false;
        }

        return true;
    }

    private async Task<bool> DiscardCorruptedMessageAsync(
        GrpcMessageView message,
        bool fifo,
        int maxDeliveryAttempts,
        CancellationToken cancellationToken)
    {
        _logger.LogError(
            "Discarding corrupted RocketMQ message {MessageId} on topic {Topic}: {Reason}",
            message.MessageId,
            message.Topic,
            message.CorruptionReason);
        try
        {
            if (fifo)
            {
                await _engine.ForwardToDeadLetterQueueAsync(
                    message,
                    maxDeliveryAttempts,
                    cancellationToken).ConfigureAwait(false);
            }
            else
            {
                await _engine.ChangeInvisibleDurationAsync(
                    message,
                    _engine.GetRetryDelay(message, _options.RetryDelay),
                    cancellationToken).ConfigureAwait(false);
            }

            return true;
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception exception)
        {
            _logger.LogWarning(
                exception,
                "Failed to discard corrupted RocketMQ message {MessageId}; it may be redelivered",
                message.MessageId);
            return false;
        }
    }

    private async ValueTask<ConsumeResult> HandleMessageAsync(
        GrpcMessageView message,
        bool fifo,
        int maxDeliveryAttempts,
        CancellationToken fifoRetryCancellationToken,
        CancellationToken cancellationToken)
    {
        var attempt = Math.Max(1, message.DeliveryAttempt);
        message.DeliveryAttempt = attempt;
        while (true)
        {
            ConsumeResult result;
            using var telemetry = _telemetry.StartProcess(
                message.Topic,
                _options.GroupName,
                message.MessageId,
                message.QueueId,
                message.Body.LongLength,
                message.Properties,
                message.ReceiveActivityContext);
            try
            {
                result = await _messageHandler(message, cancellationToken).ConfigureAwait(false);
                telemetry.Complete(result == ConsumeResult.Success, result.ToString());
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                telemetry.Complete(new OperationCanceledException(cancellationToken));
                throw;
            }
            catch (Exception exception)
            {
                telemetry.Complete(exception);
                _logger.LogError(exception, "Message handler failed for message {MessageId}", message.MessageId);
                result = ConsumeResult.Retry;
            }

            if (!fifo || result != ConsumeResult.Retry)
            {
                return result;
            }

            if (attempt >= maxDeliveryAttempts)
            {
                return ConsumeResult.DeadLetter;
            }

            var retryDelay = _engine.GetRetryDelay(message, attempt, _options.RetryDelay);
            attempt++;
            message.DeliveryAttempt = attempt;
            _logger.LogDebug(
                "Retrying FIFO message {MessageId} locally with attempt {Attempt} of {MaxAttempts} in {Delay}",
                message.MessageId,
                attempt,
                maxDeliveryAttempts,
                retryDelay);
            await DelayFifoRetryAsync(
                retryDelay,
                fifoRetryCancellationToken,
                cancellationToken).ConfigureAwait(false);
        }
    }

    private static async Task DelayFifoRetryAsync(
        TimeSpan delay,
        CancellationToken fifoRetryCancellationToken,
        CancellationToken cancellationToken)
    {
        if (!fifoRetryCancellationToken.CanBeCanceled)
        {
            await Task.Delay(delay, cancellationToken).ConfigureAwait(false);
            return;
        }

        using var linked = CancellationTokenSource.CreateLinkedTokenSource(
            fifoRetryCancellationToken,
            cancellationToken);
        await Task.Delay(delay, linked.Token).ConfigureAwait(false);
    }

    private async Task RenewInvisibleDurationAsync(GrpcMessageView message, CancellationToken cancellationToken)
    {
        var invisibleDuration = message.InvisibleDuration is { } brokerInvisibleDuration && brokerInvisibleDuration > TimeSpan.Zero
            ? brokerInvisibleDuration
            : _options.InvisibleDuration;
        var delay = TimeSpan.FromTicks(Math.Max(TimeSpan.FromSeconds(1).Ticks, invisibleDuration.Ticks / 2));
        while (true)
        {
            await Task.Delay(delay, cancellationToken).ConfigureAwait(false);
            await _engine.ChangeInvisibleDurationAsync(message, invisibleDuration, cancellationToken).ConfigureAwait(false);
        }
    }

    private Channel<GrpcMessageView> CreateMessageChannel() =>
        Channel.CreateBounded<GrpcMessageView>(new BoundedChannelOptions(_options.MaxCachedMessages)
        {
            FullMode = BoundedChannelFullMode.Wait,
            SingleReader = _options.MaxConcurrency == 1,
            SingleWriter = false
        });

    internal long CachedMessageBytes => _cachedMessageBytes.Values.Sum(static reservation => (long)reservation.MessageBytes);

    private FifoOrder? ReserveFifoOrder(GrpcMessageView message)
    {
        if (string.IsNullOrEmpty(message.MessageGroup))
        {
            return null;
        }

        var key = $"{message.Topic}\0{message.MessageGroup}";
        lock (_fifoGate)
        {
            var predecessor = _fifoTails.TryGetValue(key, out var tail) ? tail : Task.CompletedTask;
            var order = new FifoOrder(key, predecessor);
            _fifoTails[key] = order.Completion.Task;
            if (!_fifoOrders.TryAdd(message, order))
            {
                throw new InvalidOperationException($"Message '{message.MessageId}' was enqueued more than once.");
            }

            return order;
        }
    }

    private void CompleteFifoOrder(GrpcMessageView message, FifoOrder? order)
    {
        if (order is null)
        {
            return;
        }

        _fifoOrders.TryRemove(message, out _);
        order.Completion.TrySetResult();
        lock (_fifoGate)
        {
            if (_fifoTails.TryGetValue(order.Key, out var tail) && ReferenceEquals(tail, order.Completion.Task))
            {
                _fifoTails.Remove(order.Key);
            }
        }
    }

    private void BlockFifoOrder(GrpcMessageView message, FifoOrder order)
    {
        _fifoOrders.TryRemove(message, out _);
        lock (_fifoGate)
        {
            _blockedFifoGroups.Add(order.Key);
        }

        _fifoBlockedSignal.TrySetResult();
    }

    private void ReleaseCachedMessageBytes(GrpcMessageView message)
    {
        if (_cachedMessageBytes.TryRemove(message, out var reservation))
        {
            reservation.Capacity.Release(reservation.ReservedBytes);
        }
    }

    private void ReleaseAllCachedMessageBytes()
    {
        foreach (var message in _cachedMessageBytes.Keys)
        {
            ReleaseCachedMessageBytes(message);
        }
    }

    private void DrainMessageChannel()
    {
        while (_messages.Reader.TryRead(out var message))
        {
            ReleaseCachedMessageBytes(message);
        }
    }

    private static string GetAssignmentKey(Proto.MessageQueue queue)
    {
        var addresses = string.Join(",", queue.Broker.Endpoints.Addresses.Select(static address => $"{address.Host}:{address.Port}"));
        return $"{queue.Topic.ResourceNamespace}\0{queue.Topic.Name}\0{queue.Broker.Name}\0{queue.Id}\0{queue.Broker.Endpoints.Scheme}\0{addresses}";
    }

    private sealed class FifoOrder
    {
        public FifoOrder(string key, Task predecessor)
        {
            Key = key;
            Predecessor = predecessor;
            Completion = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        }

        public string Key { get; }
        public Task Predecessor { get; }
        public TaskCompletionSource Completion { get; }
    }

    private sealed class AssignmentReceiver(
        string key,
        CancellationTokenSource cancellation,
        Task task) : IDisposable
    {
        public string Key { get; } = key;
        public Task Task { get; } = task;
        public bool IsCancellationRequested => cancellation.IsCancellationRequested;
        public void Cancel() => cancellation.Cancel();
        public void Dispose() => cancellation.Dispose();
    }

    private sealed class TopicReceiver : IDisposable
    {
        public TopicReceiver(string topic, FilterExpression filter, CancellationToken stopping)
        {
            Topic = topic;
            Filter = filter;
            Stopping = CancellationTokenSource.CreateLinkedTokenSource(stopping);
        }

        public string Topic { get; }
        public FilterExpression Filter { get; }
        public CancellationTokenSource Stopping { get; }
        public Task Task { get; set; } = Task.CompletedTask;
        public bool IsCancellationRequested => Stopping.IsCancellationRequested;
        public void Cancel() => Stopping.Cancel();
        public void Dispose() => Stopping.Dispose();
    }

    private readonly record struct ByteReservation(
        ByteCapacityGate Capacity,
        int ReservedBytes,
        int MessageBytes);

    private sealed class ByteCapacityGate
    {
        private readonly object _gate = new();
        private readonly int _capacity;
        private int _available;
        private TaskCompletionSource _changed = NewSignal();

        public ByteCapacityGate(int capacity)
        {
            ArgumentOutOfRangeException.ThrowIfNegativeOrZero(capacity);
            _capacity = capacity;
            _available = capacity;
        }

        public async ValueTask<int> ReserveAsync(int bytes, CancellationToken cancellationToken)
        {
            ArgumentOutOfRangeException.ThrowIfNegative(bytes);
            var reservedBytes = Math.Min(bytes, _capacity);

            while (true)
            {
                cancellationToken.ThrowIfCancellationRequested();
                Task changed;
                lock (_gate)
                {
                    if (_available >= reservedBytes)
                    {
                        _available -= reservedBytes;
                        return reservedBytes;
                    }

                    changed = _changed.Task;
                }

                await changed.WaitAsync(cancellationToken).ConfigureAwait(false);
            }
        }

        public void Release(int bytes)
        {
            TaskCompletionSource changed;
            lock (_gate)
            {
                _available = Math.Min(_capacity, _available + bytes);
                changed = _changed;
                _changed = NewSignal();
            }

            changed.TrySetResult();
        }

        private static TaskCompletionSource NewSignal() =>
            new(TaskCreationOptions.RunContinuationsAsynchronously);
    }

    private static TaskCompletionSource NewFifoBlockedSignal() =>
        new(TaskCreationOptions.RunContinuationsAsynchronously);
}
