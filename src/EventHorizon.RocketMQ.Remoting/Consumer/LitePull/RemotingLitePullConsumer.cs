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

using EventHorizon.RocketMQ.Remoting.Consumer.Coordination;
using EventHorizon.RocketMQ.Remoting.Consumer.Coordination.Rebalance;
using EventHorizon.RocketMQ.Remoting.Consumer.LitePull.Assignment;
using EventHorizon.RocketMQ.Remoting.Consumer.LitePull.Receive;
using EventHorizon.RocketMQ.Remoting.Protocol;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace EventHorizon.RocketMQ.Remoting.Consumer.LitePull;

internal sealed class RemotingLitePullConsumer : IRemotingLitePullConsumer
{
    private readonly RemotingLitePullConsumerOptions _options;
    private readonly IRemotingConsumerEngine _consumerEngine;
    private readonly LitePullSubscriptionState _subscriptionState;
    private readonly LitePullConsumerRunLifecycle _runLifecycle;
    private readonly TimeProvider _timeProvider;
    private readonly bool _hasLocalGroupPeer;
    private readonly SemaphoreSlim _lifecycleGate = new(1, 1);
    private RemotingLitePullConsumerRun? _run;
    private int _disposed;

    public RemotingLitePullConsumer(
        IOptions<RemotingLitePullConsumerOptions> options,
        IOptions<RemotingClientOptions> clientOptions,
        IRemotingConsumerEngine consumerEngine,
        IRemotingClient remotingClient,
        IRemotingRebalanceService rebalanceService,
        TimeProvider timeProvider,
        ILogger<RemotingLitePullConsumer> logger,
        bool hasLocalGroupPeer = false)
    {
        _options = options.Value;
        _consumerEngine = consumerEngine;
        _timeProvider = timeProvider;
        _hasLocalGroupPeer = hasLocalGroupPeer;
        var configuredClientOptions = clientOptions.Value;
        var groupClient = new RemotingConsumerGroupClient(
            remotingClient,
            configuredClientOptions,
            _options.GroupName,
            logger);
        _subscriptionState = new LitePullSubscriptionState(_options.Subscriptions);
        _runLifecycle = new LitePullConsumerRunLifecycle(
            _options,
            configuredClientOptions,
            consumerEngine,
            rebalanceService,
            groupClient,
            _subscriptionState,
            timeProvider,
            logger);
    }

    public IReadOnlyList<RemotingConsumerQueue> Assignment
    {
        get
        {
            _lifecycleGate.Wait();
            try
            {
                var run = _run;
                return run is null ? [] : run.DeliveryState.GetAssignment();
            }
            finally
            {
                _lifecycleGate.Release();
            }
        }
    }

    public async Task<IReadOnlyList<RemotingConsumerQueue>> GetMessageQueuesAsync(
        string topic,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(topic);
        using var runOperation = await EnterRunOperationAsync(cancellationToken).ConfigureAwait(false);
        var run = runOperation.Run;
        using var operation = await run.OperationGate.EnterAsync(runOperation.CancellationToken).ConfigureAwait(false);
        return await _consumerEngine.GetMessageQueuesAsync(
            topic,
            runOperation.CancellationToken).ConfigureAwait(false);
    }

    public async ValueTask StartAsync(CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);
        await _lifecycleGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);
            if (_run is not null)
            {
                return;
            }

            _run = await _runLifecycle.StartAsync(cancellationToken).ConfigureAwait(false);
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
            var run = _run;
            if (run is null)
            {
                return;
            }

            _run = null;
            await _runLifecycle.ShutdownAsync(run).ConfigureAwait(false);
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
        ArgumentException.ThrowIfNullOrWhiteSpace(topic);
        ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);
        ThrowIfRuntimeSubscriptionChangesAreUnsupported();
        var filter = expression ?? FilterExpression.All;
        await _lifecycleGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);
            var run = _run;
            if (run is null)
            {
                _subscriptionState.Set(topic, filter);
            }
            else
            {
                await run.AssignmentCoordinator.SubscribeAsync(run, topic, filter, cancellationToken)
                    .ConfigureAwait(false);
            }
        }
        finally
        {
            _lifecycleGate.Release();
        }
    }

    public async Task UnsubscribeAsync(string topic, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(topic);
        ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);
        ThrowIfRuntimeSubscriptionChangesAreUnsupported();
        await _lifecycleGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);
            var run = _run;
            if (run is null)
            {
                _subscriptionState.Remove(topic);
            }
            else
            {
                await run.AssignmentCoordinator.UnsubscribeAsync(run, topic, cancellationToken)
                    .ConfigureAwait(false);
            }
        }
        finally
        {
            _lifecycleGate.Release();
        }
    }

    public async Task AssignAsync(
        IEnumerable<RemotingConsumerQueue> queues,
        IReadOnlyDictionary<string, FilterExpression>? filters = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(queues);
        filters ??= new Dictionary<string, FilterExpression>(StringComparer.Ordinal);
        ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);
        var requestedQueues = queues.ToArray();
        if (requestedQueues.Any(static queue => queue is null))
        {
            throw new ArgumentException("Assigned queues cannot contain null.", nameof(queues));
        }

        await _lifecycleGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);
            var run = _run ?? throw new InvalidOperationException("Lite Pull consumer has not been started.");
            if (!_subscriptionState.IsEmpty)
            {
                throw new InvalidOperationException(
                    "Manual assignment requires a Lite Pull consumer with no topic subscriptions.");
            }

            await run.AssignmentCoordinator.AssignAsync(
                run,
                requestedQueues,
                filters,
                cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            _lifecycleGate.Release();
        }
    }

    public async Task<IReadOnlyList<RemotingMessageView>> PollAsync(
        TimeSpan? timeout = null,
        CancellationToken cancellationToken = default)
    {
        if (timeout is { } localTimeout && localTimeout < TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(nameof(timeout), "Poll timeout cannot be negative.");
        }

        using var runOperation = await EnterRunOperationAsync(cancellationToken).ConfigureAwait(false);
        var run = runOperation.Run;
        var configuredTimeout = timeout ?? _options.PollTimeout;
        using var timeoutCancellation = configuredTimeout > TimeSpan.Zero
            ? new CancellationTokenSource(configuredTimeout, _timeProvider)
            : null;
        using var pollCancellation = timeoutCancellation is null
            ? null
            : CancellationTokenSource.CreateLinkedTokenSource(
                runOperation.CancellationToken,
                timeoutCancellation.Token);
        var pollToken = pollCancellation?.Token ?? runOperation.CancellationToken;
        if (configuredTimeout == TimeSpan.Zero)
        {
            runOperation.CancellationToken.ThrowIfCancellationRequested();
            using var operation = run.OperationGate.TryEnter();
            return operation is null ? [] : run.DeliveryState.TryDrainBuffer().Messages;
        }

        while (true)
        {
            try
            {
                LitePullDeliveryState.BufferPollAttempt attempt;
                using (await run.OperationGate.EnterAsync(pollToken).ConfigureAwait(false))
                {
                    attempt = run.DeliveryState.TryDrainBuffer();
                }

                if (attempt.Messages.Count > 0)
                {
                    return attempt.Messages;
                }

                await attempt.StateChanged.WaitAsync(pollToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (
                !cancellationToken.IsCancellationRequested &&
                !run.Stopping.IsCancellationRequested &&
                timeoutCancellation?.IsCancellationRequested == true)
            {
                return [];
            }
        }
    }

    public long Position(RemotingConsumerQueue queue)
    {
        ArgumentNullException.ThrowIfNull(queue);
        using var runOperation = EnterRunOperation();
        return runOperation.Run.DeliveryState.GetRequiredQueuePosition(queue).Offset;
    }

    public void Seek(RemotingConsumerQueue queue, long offset)
    {
        ArgumentNullException.ThrowIfNull(queue);
        ArgumentOutOfRangeException.ThrowIfNegative(offset);
        using var runOperation = EnterRunOperation();
        var run = runOperation.Run;
        using var operation = run.OperationGate.Enter();
        run.AssignmentReconciler.StartReceiver(run, run.DeliveryState.ReplacePosition(queue, offset));
    }

    public async Task SeekToBeginningAsync(
        RemotingConsumerQueue queue,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(queue);
        using var runOperation = await EnterRunOperationAsync(cancellationToken).ConfigureAwait(false);
        var run = runOperation.Run;
        using var operation = await run.OperationGate.EnterAsync(runOperation.CancellationToken).ConfigureAwait(false);
        var assignedQueue = run.DeliveryState.GetRequiredQueue(queue);
        var offset = await _consumerEngine.QueryOffsetAsync(
            assignedQueue,
            ConsumeFromPosition.Beginning,
            cancellationToken: runOperation.CancellationToken).ConfigureAwait(false);
        if (run.OperationGate.IsOpen)
        {
            run.AssignmentReconciler.StartReceiver(
                run,
                run.DeliveryState.ReplacePosition(assignedQueue, offset));
        }
    }

    public async Task SeekToEndAsync(RemotingConsumerQueue queue, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(queue);
        using var runOperation = await EnterRunOperationAsync(cancellationToken).ConfigureAwait(false);
        var run = runOperation.Run;
        using var operation = await run.OperationGate.EnterAsync(runOperation.CancellationToken).ConfigureAwait(false);
        var assignedQueue = run.DeliveryState.GetRequiredQueue(queue);
        var offset = await _consumerEngine.QueryOffsetAsync(
            assignedQueue,
            ConsumeFromPosition.End,
            cancellationToken: runOperation.CancellationToken).ConfigureAwait(false);
        if (run.OperationGate.IsOpen)
        {
            run.AssignmentReconciler.StartReceiver(
                run,
                run.DeliveryState.ReplacePosition(assignedQueue, offset));
        }
    }

    public async Task SeekToTimestampAsync(
        RemotingConsumerQueue queue,
        DateTimeOffset timestamp,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(queue);
        using var runOperation = await EnterRunOperationAsync(cancellationToken).ConfigureAwait(false);
        var run = runOperation.Run;
        using var operation = await run.OperationGate.EnterAsync(runOperation.CancellationToken).ConfigureAwait(false);
        var assignedQueue = run.DeliveryState.GetRequiredQueue(queue);
        var offset = await _consumerEngine.QueryOffsetAsync(
            assignedQueue,
            ConsumeFromPosition.Timestamp,
            timestamp,
            runOperation.CancellationToken).ConfigureAwait(false);
        if (run.OperationGate.IsOpen)
        {
            run.AssignmentReconciler.StartReceiver(
                run,
                run.DeliveryState.ReplacePosition(assignedQueue, offset));
        }
    }

    public async Task<long?> GetCommittedOffsetAsync(
        RemotingConsumerQueue queue,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(queue);
        using var runOperation = await EnterRunOperationAsync(cancellationToken).ConfigureAwait(false);
        var run = runOperation.Run;
        using var operation = await run.OperationGate.EnterAsync(runOperation.CancellationToken).ConfigureAwait(false);
        var assignedQueue = run.DeliveryState.GetRequiredQueue(queue);
        var offset = await _consumerEngine.GetOffsetAsync(
            assignedQueue,
            runOperation.CancellationToken).ConfigureAwait(false);
        return offset >= 0 ? offset : null;
    }

    public void Pause(IEnumerable<RemotingConsumerQueue> queues)
    {
        ArgumentNullException.ThrowIfNull(queues);
        using var runOperation = EnterRunOperation();
        var run = runOperation.Run;
        using var operation = run.OperationGate.Enter();
        foreach (var queue in queues)
        {
            ArgumentNullException.ThrowIfNull(queue);
            run.DeliveryState.SetPaused(queue, true);
        }
    }

    public void Resume(IEnumerable<RemotingConsumerQueue> queues)
    {
        ArgumentNullException.ThrowIfNull(queues);
        using var runOperation = EnterRunOperation();
        var run = runOperation.Run;
        using var operation = run.OperationGate.Enter();
        foreach (var queue in queues)
        {
            ArgumentNullException.ThrowIfNull(queue);
            run.DeliveryState.SetPaused(queue, false);
        }
    }

    public async Task CommitAsync(CancellationToken cancellationToken = default)
    {
        using var runOperation = await EnterRunOperationAsync(cancellationToken).ConfigureAwait(false);
        await runOperation.Run.CommitLoop.CommitAllAsync(
            runOperation.Run,
            runOperation.CancellationToken).ConfigureAwait(false);
    }

    public async Task CommitAsync(RemotingConsumerQueue queue, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(queue);
        using var runOperation = await EnterRunOperationAsync(cancellationToken).ConfigureAwait(false);
        await runOperation.Run.CommitLoop.CommitQueueAsync(
            runOperation.Run,
            queue,
            runOperation.CancellationToken).ConfigureAwait(false);
    }

    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
        {
            return;
        }

        await StopAsync(CancellationToken.None).ConfigureAwait(false);
        await _consumerEngine.DisposeAsync().ConfigureAwait(false);
    }

    private void ThrowIfRuntimeSubscriptionChangesAreUnsupported()
    {
        if (_hasLocalGroupPeer)
        {
            throw new InvalidOperationException(
                $"Runtime subscription changes are not supported when multiple Remoting Lite Pull consumers in the " +
                $"same consumer group '{_options.GroupName}' share a client registration. Configure identical " +
                "subscriptions for all group members and restart them together.");
        }
    }

    private async Task<RunOperation> EnterRunOperationAsync(CancellationToken cancellationToken)
    {
        await _lifecycleGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);
            return CreateRunOperation(
                _run ?? throw new InvalidOperationException("Lite Pull consumer has not been started."),
                cancellationToken);
        }
        finally
        {
            _lifecycleGate.Release();
        }
    }

    private RunOperation EnterRunOperation()
    {
        _lifecycleGate.Wait();
        try
        {
            ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);
            return CreateRunOperation(
                _run ?? throw new InvalidOperationException("Lite Pull consumer has not been started."),
                CancellationToken.None);
        }
        finally
        {
            _lifecycleGate.Release();
        }
    }

    private static RunOperation CreateRunOperation(
        RemotingLitePullConsumerRun run,
        CancellationToken cancellationToken)
    {
        var lease = run.PublicOperations.Enter();
        try
        {
            return new RunOperation(
                run,
                lease,
                CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, run.Stopping.Token));
        }
        catch
        {
            lease.Dispose();
            throw;
        }
    }

    private sealed class RunOperation(
        RemotingLitePullConsumerRun run,
        IDisposable lease,
        CancellationTokenSource cancellation) : IDisposable
    {
        public RemotingLitePullConsumerRun Run { get; } = run;

        public CancellationToken CancellationToken => cancellation.Token;

        public void Dispose()
        {
            cancellation.Dispose();
            lease.Dispose();
        }
    }

}
