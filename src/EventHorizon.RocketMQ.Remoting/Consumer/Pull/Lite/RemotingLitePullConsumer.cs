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
using EventHorizon.RocketMQ.Remoting.Protocol;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace EventHorizon.RocketMQ.Remoting.Consumer.Pull.Lite;

internal sealed class RemotingLitePullConsumer : IRemotingLitePullConsumer
{
    private readonly RemotingLitePullConsumerOptions _options;
    private readonly RemotingClientOptions _clientOptions;
    private readonly IRemotingConsumerEngine _consumerEngine;
    private readonly IRemotingRebalanceService _rebalanceService;
    private readonly RemotingConsumerGroupSession _groupSession;
    private readonly TimeProvider _timeProvider;
    private readonly ILogger<RemotingLitePullConsumer> _logger;
    private readonly bool _hasLocalGroupPeer;
    private readonly ConcurrentDictionary<string, FilterExpression> _subscriptions;
    private readonly SemaphoreSlim _lifecycleGate = new(1, 1);
    private RunState? _run;
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
        _clientOptions = clientOptions.Value;
        _consumerEngine = consumerEngine;
        _rebalanceService = rebalanceService;
        _timeProvider = timeProvider;
        _hasLocalGroupPeer = hasLocalGroupPeer;
        _groupSession = new RemotingConsumerGroupSession(
            remotingClient,
            _clientOptions,
            _options.GroupName,
            logger);
        _subscriptions = new ConcurrentDictionary<string, FilterExpression>(
            _options.Subscriptions,
            StringComparer.Ordinal);
        _logger = logger;
    }

    public IReadOnlyList<RemotingPullMessageQueue> Assignment
    {
        get
        {
            var run = _run;
            return run is null ? [] : run.GetAssignment();
        }
    }

    public async Task<IReadOnlyList<RemotingPullMessageQueue>> GetMessageQueuesAsync(
        string topic,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(topic);
        var run = await GetActiveRunAsync(cancellationToken).ConfigureAwait(false);
        using var operationCancellation = CancellationTokenSource.CreateLinkedTokenSource(
            cancellationToken,
            run.Stopping.Token);
        await run.OperationGate.WaitAsync(operationCancellation.Token).ConfigureAwait(false);
        try
        {
            return await _consumerEngine.GetMessageQueuesAsync(
                topic,
                operationCancellation.Token).ConfigureAwait(false);
        }
        finally
        {
            run.OperationGate.Release();
        }
    }

    public async ValueTask StartAsync(CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);
        await _lifecycleGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (_run is not null)
            {
                return;
            }

            await _consumerEngine.StartAsync(cancellationToken).ConfigureAwait(false);
            var run = new RunState(_timeProvider.GetUtcNow().ToUnixTimeMilliseconds());
            _run = run;
            try
            {
                run.RebalanceRegistration = _rebalanceService.Register(
                    _groupSession.ConsumerGroup,
                    token => RebalanceAsync(run, token));
                if (!_subscriptions.IsEmpty)
                {
                    await CoordinateAndScheduleRetryAsync(run, cancellationToken).ConfigureAwait(false);
                }
            }
            catch
            {
                _run = null;
                await ShutdownRunAsync(run).ConfigureAwait(false);
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
            var run = _run;
            if (run is null)
            {
                return;
            }

            _run = null;
            await ShutdownRunAsync(run).ConfigureAwait(false);
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
            var run = _run;
            if (run?.ManualAssignment == true)
            {
                throw new InvalidOperationException("Lite Pull consumer is using manual-assignment mode.");
            }

            _subscriptions[topic] = filter;
            _consumerEngine.SetSubscription(topic, filter);
            if (run is not null)
            {
                run.BumpSubscriptionVersion();
                await CoordinateAndScheduleRetryAsync(run, cancellationToken).ConfigureAwait(false);
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
            var run = _run;
            if (run?.ManualAssignment == true)
            {
                throw new InvalidOperationException("Lite Pull consumer is using manual-assignment mode.");
            }

            if (!_subscriptions.TryRemove(topic, out _))
            {
                return;
            }

            _consumerEngine.RemoveSubscription(topic);
            if (run is not null)
            {
                run.BumpSubscriptionVersion();
                await CoordinateAndScheduleRetryAsync(run, cancellationToken).ConfigureAwait(false);
                if (_subscriptions.IsEmpty)
                {
                    await _groupSession.UnregisterAsync(run.KnownBrokers.Values).ConfigureAwait(false);
                }
            }
        }
        finally
        {
            _lifecycleGate.Release();
        }
    }

    public async Task AssignAsync(
        IEnumerable<RemotingPullMessageQueue> queues,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(queues);
        ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);
        var requestedQueues = queues.ToArray();
        if (requestedQueues.Any(static queue => queue is null))
        {
            throw new ArgumentException("Assigned queues cannot contain null.", nameof(queues));
        }

        await _lifecycleGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            var run = _run ?? throw new InvalidOperationException("Lite Pull consumer has not been started.");
            if (!_subscriptions.IsEmpty)
            {
                throw new InvalidOperationException(
                    "Manual assignment requires a Lite Pull consumer with no topic subscriptions.");
            }

            await run.CoordinationGate.WaitAsync(cancellationToken).ConfigureAwait(false);
            try
            {
                var wasManualAssignment = run.ManualAssignment;
                var hasManualAssignment = requestedQueues.Length > 0;
                run.ManualAssignment = hasManualAssignment;
                await ReconcileAssignmentsAsync(run, requestedQueues, cancellationToken).ConfigureAwait(false);
                if (hasManualAssignment)
                {
                    // Manual assignment does not use group allocation, but LitePull still represents an active
                    // classic group member. Retain every assigned Broker so the initial and periodic heartbeats
                    // keep that membership visible while the manual assignment remains active.
                    AddKnownBrokers(run, requestedQueues);
                    await SendHeartbeatsAsync(run, [], cancellationToken).ConfigureAwait(false);
                }
                else if (wasManualAssignment)
                {
                    // Clearing the final manual assignment ends this consumer's group membership immediately.
                    // Keep the Broker endpoints for ShutdownRunAsync so a transient unregister failure is retried.
                    await _groupSession.UnregisterAsync(run.KnownBrokers.Values).ConfigureAwait(false);
                }
            }
            finally
            {
                run.CoordinationGate.Release();
            }
        }
        finally
        {
            _lifecycleGate.Release();
        }
    }

    public async Task<RemotingPullResult> PollAsync(
        TimeSpan? timeout = null,
        CancellationToken cancellationToken = default)
    {
        if (timeout is { } localTimeout && localTimeout <= TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(nameof(timeout), "Poll timeout must be positive.");
        }

        var run = await GetActiveRunAsync(cancellationToken).ConfigureAwait(false);
        using var operationCancellation = CancellationTokenSource.CreateLinkedTokenSource(
            cancellationToken,
            run.Stopping.Token);
        await run.OperationGate.WaitAsync(operationCancellation.Token).ConfigureAwait(false);
        try
        {
            var selection = run.SelectNextActiveQueue();
            using var pullCancellation = CancellationTokenSource.CreateLinkedTokenSource(
                operationCancellation.Token,
                selection.CancellationToken);
            if (timeout is { } configuredTimeout)
            {
                pullCancellation.CancelAfter(configuredTimeout);
            }

            var result = await _consumerEngine.PullAsync(
                selection.Queue,
                selection.Offset,
                cancellationToken: pullCancellation.Token).ConfigureAwait(false);
            if (!run.AdvanceOffset(selection.Key, selection.State, selection.Version, result.NextOffset))
            {
                throw new OperationCanceledException(pullCancellation.Token);
            }

            return result;
        }
        finally
        {
            run.OperationGate.Release();
        }
    }

    public void Seek(RemotingPullMessageQueue queue, long offset)
    {
        ArgumentNullException.ThrowIfNull(queue);
        ArgumentOutOfRangeException.ThrowIfNegative(offset);
        var run = RequireActiveRun();
        run.SetOffset(queue, offset);
    }

    public async Task SeekToBeginningAsync(
        RemotingPullMessageQueue queue,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(queue);
        var run = await GetActiveRunAsync(cancellationToken).ConfigureAwait(false);
        using var operationCancellation = CancellationTokenSource.CreateLinkedTokenSource(
            cancellationToken,
            run.Stopping.Token);
        await run.OperationGate.WaitAsync(operationCancellation.Token).ConfigureAwait(false);
        try
        {
            var assignedQueue = run.GetRequiredQueue(queue);
            var offset = await _consumerEngine.QueryOffsetAsync(
                assignedQueue,
                QueryOffsetPolicy.Beginning,
                cancellationToken: operationCancellation.Token).ConfigureAwait(false);
            run.SetOffset(assignedQueue, offset);
        }
        finally
        {
            run.OperationGate.Release();
        }
    }

    public async Task SeekToEndAsync(RemotingPullMessageQueue queue, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(queue);
        var run = await GetActiveRunAsync(cancellationToken).ConfigureAwait(false);
        using var operationCancellation = CancellationTokenSource.CreateLinkedTokenSource(
            cancellationToken,
            run.Stopping.Token);
        await run.OperationGate.WaitAsync(operationCancellation.Token).ConfigureAwait(false);
        try
        {
            var assignedQueue = run.GetRequiredQueue(queue);
            var offset = await _consumerEngine.QueryOffsetAsync(
                assignedQueue,
                QueryOffsetPolicy.End,
                cancellationToken: operationCancellation.Token).ConfigureAwait(false);
            run.SetOffset(assignedQueue, offset);
        }
        finally
        {
            run.OperationGate.Release();
        }
    }

    public void Pause(IEnumerable<RemotingPullMessageQueue> queues)
    {
        ArgumentNullException.ThrowIfNull(queues);
        var run = RequireActiveRun();
        foreach (var queue in queues)
        {
            ArgumentNullException.ThrowIfNull(queue);
            run.SetPaused(queue, true);
        }
    }

    public void Resume(IEnumerable<RemotingPullMessageQueue> queues)
    {
        ArgumentNullException.ThrowIfNull(queues);
        var run = RequireActiveRun();
        foreach (var queue in queues)
        {
            ArgumentNullException.ThrowIfNull(queue);
            run.SetPaused(queue, false);
        }
    }

    public async Task CommitAsync(CancellationToken cancellationToken = default)
    {
        var run = await GetActiveRunAsync(cancellationToken).ConfigureAwait(false);
        await CommitAsync(run, static state => state.GetPositions(), cancellationToken).ConfigureAwait(false);
    }

    public async Task CommitAsync(RemotingPullMessageQueue queue, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(queue);
        var run = await GetActiveRunAsync(cancellationToken).ConfigureAwait(false);
        await CommitAsync(
            run,
            state => [state.GetRequiredQueuePosition(queue)],
            cancellationToken).ConfigureAwait(false);
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

    private async Task<bool> RebalanceAsync(RunState run, CancellationToken cancellationToken)
    {
        if (run.Stopping.IsCancellationRequested)
        {
            return true;
        }

        using var linkedCancellation = CancellationTokenSource.CreateLinkedTokenSource(
            cancellationToken,
            run.Stopping.Token);
        try
        {
            return await CoordinateOnceAsync(run, linkedCancellation.Token).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (run.Stopping.IsCancellationRequested)
        {
            return true;
        }
    }

    private async Task CoordinateAndScheduleRetryAsync(RunState run, CancellationToken cancellationToken)
    {
        if (!await CoordinateOnceAsync(run, cancellationToken).ConfigureAwait(false))
        {
            run.RebalanceRegistration?.Wakeup();
        }
    }

    private async Task<bool> CoordinateOnceAsync(RunState run, CancellationToken cancellationToken)
    {
        await run.CoordinationGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (run.ManualAssignment)
            {
                await SendHeartbeatsAsync(run, [], cancellationToken).ConfigureAwait(false);
                return true;
            }

            var subscriptions = _subscriptions
                .OrderBy(static entry => entry.Key, StringComparer.Ordinal)
                .ToArray();
            if (subscriptions.Length == 0)
            {
                await ReconcileAssignmentsAsync(run, [], cancellationToken).ConfigureAwait(false);
                return true;
            }

            var queuesByTopic = new Dictionary<string, IReadOnlyList<RemotingPullMessageQueue>>(
                StringComparer.Ordinal);
            foreach (var subscription in subscriptions)
            {
                var queues = await _consumerEngine.GetMessageQueuesAsync(
                    subscription.Key,
                    cancellationToken).ConfigureAwait(false);
                queuesByTopic[subscription.Key] = queues;
                AddKnownBrokers(run, queues);
            }

            await SendHeartbeatsAsync(run, subscriptions, cancellationToken).ConfigureAwait(false);
            var assigned = new List<RemotingPullMessageQueue>();
            var balanced = true;
            foreach (var (_, queues) in queuesByTopic)
            {
                if (queues.Count == 0)
                {
                    continue;
                }

                var consumerIds = await _groupSession.GetConsumerIdsAsync(queues[0], cancellationToken)
                    .ConfigureAwait(false);
                if (!consumerIds.Contains(_groupSession.ClientId, StringComparer.Ordinal))
                {
                    balanced = false;
                    _logger.LogWarning(
                        "Broker membership for legacy Lite Pull group {GroupName} does not contain client {ClientId}",
                        _options.GroupName,
                        _groupSession.ClientId);
                    continue;
                }

                assigned.AddRange(LegacyConsumerProtocol.Allocate(
                    queues,
                    consumerIds,
                    _groupSession.ClientId));
            }

            await ReconcileAssignmentsAsync(run, assigned, cancellationToken).ConfigureAwait(false);
            return balanced;
        }
        finally
        {
            run.CoordinationGate.Release();
        }
    }

    private async Task SendHeartbeatsAsync(
        RunState run,
        IReadOnlyCollection<KeyValuePair<string, FilterExpression>> subscriptions,
        CancellationToken cancellationToken)
    {
        var wireSubscriptions = subscriptions.ToDictionary(
            entry => GetWireTopic(entry.Key),
            static entry => entry.Value,
            StringComparer.Ordinal);
        var body = LegacyConsumerProtocol.EncodeHeartbeat(
            _groupSession.ClientId,
            _groupSession.ConsumerGroup,
            wireSubscriptions,
            run.SubscriptionVersion,
            "CONSUME_ACTIVELY",
            "CLUSTERING",
            GetConsumeFromWhere(),
            _clientOptions.UnitMode);
        await _groupSession.SendHeartbeatsAsync(
            run.KnownBrokers.Values,
            body,
            cancellationToken).ConfigureAwait(false);
    }

    private static void AddKnownBrokers(
        RunState run,
        IEnumerable<RemotingPullMessageQueue> queues)
    {
        foreach (var queue in queues)
        {
            foreach (var address in queue.BrokerAddresses.Values)
            {
                run.KnownBrokers[address] = new RemotingBrokerEndpoint(queue.BrokerName, address);
            }
        }
    }

    private async Task ReconcileAssignmentsAsync(
        RunState run,
        IReadOnlyCollection<RemotingPullMessageQueue> desiredQueues,
        CancellationToken cancellationToken)
    {
        var desired = desiredQueues
            .GroupBy(QueueKey.Create)
            .ToDictionary(static entry => entry.Key, static entry => entry.First());
        run.CancelQueuesNotIn(desired);
        await run.OperationGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            var additions = run.ApplyDesiredQueues(desired);
            foreach (var queue in additions)
            {
                var offset = await InitializeOffsetAsync(queue, cancellationToken).ConfigureAwait(false);
                run.AddInitializedQueue(queue, offset, desired);
            }
        }
        finally
        {
            run.OperationGate.Release();
        }
    }

    private async Task<long> InitializeOffsetAsync(
        RemotingPullMessageQueue queue,
        CancellationToken cancellationToken)
    {
        var committed = await _consumerEngine.GetOffsetAsync(queue, cancellationToken).ConfigureAwait(false);
        if (committed >= 0)
        {
            return committed;
        }

        return await _consumerEngine.QueryOffsetAsync(
            queue,
            _options.InitialOffset,
            _options.InitialOffsetTimestamp,
            cancellationToken).ConfigureAwait(false);
    }

    private async Task ShutdownRunAsync(RunState run)
    {
        run.Stopping.Cancel();
        run.CancelAllQueues();
        if (run.RebalanceRegistration is { } rebalanceRegistration)
        {
            run.RebalanceRegistration = null;
            await rebalanceRegistration.DisposeAsync().ConfigureAwait(false);
        }

        await run.OperationGate.WaitAsync(CancellationToken.None).ConfigureAwait(false);
        run.OperationGate.Release();
        await _groupSession.UnregisterAsync(run.KnownBrokers.Values).ConfigureAwait(false);
        try
        {
            await _consumerEngine.StopAsync(CancellationToken.None).ConfigureAwait(false);
        }
        finally
        {
            run.CompleteShutdown();
        }
    }

    private async Task CommitAsync(
        RunState run,
        Func<RunState, IReadOnlyList<QueuePosition>> getPositions,
        CancellationToken cancellationToken)
    {
        using var operationCancellation = CancellationTokenSource.CreateLinkedTokenSource(
            cancellationToken,
            run.Stopping.Token);
        await run.OperationGate.WaitAsync(operationCancellation.Token).ConfigureAwait(false);
        try
        {
            var positions = getPositions(run);
            await Task.WhenAll(positions.Select(position => UpdateOffsetAsync(
                run,
                position,
                operationCancellation.Token))).ConfigureAwait(false);
        }
        finally
        {
            run.OperationGate.Release();
        }
    }

    private async Task UpdateOffsetAsync(
        RunState run,
        QueuePosition position,
        CancellationToken cancellationToken)
    {
        using var queueCancellation = CancellationTokenSource.CreateLinkedTokenSource(
            cancellationToken,
            position.CancellationToken);
        await _consumerEngine.UpdateOffsetAsync(
            position.Queue,
            position.Offset,
            queueCancellation.Token).ConfigureAwait(false);
        if (!run.IsCurrent(position))
        {
            throw new OperationCanceledException(queueCancellation.Token);
        }
    }

    private async Task<RunState> GetActiveRunAsync(CancellationToken cancellationToken)
    {
        await _lifecycleGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);
            return _run ?? throw new InvalidOperationException("Lite Pull consumer has not been started.");
        }
        finally
        {
            _lifecycleGate.Release();
        }
    }

    private RunState RequireActiveRun()
    {
        ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);
        return _run ?? throw new InvalidOperationException("Lite Pull consumer has not been started.");
    }

    private string GetConsumeFromWhere() => _options.InitialOffset switch
    {
        QueryOffsetPolicy.Beginning => "CONSUME_FROM_FIRST_OFFSET",
        QueryOffsetPolicy.End => "CONSUME_FROM_LAST_OFFSET",
        QueryOffsetPolicy.Timestamp => "CONSUME_FROM_TIMESTAMP",
        _ => throw new ArgumentOutOfRangeException(nameof(_options.InitialOffset), _options.InitialOffset, null)
    };

    private string GetWireTopic(string topic) => LegacyNamespace.Wrap(_clientOptions.Namespace, topic);

    private sealed class RunState
    {
        private const string QueueNotAssignedMessage =
            "The message queue is not assigned to this Lite Pull consumer.";

        private readonly object _stateGate = new();
        private readonly Dictionary<QueueKey, QueueState> _assignments = [];
        private int _nextQueueIndex;

        public RunState(long subscriptionVersion)
        {
            SubscriptionVersion = subscriptionVersion;
        }

        public CancellationTokenSource Stopping { get; } = new();
        public SemaphoreSlim CoordinationGate { get; } = new(1, 1);
        public SemaphoreSlim OperationGate { get; } = new(1, 1);
        public Dictionary<string, RemotingBrokerEndpoint> KnownBrokers { get; } = new(StringComparer.Ordinal);
        public IRemotingRebalanceRegistration? RebalanceRegistration { get; set; }
        public bool ManualAssignment { get; set; }
        public long SubscriptionVersion { get; private set; }

        public void BumpSubscriptionVersion() => SubscriptionVersion++;

        public IReadOnlyList<RemotingPullMessageQueue> GetAssignment()
        {
            lock (_stateGate)
            {
                return _assignments
                    .OrderBy(static entry => entry.Key.Topic, StringComparer.Ordinal)
                    .ThenBy(static entry => entry.Key.BrokerName, StringComparer.Ordinal)
                    .ThenBy(static entry => entry.Key.QueueId)
                    .Select(static entry => entry.Value.Queue)
                    .ToArray();
            }
        }

        public IReadOnlyList<RemotingPullMessageQueue> ApplyDesiredQueues(
            IReadOnlyDictionary<QueueKey, RemotingPullMessageQueue> desired)
        {
            lock (_stateGate)
            {
                foreach (var key in _assignments.Keys.Except(desired.Keys).ToArray())
                {
                    _assignments[key].CancelPolling();
                    _assignments.Remove(key);
                }

                var additions = new List<RemotingPullMessageQueue>();
                foreach (var (key, queue) in desired)
                {
                    if (_assignments.TryGetValue(key, out var state))
                    {
                        state.Queue = queue;
                    }
                    else
                    {
                        additions.Add(queue);
                    }
                }

                return additions;
            }
        }

        public void CancelQueuesNotIn(IReadOnlyDictionary<QueueKey, RemotingPullMessageQueue> desired)
        {
            lock (_stateGate)
            {
                foreach (var (key, state) in _assignments)
                {
                    if (!desired.ContainsKey(key))
                    {
                        state.CancelPolling();
                    }
                }
            }
        }

        public void CancelAllQueues()
        {
            lock (_stateGate)
            {
                foreach (var state in _assignments.Values)
                {
                    state.CancelPolling();
                }
            }
        }

        public void AddInitializedQueue(
            RemotingPullMessageQueue queue,
            long offset,
            IReadOnlyDictionary<QueueKey, RemotingPullMessageQueue> desired)
        {
            var key = QueueKey.Create(queue);
            lock (_stateGate)
            {
                if (desired.ContainsKey(key) && !_assignments.ContainsKey(key))
                {
                    _assignments[key] = new QueueState(queue, offset);
                }
            }
        }

        public QueueSelection SelectNextActiveQueue()
        {
            lock (_stateGate)
            {
                var active = _assignments
                    .Where(static entry => !entry.Value.Paused)
                    .OrderBy(static entry => entry.Key.Topic, StringComparer.Ordinal)
                    .ThenBy(static entry => entry.Key.BrokerName, StringComparer.Ordinal)
                    .ThenBy(static entry => entry.Key.QueueId)
                    .ToArray();
                if (active.Length == 0)
                {
                    throw new InvalidOperationException("Lite Pull consumer has no active assigned queues.");
                }

                var index = (int)((uint)_nextQueueIndex++ % (uint)active.Length);
                var selected = active[index];
                return new QueueSelection(
                    selected.Key,
                    selected.Value,
                    selected.Value.Queue,
                    selected.Value.Offset,
                    selected.Value.Version,
                    selected.Value.PollCancellation.Token);
            }
        }

        public bool AdvanceOffset(QueueKey key, QueueState state, long version, long offset)
        {
            lock (_stateGate)
            {
                if (_assignments.TryGetValue(key, out var current) &&
                    ReferenceEquals(current, state) &&
                    current.Version == version &&
                    !current.PollCancellation.IsCancellationRequested)
                {
                    current.Offset = offset;
                    return true;
                }

                return false;
            }
        }

        public RemotingPullMessageQueue GetRequiredQueue(RemotingPullMessageQueue queue)
        {
            lock (_stateGate)
            {
                return _assignments.TryGetValue(QueueKey.Create(queue), out var state)
                    ? state.Queue
                    : throw new InvalidOperationException(QueueNotAssignedMessage);
            }
        }

        public QueuePosition GetRequiredQueuePosition(RemotingPullMessageQueue queue)
        {
            lock (_stateGate)
            {
                return _assignments.TryGetValue(QueueKey.Create(queue), out var state)
                    ? new QueuePosition(
                        QueueKey.Create(queue),
                        state,
                        state.Queue,
                        state.Offset,
                        state.Version,
                        state.PollCancellation.Token)
                    : throw new InvalidOperationException(QueueNotAssignedMessage);
            }
        }

        public void SetOffset(RemotingPullMessageQueue queue, long offset)
        {
            lock (_stateGate)
            {
                if (!_assignments.TryGetValue(QueueKey.Create(queue), out var state))
                {
                    throw new InvalidOperationException(QueueNotAssignedMessage);
                }

                state.Offset = offset;
                state.Version++;
            }
        }

        public void SetPaused(RemotingPullMessageQueue queue, bool paused)
        {
            lock (_stateGate)
            {
                if (!_assignments.TryGetValue(QueueKey.Create(queue), out var state))
                {
                    throw new InvalidOperationException(QueueNotAssignedMessage);
                }

                state.Paused = paused;
            }
        }

        public IReadOnlyList<QueuePosition> GetPositions()
        {
            lock (_stateGate)
            {
                return _assignments.Values
                    .Select(static state => new QueuePosition(
                        QueueKey.Create(state.Queue),
                        state,
                        state.Queue,
                        state.Offset,
                        state.Version,
                        state.PollCancellation.Token))
                    .ToArray();
            }
        }

        public bool IsCurrent(QueuePosition position)
        {
            lock (_stateGate)
            {
                return _assignments.TryGetValue(position.Key, out var current) &&
                       ReferenceEquals(current, position.State) &&
                       current.Version == position.Version &&
                       !current.PollCancellation.IsCancellationRequested;
            }
        }

        public void CompleteShutdown()
        {
            lock (_stateGate)
            {
                _assignments.Clear();
            }
        }
    }

    private sealed class QueueState(RemotingPullMessageQueue queue, long offset)
    {
        public RemotingPullMessageQueue Queue { get; set; } = queue;
        public long Offset { get; set; } = offset;
        public bool Paused { get; set; }
        public long Version { get; set; }
        public CancellationTokenSource PollCancellation { get; } = new();

        public void CancelPolling() => PollCancellation.Cancel();
    }

    private readonly record struct QueueKey(string Topic, string BrokerName, int QueueId)
    {
        public static QueueKey Create(RemotingPullMessageQueue queue) =>
            new(queue.Topic, queue.BrokerName, queue.QueueId);
    }

    private readonly record struct QueuePosition(
        QueueKey Key,
        QueueState State,
        RemotingPullMessageQueue Queue,
        long Offset,
        long Version,
        CancellationToken CancellationToken);
    private readonly record struct QueueSelection(
        QueueKey Key,
        QueueState State,
        RemotingPullMessageQueue Queue,
        long Offset,
        long Version,
        CancellationToken CancellationToken);
}
