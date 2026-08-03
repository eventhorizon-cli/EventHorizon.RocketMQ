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

    public IReadOnlyList<RemotingConsumerQueue> Assignment
    {
        get
        {
            var run = _run;
            return run is null ? [] : run.GetAssignment();
        }
    }

    public async Task<IReadOnlyList<RemotingConsumerQueue>> GetMessageQueuesAsync(
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
            var run = new RunState(
                _timeProvider.GetUtcNow().ToUnixTimeMilliseconds(),
                _options.MaxCachedMessages,
                _options.MaxCachedMessageBytes);
            _run = run;
            try
            {
                run.AutoCommitTask = _options.EnableAutoCommit
                    ? RunAutoCommitAsync(run)
                    : Task.CompletedTask;
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
            var run = _run ?? throw new InvalidOperationException("Lite Pull consumer has not been started.");
            if (!_subscriptions.IsEmpty)
            {
                throw new InvalidOperationException(
                    "Manual assignment requires a Lite Pull consumer with no topic subscriptions.");
            }

            foreach (var queue in requestedQueues)
            {
                _consumerEngine.SetSubscription(
                    queue.Topic,
                    filters.TryGetValue(queue.Topic, out var filter) ? filter : FilterExpression.All);
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

    public async Task<IReadOnlyList<RemotingMessageView>> PollAsync(
        TimeSpan? timeout = null,
        CancellationToken cancellationToken = default)
    {
        if (timeout is { } localTimeout && localTimeout < TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(nameof(timeout), "Poll timeout cannot be negative.");
        }

        var run = await GetActiveRunAsync(cancellationToken).ConfigureAwait(false);
        using var operationCancellation = CancellationTokenSource.CreateLinkedTokenSource(
            cancellationToken,
            run.Stopping.Token);
        var configuredTimeout = timeout ?? _options.PollTimeout;
        using var timeoutCancellation = configuredTimeout > TimeSpan.Zero
            ? new CancellationTokenSource(configuredTimeout, _timeProvider)
            : null;
        using var pollCancellation = timeoutCancellation is null
            ? null
            : CancellationTokenSource.CreateLinkedTokenSource(
                operationCancellation.Token,
                timeoutCancellation.Token);
        var pollToken = pollCancellation?.Token ?? operationCancellation.Token;
        while (true)
        {
            var attempt = run.TryDrainBuffer();
            if (attempt.Messages.Count > 0)
            {
                return attempt.Messages;
            }

            if (configuredTimeout == TimeSpan.Zero)
            {
                return [];
            }

            try
            {
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
        return RequireActiveRun().GetRequiredQueuePosition(queue).Offset;
    }

    public void Seek(RemotingConsumerQueue queue, long offset)
    {
        ArgumentNullException.ThrowIfNull(queue);
        ArgumentOutOfRangeException.ThrowIfNegative(offset);
        var run = RequireActiveRun();
        StartReceiver(run, run.ReplacePosition(queue, offset));
    }

    public async Task SeekToBeginningAsync(
        RemotingConsumerQueue queue,
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
                ConsumeFromPosition.Beginning,
                cancellationToken: operationCancellation.Token).ConfigureAwait(false);
            StartReceiver(run, run.ReplacePosition(assignedQueue, offset));
        }
        finally
        {
            run.OperationGate.Release();
        }
    }

    public async Task SeekToEndAsync(RemotingConsumerQueue queue, CancellationToken cancellationToken = default)
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
                ConsumeFromPosition.End,
                cancellationToken: operationCancellation.Token).ConfigureAwait(false);
            StartReceiver(run, run.ReplacePosition(assignedQueue, offset));
        }
        finally
        {
            run.OperationGate.Release();
        }
    }

    public async Task SeekToTimestampAsync(
        RemotingConsumerQueue queue,
        DateTimeOffset timestamp,
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
                ConsumeFromPosition.Timestamp,
                timestamp,
                operationCancellation.Token).ConfigureAwait(false);
            StartReceiver(run, run.ReplacePosition(assignedQueue, offset));
        }
        finally
        {
            run.OperationGate.Release();
        }
    }

    public async Task<long?> GetCommittedOffsetAsync(
        RemotingConsumerQueue queue,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(queue);
        var run = await GetActiveRunAsync(cancellationToken).ConfigureAwait(false);
        var assignedQueue = run.GetRequiredQueue(queue);
        var offset = await _consumerEngine.GetOffsetAsync(assignedQueue, cancellationToken).ConfigureAwait(false);
        return offset >= 0 ? offset : null;
    }

    public void Pause(IEnumerable<RemotingConsumerQueue> queues)
    {
        ArgumentNullException.ThrowIfNull(queues);
        var run = RequireActiveRun();
        foreach (var queue in queues)
        {
            ArgumentNullException.ThrowIfNull(queue);
            run.SetPaused(queue, true);
        }
    }

    public void Resume(IEnumerable<RemotingConsumerQueue> queues)
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

    public async Task CommitAsync(RemotingConsumerQueue queue, CancellationToken cancellationToken = default)
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

            var subscribedTopics = subscriptions
                .Select(static entry => entry.Key)
                .ToHashSet(StringComparer.Ordinal);
            var desiredByTopic = run.GetAssignment()
                .Where(queue => subscribedTopics.Contains(queue.Topic))
                .GroupBy(static queue => queue.Topic, StringComparer.Ordinal)
                .ToDictionary(
                    static group => group.Key,
                    static group => (IReadOnlyList<RemotingConsumerQueue>)group.ToArray(),
                    StringComparer.Ordinal);
            var snapshots = new List<TopicSnapshot>(subscriptions.Length);
            var balanced = true;
            foreach (var subscription in subscriptions)
            {
                try
                {
                    var queues = await _consumerEngine.GetMessageQueuesAsync(
                        subscription.Key,
                        cancellationToken).ConfigureAwait(false);
                    snapshots.Add(new TopicSnapshot(subscription.Key, queues));
                    AddKnownBrokers(run, queues);
                }
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                {
                    throw;
                }
                catch (Exception exception)
                {
                    balanced = false;
                    _logger.LogWarning(
                        exception,
                        "Unable to refresh legacy Lite Pull route for topic {Topic}",
                        subscription.Key);
                }
            }

            await SendHeartbeatsAsync(run, subscriptions, cancellationToken).ConfigureAwait(false);
            foreach (var snapshot in snapshots)
            {
                if (snapshot.Queues.Count == 0)
                {
                    desiredByTopic[snapshot.Topic] = [];
                    continue;
                }

                IReadOnlyList<string> consumerIds;
                try
                {
                    consumerIds = await _groupSession.GetConsumerIdsAsync(
                        snapshot.Queues[0],
                        cancellationToken).ConfigureAwait(false);
                }
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                {
                    throw;
                }
                catch (Exception exception)
                {
                    balanced = false;
                    _logger.LogWarning(
                        exception,
                        "Unable to discover consumers for legacy Lite Pull group {GroupName} and topic {Topic}",
                        _options.GroupName,
                        snapshot.Topic);
                    continue;
                }

                if (!consumerIds.Contains(_groupSession.ClientId, StringComparer.Ordinal))
                {
                    balanced = false;
                    _logger.LogWarning(
                        "Broker membership for legacy Lite Pull group {GroupName} does not contain client {ClientId}",
                        _options.GroupName,
                        _groupSession.ClientId);
                    desiredByTopic[snapshot.Topic] = [];
                    continue;
                }

                desiredByTopic[snapshot.Topic] = LegacyConsumerProtocol.Allocate(
                    snapshot.Queues,
                    consumerIds,
                    _groupSession.ClientId);
            }

            await ReconcileAssignmentsAsync(
                run,
                desiredByTopic.Values.SelectMany(static queues => queues).ToArray(),
                cancellationToken).ConfigureAwait(false);
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
        IEnumerable<RemotingConsumerQueue> queues)
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
        IReadOnlyCollection<RemotingConsumerQueue> desiredQueues,
        CancellationToken cancellationToken)
    {
        var desired = desiredQueues
            .GroupBy(QueueKey.Create)
            .ToDictionary(static entry => entry.Key, static entry => entry.First());
        await run.OperationGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            var changes = run.ApplyDesiredQueues(desired);
            await AwaitReceiversAsync(changes.Removed).ConfigureAwait(false);
            foreach (var queue in changes.Additions)
            {
                var offset = await InitializeOffsetAsync(queue, cancellationToken).ConfigureAwait(false);
                var state = run.AddInitializedQueue(queue, offset, desired);
                if (state is not null)
                {
                    StartReceiver(run, state);
                }
            }
        }
        finally
        {
            run.OperationGate.Release();
        }
    }

    private async Task<long> InitializeOffsetAsync(
        RemotingConsumerQueue queue,
        CancellationToken cancellationToken)
    {
        var committed = await _consumerEngine.GetOffsetAsync(queue, cancellationToken).ConfigureAwait(false);
        if (committed >= 0)
        {
            return committed;
        }

        return await _consumerEngine.QueryOffsetAsync(
            queue,
            _options.InitialPosition,
            _options.ConsumeTimestamp,
            cancellationToken).ConfigureAwait(false);
    }

    private void StartReceiver(RunState run, QueueState state) =>
        state.ReceiverTask = ReceiveQueueAsync(run, state);

    private async Task ReceiveQueueAsync(RunState run, QueueState state)
    {
        using var receiveCancellation = CancellationTokenSource.CreateLinkedTokenSource(
            run.Stopping.Token,
            state.ReceiveCancellation.Token);
        var cancellationToken = receiveCancellation.Token;
        try
        {
            while (true)
            {
                var selection = run.SelectReceive(state, _options.PullBatchSize);
                if (!selection.IsCurrent)
                {
                    return;
                }

                if (!selection.CanReceive)
                {
                    await selection.StateChanged.WaitAsync(cancellationToken).ConfigureAwait(false);
                    continue;
                }

                RemotingPullResult result;
                try
                {
                    result = await _consumerEngine.PullAsync(
                        selection.Queue!,
                        selection.Offset,
                        selection.MaxMessages,
                        cancellationToken).ConfigureAwait(false);
                }
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                {
                    return;
                }
                catch (Exception exception)
                {
                    _logger.LogWarning(
                        exception,
                        "Legacy Lite Pull receive failed for {Topic}/{BrokerName}/{QueueId}; retrying",
                        state.Queue.Topic,
                        state.Queue.BrokerName,
                        state.Queue.QueueId);
                    await Task.Delay(TimeSpan.FromSeconds(1), cancellationToken).ConfigureAwait(false);
                    continue;
                }

                if (!await run.AdmitAsync(state, result, cancellationToken).ConfigureAwait(false))
                {
                    return;
                }

                await Task.Yield();
            }
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
        }
        finally
        {
            run.ReceiverCompleted(state);
            state.Dispose();
        }
    }

    private async Task RunAutoCommitAsync(RunState run)
    {
        while (!run.Stopping.IsCancellationRequested)
        {
            try
            {
                await Task.Delay(
                    _options.AutoCommitInterval,
                    _timeProvider,
                    run.Stopping.Token).ConfigureAwait(false);
                await CommitAsync(
                    run,
                    static state => state.GetPositions(),
                    run.Stopping.Token).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (run.Stopping.IsCancellationRequested)
            {
                return;
            }
            catch (Exception exception)
            {
                _logger.LogWarning(
                    exception,
                    "Unable to auto-commit legacy Lite Pull positions for group {GroupName}",
                    _options.GroupName);
            }
        }
    }

    private static async Task AwaitReceiversAsync(IEnumerable<QueueState> states)
    {
        foreach (var state in states)
        {
            try
            {
                await state.ReceiverTask.ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
            }
        }
    }

    private async Task ShutdownRunAsync(RunState run)
    {
        run.Stopping.Cancel();
        var receivers = run.CancelAllQueues();
        if (run.RebalanceRegistration is { } rebalanceRegistration)
        {
            run.RebalanceRegistration = null;
            await rebalanceRegistration.DisposeAsync().ConfigureAwait(false);
        }

        await AwaitReceiversAsync(receivers).ConfigureAwait(false);
        try
        {
            await run.AutoCommitTask.ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
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

    private string GetConsumeFromWhere() => _options.InitialPosition switch
    {
        ConsumeFromPosition.Beginning => "CONSUME_FROM_FIRST_OFFSET",
        ConsumeFromPosition.End => "CONSUME_FROM_LAST_OFFSET",
        ConsumeFromPosition.Timestamp => "CONSUME_FROM_TIMESTAMP",
        _ => throw new ArgumentOutOfRangeException(nameof(_options.InitialPosition), _options.InitialPosition, null)
    };

    private string GetWireTopic(string topic) => LegacyNamespace.Wrap(_clientOptions.Namespace, topic);

    private sealed class RunState
    {
        private const string QueueNotAssignedMessage =
            "The message queue is not assigned to this Lite Pull consumer.";

        private readonly object _stateGate = new();
        private readonly Dictionary<QueueKey, QueueState> _assignments = [];
        private readonly LinkedList<BufferedMessage> _buffer = [];
        private readonly HashSet<QueueState> _receivers = [];
        private readonly int _maxCachedMessages;
        private readonly int _maxCachedMessageBytes;
        private TaskCompletionSource _stateChanged = CreateStateSignal();
        private int _cachedMessageBytes;

        public RunState(
            long subscriptionVersion,
            int maxCachedMessages,
            int maxCachedMessageBytes)
        {
            SubscriptionVersion = subscriptionVersion;
            _maxCachedMessages = maxCachedMessages;
            _maxCachedMessageBytes = maxCachedMessageBytes;
        }

        public CancellationTokenSource Stopping { get; } = new();
        public SemaphoreSlim CoordinationGate { get; } = new(1, 1);
        public SemaphoreSlim OperationGate { get; } = new(1, 1);
        public Dictionary<string, RemotingBrokerEndpoint> KnownBrokers { get; } = new(StringComparer.Ordinal);
        public IRemotingRebalanceRegistration? RebalanceRegistration { get; set; }
        public Task AutoCommitTask { get; set; } = Task.CompletedTask;
        public bool ManualAssignment { get; set; }
        public long SubscriptionVersion { get; private set; }

        public void BumpSubscriptionVersion() => SubscriptionVersion++;

        public IReadOnlyList<RemotingConsumerQueue> GetAssignment()
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

        public AssignmentChanges ApplyDesiredQueues(
            IReadOnlyDictionary<QueueKey, RemotingConsumerQueue> desired)
        {
            lock (_stateGate)
            {
                var removed = new List<QueueState>();
                foreach (var key in _assignments.Keys.Except(desired.Keys).ToArray())
                {
                    var state = _assignments[key];
                    state.CancelReceive();
                    _assignments.Remove(key);
                    RemoveBufferedMessages(state);
                    removed.Add(state);
                }

                var additions = new List<RemotingConsumerQueue>();
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

                SignalStateChanged();
                return new AssignmentChanges(additions, removed);
            }
        }

        public QueueState? AddInitializedQueue(
            RemotingConsumerQueue queue,
            long offset,
            IReadOnlyDictionary<QueueKey, RemotingConsumerQueue> desired)
        {
            var key = QueueKey.Create(queue);
            lock (_stateGate)
            {
                if (!desired.ContainsKey(key) || _assignments.ContainsKey(key))
                {
                    return null;
                }

                var state = new QueueState(queue, offset);
                _assignments[key] = state;
                _receivers.Add(state);
                SignalStateChanged();
                return state;
            }
        }

        public QueueState ReplacePosition(RemotingConsumerQueue queue, long offset)
        {
            lock (_stateGate)
            {
                var key = QueueKey.Create(queue);
                if (!_assignments.TryGetValue(key, out var current))
                {
                    throw new InvalidOperationException(QueueNotAssignedMessage);
                }

                current.CancelReceive();
                RemoveBufferedMessages(current);
                var replacement = new QueueState(current.Queue, offset)
                {
                    Paused = current.Paused
                };
                _assignments[key] = replacement;
                _receivers.Add(replacement);
                SignalStateChanged();
                return replacement;
            }
        }

        public IReadOnlyList<QueueState> CancelAllQueues()
        {
            lock (_stateGate)
            {
                foreach (var state in _receivers)
                {
                    state.CancelReceive();
                }

                _buffer.Clear();
                _cachedMessageBytes = 0;
                SignalStateChanged();
                return _receivers.ToArray();
            }
        }

        public void ReceiverCompleted(QueueState state)
        {
            lock (_stateGate)
            {
                _receivers.Remove(state);
                SignalStateChanged();
            }
        }

        public ReceiveSelection SelectReceive(QueueState state, int pullBatchSize)
        {
            lock (_stateGate)
            {
                if (!IsCurrentState(state))
                {
                    return ReceiveSelection.Stale;
                }

                if (state.Paused ||
                    _buffer.Count >= _maxCachedMessages ||
                    _cachedMessageBytes >= _maxCachedMessageBytes)
                {
                    return ReceiveSelection.Wait(_stateChanged.Task);
                }

                return ReceiveSelection.Ready(
                    state.Queue,
                    state.FetchOffset,
                    Math.Min(pullBatchSize, _maxCachedMessages - _buffer.Count));
            }
        }

        public async Task<bool> AdmitAsync(
            QueueState state,
            RemotingPullResult result,
            CancellationToken cancellationToken)
        {
            if (result.Messages.Count == 0)
            {
                lock (_stateGate)
                {
                    if (!IsCurrentState(state))
                    {
                        return false;
                    }

                    state.FetchOffset = result.NextOffset;
                    return true;
                }
            }

            long fetchOffset;
            lock (_stateGate)
            {
                if (!IsCurrentState(state))
                {
                    return false;
                }

                fetchOffset = state.FetchOffset;
            }

            var messages = result.Messages
                .Where(message => message.QueueOffset >= fetchOffset)
                .OrderBy(static message => message.QueueOffset)
                .DistinctBy(static message => message.QueueOffset)
                .ToArray();
            for (var index = 0; index < messages.Length; index++)
            {
                var message = messages[index];
                var messageBytes = Math.Min(message.Body.Length, _maxCachedMessageBytes);
                while (true)
                {
                    Task stateChanged;
                    lock (_stateGate)
                    {
                        if (!IsCurrentState(state))
                        {
                            return false;
                        }

                        if (_buffer.Count < _maxCachedMessages &&
                            _cachedMessageBytes + messageBytes <= _maxCachedMessageBytes)
                        {
                            var messageNextOffset = checked(message.QueueOffset + 1);
                            var deliveredOffset = index == messages.Length - 1
                                ? Math.Max(messageNextOffset, result.NextOffset)
                                : messageNextOffset;
                            _buffer.AddLast(new BufferedMessage(
                                QueueKey.Create(state.Queue),
                                state,
                                message,
                                deliveredOffset,
                                messageBytes));
                            _cachedMessageBytes += messageBytes;
                            state.FetchOffset = Math.Max(state.FetchOffset, messageNextOffset);
                            SignalStateChanged();
                            break;
                        }

                        stateChanged = _stateChanged.Task;
                    }

                    await stateChanged.WaitAsync(cancellationToken).ConfigureAwait(false);
                }
            }

            lock (_stateGate)
            {
                if (!IsCurrentState(state))
                {
                    return false;
                }

                state.FetchOffset = Math.Max(state.FetchOffset, result.NextOffset);
                return true;
            }
        }

        public BufferPollAttempt TryDrainBuffer()
        {
            lock (_stateGate)
            {
                if (_assignments.Count == 0)
                {
                    throw new InvalidOperationException("Lite Pull consumer has no assigned queues.");
                }

                if (_buffer.Count == 0)
                {
                    return new BufferPollAttempt([], _stateChanged.Task);
                }

                var messages = new List<RemotingMessageView>(_buffer.Count);
                while (_buffer.First is { } node)
                {
                    _buffer.RemoveFirst();
                    var buffered = node.Value;
                    _cachedMessageBytes -= buffered.BodyBytes;
                    if (_assignments.TryGetValue(buffered.Key, out var current) &&
                        ReferenceEquals(current, buffered.State))
                    {
                        current.DeliveredOffset = buffered.DeliveredOffset;
                        messages.Add(buffered.Message);
                    }
                }

                SignalStateChanged();
                return new BufferPollAttempt(messages, _stateChanged.Task);
            }
        }

        public RemotingConsumerQueue GetRequiredQueue(RemotingConsumerQueue queue)
        {
            lock (_stateGate)
            {
                return _assignments.TryGetValue(QueueKey.Create(queue), out var state)
                    ? state.Queue
                    : throw new InvalidOperationException(QueueNotAssignedMessage);
            }
        }

        public QueuePosition GetRequiredQueuePosition(RemotingConsumerQueue queue)
        {
            lock (_stateGate)
            {
                return _assignments.TryGetValue(QueueKey.Create(queue), out var state)
                    ? new QueuePosition(
                        QueueKey.Create(queue),
                        state,
                        state.Queue,
                        state.DeliveredOffset,
                        state.ReceiveCancellation.Token)
                    : throw new InvalidOperationException(QueueNotAssignedMessage);
            }
        }

        public void SetPaused(RemotingConsumerQueue queue, bool paused)
        {
            lock (_stateGate)
            {
                if (!_assignments.TryGetValue(QueueKey.Create(queue), out var state))
                {
                    throw new InvalidOperationException(QueueNotAssignedMessage);
                }

                state.Paused = paused;
                SignalStateChanged();
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
                        state.DeliveredOffset,
                        state.ReceiveCancellation.Token))
                    .ToArray();
            }
        }

        public bool IsCurrent(QueuePosition position)
        {
            lock (_stateGate)
            {
                return _assignments.TryGetValue(position.Key, out var current) &&
                       ReferenceEquals(current, position.State) &&
                       !current.ReceiveCancellation.IsCancellationRequested;
            }
        }

        public void CompleteShutdown()
        {
            lock (_stateGate)
            {
                _assignments.Clear();
                _buffer.Clear();
                _receivers.Clear();
                _cachedMessageBytes = 0;
                SignalStateChanged();
            }
        }

        private bool IsCurrentState(QueueState state) =>
            _assignments.TryGetValue(QueueKey.Create(state.Queue), out var current) &&
            ReferenceEquals(current, state) &&
            !state.ReceiveCancellation.IsCancellationRequested;

        private void RemoveBufferedMessages(QueueState state)
        {
            var node = _buffer.First;
            while (node is not null)
            {
                var next = node.Next;
                if (ReferenceEquals(node.Value.State, state))
                {
                    _cachedMessageBytes -= node.Value.BodyBytes;
                    _buffer.Remove(node);
                }

                node = next;
            }
        }

        private void SignalStateChanged()
        {
            var signal = _stateChanged;
            _stateChanged = CreateStateSignal();
            signal.TrySetResult();
        }

        private static TaskCompletionSource CreateStateSignal() =>
            new(TaskCreationOptions.RunContinuationsAsynchronously);
    }

    private sealed class QueueState(RemotingConsumerQueue queue, long offset) : IDisposable
    {
        public RemotingConsumerQueue Queue { get; set; } = queue;
        public long FetchOffset { get; set; } = offset;
        public long DeliveredOffset { get; set; } = offset;
        public bool Paused { get; set; }
        public CancellationTokenSource ReceiveCancellation { get; } = new();
        public Task ReceiverTask { get; set; } = Task.CompletedTask;

        private int _canceled;
        private int _disposed;

        public void CancelReceive()
        {
            if (Interlocked.Exchange(ref _canceled, 1) == 0)
            {
                ReceiveCancellation.Cancel();
            }
        }

        public void Dispose()
        {
            if (Interlocked.Exchange(ref _disposed, 1) == 0)
            {
                ReceiveCancellation.Dispose();
            }
        }
    }

    private readonly record struct QueueKey(string Topic, string BrokerName, int QueueId)
    {
        public static QueueKey Create(RemotingConsumerQueue queue) =>
            new(queue.Topic, queue.BrokerName, queue.QueueId);
    }

    private readonly record struct QueuePosition(
        QueueKey Key,
        QueueState State,
        RemotingConsumerQueue Queue,
        long Offset,
        CancellationToken CancellationToken);

    private sealed record AssignmentChanges(
        IReadOnlyList<RemotingConsumerQueue> Additions,
        IReadOnlyList<QueueState> Removed);

    private sealed record BufferedMessage(
        QueueKey Key,
        QueueState State,
        RemotingMessageView Message,
        long DeliveredOffset,
        int BodyBytes);

    private readonly record struct BufferPollAttempt(
        IReadOnlyList<RemotingMessageView> Messages,
        Task StateChanged);

    private readonly record struct ReceiveSelection(
        bool IsCurrent,
        bool CanReceive,
        RemotingConsumerQueue? Queue,
        long Offset,
        int MaxMessages,
        Task StateChanged)
    {
        public static ReceiveSelection Stale => new(false, false, null, 0, 0, Task.CompletedTask);

        public static ReceiveSelection Wait(Task stateChanged) =>
            new(true, false, null, 0, 0, stateChanged);

        public static ReceiveSelection Ready(RemotingConsumerQueue queue, long offset, int maxMessages) =>
            new(true, true, queue, offset, maxMessages, Task.CompletedTask);
    }

    private sealed record TopicSnapshot(
        string Topic,
        IReadOnlyList<RemotingConsumerQueue> Queues);
}
