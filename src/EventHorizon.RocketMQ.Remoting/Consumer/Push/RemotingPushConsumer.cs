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
using EventHorizon.RocketMQ.Remoting.Consumer.Pull;
using EventHorizon.RocketMQ.Remoting.Consumer.Push.Assignment;
using EventHorizon.RocketMQ.Remoting.Consumer.Push.Dispatch;
using EventHorizon.RocketMQ.Remoting.Consumer.Push.Offset;
using EventHorizon.RocketMQ.Remoting.Consumer.Push.Orderly;
using EventHorizon.RocketMQ.Remoting.Consumer.Push.Pop;
using EventHorizon.RocketMQ.Remoting.Consumer.Push.Pull;
using EventHorizon.RocketMQ.Remoting.Exceptions;
using EventHorizon.RocketMQ.Remoting.Instrumentation;
using EventHorizon.RocketMQ.Remoting.Protocol;
using EventHorizon.RocketMQ.Remoting.Protocol.Route;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace EventHorizon.RocketMQ.Remoting.Consumer.Push;

internal sealed class RemotingPushConsumer : IRemotingPushConsumer
{
    private const int ReadPermission = 1 << 2;
    private static readonly TimeSpan MaximumBrokerLockLiveTime = TimeSpan.FromSeconds(30);

    private readonly RemotingPushConsumerOptions _options;
    private readonly Func<IReadOnlyList<RemotingMessageView>, RemotingPushConsumeContext, CancellationToken, ValueTask<ConsumeResult>>
        _messageHandler;
    private readonly RemotingClientOptions _clientOptions;
    private readonly IRemotingConsumerEngine _consumerEngine;
    private readonly ITopicRouteService _routes;
    private readonly IRemotingClient _remotingClient;
    private readonly IRemotingRebalanceService _rebalanceService;
    private readonly RemotingConsumerGroupSession _groupSession;
    private readonly RemotingPushQueueLockManager _queueLockManager;
    private readonly PopWireClient _popClient;
    private readonly TimeProvider _timeProvider;
    private readonly IRemotingRocketMQTelemetry _telemetry;
    private readonly ILogger<RemotingPushConsumer> _logger;
    private readonly bool _hasLocalGroupPeer;
    private readonly IDisposable? _resetConsumerOffsetRegistration;
    private readonly BroadcastOffsetStore? _broadcastOffsetStore;
    private readonly ConcurrentDictionary<string, FilterExpression> _subscriptions;
    private readonly ConcurrentDictionary<ResetQueueKey, long> _resetBoundaries = new();
    private readonly SemaphoreSlim _lifecycle = new(1, 1);
    private RunState? _run;
    private int _disposed;

    public RemotingPushConsumer(
        IOptions<RemotingPushConsumerOptions> options,
        IOptions<RemotingClientOptions> clientOptions,
        IRemotingConsumerEngine consumerEngine,
        ITopicRouteService routes,
        IRemotingClient remotingClient,
        IRemotingRebalanceService rebalanceService,
        TimeProvider timeProvider,
        ILogger<RemotingPushConsumer> logger,
        Func<IReadOnlyList<RemotingMessageView>, RemotingPushConsumeContext, CancellationToken, ValueTask<ConsumeResult>> messageHandler,
        IRemotingRocketMQTelemetry? telemetry = null,
        bool hasLocalGroupPeer = false)
    {
        _options = options.Value;
        _messageHandler = messageHandler ?? throw new ArgumentNullException(nameof(messageHandler));

        _clientOptions = clientOptions.Value;
        _consumerEngine = consumerEngine;
        _routes = routes;
        _remotingClient = remotingClient;
        _rebalanceService = rebalanceService;
        _timeProvider = timeProvider;
        _telemetry = telemetry ?? RemotingRocketMQTelemetry.Disabled;
        _logger = logger;
        _hasLocalGroupPeer = hasLocalGroupPeer;
        _groupSession = new RemotingConsumerGroupSession(
            remotingClient,
            _clientOptions,
            _options.GroupName,
            logger);
        _queueLockManager = new RemotingPushQueueLockManager(
            remotingClient,
            _clientOptions,
            _groupSession,
            logger);
        _popClient = new PopWireClient(
            _options,
            _clientOptions,
            remotingClient,
            timeProvider,
            _telemetry);
        _subscriptions = new ConcurrentDictionary<string, FilterExpression>(
            _options.Subscriptions,
            StringComparer.Ordinal);
        if (_options.ConsumerMode == ConsumerMode.Broadcasting)
        {
            _broadcastOffsetStore = new BroadcastOffsetStore(
                _options.LocalOffsetStorePath,
                _groupSession.ClientId,
                _groupSession.ConsumerGroup);
        }

        if (remotingClient is IRemotingRequestHandlerRegistry requestHandlers)
        {
            _resetConsumerOffsetRegistration = requestHandlers.RegisterRequestHandler(
                RequestCode.ResetConsumerOffset,
                HandleResetConsumerOffsetAsync);
        }
    }

    internal IReadOnlyList<RemotingConsumerQueue> Assignment => _run?.Receivers.Values
        .Where(static receiver => receiver.Queue.QueueId >= 0)
        .Select(static receiver => receiver.Queue)
        .OrderBy(static queue => queue.Topic, StringComparer.Ordinal)
        .ThenBy(static queue => queue.BrokerName, StringComparer.Ordinal)
        .ThenBy(static queue => queue.QueueId)
        .ToArray() ?? [];

    internal IReadOnlyList<RemotingPushAssignment> ReceiveAssignments => _run?.Receivers.Values
        .Select(static receiver => receiver.Assignment)
        .OrderBy(static assignment => assignment.Topic, StringComparer.Ordinal)
        .ThenBy(static assignment => assignment.BrokerName, StringComparer.Ordinal)
        .ThenBy(static assignment => assignment.QueueId)
        .ThenBy(static assignment => assignment.Mode)
        .ToArray() ?? [];

    public async ValueTask StartAsync(CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);
        await _lifecycle.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (_run is not null)
            {
                return;
            }

            await _consumerEngine.StartAsync(cancellationToken).ConfigureAwait(false);
            var run = new RunState(
                _timeProvider.GetUtcNow().ToUnixTimeMilliseconds(),
                _options.MaxConcurrency,
                _options.PullMaxCachedMessages);
            _run = run;
            try
            {
                if (!_options.ConsumeOrderly)
                {
                    run.ConcurrentConsumeDispatcher = new ConcurrentConsumeDispatcher(
                        _options.MaxConcurrency,
                        _options.ConsumeMessageBatchSize,
                        (request, token) => ProcessConsumeRequestAsync(run, request, token),
                        run.ReleaseDeliverySlots,
                        run.Stopping.Token,
                        _logger);
                }

                run.RebalanceRegistration = _rebalanceService.Register(
                    _groupSession.ConsumerGroup,
                    token => RebalanceAsync(run, token));
                if (!await CoordinateOnceAsync(run, cancellationToken).ConfigureAwait(false))
                {
                    run.RebalanceRegistration.Wakeup();
                }
            }
            catch
            {
                _run = null;
                await ShutdownRunAsync(run, CancellationToken.None).ConfigureAwait(false);
                throw;
            }
        }
        finally
        {
            _lifecycle.Release();
        }
    }

    public async ValueTask StopAsync(CancellationToken cancellationToken = default)
    {
        await _lifecycle.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            var run = _run;
            if (run is null)
            {
                return;
            }

            _run = null;
            await ShutdownRunAsync(run, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            _lifecycle.Release();
        }
    }

    public async Task SubscribeAsync(
        string topic,
        FilterExpression? expression = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(topic);
        ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);
        cancellationToken.ThrowIfCancellationRequested();
        ThrowIfRuntimeSubscriptionChangesAreUnsupported();
        var filter = expression ?? FilterExpression.All;

        await _lifecycle.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);
            cancellationToken.ThrowIfCancellationRequested();
            _subscriptions[topic] = filter;
            _consumerEngine.SetSubscription(topic, filter);
            var run = _run;
            if (run is not null)
            {
                run.BumpSubscriptionVersion();
                await ApplySubscriptionChangeAsync(run, topic, cancellationToken).ConfigureAwait(false);
            }
        }
        finally
        {
            _lifecycle.Release();
        }
    }

    public async Task UnsubscribeAsync(string topic, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(topic);
        ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);
        cancellationToken.ThrowIfCancellationRequested();
        ThrowIfRuntimeSubscriptionChangesAreUnsupported();

        await _lifecycle.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);
            cancellationToken.ThrowIfCancellationRequested();
            _subscriptions.TryRemove(topic, out _);
            RemoveResetBoundaries(topic);
            _consumerEngine.RemoveSubscription(topic);
            var run = _run;
            if (run is not null)
            {
                run.BumpSubscriptionVersion();
                await ApplySubscriptionChangeAsync(run, topic, cancellationToken).ConfigureAwait(false);
            }
        }
        finally
        {
            _lifecycle.Release();
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
        {
            return;
        }

        _resetConsumerOffsetRegistration?.Dispose();
        await StopAsync().ConfigureAwait(false);
        await _consumerEngine.DisposeAsync().ConfigureAwait(false);
    }

    private void ThrowIfRuntimeSubscriptionChangesAreUnsupported()
    {
        if (_hasLocalGroupPeer)
        {
            throw new InvalidOperationException(
                $"Runtime subscription changes are not supported when multiple Remoting Push consumers in the " +
                $"same consumer group '{_options.GroupName}' share a client registration. Configure identical " +
                "subscriptions for all group members and restart them together.");
        }
    }

    private async ValueTask<RemotingRequestResult> HandleResetConsumerOffsetAsync(RemotingRequestContext context)
    {
        if (!context.Request.ExtFields.TryGetValue("group", out var groupValue) ||
            groupValue is not string group ||
            !string.Equals(group, _groupSession.ConsumerGroup, StringComparison.Ordinal) ||
            !context.Request.ExtFields.TryGetValue("topic", out var topicValue) ||
            topicValue is not string wireTopic)
        {
            return RemotingRequestResult.NotHandled;
        }

        var logicalTopic = LegacyNamespace.WithoutNamespace(_clientOptions.Namespace, wireTopic);
        if (!string.Equals(GetWireTopic(logicalTopic), wireTopic, StringComparison.Ordinal) ||
            !_subscriptions.ContainsKey(logicalTopic))
        {
            return RemotingRequestResult.NotHandled;
        }

        var offsets = LegacyResetOffsetDecoder.Decode(context.Request.Body);
        if (offsets.Any(offset => !string.Equals(offset.Topic, wireTopic, StringComparison.Ordinal)))
        {
            throw new InvalidDataException(
                $"The reset-offset body contains a queue outside the requested topic '{wireTopic}'.");
        }

        await _lifecycle.WaitAsync(context.CancellationToken).ConfigureAwait(false);
        try
        {
            if (!_subscriptions.ContainsKey(logicalTopic))
            {
                return RemotingRequestResult.NotHandled;
            }

            if (offsets.Count == 0)
            {
                return RemotingRequestResult.NoResponse;
            }

            if (_run is { } run)
            {
                await ResetConsumerOffsetsAsync(
                    run,
                    logicalTopic,
                    offsets,
                    context.CancellationToken).ConfigureAwait(false);
            }
            else
            {
                SetResetBoundaries(logicalTopic, offsets);
            }
        }
        finally
        {
            _lifecycle.Release();
        }

        return RemotingRequestResult.NoResponse;
    }

    private async Task ResetConsumerOffsetsAsync(
        RunState run,
        string logicalTopic,
        IReadOnlyList<LegacyResetOffset> offsets,
        CancellationToken cancellationToken)
    {
        var desiredOffsets = offsets.ToDictionary(
            static offset => (offset.BrokerName, offset.QueueId),
            static offset => offset.Offset);
        await run.CoordinationGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        var removed = new List<ResetReceiver>();
        try
        {
            foreach (var entry in run.Receivers)
            {
                if (!string.Equals(entry.Key.Topic, logicalTopic, StringComparison.Ordinal) ||
                    !desiredOffsets.TryGetValue((entry.Key.BrokerName, entry.Key.QueueId), out var offset) ||
                    !run.Receivers.TryRemove(entry.Key, out var receiver))
                {
                    continue;
                }

                DropReceiver(run, receiver);
                removed.Add(new ResetReceiver(receiver, offset));
            }

            foreach (var reset in removed)
            {
                try
                {
                    await reset.Receiver.Task.ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                }
                catch (Exception exception)
                {
                    _logger.LogWarning(
                        exception,
                        "Legacy receiver failed while resetting {Topic}/{BrokerName}/{QueueId}",
                        reset.Receiver.Queue.Topic,
                        reset.Receiver.Queue.BrokerName,
                        reset.Receiver.Queue.QueueId);
                }
            }

            SetResetBoundaries(logicalTopic, offsets);

            if (_options.ConsumerMode == ConsumerMode.Broadcasting)
            {
                foreach (var offset in offsets)
                {
                    var key = new ResetQueueKey(logicalTopic, offset.BrokerName, offset.QueueId);
                    await _broadcastOffsetStore!.UpdateAsync(
                        logicalTopic,
                        offset.BrokerName,
                        offset.QueueId,
                        offset.Offset,
                        cancellationToken).ConfigureAwait(false);
                    TryRemoveResetBoundary(key, offset.Offset);
                }
            }
            else
            {
                foreach (var reset in removed)
                {
                    await PersistOffsetAndClearResetBoundaryAsync(
                        reset.Receiver.Queue,
                        reset.Offset,
                        cancellationToken).ConfigureAwait(false);
                }
            }
        }
        finally
        {
            foreach (var reset in removed)
            {
                reset.Receiver.Dispose();
            }

            if (offsets.Count > 0)
            {
                run.RebalanceRegistration?.Wakeup();
            }

            run.CoordinationGate.Release();
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

    private async Task<bool> CoordinateOnceAsync(RunState run, CancellationToken cancellationToken)
    {
        await run.CoordinationGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            return await CoordinateOnceCoreAsync(run, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            run.CoordinationGate.Release();
        }
    }

    private async Task ApplySubscriptionChangeAsync(
        RunState run,
        string topic,
        CancellationToken cancellationToken)
    {
        await run.CoordinationGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            var balanced = await ReconcileTopicAsync(
                run,
                topic,
                Array.Empty<RemotingConsumerQueue>(),
                cancellationToken).ConfigureAwait(false);
            balanced &= await CoordinateOnceCoreAsync(run, cancellationToken).ConfigureAwait(false);
            if (!balanced)
            {
                run.RebalanceRegistration?.Wakeup();
            }
        }
        finally
        {
            run.CoordinationGate.Release();
        }
    }

    private async Task<bool> CoordinateOnceCoreAsync(RunState run, CancellationToken cancellationToken)
    {
        var retryTopic = GetRetryTopic();
        var topics = (_options.ConsumerMode == ConsumerMode.Broadcasting
                ? _subscriptions.Keys
                : _subscriptions.Keys.Append(retryTopic))
            .Distinct(StringComparer.Ordinal)
            .OrderBy(static topic => topic, StringComparer.Ordinal)
            .ToArray();
        var snapshots = new List<TopicSnapshot>(topics.Length);
        var balanced = true;
        foreach (var topic in topics)
        {
            try
            {
                var route = await _routes.GetAsync(
                    GetWireTopic(topic),
                    forceRefresh: true,
                    cancellationToken).ConfigureAwait(false);
                var queues = BuildQueues(topic, route);
                snapshots.Add(new TopicSnapshot(topic, queues, route));
                foreach (var broker in route.BrokerDatas)
                {
                    foreach (var address in broker.BrokerAddrs.Values)
                    {
                        run.KnownBrokers[address] = new RemotingBrokerEndpoint(broker.BrokerName, address);
                    }
                }
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception exception)
            {
                if (topic == retryTopic)
                {
                    if (run.RetryTopicRequired)
                    {
                        balanced = false;
                    }

                    _logger.LogDebug(exception, "Legacy retry topic {RetryTopic} is not available yet", retryTopic);
                }
                else
                {
                    balanced = false;
                    _logger.LogWarning(exception, "Unable to refresh legacy route for topic {Topic}", topic);
                }
            }
        }

        await SendHeartbeatsAsync(run, run.KnownBrokers.Values, cancellationToken).ConfigureAwait(false);
        if (UsesBrokerAssignment)
        {
            balanced &= await CoordinateBrokerAssignmentsAsync(run, snapshots, cancellationToken).ConfigureAwait(false);
            return balanced;
        }

        if (UsesBrokerQueueLocks)
        {
            balanced &= await RenewQueueLocksAsync(run, cancellationToken).ConfigureAwait(false);
        }

        foreach (var snapshot in snapshots)
        {
            if (snapshot.Queues.Count == 0)
            {
                balanced &= await ReconcileTopicAsync(
                    run,
                    snapshot.Topic,
                    Array.Empty<RemotingConsumerQueue>(),
                    cancellationToken).ConfigureAwait(false);
                continue;
            }

            if (_options.ConsumerMode == ConsumerMode.Broadcasting)
            {
                balanced &= await ReconcileTopicAsync(run, snapshot.Topic, snapshot.Queues, cancellationToken)
                    .ConfigureAwait(false);
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
                    "Unable to discover consumers for legacy group {GroupName} and topic {Topic}",
                    _options.GroupName,
                    snapshot.Topic);
                continue;
            }

            if (!consumerIds.Contains(_groupSession.ClientId, StringComparer.Ordinal))
            {
                balanced = false;
                _logger.LogWarning(
                    "Broker membership for legacy group {GroupName} does not contain client {ClientId}",
                    _options.GroupName,
                    _groupSession.ClientId);
                await ReconcileTopicAsync(
                    run,
                    snapshot.Topic,
                    Array.Empty<RemotingConsumerQueue>(),
                    cancellationToken).ConfigureAwait(false);
                continue;
            }

            var assigned = LegacyConsumerProtocol.Allocate(
                snapshot.Queues,
                consumerIds,
                _groupSession.ClientId);
            balanced &= await ReconcileTopicAsync(run, snapshot.Topic, assigned, cancellationToken)
                .ConfigureAwait(false);
        }

        return balanced;
    }

    private async Task<bool> CoordinateBrokerAssignmentsAsync(
        RunState run,
        IReadOnlyList<TopicSnapshot> snapshots,
        CancellationToken cancellationToken)
    {
        var balanced = true;
        foreach (var snapshot in snapshots)
        {
            try
            {
                var broker = snapshot.Route.BrokerDatas
                    .Where(static value => value.BrokerAddrs.Count > 0)
                    .OrderBy(static value => value.BrokerName, StringComparer.Ordinal)
                    .FirstOrDefault();
                if (broker is null)
                {
                    throw new InvalidOperationException(
                        $"The route for topic '{snapshot.Topic}' does not contain a readable Broker endpoint.");
                }

                var response = await _remotingClient.InvokeAsync(
                    EndpointParser.Parse(GetPrimaryBrokerAddress(broker)),
                    new RemotingCommand
                    {
                        Code = RequestCode.QueryAssignment,
                        Body = LegacyConsumerProtocol.EncodeQueryAssignment(
                            GetWireTopic(snapshot.Topic),
                            _groupSession.ConsumerGroup,
                            _groupSession.ClientId)
                    },
                    _clientOptions.RequestTimeout,
                    cancellationToken).ConfigureAwait(false);
                EnsureBrokerSuccess(response, "query message assignments");
                var update = LegacyConsumerProtocol.DecodeQueryAssignment(response.Body);
                if (!update.ShouldReplace)
                {
                    continue;
                }

                var assignments = update.Assignments.Select(assignment =>
                {
                    var logicalTopic = LegacyNamespace.WithoutNamespace(
                        _clientOptions.Namespace,
                        assignment.Topic);
                    if (!string.Equals(logicalTopic, snapshot.Topic, StringComparison.Ordinal))
                    {
                        throw new InvalidDataException(
                            $"Broker returned topic '{assignment.Topic}' while querying assignment for '{snapshot.Topic}'.");
                    }

                    return assignment with { Topic = logicalTopic };
                }).ToArray();
                balanced &= await ReconcileBrokerTopicAsync(
                    run,
                    snapshot,
                    assignments,
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
                    "Unable to query Broker assignment for legacy group {GroupName} and topic {Topic}",
                    _options.GroupName,
                    snapshot.Topic);
            }
        }

        return balanced;
    }

    private async Task SendHeartbeatsAsync(
        RunState run,
        IEnumerable<RemotingBrokerEndpoint> brokers,
        CancellationToken cancellationToken)
    {
        var subscriptions = _subscriptions.ToDictionary(
            static entry => entry.Key,
            static entry => entry.Value,
            StringComparer.Ordinal);
        if (_options.ConsumerMode == ConsumerMode.Clustering)
        {
            subscriptions[GetRetryTopic()] = FilterExpression.All;
        }

        var wireSubscriptions = subscriptions.ToDictionary(
            entry => GetWireTopic(entry.Key),
            static entry => entry.Value,
            StringComparer.Ordinal);
        var body = LegacyConsumerProtocol.EncodeHeartbeat(
            _groupSession.ClientId,
            _groupSession.ConsumerGroup,
            wireSubscriptions,
            run.SubscriptionVersion,
            _options.ConsumerMode,
            _options.InitialPosition,
            _clientOptions.UnitMode);
        await _groupSession.SendHeartbeatsAsync(brokers, body, cancellationToken).ConfigureAwait(false);
    }

    private async Task<bool> ReconcileTopicAsync(
        RunState run,
        string topic,
        IReadOnlyList<RemotingConsumerQueue> desiredQueues,
        CancellationToken cancellationToken)
    {
        var targets = desiredQueues.Select(static queue => new ReceiveTarget(
            new RemotingPushAssignment(
                queue.Topic,
                queue.BrokerName,
                queue.QueueId,
                RemotingPushReceiveMode.Pull,
                new Dictionary<string, string>(StringComparer.Ordinal)),
            queue)).ToArray();
        return await ReconcileReceiveTargetsAsync(run, topic, targets, cancellationToken).ConfigureAwait(false);
    }

    private async Task<bool> ReconcileBrokerTopicAsync(
        RunState run,
        TopicSnapshot snapshot,
        IReadOnlyList<RemotingPushAssignment> assignments,
        CancellationToken cancellationToken)
    {
        var targets = new ReceiveTarget[assignments.Count];
        for (var index = 0; index < assignments.Count; index++)
        {
            var assignment = assignments[index];
            var broker = snapshot.Route.BrokerDatas.FirstOrDefault(value =>
                string.Equals(value.BrokerName, assignment.BrokerName, StringComparison.Ordinal));
            if (broker is null || broker.BrokerAddrs.Count == 0)
            {
                throw new InvalidDataException(
                    $"Broker assignment '{assignment.Topic}/{assignment.BrokerName}/{assignment.QueueId}' " +
                    "does not have a current route.");
            }

            targets[index] = new ReceiveTarget(
                assignment,
                new RemotingConsumerQueue(
                    assignment.Topic,
                    assignment.QueueId,
                    assignment.BrokerName,
                    GetPrimaryBrokerAddress(broker)));
        }

        return await ReconcileReceiveTargetsAsync(
            run,
            snapshot.Topic,
            targets,
            cancellationToken).ConfigureAwait(false);
    }

    private async Task<bool> ReconcileReceiveTargetsAsync(
        RunState run,
        string topic,
        IReadOnlyList<ReceiveTarget> desiredTargets,
        CancellationToken cancellationToken)
    {
        var desired = desiredTargets.ToDictionary(static target => QueueKey.Create(target));
        var removed = new List<IRemotingPushReceiver>();
        foreach (var entry in run.Receivers)
        {
            if (entry.Key.Topic == topic && !desired.ContainsKey(entry.Key) &&
                run.Receivers.TryRemove(entry.Key, out var receiver))
            {
                DropReceiver(run, receiver);
                removed.Add(receiver);
            }
        }

        try
        {
            await AwaitReceiversAsync(removed).ConfigureAwait(false);
        }
        finally
        {
            if (UsesBrokerQueueLocks && removed.Count > 0)
            {
                await _queueLockManager.UnlockAsync(
                    removed.Select(static receiver => receiver.Queue),
                    oneway: true,
                    CancellationToken.None).ConfigureAwait(false);
            }

            foreach (var receiver in removed)
            {
                receiver.Dispose();
            }
        }

        var additions = desired
            .Where(entry => !run.Receivers.ContainsKey(entry.Key))
            .ToArray();
        var queueLockResult = UsesBrokerQueueLocks
            ? await _queueLockManager.LockAsync(
                    additions.Select(static entry => entry.Value.Queue),
                    cancellationToken)
                .ConfigureAwait(false)
            : null;
        foreach (var entry in additions)
        {
            if (queueLockResult is not null && !queueLockResult.LockedQueues.Contains(entry.Value.Queue))
            {
                continue;
            }

            IRemotingPushReceiver receiver = entry.Value.Assignment.Mode == RemotingPushReceiveMode.Pop
                ? new PopAssignmentReceiver(
                    entry.Value.Queue,
                    entry.Value.Assignment,
                    _options,
                    _popClient,
                    _consumerEngine,
                    _timeProvider,
                    _logger,
                    _subscriptions.TryGetValue(entry.Value.Assignment.Topic, out var filter)
                        ? filter
                        : FilterExpression.All,
                    (messages, token) => HandlePopMessagesAsync(run, messages, token),
                    run.Stopping.Token)
                : new PullAssignmentReceiver(
                    entry.Value.Queue,
                    entry.Value.Assignment,
                    run.Stopping.Token,
                    createProcessQueue: !_options.ConsumeOrderly);
            if (UsesBrokerQueueLocks && receiver is PullAssignmentReceiver lockedPullReceiver)
            {
                lockedPullReceiver.MarkBrokerLockAcquired(_timeProvider.GetUtcNow());
            }

            if (!run.Receivers.TryAdd(entry.Key, receiver))
            {
                receiver.Dispose();
                continue;
            }

            if (receiver is PopAssignmentReceiver popReceiver)
            {
                popReceiver.Start();
            }
            else
            {
                var pullReceiver = (PullAssignmentReceiver)receiver;
                pullReceiver.Start(_ => _options.ConsumeOrderly
                    ? PollOrderlyAsync(run, pullReceiver)
                    : RunConcurrentQueueAsync(run, pullReceiver));
            }
        }

        return desired.Keys.All(run.Receivers.ContainsKey);
    }

    private bool UsesBrokerAssignment =>
        _options.QueueAssignmentMode == RemotingPushQueueAssignmentMode.Broker &&
        _options.ConsumerMode == ConsumerMode.Clustering &&
        !_options.ConsumeOrderly;

    private bool UsesBrokerQueueLocks =>
        _options.ConsumeOrderly && _options.ConsumerMode == ConsumerMode.Clustering;

    private async Task<bool> RenewQueueLocksAsync(RunState run, CancellationToken cancellationToken)
    {
        var receivers = run.Receivers.Values.OfType<PullAssignmentReceiver>().ToArray();
        if (receivers.Length == 0)
        {
            return true;
        }

        var queueLockResult = await _queueLockManager.LockAsync(
            receivers.Select(static receiver => receiver.Queue),
            cancellationToken).ConfigureAwait(false);
        var timestamp = _timeProvider.GetUtcNow();
        var renewed = true;
        foreach (var receiver in receivers)
        {
            if (!queueLockResult.RefreshedQueues.Contains(receiver.Queue))
            {
                renewed = false;
                continue;
            }

            if (queueLockResult.LockedQueues.Contains(receiver.Queue))
            {
                receiver.MarkBrokerLockAcquired(timestamp);
                continue;
            }

            renewed = false;

            if (receiver.HasValidBrokerLock(_timeProvider, MaximumBrokerLockLiveTime))
            {
                _logger.LogWarning(
                    "Legacy orderly consumer lost the Broker queue lock for {Topic}/{BrokerName}/{QueueId}",
                    receiver.Queue.Topic,
                    receiver.Queue.BrokerName,
                    receiver.Queue.QueueId);
            }

            receiver.MarkBrokerLockLost();
        }

        return renewed;
    }

    private async Task RunConcurrentQueueAsync(RunState run, PullAssignmentReceiver receiver)
    {
        var processQueue = receiver.ProcessQueue!;
        var cancellationToken = receiver.Cancellation;
        var initialOffset = await InitializeOffsetAsync(run, receiver.Queue, cancellationToken).ConfigureAwait(false);
        var persistInitialOffset = _resetBoundaries.ContainsKey(ResetQueueKey.Create(receiver.Queue));
        processQueue.Initialize(initialOffset, persistInitialOffset);

        await Task.WhenAll(
            PollIntoProcessQueueAsync(
                run,
                processQueue,
                initialOffset,
                persistInitialOffset,
                cancellationToken),
            PersistProcessQueueOffsetsAsync(processQueue, cancellationToken)).ConfigureAwait(false);
    }

    private async ValueTask<IReadOnlyList<ConsumeResult>> HandlePopMessagesAsync(
        RunState run,
        IReadOnlyList<RemotingMessageView> messages,
        CancellationToken cancellationToken)
    {
        BatchDeliveryResult result;
        await run.OrderlyConcurrency.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            try
            {
                result = await InvokeMessageHandlerAsync(
                    run,
                    messages,
                    cancellationToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception exception)
            {
                _logger.LogError(exception, "Legacy POP message handler failed; the batch will be retried");
                result = BatchDeliveryResult.Retry(
                    messages.Count,
                    GetDelayLevel(_options.RetryDelay));
            }
        }
        finally
        {
            run.OrderlyConcurrency.Release();
        }

        return Enumerable.Range(0, messages.Count)
            .Select(index => result.GetOutcome(index).Result)
            .ToArray();
    }

    private async Task PollIntoProcessQueueAsync(
        RunState run,
        ProcessQueue processQueue,
        long initialOffset,
        bool persistInitialOffset,
        CancellationToken cancellationToken)
    {
        var queue = processQueue.MessageQueue;
        var offset = initialOffset;
        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                var result = await _consumerEngine.PullAsync(queue, offset, cancellationToken: cancellationToken)
                    .ConfigureAwait(false);
                if (result.Status == RemotingPullStatus.OffsetIllegal)
                {
                    await processQueue.WaitUntilEmptyAsync(cancellationToken).ConfigureAwait(false);
                    processQueue.RequestOffsetReset(result.NextOffset);
                    await processQueue.WaitUntilOffsetPersistedAsync(result.NextOffset, cancellationToken)
                        .ConfigureAwait(false);
                    persistInitialOffset = false;
                    offset = result.NextOffset;
                    continue;
                }

                if (result.Messages.Count > 0)
                {
                    foreach (var messages in result.Messages
                                 .OrderBy(static message => message.QueueOffset)
                                 .Chunk(_options.ConsumeMessageBatchSize))
                    {
                        var slotsReserved = false;
                        try
                        {
                            await processQueue.WaitUntilAdmissionAllowedAsync(cancellationToken)
                                .ConfigureAwait(false);
                            await run.ReserveDeliverySlotsAsync(messages.Length, cancellationToken)
                                .ConfigureAwait(false);
                            slotsReserved = true;
                            var added = processQueue.PutMessages(
                                messages,
                                entry => run.ReserveFifoOrder(processQueue, entry));
                            var unusedSlots = messages.Length - added.Count;
                            if (unusedSlots > 0)
                            {
                                run.ReleaseDeliverySlots(unusedSlots);
                            }

                            slotsReserved = false;
                            run.ReleaseDeliverySlots(processQueue.ReleaseAdmissionBlockedDeliverySlots());
                            run.ConcurrentConsumeDispatcher!.NotifyReady(processQueue);
                        }
                        finally
                        {
                            if (slotsReserved)
                            {
                                run.ReleaseDeliverySlots(messages.Length);
                            }
                        }
                    }
                }

                processQueue.AdvanceNextPullOffset(result.NextOffset);
                if (persistInitialOffset)
                {
                    await processQueue.WaitUntilOffsetPersistedAsync(initialOffset, cancellationToken)
                        .ConfigureAwait(false);
                    persistInitialOffset = false;
                }

                offset = result.NextOffset;
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                return;
            }
            catch (Exception exception)
            {
                _logger.LogWarning(
                    exception,
                    "Legacy pull failed for {Topic}/{BrokerName}/{QueueId}; retrying",
                    queue.Topic,
                    queue.BrokerName,
                    queue.QueueId);
                await Task.Delay(_options.RetryDelay, cancellationToken).ConfigureAwait(false);
            }
        }
    }

    private async Task PersistProcessQueueOffsetsAsync(
        ProcessQueue processQueue,
        CancellationToken cancellationToken)
    {
        var queue = processQueue.MessageQueue;
        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                await processQueue.WaitForCommitAsync(cancellationToken).ConfigureAwait(false);
                while (processQueue.TryGetOffsetCommit(out var commit))
                {
                    try
                    {
                        await PersistOffsetAndClearResetBoundaryAsync(queue, commit.Offset, cancellationToken)
                            .ConfigureAwait(false);
                        processQueue.MarkOffsetPersisted(commit);
                    }
                    catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                    {
                        return;
                    }
                    catch (Exception exception)
                    {
                        _logger.LogWarning(
                            exception,
                            "Unable to persist legacy process-queue offset for {Topic}/{BrokerName}/{QueueId}; retrying",
                            queue.Topic,
                            queue.BrokerName,
                            queue.QueueId);
                        await Task.Delay(_options.RetryDelay, cancellationToken).ConfigureAwait(false);
                    }
                }
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                return;
            }
        }
    }

    private async Task PollOrderlyAsync(RunState run, PullAssignmentReceiver receiver)
    {
        var queue = receiver.Queue;
        var cancellationToken = receiver.Cancellation;
        var offset = await InitializeOffsetAsync(run, queue, cancellationToken).ConfigureAwait(false);
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

                var result = await _consumerEngine.PullAsync(queue, offset, cancellationToken: cancellationToken)
                    .ConfigureAwait(false);
                if (!HasValidBrokerQueueLock(receiver))
                {
                    continue;
                }

                if (result.Status == RemotingPullStatus.OffsetIllegal)
                {
                    await PersistOffsetAndClearResetBoundaryAsync(queue, result.NextOffset, cancellationToken)
                        .ConfigureAwait(false);
                    offset = result.NextOffset;
                    continue;
                }

                if (result.Messages.Count == 0)
                {
                    var hasResetBoundary = _resetBoundaries.ContainsKey(ResetQueueKey.Create(queue));
                    if (hasResetBoundary ||
                        (_options.ConsumerMode == ConsumerMode.Broadcasting && result.NextOffset != offset))
                    {
                        await PersistOffsetAndClearResetBoundaryAsync(
                            queue,
                            result.NextOffset,
                            cancellationToken).ConfigureAwait(false);
                    }

                    offset = result.NextOffset;
                    continue;
                }

                var completed = true;
                foreach (var message in result.Messages.OrderBy(static message => message.QueueOffset))
                {
                    var outcome = await HandleOrderlyMessageAsync(
                        run,
                        receiver,
                        message,
                        cancellationToken).ConfigureAwait(false);
                    if (run.ShouldAbandonHandlerResults)
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
                        await _consumerEngine.SendBackAsync(
                            queue,
                            message,
                            delayLevel: -1,
                            maxReconsumeTimes: _options.MaxDeliveryAttempts,
                            cancellationToken: cancellationToken).ConfigureAwait(false);
                    }

                    if (!HasValidBrokerQueueLock(receiver))
                    {
                        completed = false;
                        break;
                    }

                    var nextOffset = checked(message.QueueOffset + 1);
                    await PersistOffsetAndClearResetBoundaryAsync(queue, nextOffset, cancellationToken)
                        .ConfigureAwait(false);
                    offset = nextOffset;
                }

                if (!completed)
                {
                    continue;
                }

                if (result.NextOffset > offset)
                {
                    await PersistOffsetAndClearResetBoundaryAsync(queue, result.NextOffset, cancellationToken)
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

    private bool HasValidBrokerQueueLock(PullAssignmentReceiver receiver) =>
        !UsesBrokerQueueLocks || receiver.HasValidBrokerLock(_timeProvider, MaximumBrokerLockLiveTime);

    private async ValueTask<OrderlyDeliveryOutcome> HandleOrderlyMessageAsync(
        RunState run,
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
            await run.OrderlyConcurrency.WaitAsync(cancellationToken).ConfigureAwait(false);
            try
            {
                var batchResult = await InvokeMessageHandlerAsync(run, new[] { message }, cancellationToken)
                    .ConfigureAwait(false);
                if (run.ShouldAbandonHandlerResults)
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
            finally
            {
                run.OrderlyConcurrency.Release();
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
                    throw new InvalidOperationException(
                        $"Unsupported ordered consume result '{result}'.");
            }
        }
    }

    private async Task<long> InitializeOffsetAsync(
        RunState run,
        RemotingConsumerQueue queue,
        CancellationToken cancellationToken)
    {
        if (_resetBoundaries.TryGetValue(ResetQueueKey.Create(queue), out var resetOffset))
        {
            return resetOffset;
        }

        while (true)
        {
            try
            {
                if (_options.ConsumerMode == ConsumerMode.Broadcasting)
                {
                    var localOffset = await _broadcastOffsetStore!.ReadAsync(queue, cancellationToken)
                        .ConfigureAwait(false);
                    if (localOffset.HasValue)
                    {
                        return localOffset.Value;
                    }

                    var initialBroadcastOffset = await QueryInitialOffsetAsync(queue, cancellationToken)
                        .ConfigureAwait(false);
                    await _broadcastOffsetStore.UpdateAsync(queue, initialBroadcastOffset, cancellationToken)
                        .ConfigureAwait(false);
                    return initialBroadcastOffset;
                }

                var committed = await _consumerEngine.GetOffsetAsync(queue, cancellationToken).ConfigureAwait(false);
                if (committed >= 0)
                {
                    return committed;
                }

                long initial;
                if (queue.Topic == GetRetryTopic())
                {
                    initial = run.RetryBoundaries.TryGetValue(QueueKey.Create(queue), out var retryBoundary)
                        ? retryBoundary
                        : 0;
                }
                else
                {
                    initial = await QueryInitialOffsetAsync(queue, cancellationToken).ConfigureAwait(false);
                }

                await _consumerEngine.UpdateOffsetAsync(queue, initial, cancellationToken).ConfigureAwait(false);
                return initial;
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception exception)
            {
                _logger.LogDebug(
                    exception,
                    "Legacy queue {Topic}/{BrokerName}/{QueueId} is not available yet",
                    queue.Topic,
                    queue.BrokerName,
                    queue.QueueId);
                await Task.Delay(TimeSpan.FromSeconds(1), cancellationToken).ConfigureAwait(false);
            }
        }
    }

    private Task<long> QueryInitialOffsetAsync(
        RemotingConsumerQueue queue,
        CancellationToken cancellationToken)
    {
        return _consumerEngine.QueryOffsetAsync(
            queue,
            _options.InitialPosition,
            _options.InitialPosition == ConsumeFromPosition.Timestamp ? _options.ConsumeTimestamp : null,
            cancellationToken);
    }

    private Task PersistOffsetAsync(
        RemotingConsumerQueue queue,
        long offset,
        CancellationToken cancellationToken) =>
        _options.ConsumerMode == ConsumerMode.Broadcasting
            ? _broadcastOffsetStore!.UpdateAsync(queue, offset, cancellationToken)
            : _consumerEngine.UpdateOffsetAsync(queue, offset, cancellationToken);

    private async Task PersistOffsetAndClearResetBoundaryAsync(
        RemotingConsumerQueue queue,
        long offset,
        CancellationToken cancellationToken)
    {
        var key = ResetQueueKey.Create(queue);
        var hasResetBoundary = _resetBoundaries.TryGetValue(key, out var resetBoundary);
        await PersistOffsetAsync(queue, offset, cancellationToken).ConfigureAwait(false);
        if (hasResetBoundary)
        {
            TryRemoveResetBoundary(key, resetBoundary);
        }
    }

    private bool TryRemoveResetBoundary(ResetQueueKey key, long expectedOffset) =>
        ((ICollection<KeyValuePair<ResetQueueKey, long>>)_resetBoundaries).Remove(
            new KeyValuePair<ResetQueueKey, long>(key, expectedOffset));

    private void SetResetBoundaries(string logicalTopic, IEnumerable<LegacyResetOffset> offsets)
    {
        foreach (var offset in offsets)
        {
            _resetBoundaries[new ResetQueueKey(logicalTopic, offset.BrokerName, offset.QueueId)] = offset.Offset;
        }
    }

    private void RemoveResetBoundaries(string logicalTopic)
    {
        foreach (var entry in _resetBoundaries)
        {
            if (string.Equals(entry.Key.Topic, logicalTopic, StringComparison.Ordinal))
            {
                ((ICollection<KeyValuePair<ResetQueueKey, long>>)_resetBoundaries).Remove(entry);
            }
        }
    }

    private async Task<RetryOffsetInitialization?> PrepareRetryOffsetAsync(
        RunState run,
        RemotingConsumerQueue sourceQueue,
        CancellationToken cancellationToken)
    {
        var retryQueue = CreateRetryQueue(sourceQueue);
        var retryQueueKey = QueueKey.Create(retryQueue);
        if (run.Receivers.ContainsKey(retryQueueKey))
        {
            return null;
        }

        if (run.RetryBoundaries.TryGetValue(retryQueueKey, out var pendingBoundary))
        {
            return new RetryOffsetInitialization(retryQueue, pendingBoundary);
        }

        try
        {
            var committed = await _consumerEngine.GetOffsetAsync(retryQueue, cancellationToken).ConfigureAwait(false);
            if (committed >= 0)
            {
                return null;
            }

            var end = await _consumerEngine.QueryOffsetAsync(
                retryQueue,
                ConsumeFromPosition.End,
                cancellationToken: cancellationToken).ConfigureAwait(false);
            var boundary = run.RetryBoundaries.GetOrAdd(retryQueueKey, end);
            return new RetryOffsetInitialization(retryQueue, boundary);
        }
        catch (RemotingCommandException exception) when (exception.ResponseCode == ResponseCodes.ResTopicNotExist)
        {
            var boundary = run.RetryBoundaries.GetOrAdd(retryQueueKey, 0);
            return new RetryOffsetInitialization(retryQueue, boundary);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
    }

    private async Task PersistRetryOffsetAsync(
        RetryOffsetInitialization initialization,
        CancellationToken cancellationToken)
    {
        var committed = await _consumerEngine.GetOffsetAsync(initialization.Queue, cancellationToken)
            .ConfigureAwait(false);
        if (committed < 0)
        {
            await _consumerEngine.UpdateOffsetAsync(
                initialization.Queue,
                initialization.Offset,
                cancellationToken).ConfigureAwait(false);
        }
    }

    private async ValueTask ProcessConsumeRequestAsync(
        RunState run,
        ConsumeRequest request,
        CancellationToken cancellationToken)
    {
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

            var batchResult = default(BatchDeliveryResult);
            if (!request.IsSettlementRetry)
            {
                try
                {
                    batchResult = UsesConcurrentTimeoutRecovery(request)
                        ? await InvokeConcurrentConsumeRequestAsync(run, request, cancellationToken).ConfigureAwait(false)
                        : await HandleConsumeRequestAsync(run, request, cancellationToken).ConfigureAwait(false);
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
                    batchResult = BatchDeliveryResult.Retry(
                        request.Messages.Count,
                        GetDelayLevel(_options.RetryDelay));
                }

                if (run.ShouldAbandonHandlerResults)
                {
                    cancellationToken.ThrowIfCancellationRequested();
                }
            }

            for (var index = 0; index < request.Entries.Count; index++)
            {
                var entry = request.Entries[index];
                var outcome = request.IsSettlementRetry
                    ? new MessageConsumeResult(
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
                run.ReleaseDeliverySlots(request.ProcessQueue.BeginSettlement(
                    entry,
                    deadLetter ? ConsumeResult.DeadLetter : ConsumeResult.Retry,
                    deadLetter ? -1 : outcome.DelayLevelWhenNextConsume));
                RetryOffsetInitialization? retryOffset;
                try
                {
                    retryOffset = deadLetter
                        ? null
                        : await PrepareRetryOffsetAsync(run, request.ProcessQueue.MessageQueue, cancellationToken)
                            .ConfigureAwait(false);
                    await _consumerEngine.SendBackAsync(
                        request.ProcessQueue.MessageQueue,
                        message,
                        deadLetter ? -1 : outcome.DelayLevelWhenNextConsume,
                        _options.MaxDeliveryAttempts,
                        cancellationToken).ConfigureAwait(false);
                    if (!deadLetter)
                    {
                        run.RequireRetryTopic();
                    }

                    completed[index] = true;
                }
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                {
                    throw;
                }
                catch (Exception exception)
                {
                    retryLocally = true;
                    request.ProcessQueue.DelaySettlementRetry(entry);
                    _logger.LogWarning(
                        exception,
                        "Unable to settle message {MessageId} from {Topic}/{BrokerName}/{QueueId}; retrying locally",
                        entry.Message.MessageId,
                        request.ProcessQueue.MessageQueue.Topic,
                        request.ProcessQueue.MessageQueue.BrokerName,
                        request.ProcessQueue.MessageQueue.QueueId);
                    continue;
                }

                if (!deadLetter)
                {
                    if (retryOffset is { } initialization)
                    {
                        try
                        {
                            await PersistRetryOffsetAsync(initialization, CancellationToken.None).ConfigureAwait(false);
                        }
                        catch (Exception exception)
                        {
                            _logger.LogWarning(
                                exception,
                                "Unable to initialize retry-queue offset for {Topic}/{BrokerName}/{QueueId}",
                                initialization.Queue.Topic,
                                initialization.Queue.BrokerName,
                                initialization.Queue.QueueId);
                        }
                    }

                    run.RebalanceRegistration?.Wakeup();
                }
            }

            for (var index = 0; index < request.Entries.Count; index++)
            {
                if (completed[index])
                {
                    run.CompleteFifoOrder(request.Entries[index]);
                    fifoReleased[index] = true;
                }
            }

            var hasPending = request.ProcessQueue.CompleteConsumeRequest(request, completed);
            if (retryLocally)
            {
                _ = NotifyProcessQueueAfterDelayAsync(run, request.ProcessQueue, request.Entries);
            }
            else if (hasPending)
            {
                run.ConcurrentConsumeDispatcher!.NotifyReady(request.ProcessQueue);
            }
        }
        finally
        {
            for (var index = 0; index < request.Entries.Count; index++)
            {
                if (!fifoReleased[index] && (completed[index] || request.ProcessQueue.IsDropped))
                {
                    run.CompleteFifoOrder(request.Entries[index]);
                }
            }
        }
    }

    private async Task NotifyProcessQueueAfterDelayAsync(
        RunState run,
        ProcessQueue processQueue,
        IReadOnlyList<ProcessQueue.MessageEntry> entries)
    {
        try
        {
            await Task.Delay(_options.RetryDelay, processQueue.QueueCancellation).ConfigureAwait(false);
            if (processQueue.ActivateSettlementRetries(entries))
            {
                run.ConcurrentConsumeDispatcher!.NotifyReady(processQueue);
            }
        }
        catch (OperationCanceledException) when (processQueue.QueueCancellation.IsCancellationRequested)
        {
        }
    }

    private async ValueTask<BatchDeliveryResult> HandleConsumeRequestAsync(
        RunState run,
        ConsumeRequest request,
        CancellationToken cancellationToken)
    {
        if (request.Entries.Count == 1)
        {
            var outcome = await HandleMessageAsync(run, request.Entries[0], cancellationToken).ConfigureAwait(false);
            return BatchDeliveryResult.All(outcome.Result, 1, outcome.DelayLevelWhenNextConsume);
        }

        return await InvokeMessageHandlerAsync(
            run,
            request.Messages,
            cancellationToken).ConfigureAwait(false);
    }

    private async ValueTask<MessageConsumeResult> HandleMessageAsync(
        RunState run,
        ProcessQueue.MessageEntry entry,
        CancellationToken cancellationToken)
    {
        var attempt = entry.Message.DeliveryAttempt;
        while (true)
        {
            MessageConsumeResult outcome;
            try
            {
                var batchResult = await InvokeMessageHandlerAsync(
                    run,
                    new[] { entry.Message },
                    cancellationToken).ConfigureAwait(false);
                outcome = entry.FifoOrder is null
                    ? batchResult.GetOutcome(0)
                    : new MessageConsumeResult(batchResult.Result, batchResult.DelayLevelWhenNextConsume);
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
                outcome = MessageConsumeResult.Retry(GetDelayLevel(_options.RetryDelay));
            }

            if (_options.ConsumerMode == ConsumerMode.Broadcasting ||
                entry.FifoOrder is null || outcome.Result != ConsumeResult.Retry)
            {
                return outcome;
            }

            if (attempt >= _options.MaxDeliveryAttempts)
            {
                return new MessageConsumeResult(ConsumeResult.DeadLetter, outcome.DelayLevelWhenNextConsume);
            }

            attempt++;
            await Task.Delay(_options.RetryDelay, cancellationToken).ConfigureAwait(false);
        }
    }

    private ValueTask<BatchDeliveryResult> InvokeMessageHandlerAsync(
        RunState run,
        IReadOnlyList<RemotingMessageView> messages,
        CancellationToken cancellationToken)
    {
        var context = CreateConsumeContext();
        return InvokeTrackedMessageHandlerAsync(
            messages,
            context,
            cancellationToken,
            run,
            StartMessageHandlerTelemetry(messages));
    }

    private MessageHandlerTelemetry StartMessageHandlerTelemetry(IReadOnlyList<RemotingMessageView> messages) =>
        new(_telemetry.StartProcessBatch(
            messages[0].Topic,
            _options.GroupName,
            messages));

    private async ValueTask<BatchDeliveryResult> InvokeTrackedMessageHandlerAsync(
        IReadOnlyList<RemotingMessageView> messages,
        RemotingPushConsumeContext context,
        CancellationToken cancellationToken,
        RunState run,
        MessageHandlerTelemetry telemetry)
    {
        try
        {
            var result = await _messageHandler(messages, context, cancellationToken).ConfigureAwait(false);
            if (run.ShouldAbandonHandlerResults)
            {
                cancellationToken.ThrowIfCancellationRequested();
            }

            var batchResult = BatchDeliveryResult.Create(
                result,
                context.AckIndex,
                context.DelayLevelWhenNextConsume,
                messages.Count);
            telemetry.Complete(batchResult.IsFullySuccessful, batchResult.TelemetryOutcome);
            return batchResult;
        }
        catch (Exception exception)
        {
            telemetry.Complete(exception);
            throw;
        }
    }

    private RemotingPushConsumeContext CreateConsumeContext() => new()
    {
        DelayLevelWhenNextConsume = GetDelayLevel(_options.RetryDelay)
    };

    private bool UsesConcurrentTimeoutRecovery(ConsumeRequest request) =>
        _options.ConsumerMode == ConsumerMode.Clustering &&
        request.Entries.All(static entry => entry.FifoOrder is null);

    private async ValueTask<BatchDeliveryResult> InvokeConcurrentConsumeRequestAsync(
        RunState run,
        ConsumeRequest request,
        CancellationToken cancellationToken)
    {
        CancellationTokenSource? handlerCancellation = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        using var timeoutCancellation = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        try
        {
            var activeHandlerCancellation = handlerCancellation;
            var messages = request.Messages;
            var context = CreateConsumeContext();
            var telemetry = StartMessageHandlerTelemetry(messages);
            var handler = InvokeTrackedMessageHandlerAsync(
                messages,
                context,
                activeHandlerCancellation.Token,
                run,
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
                _ = ObserveTimedOutConsumeRequestAsync(handlerTask, activeHandlerCancellation, request.Messages.Count);
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

            _ = ObserveTimedOutConsumeRequestAsync(handlerTask, activeHandlerCancellation, request.Messages.Count);
            handlerCancellation = null;
            _logger.LogWarning(
                "Legacy message handler exceeded the configured consume timeout of {ConsumeTimeout} for a batch containing {MessageCount} messages; cancellation was requested and the batch will be retried",
                _options.ConsumeTimeout,
                request.Messages.Count);
            return BatchDeliveryResult.Retry(request.Messages.Count, GetDelayLevel(_options.RetryDelay));
        }
        finally
        {
            handlerCancellation?.Dispose();
        }
    }

    private async Task ObserveTimedOutConsumeRequestAsync(
        Task<BatchDeliveryResult> handlerTask,
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
                "Legacy message handler for a timed-out batch containing {MessageCount} messages completed with an exception after timeout",
                messageCount);
        }
        finally
        {
            handlerCancellation.Dispose();
        }
    }

    private async Task ShutdownRunAsync(RunState run, CancellationToken cancellationToken)
    {
        using var abandonHandlerResultsRegistration = cancellationToken.Register(run.AbandonHandlerResults);
        Task? detachedTasks = null;
        IRemotingPushReceiver[] receivers = [];
        run.Stopping.Cancel();
        if (run.RebalanceRegistration is { } rebalanceRegistration)
        {
            run.RebalanceRegistration = null;
            await rebalanceRegistration.DisposeAsync().ConfigureAwait(false);
        }

        run.ConcurrentConsumeDispatcher?.Complete();

        receivers = run.Receivers.Values.ToArray();
        foreach (var receiver in receivers)
        {
            DropReceiver(run, receiver);
        }

        var activeTasks = receivers
            .Select(static receiver => receiver.Task)
            .Concat(run.ConcurrentConsumeDispatcher?.ConsumeLoopTasks ?? [])
            .ToArray();
        try
        {
            await AwaitTasksAsync(activeTasks, cancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            run.AbandonHandlerResults();
            detachedTasks = Task.WhenAll(activeTasks);
        }

        if (UsesBrokerQueueLocks && receivers.Length > 0)
        {
            await _queueLockManager.UnlockAsync(
                receivers.Select(static receiver => receiver.Queue),
                oneway: false,
                CancellationToken.None).ConfigureAwait(false);
        }

        await _groupSession.UnregisterAsync(run.KnownBrokers.Values).ConfigureAwait(false);
        try
        {
            if (_broadcastOffsetStore is not null)
            {
                await _broadcastOffsetStore.FlushAsync(CancellationToken.None).ConfigureAwait(false);
            }
        }
        finally
        {
            await _consumerEngine.StopAsync(CancellationToken.None).ConfigureAwait(false);

            if (detachedTasks is null)
            {
                DisposeRunResources(run, receivers);
            }
            else
            {
                ObserveDetachedConsumeLoops(run, receivers, detachedTasks);
            }
        }

        if (detachedTasks is not null)
        {
            cancellationToken.ThrowIfCancellationRequested();
        }
    }

    private static IReadOnlyList<RemotingConsumerQueue> BuildQueues(string topic, TopicRouteData route)
    {
        var queues = new List<RemotingConsumerQueue>();
        foreach (var queueData in route.QueueDatas
                     .Where(static queue => (queue.Perm & ReadPermission) == ReadPermission)
                     .OrderBy(static queue => queue.BrokerName, StringComparer.Ordinal))
        {
            var broker = route.BrokerDatas.FirstOrDefault(value => value.BrokerName == queueData.BrokerName);
            if (broker is null || broker.BrokerAddrs.Count == 0)
            {
                continue;
            }

            var address = broker.BrokerAddrs.TryGetValue(0, out var master)
                ? master
                : broker.BrokerAddrs.OrderBy(static entry => entry.Key).First().Value;
            for (var queueId = 0; queueId < queueData.ReadQueueNums; queueId++)
            {
                queues.Add(new RemotingConsumerQueue(topic, queueId, queueData.BrokerName, address));
            }
        }

        return queues;
    }

    private string GetRetryTopic() => $"%RETRY%{_options.GroupName}";

    private string GetWireTopic(string topic) => LegacyNamespace.Wrap(_clientOptions.Namespace, topic);

    private static string GetPrimaryBrokerAddress(BrokerData broker) =>
        broker.BrokerAddrs.TryGetValue(0, out var master)
            ? master
            : broker.BrokerAddrs.OrderBy(static entry => entry.Key).First().Value;

    private static void EnsureBrokerSuccess(RemotingCommand response, string operation)
    {
        if (response.Code != ResponseCodes.ResSuccess)
        {
            throw new RemotingCommandException(
                response.Code,
                response.Remark ?? $"Broker rejected the request to {operation}.");
        }
    }

    private RemotingConsumerQueue CreateRetryQueue(RemotingConsumerQueue sourceQueue) => new(
        GetRetryTopic(),
        0,
        sourceQueue.BrokerName,
        sourceQueue.BrokerAddress);

    private static void DropReceiver(RunState run, IRemotingPushReceiver receiver)
    {
        var reservedDeliverySlots = receiver is PullAssignmentReceiver pullReceiver
            ? pullReceiver.ProcessQueue?.MarkDropped(run.DropFifoOrder) ?? 0
            : 0;
        if (reservedDeliverySlots > 0)
        {
            run.ReleaseDeliverySlots(reservedDeliverySlots);
        }

        receiver.Stop();
    }

    private static async Task AwaitReceiversAsync(IEnumerable<IRemotingPushReceiver> receivers) =>
        await AwaitTasksAsync(receivers.Select(static receiver => receiver.Task)).ConfigureAwait(false);

    private static async Task AwaitTasksAsync(
        IEnumerable<Task> tasks,
        CancellationToken cancellationToken = default)
    {
        foreach (var task in tasks)
        {
            await AwaitTaskAsync(task, cancellationToken).ConfigureAwait(false);
        }
    }

    private static async Task AwaitTaskAsync(Task task, CancellationToken cancellationToken = default)
    {
        try
        {
            if (cancellationToken.CanBeCanceled)
            {
                await task.WaitAsync(cancellationToken).ConfigureAwait(false);
            }
            else
            {
                await task.ConfigureAwait(false);
            }
        }
        catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
        {
        }
    }

    private void ObserveDetachedConsumeLoops(
        RunState run,
        IRemotingPushReceiver[] receivers,
        Task consumeLoops) =>
        _ = ObserveDetachedConsumeLoopsAsync(run, receivers, consumeLoops);

    private async Task ObserveDetachedConsumeLoopsAsync(
        RunState run,
        IRemotingPushReceiver[] receivers,
        Task consumeLoops)
    {
        try
        {
            await consumeLoops.ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
        }
        catch (Exception exception)
        {
            _logger.LogDebug(
                exception,
                "A legacy message processing task completed with an exception after consumer shutdown was canceled");
        }
        finally
        {
            DisposeRunResources(run, receivers);
        }
    }

    private static void DisposeRunResources(RunState run, IEnumerable<IRemotingPushReceiver> receivers)
    {
        foreach (var receiver in receivers)
        {
            receiver.Dispose();
        }

        run.Dispose();
    }

    private static int GetDelayLevel(TimeSpan delay)
    {
        ReadOnlySpan<long> delayTicks =
        [
            TimeSpan.TicksPerSecond,
            TimeSpan.TicksPerSecond * 5,
            TimeSpan.TicksPerSecond * 10,
            TimeSpan.TicksPerSecond * 30,
            TimeSpan.TicksPerMinute,
            TimeSpan.TicksPerMinute * 2,
            TimeSpan.TicksPerMinute * 3,
            TimeSpan.TicksPerMinute * 4,
            TimeSpan.TicksPerMinute * 5,
            TimeSpan.TicksPerMinute * 6,
            TimeSpan.TicksPerMinute * 7,
            TimeSpan.TicksPerMinute * 8,
            TimeSpan.TicksPerMinute * 9,
            TimeSpan.TicksPerMinute * 10,
            TimeSpan.TicksPerMinute * 20,
            TimeSpan.TicksPerMinute * 30,
            TimeSpan.TicksPerHour,
            TimeSpan.TicksPerHour * 2
        ];
        for (var index = 0; index < delayTicks.Length; index++)
        {
            if (delay.Ticks <= delayTicks[index])
            {
                return index + 1;
            }
        }

        return delayTicks.Length;
    }

    private sealed class RunState : IDisposable
    {
        public RunState(
            long subscriptionVersion,
            int maxConcurrency,
            int maxCachedMessages)
        {
            _subscriptionVersion = subscriptionVersion;
            OrderlyConcurrency = new SemaphoreSlim(maxConcurrency, maxConcurrency);
            DeliverySlots = new SemaphoreSlim(maxCachedMessages, maxCachedMessages);
        }

        public long SubscriptionVersion => Interlocked.Read(ref _subscriptionVersion);
        public bool RetryTopicRequired => Volatile.Read(ref _retryTopicRequired) != 0;
        public CancellationTokenSource Stopping { get; } = new();
        public SemaphoreSlim CoordinationGate { get; } = new(1, 1);
        public SemaphoreSlim OrderlyConcurrency { get; }
        public SemaphoreSlim DeliverySlots { get; }
        public SemaphoreSlim DeliveryReservationGate { get; } = new(1, 1);
        public ConcurrentDictionary<QueueKey, IRemotingPushReceiver> Receivers { get; } = new();
        public ConcurrentDictionary<QueueKey, long> RetryBoundaries { get; } = new();
        public ConcurrentDictionary<string, RemotingBrokerEndpoint> KnownBrokers { get; } = new(StringComparer.Ordinal);
        public ConcurrentConsumeDispatcher? ConcurrentConsumeDispatcher { get; set; }
        public IRemotingRebalanceRegistration? RebalanceRegistration { get; set; }

        private object FifoGate { get; } = new();
        private Dictionary<string, Task> FifoTails { get; } = new(StringComparer.Ordinal);
        private int _abandonHandlerResults;
        private int _retryTopicRequired;
        private long _subscriptionVersion;

        public void BumpSubscriptionVersion() => Interlocked.Increment(ref _subscriptionVersion);

        public void RequireRetryTopic() => Interlocked.Exchange(ref _retryTopicRequired, 1);

        public bool ShouldAbandonHandlerResults => Volatile.Read(ref _abandonHandlerResults) != 0;

        public void AbandonHandlerResults() => Interlocked.Exchange(ref _abandonHandlerResults, 1);

        public async ValueTask ReserveDeliverySlotsAsync(int count, CancellationToken cancellationToken)
        {
            await DeliveryReservationGate.WaitAsync(cancellationToken).ConfigureAwait(false);
            var reserved = 0;
            try
            {
                while (reserved < count)
                {
                    await DeliverySlots.WaitAsync(cancellationToken).ConfigureAwait(false);
                    reserved++;
                }
            }
            catch
            {
                if (reserved > 0)
                {
                    DeliverySlots.Release(reserved);
                }

                throw;
            }
            finally
            {
                DeliveryReservationGate.Release();
            }
        }

        public void ReleaseDeliverySlots(int count)
        {
            if (count > 0)
            {
                DeliverySlots.Release(count);
            }
        }

        public void ReserveFifoOrder(ProcessQueue processQueue, ProcessQueue.MessageEntry entry)
        {
            if (string.IsNullOrEmpty(entry.Message.MessageGroup))
            {
                return;
            }

            var key = $"{entry.Message.Topic}\0{entry.Message.MessageGroup}";
            lock (FifoGate)
            {
                var predecessor = FifoTails.TryGetValue(key, out var tail) ? tail : Task.CompletedTask;
                entry.FifoOrder = new ProcessQueue.FifoOrder(key, predecessor);
                FifoTails[key] = entry.FifoOrder.Completion.Task;
            }

            if (!entry.FifoOrder.Predecessor.IsCompleted)
            {
                _ = NotifyFifoSuccessorAsync(processQueue, entry.FifoOrder.Predecessor);
            }
        }

        public void DropFifoOrder(ProcessQueue.MessageEntry entry)
        {
            if (entry.FifoOrder is not { } order)
            {
                return;
            }

            if (order.Predecessor.IsCompleted)
            {
                CompleteFifoOrder(entry);
            }
            else
            {
                _ = CompleteDroppedFifoOrderAsync(entry, order.Predecessor);
            }
        }

        public void CompleteFifoOrder(ProcessQueue.MessageEntry entry)
        {
            if (entry.FifoOrder is not { } order)
            {
                return;
            }

            order.Completion.TrySetResult();
            lock (FifoGate)
            {
                if (FifoTails.TryGetValue(order.Key, out var tail) &&
                    ReferenceEquals(tail, order.Completion.Task))
                {
                    FifoTails.Remove(order.Key);
                }
            }
        }

        private async Task NotifyFifoSuccessorAsync(ProcessQueue processQueue, Task predecessor)
        {
            await predecessor.ConfigureAwait(false);
            processQueue.NotifyFifoPredecessorCompleted();
            ConcurrentConsumeDispatcher?.NotifyReady(processQueue);
        }

        private async Task CompleteDroppedFifoOrderAsync(ProcessQueue.MessageEntry entry, Task predecessor)
        {
            await predecessor.ConfigureAwait(false);
            CompleteFifoOrder(entry);
        }

        public void Dispose()
        {
            lock (FifoGate)
            {
                FifoTails.Clear();
            }

            CoordinationGate.Dispose();
            OrderlyConcurrency.Dispose();
            DeliveryReservationGate.Dispose();
            DeliverySlots.Dispose();
            Stopping.Dispose();
        }
    }

    private sealed class MessageHandlerTelemetry(IRemotingRocketMQTelemetryOperation operation)
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

    private readonly record struct MessageConsumeResult(ConsumeResult Result, int DelayLevelWhenNextConsume)
    {
        public static MessageConsumeResult Retry(int delayLevelWhenNextConsume) =>
            new(ConsumeResult.Retry, delayLevelWhenNextConsume);
    }

    private readonly record struct BatchDeliveryResult(
        ConsumeResult Result,
        int AcknowledgedCount,
        int DelayLevelWhenNextConsume,
        int MessageCount)
    {
        public bool IsFullySuccessful => Result == ConsumeResult.Success && AcknowledgedCount == MessageCount;

        public string TelemetryOutcome =>
            Result == ConsumeResult.Success && !IsFullySuccessful ? "partial_success" : Result.ToString();

        public static BatchDeliveryResult Create(
            ConsumeResult result,
            int acknowledgementIndex,
            int delayLevelWhenNextConsume,
            int messageCount)
        {
            if (messageCount <= 0)
            {
                throw new ArgumentOutOfRangeException(nameof(messageCount), messageCount, "Message count must be positive.");
            }

            var acknowledgedCount = result == ConsumeResult.Success
                ? Math.Min(acknowledgementIndex, messageCount - 1) + 1
                : 0;
            return new BatchDeliveryResult(result, acknowledgedCount, delayLevelWhenNextConsume, messageCount);
        }

        public static BatchDeliveryResult All(
            ConsumeResult result,
            int messageCount,
            int delayLevelWhenNextConsume) =>
            new(result, result == ConsumeResult.Success ? messageCount : 0, delayLevelWhenNextConsume, messageCount);

        public static BatchDeliveryResult Retry(int messageCount, int delayLevelWhenNextConsume) =>
            new(ConsumeResult.Retry, 0, delayLevelWhenNextConsume, messageCount);

        public MessageConsumeResult GetOutcome(int index)
        {
            if ((uint)index >= (uint)MessageCount)
            {
                throw new ArgumentOutOfRangeException(nameof(index), index, "Message index is outside the batch.");
            }

            return new MessageConsumeResult(
                Result == ConsumeResult.Success && index >= AcknowledgedCount ? ConsumeResult.Retry : Result,
                DelayLevelWhenNextConsume);
        }
    }

    private readonly record struct QueueKey(
        string Topic,
        string BrokerName,
        int QueueId,
        string BrokerAddress,
        RemotingPushReceiveMode Mode)
    {
        public static QueueKey Create(ReceiveTarget target) =>
            new(
                target.Assignment.Topic,
                target.Assignment.BrokerName,
                target.Assignment.QueueId,
                target.Queue.BrokerAddress,
                target.Assignment.Mode);

        public static QueueKey Create(RemotingConsumerQueue queue) =>
            new(
                queue.Topic,
                queue.BrokerName,
                queue.QueueId,
                queue.BrokerAddress,
                RemotingPushReceiveMode.Pull);
    }

    private readonly record struct ResetQueueKey(string Topic, string BrokerName, int QueueId)
    {
        public static ResetQueueKey Create(RemotingConsumerQueue queue) =>
            new(queue.Topic, queue.BrokerName, queue.QueueId);
    }

    private sealed record TopicSnapshot(
        string Topic,
        IReadOnlyList<RemotingConsumerQueue> Queues,
        TopicRouteData Route);
    private sealed record ReceiveTarget(RemotingPushAssignment Assignment, RemotingConsumerQueue Queue);
    private sealed record RetryOffsetInitialization(RemotingConsumerQueue Queue, long Offset);
    private sealed record ResetReceiver(IRemotingPushReceiver Receiver, long Offset);

    private enum OrderlyDeliveryOutcome
    {
        Success,
        Retry,
        DeadLetter,
        LeaseLost
    }
}
