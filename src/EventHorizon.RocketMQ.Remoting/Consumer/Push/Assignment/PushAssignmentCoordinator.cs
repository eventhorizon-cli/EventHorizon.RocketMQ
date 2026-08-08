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
using EventHorizon.RocketMQ.Remoting.Consumer.Push.Pull.Orderly;
using EventHorizon.RocketMQ.Remoting.Consumer.Push.Pull.Receive;
using EventHorizon.RocketMQ.Remoting.Consumer.Route;
using EventHorizon.RocketMQ.Remoting.Exceptions;
using EventHorizon.RocketMQ.Remoting.Protocol;
using Microsoft.Extensions.Logging;

namespace EventHorizon.RocketMQ.Remoting.Consumer.Push.Assignment;

internal sealed class PushAssignmentCoordinator
{
    private static readonly TimeSpan MaximumBrokerLockLiveTime = TimeSpan.FromSeconds(30);

    private readonly RemotingPushConsumerOptions _options;
    private readonly RemotingClientOptions _clientOptions;
    private readonly RemotingConsumerRouteResolver _routes;
    private readonly IRemotingClient _remotingClient;
    private readonly OrderlyPullQueueLockClient _queueLockClient;
    private readonly PushReceiverFactory _receiverFactory;
    private readonly PushSubscriptionState _subscriptions;
    private readonly TimeProvider _timeProvider;
    private readonly ILogger _logger;

    public PushAssignmentCoordinator(
        RemotingPushConsumerOptions options,
        RemotingClientOptions clientOptions,
        RemotingConsumerRouteResolver routes,
        IRemotingClient remotingClient,
        OrderlyPullQueueLockClient queueLockClient,
        PushReceiverFactory receiverFactory,
        PushSubscriptionState subscriptions,
        TimeProvider timeProvider,
        ILogger logger)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(clientOptions);
        ArgumentNullException.ThrowIfNull(routes);
        ArgumentNullException.ThrowIfNull(remotingClient);
        ArgumentNullException.ThrowIfNull(queueLockClient);
        ArgumentNullException.ThrowIfNull(receiverFactory);
        ArgumentNullException.ThrowIfNull(subscriptions);
        ArgumentNullException.ThrowIfNull(timeProvider);
        ArgumentNullException.ThrowIfNull(logger);

        _options = options;
        _clientOptions = clientOptions;
        _routes = routes;
        _remotingClient = remotingClient;
        _queueLockClient = queueLockClient;
        _receiverFactory = receiverFactory;
        _subscriptions = subscriptions;
        _timeProvider = timeProvider;
        _logger = logger;
    }

    public bool UsesBrokerQueueLocks =>
        _options.ConsumeOrderly && _options.ConsumerMode == ConsumerMode.Clustering;

    public async Task StartAsync(
        RemotingPushConsumerRun run,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(run);
        run.GroupSession.RegisterRebalance(token => RebalanceAsync(run, token));
        if (!await CoordinateOnceAsync(run, cancellationToken).ConfigureAwait(false))
        {
            run.GroupSession.Wakeup();
        }
    }

    public Task SubscribeAsync(
        RemotingPushConsumerRun run,
        string topic,
        FilterExpression filter,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(filter);
        return ChangeSubscriptionAsync(run, topic, filter, cancellationToken);
    }

    public Task UnsubscribeAsync(
        RemotingPushConsumerRun run,
        string topic,
        CancellationToken cancellationToken) =>
        ChangeSubscriptionAsync(run, topic, filter: null, cancellationToken);

    private async Task ChangeSubscriptionAsync(
        RemotingPushConsumerRun run,
        string topic,
        FilterExpression? filter,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(run);
        ArgumentException.ThrowIfNullOrWhiteSpace(topic);
        await run.CoordinationGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            var changed = filter is null
                ? _subscriptions.Remove(topic)
                : _subscriptions.Set(topic, filter);
            if (!changed)
            {
                return;
            }

            run.GroupSession.BumpSubscriptionVersion();
            var subscriptions = _subscriptions.Snapshot();
            var subscriptionVersion = run.GroupSession.SubscriptionVersion;
            var balanced = await ReconcileTopicAsync(
                run,
                topic,
                [],
                filter ?? FilterExpression.All,
                route: null,
                cancellationToken).ConfigureAwait(false);
            balanced &= await CoordinateOnceCoreAsync(
                run,
                subscriptions,
                subscriptionVersion,
                cancellationToken).ConfigureAwait(false);
            if (!balanced)
            {
                run.GroupSession.Wakeup();
            }
        }
        finally
        {
            run.CoordinationGate.Release();
        }
    }

    public async Task UnlockAsync(
        IEnumerable<IRemotingPushReceiver> receivers,
        bool oneway,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(receivers);
        if (!UsesBrokerQueueLocks)
        {
            return;
        }

        await _queueLockClient.UnlockAsync(
            receivers.OfType<PullAssignmentReceiver>().Select(CreateQueueLockTarget),
            oneway,
            cancellationToken).ConfigureAwait(false);
    }

    private async Task<bool> RebalanceAsync(
        RemotingPushConsumerRun run,
        CancellationToken cancellationToken)
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

    private async Task<bool> CoordinateOnceAsync(
        RemotingPushConsumerRun run,
        CancellationToken cancellationToken)
    {
        await run.CoordinationGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            return await CoordinateOnceCoreAsync(
                run,
                _subscriptions.Snapshot(),
                run.GroupSession.SubscriptionVersion,
                cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            run.CoordinationGate.Release();
        }
    }

    private async Task<bool> CoordinateOnceCoreAsync(
        RemotingPushConsumerRun run,
        IReadOnlyCollection<KeyValuePair<string, FilterExpression>> configuredSubscriptions,
        long subscriptionVersion,
        CancellationToken cancellationToken)
    {
        var retryTopic = GetRetryTopic();
        var subscriptions = configuredSubscriptions.ToDictionary(
            static entry => entry.Key,
            static entry => entry.Value,
            StringComparer.Ordinal);
        if (_options.ConsumerMode == ConsumerMode.Clustering)
        {
            // Classic Java Push adds %RETRY%{group} with SUB_ALL only for clustering consumers. It participates in the
            // same heartbeat and assignment snapshot as application topics; the Broker forces retry-topic assignments
            // to PULL even when an application topic uses POP.
            // https://github.com/apache/rocketmq/blob/rocketmq-all-5.5.0/client/src/main/java/org/apache/rocketmq/client/impl/consumer/DefaultMQPushConsumerImpl.java#L1228-L1235
            // https://github.com/apache/rocketmq/blob/rocketmq-all-5.5.0/broker/src/main/java/org/apache/rocketmq/broker/processor/QueryAssignmentProcessor.java#L112-L129
            subscriptions[retryTopic] = FilterExpression.All;
        }

        var snapshots = new List<PushTopicSnapshot>(subscriptions.Count);
        var balanced = true;
        foreach (var (topic, filter) in subscriptions.OrderBy(static entry => entry.Key, StringComparer.Ordinal))
        {
            try
            {
                var route = await _routes.GetRouteAsync(topic, cancellationToken).ConfigureAwait(false);
                snapshots.Add(new PushTopicSnapshot(topic, filter, route));
                run.GroupSession.AddKnownBrokers(route.AllBrokerEndpoints);
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

        await SendHeartbeatsAsync(run, subscriptions, subscriptionVersion, cancellationToken).ConfigureAwait(false);
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
                    [],
                    snapshot.Filter,
                    snapshot.Route,
                    cancellationToken).ConfigureAwait(false);
                continue;
            }

            if (_options.ConsumerMode == ConsumerMode.Broadcasting)
            {
                balanced &= await ReconcileTopicAsync(
                    run,
                    snapshot.Topic,
                    snapshot.Queues,
                    snapshot.Filter,
                    snapshot.Route,
                    cancellationToken).ConfigureAwait(false);
                continue;
            }

            IReadOnlyList<string> consumerIds;
            try
            {
                consumerIds = await run.GroupSession.GetConsumerIdsAsync(
                    snapshot.Queues[0],
                    snapshot.Route.GetBroker(snapshot.Queues[0].BrokerName),
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

            if (!consumerIds.Contains(run.GroupSession.ClientId, StringComparer.Ordinal))
            {
                balanced = false;
                _logger.LogWarning(
                    "Broker membership for legacy group {GroupName} does not contain client {ClientId}",
                    _options.GroupName,
                    run.GroupSession.ClientId);
                await ReconcileTopicAsync(
                    run,
                    snapshot.Topic,
                    [],
                    snapshot.Filter,
                    snapshot.Route,
                    cancellationToken).ConfigureAwait(false);
                continue;
            }

            var assigned = AverageQueueAllocator.Allocate(
                snapshot.Queues,
                consumerIds,
                run.GroupSession.ClientId);
            balanced &= await ReconcileTopicAsync(
                run,
                snapshot.Topic,
                assigned,
                snapshot.Filter,
                snapshot.Route,
                cancellationToken).ConfigureAwait(false);
        }

        return balanced;
    }

    private async Task<bool> CoordinateBrokerAssignmentsAsync(
        RemotingPushConsumerRun run,
        IReadOnlyList<PushTopicSnapshot> snapshots,
        CancellationToken cancellationToken)
    {
        var balanced = true;
        foreach (var snapshot in snapshots)
        {
            try
            {
                var brokers = snapshot.Route.Brokers;
                if (brokers.Count == 0)
                {
                    throw new InvalidOperationException(
                        $"The route for topic '{snapshot.Topic}' does not contain a readable Broker endpoint.");
                }

                var broker = brokers[0];
                var response = await _remotingClient.InvokeAsync(
                    EndpointParser.Parse(broker.Address),
                    new RemotingCommand
                    {
                        Code = RequestCode.QueryAssignment,
                        Body = LegacyAssignmentProtocol.EncodeQuery(
                            GetWireTopic(snapshot.Topic),
                            run.GroupSession.ConsumerGroup,
                            run.GroupSession.ClientId)
                    },
                    _clientOptions.RequestTimeout,
                    cancellationToken).ConfigureAwait(false);
                EnsureBrokerSuccess(response, "query message assignments");
                var update = LegacyAssignmentProtocol.DecodeResponse(response.Body);
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
                            $"Broker returned topic '{assignment.Topic}' while querying assignment for " +
                            $"'{snapshot.Topic}'.");
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
        RemotingPushConsumerRun run,
        IReadOnlyDictionary<string, FilterExpression> subscriptions,
        long subscriptionVersion,
        CancellationToken cancellationToken)
    {
        var wireSubscriptions = subscriptions.ToDictionary(
            entry => GetWireTopic(entry.Key),
            static entry => entry.Value,
            StringComparer.Ordinal);
        var body = LegacyHeartbeatProtocol.Encode(
            run.GroupSession.ClientId,
            run.GroupSession.ConsumerGroup,
            wireSubscriptions,
            subscriptionVersion,
            "CONSUME_PASSIVELY",
            GetMessageModel(),
            GetConsumeFromWhere(),
            _clientOptions.UnitMode);
        await run.GroupSession.SendHeartbeatsAsync(body, cancellationToken).ConfigureAwait(false);
    }

    private string GetMessageModel() => _options.ConsumerMode switch
    {
        ConsumerMode.Clustering => "CLUSTERING",
        ConsumerMode.Broadcasting => "BROADCASTING",
        _ => throw new ArgumentOutOfRangeException(nameof(_options.ConsumerMode), _options.ConsumerMode, null)
    };

    private string GetConsumeFromWhere() => _options.InitialPosition switch
    {
        ConsumeFromPosition.Beginning => "CONSUME_FROM_FIRST_OFFSET",
        ConsumeFromPosition.End => "CONSUME_FROM_LAST_OFFSET",
        ConsumeFromPosition.Timestamp => "CONSUME_FROM_TIMESTAMP",
        _ => throw new ArgumentOutOfRangeException(nameof(_options.InitialPosition), _options.InitialPosition, null)
    };

    private Task<bool> ReconcileTopicAsync(
        RemotingPushConsumerRun run,
        string topic,
        IReadOnlyList<RemotingConsumerQueue> desiredQueues,
        FilterExpression filter,
        RemotingConsumerRouteSnapshot? route,
        CancellationToken cancellationToken)
    {
        var targets = desiredQueues.Select(queue => new PushReceiveTarget(
            new RemotingPushAssignment(
                queue.Topic,
                queue.BrokerName,
                queue.QueueId,
                RemotingPushReceiveMode.Pull,
                new Dictionary<string, string>(StringComparer.Ordinal)),
            route?.GetBroker(queue.BrokerName) ?? throw new InvalidOperationException(
                $"A route is required to start PULL reception for " +
                $"'{queue.Topic}/{queue.BrokerName}/{queue.QueueId}'."),
            filter))
            .ToArray();
        return ReconcileReceiveTargetsAsync(run, topic, targets, cancellationToken);
    }

    private Task<bool> ReconcileBrokerTopicAsync(
        RemotingPushConsumerRun run,
        PushTopicSnapshot snapshot,
        IReadOnlyList<RemotingPushAssignment> assignments,
        CancellationToken cancellationToken)
    {
        var targets = new PushReceiveTarget[assignments.Count];
        for (var index = 0; index < assignments.Count; index++)
        {
            var assignment = assignments[index];
            RemotingBrokerEndpoint broker;
            try
            {
                broker = snapshot.Route.GetBroker(assignment.BrokerName);
            }
            catch (InvalidOperationException exception)
            {
                throw new InvalidDataException(
                    $"Broker assignment '{assignment.Topic}/{assignment.BrokerName}/{assignment.QueueId}' " +
                    "does not have a current route.",
                    exception);
            }

            targets[index] = new PushReceiveTarget(assignment, broker, snapshot.Filter);
        }

        return ReconcileReceiveTargetsAsync(run, snapshot.Topic, targets, cancellationToken);
    }

    private async Task<bool> ReconcileReceiveTargetsAsync(
        RemotingPushConsumerRun run,
        string topic,
        IReadOnlyList<PushReceiveTarget> desiredTargets,
        CancellationToken cancellationToken)
    {
        var desired = desiredTargets.ToDictionary(static target => PushReceiverKey.Create(target));
        var removed = new List<IRemotingPushReceiver>();
        foreach (var entry in run.Receivers.SnapshotEntries())
        {
            if (entry.Key.Topic == topic &&
                !desired.ContainsKey(entry.Key) &&
                run.Receivers.TryRemove(entry.Key, out var receiver))
            {
                run.Receivers.Drop(receiver, run.PullDeliveryCoordinator);
                removed.Add(receiver);
            }
        }

        try
        {
            await PushReceiverRegistry.AwaitAsync(removed).ConfigureAwait(false);
        }
        finally
        {
            await UnlockAsync(removed, oneway: true, CancellationToken.None).ConfigureAwait(false);
            PushReceiverRegistry.DisposeAll(removed);
        }

        var additions = desired
            .Where(entry => !run.Receivers.Contains(entry.Key))
            .ToArray();
        var queueLockResult = UsesBrokerQueueLocks
            ? await _queueLockClient.LockAsync(
                additions.Select(static entry => CreateQueueLockTarget(entry.Value)),
                cancellationToken).ConfigureAwait(false)
            : null;
        foreach (var entry in additions)
        {
            if (queueLockResult is not null && !queueLockResult.LockedQueues.Contains(entry.Value.Queue!))
            {
                continue;
            }

            var receiver = _receiverFactory.Create(entry.Value, run);
            if (!run.Receivers.TryAdd(entry.Key, receiver))
            {
                receiver.Dispose();
                continue;
            }

            try
            {
                _receiverFactory.Start(receiver, run, UsesBrokerQueueLocks);
                _ = (run.ReceiverSupervisor ?? throw new InvalidOperationException(
                    "The Push receiver supervisor has not been composed for this run."))
                    .Observe(entry.Key, receiver);
            }
            catch
            {
                if (run.Receivers.TryRemove(entry.Key, receiver))
                {
                    try
                    {
                        run.Receivers.Drop(receiver, run.PullDeliveryCoordinator);
                    }
                    catch (Exception exception)
                    {
                        _logger.LogWarning(
                            exception,
                            "Unable to stop a legacy Push receiver after its start failed for {Topic}/{BrokerName}/{QueueId}",
                            receiver.Assignment.Topic,
                            receiver.Assignment.BrokerName,
                            receiver.Assignment.QueueId);
                    }

                    try
                    {
                        receiver.Dispose();
                    }
                    catch (Exception exception)
                    {
                        _logger.LogWarning(
                            exception,
                            "Unable to dispose a legacy Push receiver after its start failed for {Topic}/{BrokerName}/{QueueId}",
                            receiver.Assignment.Topic,
                            receiver.Assignment.BrokerName,
                            receiver.Assignment.QueueId);
                    }
                }

                throw;
            }
        }

        return desired.Keys.All(run.Receivers.Contains);
    }

    private bool UsesBrokerAssignment =>
        _options.QueueAssignmentMode == RemotingPushQueueAssignmentMode.Broker &&
        _options.ConsumerMode == ConsumerMode.Clustering &&
        !_options.ConsumeOrderly;

    private async Task<bool> RenewQueueLocksAsync(
        RemotingPushConsumerRun run,
        CancellationToken cancellationToken)
    {
        var receivers = run.Receivers.Snapshot().OfType<PullAssignmentReceiver>().ToArray();
        if (receivers.Length == 0)
        {
            return true;
        }

        var queueLockResult = await _queueLockClient.LockAsync(
            receivers.Select(CreateQueueLockTarget),
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

    private static OrderlyPullQueueLockTarget CreateQueueLockTarget(PullAssignmentReceiver receiver) =>
        new(receiver.Queue, receiver.Target.Broker);

    private static OrderlyPullQueueLockTarget CreateQueueLockTarget(PushReceiveTarget target) =>
        new(
            target.Queue ?? throw new InvalidOperationException(
                "Orderly queue locking requires a physical PULL queue."),
            target.Broker);

    private string GetRetryTopic() => $"%RETRY%{_options.GroupName}";

    private string GetWireTopic(string topic) => LegacyNamespace.Wrap(_clientOptions.Namespace, topic);

    private static void EnsureBrokerSuccess(RemotingCommand response, string operation)
    {
        if (response.Code != ResponseCodes.ResSuccess)
        {
            throw new RemotingCommandException(
                response.Code,
                response.Remark ?? $"Broker rejected the request to {operation}.");
        }
    }
}
