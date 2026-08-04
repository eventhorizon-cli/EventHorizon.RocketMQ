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
using EventHorizon.RocketMQ.Remoting.Consumer.Route;
using EventHorizon.RocketMQ.Remoting.Protocol;
using Microsoft.Extensions.Logging;

namespace EventHorizon.RocketMQ.Remoting.Consumer.LitePull.Assignment;

internal sealed class LitePullAssignmentCoordinator
{
    private readonly RemotingLitePullConsumerOptions _options;
    private readonly RemotingClientOptions _clientOptions;
    private readonly IRemotingConsumerRouteClient _routeClient;
    private readonly LitePullSubscriptionState _subscriptions;
    private readonly LitePullAssignmentReconciler _reconciler;
    private readonly ILogger _logger;

    public LitePullAssignmentCoordinator(
        RemotingLitePullConsumerOptions options,
        RemotingClientOptions clientOptions,
        IRemotingConsumerRouteClient routeClient,
        LitePullSubscriptionState subscriptions,
        LitePullAssignmentReconciler reconciler,
        ILogger logger)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(clientOptions);
        ArgumentNullException.ThrowIfNull(routeClient);
        ArgumentNullException.ThrowIfNull(subscriptions);
        ArgumentNullException.ThrowIfNull(reconciler);
        ArgumentNullException.ThrowIfNull(logger);

        _options = options;
        _clientOptions = clientOptions;
        _routeClient = routeClient;
        _subscriptions = subscriptions;
        _reconciler = reconciler;
        _logger = logger;
    }

    public async Task StartAsync(
        RemotingLitePullConsumerRun run,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(run);
        run.GroupSession.RegisterRebalance(token => RebalanceAsync(run, token));
        if (!_subscriptions.IsEmpty)
        {
            await CoordinateAndScheduleRetryAsync(run, cancellationToken).ConfigureAwait(false);
        }
    }

    public async Task SubscribeAsync(
        RemotingLitePullConsumerRun run,
        string topic,
        FilterExpression filter,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(run);
        ArgumentException.ThrowIfNullOrWhiteSpace(topic);
        ArgumentNullException.ThrowIfNull(filter);
        await run.CoordinationGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            ThrowIfManualAssignment(run);
            _subscriptions.Set(topic, filter);
            run.GroupSession.BumpSubscriptionVersion();
            var balanced = await CoordinateSubscriptionsOnceAsync(
                run,
                _subscriptions.Snapshot(),
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

    public async Task UnsubscribeAsync(
        RemotingLitePullConsumerRun run,
        string topic,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(run);
        ArgumentException.ThrowIfNullOrWhiteSpace(topic);
        await run.CoordinationGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            ThrowIfManualAssignment(run);
            if (!_subscriptions.Remove(topic))
            {
                return;
            }

            run.GroupSession.BumpSubscriptionVersion();
            var subscriptions = _subscriptions.Snapshot();
            var balanced = await CoordinateSubscriptionsOnceAsync(
                run,
                subscriptions,
                cancellationToken).ConfigureAwait(false);
            if (!balanced)
            {
                run.GroupSession.Wakeup();
            }

            if (subscriptions.Length == 0)
            {
                await run.GroupSession.UnregisterAsync().ConfigureAwait(false);
            }
        }
        finally
        {
            run.CoordinationGate.Release();
        }
    }

    public async Task AssignAsync(
        RemotingLitePullConsumerRun run,
        IReadOnlyCollection<RemotingConsumerQueue> requestedQueues,
        IReadOnlyDictionary<string, FilterExpression> filters,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(run);
        ArgumentNullException.ThrowIfNull(requestedQueues);
        ArgumentNullException.ThrowIfNull(filters);
        await run.CoordinationGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (!_subscriptions.IsEmpty)
            {
                throw new InvalidOperationException(
                    "Manual assignment requires a Lite Pull consumer with no topic subscriptions.");
            }

            var previous = run.ManualAssignment;
            var desired = LitePullManualAssignment.Create(requestedQueues, filters);
            run.ManualAssignment = desired;
            run.GroupSession.BumpSubscriptionVersion();

            if (desired is null)
            {
                await _reconciler.ReconcileAsync(run, [], cancellationToken).ConfigureAwait(false);
                if (previous is not null)
                {
                    await run.GroupSession.UnregisterAsync().ConfigureAwait(false);
                }

                return;
            }

            if (!await CoordinateManualOnceAsync(run, desired, cancellationToken).ConfigureAwait(false))
            {
                run.GroupSession.Wakeup();
            }
        }
        finally
        {
            run.CoordinationGate.Release();
        }
    }

    private async Task<bool> RebalanceAsync(
        RemotingLitePullConsumerRun run,
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

    private async Task CoordinateAndScheduleRetryAsync(
        RemotingLitePullConsumerRun run,
        CancellationToken cancellationToken)
    {
        if (!await CoordinateOnceAsync(run, cancellationToken).ConfigureAwait(false))
        {
            run.GroupSession.Wakeup();
        }
    }

    private async Task<bool> CoordinateOnceAsync(
        RemotingLitePullConsumerRun run,
        CancellationToken cancellationToken)
    {
        await run.CoordinationGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            return run.ManualAssignment is { } manual
                ? await CoordinateManualOnceAsync(run, manual, cancellationToken).ConfigureAwait(false)
                : await CoordinateSubscriptionsOnceAsync(
                    run,
                    _subscriptions.Snapshot(),
                    cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            run.CoordinationGate.Release();
        }
    }

    private async Task<bool> CoordinateManualOnceAsync(
        RemotingLitePullConsumerRun run,
        LitePullManualAssignment manual,
        CancellationToken cancellationToken)
    {
        var balanced = true;
        try
        {
            await AddKnownBrokersAsync(
                run,
                manual.Queues,
                refreshTopics: true,
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
                "Unable to refresh routes for a legacy Lite Pull manual assignment");
        }

        try
        {
            await SendHeartbeatsAsync(run, manual.Subscriptions, cancellationToken).ConfigureAwait(false);
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
                "Unable to send heartbeats for a legacy Lite Pull manual assignment");
        }

        try
        {
            await _reconciler.ReconcileAsync(
                run,
                manual.GetReceiveTargets(),
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
                "Unable to reconcile queues for a legacy Lite Pull manual assignment");
        }

        return balanced;
    }

    private async Task<bool> CoordinateSubscriptionsOnceAsync(
        RemotingLitePullConsumerRun run,
        KeyValuePair<string, FilterExpression>[] subscriptions,
        CancellationToken cancellationToken)
    {
        if (subscriptions.Length == 0)
        {
            await _reconciler.ReconcileAsync(run, [], cancellationToken).ConfigureAwait(false);
            return true;
        }

        var filters = subscriptions.ToDictionary(
            static entry => entry.Key,
            static entry => entry.Value,
            StringComparer.Ordinal);
        var desiredByTopic = run.DeliveryState.GetAssignment()
            .Where(queue => filters.ContainsKey(queue.Topic))
            .GroupBy(static queue => queue.Topic, StringComparer.Ordinal)
            .ToDictionary(
                static group => group.Key,
                group => (IReadOnlyList<LitePullReceiveTarget>)group
                    .Select(queue => new LitePullReceiveTarget(queue, filters[group.Key]))
                    .ToArray(),
                StringComparer.Ordinal);
        var snapshots = new List<TopicSnapshot>(subscriptions.Length);
        var balanced = true;
        foreach (var subscription in subscriptions)
        {
            try
            {
                var queues = await _routeClient.GetMessageQueuesAsync(
                    subscription.Key,
                    cancellationToken).ConfigureAwait(false);
                var membershipBroker = await AddKnownBrokersAsync(
                    run,
                    queues,
                    refreshTopics: false,
                    cancellationToken).ConfigureAwait(false);
                snapshots.Add(new TopicSnapshot(
                    subscription.Key,
                    subscription.Value,
                    queues,
                    membershipBroker));
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
                consumerIds = await run.GroupSession.GetConsumerIdsAsync(
                    snapshot.Queues[0],
                    snapshot.MembershipBroker ?? throw new InvalidOperationException(
                        $"No Broker route is available for Lite Pull topic '{snapshot.Topic}'."),
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

            if (!consumerIds.Contains(run.GroupSession.ClientId, StringComparer.Ordinal))
            {
                balanced = false;
                _logger.LogWarning(
                    "Broker membership for legacy Lite Pull group {GroupName} does not contain client {ClientId}",
                    _options.GroupName,
                    run.GroupSession.ClientId);
                desiredByTopic[snapshot.Topic] = [];
                continue;
            }

            desiredByTopic[snapshot.Topic] = AverageQueueAllocator.Allocate(
                    snapshot.Queues,
                    consumerIds,
                    run.GroupSession.ClientId)
                .Select(queue => new LitePullReceiveTarget(queue, snapshot.Filter))
                .ToArray();
        }

        try
        {
            await _reconciler.ReconcileAsync(
                run,
                desiredByTopic.Values.SelectMany(static targets => targets).ToArray(),
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
                "Unable to reconcile queues for legacy Lite Pull subscriptions");
        }

        return balanced;
    }

    private async Task SendHeartbeatsAsync(
        RemotingLitePullConsumerRun run,
        IReadOnlyCollection<KeyValuePair<string, FilterExpression>> subscriptions,
        CancellationToken cancellationToken)
    {
        var wireSubscriptions = subscriptions.ToDictionary(
            entry => LegacyNamespace.Wrap(_clientOptions.Namespace, entry.Key),
            static entry => entry.Value,
            StringComparer.Ordinal);
        var body = LegacyHeartbeatProtocol.Encode(
            run.GroupSession.ClientId,
            run.GroupSession.ConsumerGroup,
            wireSubscriptions,
            run.GroupSession.SubscriptionVersion,
            "CONSUME_ACTIVELY",
            "CLUSTERING",
            GetConsumeFromWhere(),
            _clientOptions.UnitMode);
        await run.GroupSession.SendHeartbeatsAsync(body, cancellationToken).ConfigureAwait(false);
    }

    private async Task<RemotingBrokerEndpoint?> AddKnownBrokersAsync(
        RemotingLitePullConsumerRun run,
        IReadOnlyCollection<RemotingConsumerQueue> queues,
        bool refreshTopics,
        CancellationToken cancellationToken)
    {
        if (refreshTopics)
        {
            foreach (var topic in queues.Select(static queue => queue.Topic).Distinct(StringComparer.Ordinal))
            {
                await _routeClient.GetMessageQueuesAsync(topic, cancellationToken).ConfigureAwait(false);
            }
        }

        RemotingBrokerEndpoint? first = null;
        foreach (var queue in queues.DistinctBy(static queue => queue.BrokerName, StringComparer.Ordinal))
        {
            var broker = await _routeClient.ResolveBrokerAsync(
                queue,
                useSuggestedBroker: false,
                cancellationToken).ConfigureAwait(false);
            run.GroupSession.AddKnownBroker(broker);
            first ??= broker;
        }

        return first;
    }

    private static void ThrowIfManualAssignment(RemotingLitePullConsumerRun run)
    {
        if (run.IsManualAssignment)
        {
            throw new InvalidOperationException("Lite Pull consumer is using manual-assignment mode.");
        }
    }

    private string GetConsumeFromWhere() => _options.InitialPosition switch
    {
        ConsumeFromPosition.Beginning => "CONSUME_FROM_FIRST_OFFSET",
        ConsumeFromPosition.End => "CONSUME_FROM_LAST_OFFSET",
        ConsumeFromPosition.Timestamp => "CONSUME_FROM_TIMESTAMP",
        _ => throw new ArgumentOutOfRangeException(nameof(_options.InitialPosition), _options.InitialPosition, null)
    };

    private sealed record TopicSnapshot(
        string Topic,
        FilterExpression Filter,
        IReadOnlyList<RemotingConsumerQueue> Queues,
        RemotingBrokerEndpoint? MembershipBroker);
}
