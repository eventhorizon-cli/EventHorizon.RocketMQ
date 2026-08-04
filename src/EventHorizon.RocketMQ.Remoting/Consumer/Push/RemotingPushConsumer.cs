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
using EventHorizon.RocketMQ.Remoting.Consumer.Push.Assignment;
using EventHorizon.RocketMQ.Remoting.Consumer.Push.Pop;
using EventHorizon.RocketMQ.Remoting.Consumer.Push.Pull.Offset;
using EventHorizon.RocketMQ.Remoting.Consumer.Push.Pull.Orderly;
using EventHorizon.RocketMQ.Remoting.Consumer.Route;
using EventHorizon.RocketMQ.Remoting.Instrumentation;
using EventHorizon.RocketMQ.Remoting.Protocol;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace EventHorizon.RocketMQ.Remoting.Consumer.Push;

internal sealed class RemotingPushConsumer : IRemotingPushConsumer
{
    private readonly RemotingPushConsumerOptions _options;
    private readonly IRemotingConsumerEngine _consumerEngine;
    private readonly PushAssignmentCoordinator _assignmentCoordinator;
    private readonly PushConsumerRunLifecycle _runLifecycle;
    private readonly bool _hasLocalGroupPeer;
    private readonly PullOffsetResetHandler _offsetResetHandler;
    private readonly PullOffsetManager _pullOffsetManager;
    private readonly PushSubscriptionState _subscriptions;
    private readonly SemaphoreSlim _lifecycle = new(1, 1);
    private RemotingPushConsumerRun? _run;
    private int _disposed;

    public RemotingPushConsumer(
        IOptions<RemotingPushConsumerOptions> options,
        IOptions<RemotingClientOptions> clientOptions,
        IRemotingConsumerEngine consumerEngine,
        RemotingConsumerRouteResolver routes,
        IRemotingClient remotingClient,
        IRemotingRebalanceService rebalanceService,
        TimeProvider timeProvider,
        ILogger<RemotingPushConsumer> logger,
        Func<IReadOnlyList<RemotingMessageView>, RemotingPushConsumeContext, CancellationToken, ValueTask<ConsumeResult>> messageHandler,
        IRemotingRocketMQTelemetry? telemetry = null,
        bool hasLocalGroupPeer = false)
    {
        _options = options.Value;
        ArgumentNullException.ThrowIfNull(messageHandler);
        var resolvedClientOptions = clientOptions.Value;
        var resolvedTelemetry = telemetry ?? RemotingRocketMQTelemetry.Disabled;
        _consumerEngine = consumerEngine;
        _hasLocalGroupPeer = hasLocalGroupPeer;
        var groupClient = new RemotingConsumerGroupClient(
            remotingClient,
            resolvedClientOptions,
            _options.GroupName,
            logger);
        var queueLockClient = new OrderlyPullQueueLockClient(
            remotingClient,
            resolvedClientOptions,
            groupClient,
            logger);
        var popClient = new PopWireClient(
            _options,
            resolvedClientOptions,
            routes,
            remotingClient,
            timeProvider,
            resolvedTelemetry);
        _pullOffsetManager = new PullOffsetManager(
            _options,
            _consumerEngine,
            groupClient.ClientId,
            groupClient.ConsumerGroup,
            logger);
        _subscriptions = new PushSubscriptionState(_options.Subscriptions);
        var receiverFactory = new PushReceiverFactory(
            _options,
            popClient,
            consumerEngine,
            timeProvider,
            logger);
        _assignmentCoordinator = new PushAssignmentCoordinator(
            _options,
            resolvedClientOptions,
            routes,
            remotingClient,
            queueLockClient,
            receiverFactory,
            _subscriptions,
            timeProvider,
            logger);
        _offsetResetHandler = new PullOffsetResetHandler(
            _options,
            resolvedClientOptions,
            groupClient,
            _pullOffsetManager,
            _subscriptions,
            _lifecycle,
            () => _run,
            remotingClient,
            logger);
        _runLifecycle = new PushConsumerRunLifecycle(
            _options,
            consumerEngine,
            rebalanceService,
            groupClient,
            _pullOffsetManager,
            _assignmentCoordinator,
            timeProvider,
            resolvedTelemetry,
            logger,
            messageHandler);
    }

    internal IReadOnlyList<RemotingConsumerQueue> Assignment => _run?.Receivers.Snapshot()
        .Select(static receiver => receiver.Target.Queue)
        .OfType<RemotingConsumerQueue>()
        .OrderBy(static queue => queue.Topic, StringComparer.Ordinal)
        .ThenBy(static queue => queue.BrokerName, StringComparer.Ordinal)
        .ThenBy(static queue => queue.QueueId)
        .ToArray() ?? [];

    internal IReadOnlyList<RemotingPushAssignment> ReceiveAssignments => _run?.Receivers.Snapshot()
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
            ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);
            if (_run is not null)
            {
                return;
            }

            await _consumerEngine.StartAsync(cancellationToken).ConfigureAwait(false);
            var run = _runLifecycle.CreateRun();
            _run = run;
            try
            {
                await _runLifecycle.StartAsync(run, cancellationToken).ConfigureAwait(false);
            }
            catch
            {
                _run = null;
                await _runLifecycle.ShutdownAsync(run, CancellationToken.None).ConfigureAwait(false);
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
            await _runLifecycle.ShutdownAsync(run, cancellationToken).ConfigureAwait(false);
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
            var run = _run;
            if (run is null)
            {
                _subscriptions.Set(topic, filter);
            }
            else
            {
                await _assignmentCoordinator.SubscribeAsync(run, topic, filter, cancellationToken)
                    .ConfigureAwait(false);
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
            _pullOffsetManager.RemoveResetBoundaries(topic);
            var run = _run;
            if (run is null)
            {
                _subscriptions.Remove(topic);
            }
            else
            {
                await _assignmentCoordinator.UnsubscribeAsync(run, topic, cancellationToken)
                    .ConfigureAwait(false);
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

        _offsetResetHandler.Dispose();
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

}
