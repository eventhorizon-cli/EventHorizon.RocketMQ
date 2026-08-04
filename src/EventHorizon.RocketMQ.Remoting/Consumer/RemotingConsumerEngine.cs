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

using EventHorizon.RocketMQ.Remoting.Consumer.Offset;
using EventHorizon.RocketMQ.Remoting.Consumer.Pull;
using EventHorizon.RocketMQ.Remoting.Consumer.Route;
using EventHorizon.RocketMQ.Remoting.Consumer.Settlement;
using EventHorizon.RocketMQ.Remoting.Instrumentation;
using EventHorizon.RocketMQ.Remoting.Protocol;
using Microsoft.Extensions.Options;

namespace EventHorizon.RocketMQ.Remoting.Consumer;

internal sealed class RemotingConsumerEngine : IRemotingConsumerEngine
{
    private readonly RemotingConsumerRouteResolver _routes;
    private readonly PullWireClient _pullClient;
    private readonly RemotingConsumerOffsetClient _offsetClient;
    private readonly RemotingSettlementClient _settlementClient;
    private readonly SemaphoreSlim _lifecycleGate = new(1, 1);
    private int _started;
    private int _disposed;

    public RemotingConsumerEngine(
        RemotingConsumerSettings settings,
        IOptions<RemotingClientOptions> clientOptions,
        RemotingConsumerRouteResolver routes,
        IRemotingClient remotingClient,
        TimeProvider timeProvider,
        IRemotingRocketMQTelemetry? telemetry = null)
    {
        ArgumentNullException.ThrowIfNull(settings);
        ArgumentNullException.ThrowIfNull(clientOptions);
        ArgumentNullException.ThrowIfNull(routes);
        ArgumentNullException.ThrowIfNull(remotingClient);
        ArgumentNullException.ThrowIfNull(timeProvider);

        var resolvedClientOptions = clientOptions.Value;
        var resolvedTelemetry = telemetry ?? RemotingRocketMQTelemetry.Disabled;
        _routes = routes;
        _pullClient = new PullWireClient(
            settings,
            resolvedClientOptions,
            routes,
            remotingClient,
            timeProvider,
            resolvedTelemetry);
        _offsetClient = new RemotingConsumerOffsetClient(
            settings,
            resolvedClientOptions,
            routes,
            remotingClient,
            resolvedTelemetry);
        _settlementClient = new RemotingSettlementClient(
            settings,
            resolvedClientOptions,
            routes,
            remotingClient,
            resolvedTelemetry);
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

            Volatile.Write(ref _started, 1);
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
        }
        finally
        {
            _lifecycleGate.Release();
        }
    }

    public async Task<IReadOnlyList<RemotingConsumerQueue>> GetMessageQueuesAsync(
        string topic,
        CancellationToken cancellationToken = default)
    {
        EnsureStarted();
        ArgumentException.ThrowIfNullOrWhiteSpace(topic);
        return (await _routes.GetRouteAsync(topic, cancellationToken).ConfigureAwait(false)).Queues;
    }

    public Task<RemotingBrokerEndpoint> ResolveBrokerAsync(
        RemotingConsumerQueue queue,
        bool useSuggestedBroker,
        CancellationToken cancellationToken = default)
    {
        EnsureStarted();
        return _routes.ResolveBrokerAsync(queue, useSuggestedBroker, cancellationToken);
    }

    public Task<RemotingPullResult> PullAsync(
        RemotingConsumerQueue queue,
        long offset,
        FilterExpression filter,
        int? maxMessages = null,
        int? maxBytes = null,
        CancellationToken cancellationToken = default)
    {
        EnsureStarted();
        return _pullClient.PullAsync(queue, offset, filter, maxMessages, maxBytes, cancellationToken);
    }

    public Task<long> GetOffsetAsync(
        RemotingConsumerQueue queue,
        CancellationToken cancellationToken = default)
    {
        EnsureStarted();
        return _offsetClient.GetOffsetAsync(queue, cancellationToken);
    }

    public Task UpdateOffsetAsync(
        RemotingConsumerQueue queue,
        long offset,
        CancellationToken cancellationToken = default)
    {
        EnsureStarted();
        return _offsetClient.UpdateOffsetAsync(queue, offset, cancellationToken);
    }

    public Task<long> QueryOffsetAsync(
        RemotingConsumerQueue queue,
        ConsumeFromPosition position,
        DateTimeOffset? timestamp = null,
        CancellationToken cancellationToken = default)
    {
        EnsureStarted();
        return _offsetClient.QueryOffsetAsync(queue, position, timestamp, cancellationToken);
    }

    public Task SendBackAsync(
        RemotingConsumerQueue queue,
        RemotingMessageView message,
        int delayLevel,
        int maxReconsumeTimes,
        CancellationToken cancellationToken)
    {
        EnsureStarted();
        return _settlementClient.SendBackAsync(
            queue,
            message,
            delayLevel,
            maxReconsumeTimes,
            cancellationToken);
    }

    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
        {
            return;
        }

        await _lifecycleGate.WaitAsync().ConfigureAwait(false);
        try
        {
            Volatile.Write(ref _started, 0);
        }
        finally
        {
            _lifecycleGate.Release();
        }
    }

    private void EnsureStarted()
    {
        ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);
        if (Volatile.Read(ref _started) == 0)
        {
            throw new InvalidOperationException("Consumer engine has not been started.");
        }
    }
}
