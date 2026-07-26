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

using EventHorizon.RocketMQ.Grpc.Protocol;
using EventHorizon.RocketMQ.Grpc.Protocol.Route;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Proto = Apache.Rocketmq.V2;

namespace EventHorizon.RocketMQ.Grpc.Consumer.Lite;

internal sealed class GrpcLiteSubscriptionManager : IAsyncDisposable
{
    private readonly GrpcLitePushConsumerOptions _options;
    private readonly GrpcClientOptions _clientOptions;
    private readonly IRocketMQGrpcClient _client;
    private readonly IGrpcRouteService _routes;
    private readonly ILogger _logger;
    private readonly SemaphoreSlim _gate = new(1, 1);
    private readonly object _topicsGate = new();
    private readonly HashSet<string> _liteTopics = new(StringComparer.Ordinal);
    private CancellationTokenSource? _syncCts;
    private Task? _syncLoop;
    private int _started;
    private int _disposed;

    public GrpcLiteSubscriptionManager(
        IOptions<GrpcLitePushConsumerOptions> options,
        IOptions<GrpcClientOptions> clientOptions,
        IRocketMQGrpcClient client,
        IGrpcRouteService routes,
        ILogger<GrpcLiteSubscriptionManager> logger)
    {
        _options = options.Value;
        _clientOptions = clientOptions.Value;
        _client = client;
        _routes = routes;
        _logger = logger;
        foreach (var liteTopic in _options.LiteTopics)
        {
            _liteTopics.Add(liteTopic);
        }
    }

    public IReadOnlySet<string> LiteTopics
    {
        get
        {
            lock (_topicsGate)
            {
                return new HashSet<string>(_liteTopics, StringComparer.Ordinal);
            }
        }
    }

    public async ValueTask StartAsync(CancellationToken cancellationToken)
    {
        await _gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            ThrowIfDisposed();
            if (Volatile.Read(ref _started) != 0)
            {
                return;
            }

            await SyncAllAsync(cancellationToken).ConfigureAwait(false);
            var syncCts = new CancellationTokenSource();
            _syncCts = syncCts;
            _syncLoop = SynchronizePeriodicallyAsync(syncCts.Token);
            Volatile.Write(ref _started, 1);
        }
        finally
        {
            _gate.Release();
        }
    }

    public async ValueTask StopAsync(CancellationToken cancellationToken = default)
    {
        CancellationTokenSource? syncCts;
        Task? syncLoop;
        await _gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (Volatile.Read(ref _started) == 0)
            {
                return;
            }

            Volatile.Write(ref _started, 0);
            syncCts = _syncCts;
            syncLoop = _syncLoop;
            _syncCts = null;
            _syncLoop = null;
        }
        finally
        {
            _gate.Release();
        }

        if (syncCts is null)
        {
            return;
        }

        syncCts.Cancel();
        try
        {
            if (syncLoop is not null)
            {
                await syncLoop.ConfigureAwait(false);
            }
        }
        finally
        {
            syncCts.Dispose();
        }
    }

    public async Task SubscribeAsync(
        string liteTopic,
        GrpcLiteOffsetOption? offsetOption,
        CancellationToken cancellationToken)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(liteTopic);
        await _gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            ThrowIfDisposed();
            EnsureStarted();
            if (Contains(liteTopic))
            {
                return;
            }

            await SyncAsync(
                Proto.LiteSubscriptionAction.PartialAdd,
                [liteTopic],
                offsetOption,
                cancellationToken).ConfigureAwait(false);
            Add(liteTopic);
        }
        finally
        {
            _gate.Release();
        }
    }

    public async Task UnsubscribeAsync(string liteTopic, CancellationToken cancellationToken)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(liteTopic);
        await _gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            ThrowIfDisposed();
            EnsureStarted();
            if (!Contains(liteTopic))
            {
                return;
            }

            await SyncAsync(
                Proto.LiteSubscriptionAction.PartialRemove,
                [liteTopic],
                null,
                cancellationToken).ConfigureAwait(false);
            Remove(liteTopic);
        }
        finally
        {
            _gate.Release();
        }
    }

    public async Task HandleTelemetryCommandAsync(
        Uri endpoint,
        Proto.TelemetryCommand command,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(endpoint);
        ArgumentNullException.ThrowIfNull(command);
        if (command.CommandCase != Proto.TelemetryCommand.CommandOneofCase.NotifyUnsubscribeLiteCommand)
        {
            return;
        }

        var liteTopic = command.NotifyUnsubscribeLiteCommand.LiteTopic;
        if (string.IsNullOrWhiteSpace(liteTopic))
        {
            return;
        }

        await _gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (Remove(liteTopic))
            {
                _logger.LogInformation(
                    "RocketMQ removed LiteTopic {LiteTopic} from consumer group {GroupName} on {Endpoint}",
                    liteTopic,
                    _options.GroupName,
                    endpoint);
            }
        }
        finally
        {
            _gate.Release();
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
        {
            return;
        }

        await StopAsync(CancellationToken.None).ConfigureAwait(false);
        _gate.Dispose();
    }

    private async Task SynchronizePeriodicallyAsync(CancellationToken cancellationToken)
    {
        using var timer = new PeriodicTimer(_options.SubscriptionSyncInterval);
        try
        {
            while (await timer.WaitForNextTickAsync(cancellationToken).ConfigureAwait(false))
            {
                try
                {
                    await _gate.WaitAsync(cancellationToken).ConfigureAwait(false);
                    try
                    {
                        if (Volatile.Read(ref _started) != 0)
                        {
                            await SyncAllAsync(cancellationToken).ConfigureAwait(false);
                        }
                    }
                    finally
                    {
                        _gate.Release();
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
                        "Failed to synchronize LiteTopic subscriptions for consumer group {GroupName}",
                        _options.GroupName);
                }
            }
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
        }
    }

    private Task SyncAllAsync(CancellationToken cancellationToken) =>
        SyncAsync(
            Proto.LiteSubscriptionAction.CompleteAdd,
            Snapshot(),
            null,
            cancellationToken);

    private async Task SyncAsync(
        Proto.LiteSubscriptionAction action,
        IReadOnlyCollection<string> liteTopics,
        GrpcLiteOffsetOption? offsetOption,
        CancellationToken cancellationToken)
    {
        var request = new Proto.SyncLiteSubscriptionRequest
        {
            Action = action,
            Topic = Resource(_options.BindTopic),
            Group = Resource(_options.GroupName)
        };
        request.LiteTopicSet.AddRange(liteTopics);
        if (offsetOption is not null)
        {
            request.OffsetOption = offsetOption.ToProtobuf();
        }

        var endpoints = await GetEndpointsAsync(cancellationToken).ConfigureAwait(false);
        await Task.WhenAll(endpoints.Select(endpoint => SyncAsync(endpoint, request, cancellationToken)))
            .ConfigureAwait(false);
    }

    private async Task SyncAsync(
        Uri endpoint,
        Proto.SyncLiteSubscriptionRequest request,
        CancellationToken cancellationToken)
    {
        var response = await _client.SyncLiteSubscriptionAsync(endpoint, request, cancellationToken)
            .ConfigureAwait(false);
        GrpcStatus.EnsureSuccess(response.Status);
    }

    private async Task<Uri[]> GetEndpointsAsync(CancellationToken cancellationToken)
    {
        var queues = await _routes.GetAsync(_options.BindTopic, false, cancellationToken).ConfigureAwait(false);
        var endpoints = queues
            .Select(queue => GrpcEndpoint.FromProtobuf(queue.Broker.Endpoints, _clientOptions.UseTLS))
            .DistinctBy(static endpoint => endpoint.AbsoluteUri)
            .ToArray();
        return endpoints.Length == 0 ? [_client.AccessPoint] : endpoints;
    }

    private Proto.Resource Resource(string name) => new()
    {
        ResourceNamespace = _clientOptions.Namespace ?? string.Empty,
        Name = name
    };

    private IReadOnlyCollection<string> Snapshot()
    {
        lock (_topicsGate)
        {
            return _liteTopics.OrderBy(static topic => topic, StringComparer.Ordinal).ToArray();
        }
    }

    private bool Contains(string liteTopic)
    {
        lock (_topicsGate)
        {
            return _liteTopics.Contains(liteTopic);
        }
    }

    private void Add(string liteTopic)
    {
        lock (_topicsGate)
        {
            _liteTopics.Add(liteTopic);
        }
    }

    private bool Remove(string liteTopic)
    {
        lock (_topicsGate)
        {
            return _liteTopics.Remove(liteTopic);
        }
    }

    private void EnsureStarted()
    {
        if (Volatile.Read(ref _started) == 0)
        {
            throw new InvalidOperationException("Lite Push consumer has not been started.");
        }
    }

    private void ThrowIfDisposed() => ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);
}
