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
using EventHorizon.RocketMQ.Grpc.Protocol;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Proto = Apache.Rocketmq.V2;

namespace EventHorizon.RocketMQ.Grpc.Protocol.Telemetry;

/// <summary>
/// Coordinates the endpoint-scoped RocketMQ Telemetry RPC control sessions for one client role.
/// </summary>
internal sealed class GrpcSessionManager : IAsyncDisposable
{
    private static readonly TimeSpan InitialReconnectDelay = TimeSpan.FromSeconds(1);
    private static readonly TimeSpan MaximumReconnectDelay = TimeSpan.FromSeconds(30);
    private readonly IRocketMQGrpcClient _client;
    private readonly GrpcClientOptions _options;
    private readonly Proto.ClientType _clientType;
    private readonly string? _group;
    private readonly Func<Uri, Proto.TelemetryCommand, CancellationToken, Task>? _telemetryHandler;
    private readonly ILogger<GrpcSessionManager> _logger;
    private readonly ConcurrentDictionary<string, SessionEntry> _sessions = new(StringComparer.OrdinalIgnoreCase);
    private readonly ConcurrentDictionary<string, byte> _reconnectRequests = new(StringComparer.OrdinalIgnoreCase);
    private readonly SemaphoreSlim _gate = new(1, 1);
    private readonly CancellationTokenSource _cts = new();
    private readonly object _disposeGate = new();
    private readonly object _backgroundGate = new();
    private readonly HashSet<Task> _backgroundTasks = [];
    private Proto.Settings? _latestSettings;
    private Task? _heartbeatLoop;
    private Task? _disposeTask;
    private int _disposed;

    public GrpcSessionManager(
        IRocketMQGrpcClient client,
        IOptions<GrpcClientOptions> options,
        Proto.ClientType clientType,
        string? group,
        ILogger<GrpcSessionManager> logger,
        Func<Uri, Proto.TelemetryCommand, CancellationToken, Task>? telemetryHandler = null)
    {
        _client = client;
        _options = options.Value;
        _clientType = clientType;
        _group = group;
        _telemetryHandler = telemetryHandler;
        _logger = logger;
    }

    public void Start()
    {
        ThrowIfDisposed();
        _heartbeatLoop ??= HeartbeatLoopAsync(_cts.Token);
    }

    public async Task EnsureAsync(IEnumerable<Uri> endpoints, Proto.Settings settings, CancellationToken cancellationToken)
    {
        ThrowIfDisposed();
        await _gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            ThrowIfDisposed();
            _latestSettings = settings.Clone();
            foreach (var endpoint in endpoints.DistinctBy(static value => value.AbsoluteUri))
            {
                if (_sessions.TryGetValue(endpoint.AbsoluteUri, out var existing) &&
                    !existing.Session.Completion.IsCompleted)
                {
                    continue;
                }

                if (existing is not null)
                {
                    await DisposeSessionAsync(existing).ConfigureAwait(false);
                }

                var entry = await OpenSessionAsync(endpoint, settings, cancellationToken).ConfigureAwait(false);
                RegisterSession(entry);
            }
        }
        finally
        {
            _gate.Release();
        }
    }

    public async Task SyncSettingsAsync(Proto.Settings settings, CancellationToken cancellationToken)
    {
        ThrowIfDisposed();
        await _gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            ThrowIfDisposed();
            _latestSettings = settings.Clone();
            foreach (var entry in _sessions.Values)
            {
                if (!entry.Session.Completion.IsCompleted)
                {
                    await entry.Session.WriteSettingsAsync(settings, cancellationToken).ConfigureAwait(false);
                }
            }
        }
        finally
        {
            _gate.Release();
        }
    }

    public Proto.Settings? GetServerSettings(Uri endpoint)
    {
        ThrowIfDisposed();
        return _sessions.TryGetValue(endpoint.AbsoluteUri, out var entry)
            ? entry.Session.ServerSettings
            : null;
    }

    public ValueTask DisposeAsync()
    {
        lock (_disposeGate)
        {
            _disposeTask ??= DisposeCoreAsync();
            return new ValueTask(_disposeTask);
        }
    }

    private async Task DisposeCoreAsync()
    {
        Interlocked.Exchange(ref _disposed, 1);
        _cts.Cancel();
        if (_heartbeatLoop is not null)
        {
            try
            {
                await _heartbeatLoop.ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
            }
        }

        SessionEntry[] entries;
        await _gate.WaitAsync(CancellationToken.None).ConfigureAwait(false);
        try
        {
            entries = _sessions.Values.ToArray();
            _sessions.Clear();
        }
        finally
        {
            _gate.Release();
        }

        foreach (var entry in entries)
        {
            try
            {
                var response = await _client.NotifyTerminationAsync(entry.Endpoint, new Proto.NotifyClientTerminationRequest
                {
                    Group = string.IsNullOrEmpty(_group) ? null : Resource(_group)
                }, CancellationToken.None).ConfigureAwait(false);
                GrpcStatus.EnsureSuccess(response.Status);
            }
            catch (Exception exception)
            {
                _logger.LogDebug(exception, "Failed to notify RocketMQ endpoint {Endpoint} of client termination", entry.Endpoint);
            }

            await DisposeSessionAsync(entry).ConfigureAwait(false);
        }

        Task[] backgroundTasks;
        lock (_backgroundGate)
        {
            backgroundTasks = _backgroundTasks.ToArray();
        }
        if (backgroundTasks.Length > 0)
        {
            await Task.WhenAll(backgroundTasks).ConfigureAwait(false);
        }

        _gate.Dispose();
        _cts.Dispose();
    }

    private async Task<SessionEntry> OpenSessionAsync(
        Uri endpoint,
        Proto.Settings settings,
        CancellationToken cancellationToken)
    {
        var sessionReady = new TaskCompletionSource<IGrpcTelemetrySession>(
            TaskCreationOptions.RunContinuationsAsynchronously);
        try
        {
            var session = await _client.OpenTelemetryAsync(
                endpoint,
                settings,
                (command, token) => HandleTelemetryCommandAsync(
                    endpoint,
                    sessionReady.Task,
                    command,
                    token),
                cancellationToken).ConfigureAwait(false);
            sessionReady.TrySetResult(session);
            return new SessionEntry(endpoint, session, settings.Clone());
        }
        catch (Exception exception)
        {
            sessionReady.TrySetException(exception);
            _ = sessionReady.Task.Exception;
            throw;
        }
    }

    private async Task HandleTelemetryCommandAsync(
        Uri endpoint,
        Task<IGrpcTelemetrySession> session,
        Proto.TelemetryCommand command,
        CancellationToken cancellationToken)
    {
        if (command.CommandCase == Proto.TelemetryCommand.CommandOneofCase.ReconnectEndpointsCommand)
        {
            var key = endpoint.AbsoluteUri;
            _reconnectRequests[key] = 0;
            if (_sessions.TryGetValue(key, out var entry) && _reconnectRequests.TryRemove(key, out _))
            {
                ScheduleReplacement(entry);
            }

            return;
        }

        if (CreateDiagnosticResponse(command) is { } response)
        {
            var telemetrySession = await session.WaitAsync(cancellationToken).ConfigureAwait(false);
            await telemetrySession.WriteCommandAsync(response, cancellationToken).ConfigureAwait(false);
            return;
        }

        if (_telemetryHandler is not null)
        {
            await _telemetryHandler(endpoint, command, cancellationToken).ConfigureAwait(false);
        }
    }

    internal static Proto.TelemetryCommand? CreateDiagnosticResponse(Proto.TelemetryCommand command) =>
        command.CommandCase switch
        {
            Proto.TelemetryCommand.CommandOneofCase.PrintThreadStackTraceCommand => new Proto.TelemetryCommand
            {
                Status = new Proto.Status { Code = Proto.Code.Ok },
                ThreadStackTrace = new Proto.ThreadStackTrace
                {
                    Nonce = command.PrintThreadStackTraceCommand.Nonce,
                    ThreadStackTrace_ = Environment.StackTrace
                }
            },
            Proto.TelemetryCommand.CommandOneofCase.VerifyMessageCommand => new Proto.TelemetryCommand
            {
                Status = new Proto.Status
                {
                    Code = Proto.Code.NotImplemented,
                    Message = "Client-side message verification is not implemented."
                },
                VerifyMessageResult = new Proto.VerifyMessageResult
                {
                    Nonce = command.VerifyMessageCommand.Nonce
                }
            },
            _ => null
        };

    private void RegisterSession(SessionEntry entry)
    {
        _sessions[entry.Endpoint.AbsoluteUri] = entry;
        ObserveSession(entry);
        if (_reconnectRequests.TryRemove(entry.Endpoint.AbsoluteUri, out _))
        {
            ScheduleReplacement(entry);
        }
    }

    private void ScheduleReplacement(SessionEntry entry)
    {
        if (Volatile.Read(ref _disposed) != 0)
        {
            return;
        }

        _logger.LogInformation("RocketMQ requested telemetry reconnection for {Endpoint}", entry.Endpoint);
        TrackBackgroundTask(ReplaceSessionAfterCommandAsync(entry));
    }

    private async Task ReplaceSessionAfterCommandAsync(SessionEntry entry)
    {
        await Task.Yield();
        await ReplaceSessionAsync(entry).ConfigureAwait(false);
    }

    private void ObserveSession(SessionEntry entry)
    {
        if (Volatile.Read(ref _disposed) != 0)
        {
            return;
        }

        TrackBackgroundTask(ObserveSessionAsync(entry));
    }

    private async Task ObserveSessionAsync(SessionEntry entry)
    {
        Exception? failure = null;
        try
        {
            await entry.Session.Completion.WaitAsync(_cts.Token).ConfigureAwait(false);
        }
        catch (Exception exception)
        {
            failure = exception;
        }

        if (_cts.IsCancellationRequested)
        {
            return;
        }

        if (failure is null)
        {
            _logger.LogInformation("RocketMQ telemetry session for {Endpoint} closed; reconnecting", entry.Endpoint);
        }
        else
        {
            _logger.LogWarning(failure, "RocketMQ telemetry session for {Endpoint} failed; reconnecting", entry.Endpoint);
        }

        await ReplaceSessionAsync(entry).ConfigureAwait(false);
    }

    private async Task ReplaceSessionAsync(SessionEntry expected)
    {
        var delay = InitialReconnectDelay;
        while (!_cts.IsCancellationRequested)
        {
            try
            {
                await _gate.WaitAsync(_cts.Token).ConfigureAwait(false);
                try
                {
                    if (!_sessions.TryGetValue(expected.Endpoint.AbsoluteUri, out var current) ||
                        !ReferenceEquals(current, expected))
                    {
                        return;
                    }

                    await DisposeSessionAsync(expected).ConfigureAwait(false);
                    var settings = (_latestSettings ?? expected.Settings).Clone();
                    var replacement = await OpenSessionAsync(expected.Endpoint, settings, _cts.Token).ConfigureAwait(false);
                    if (Volatile.Read(ref _disposed) != 0)
                    {
                        await DisposeSessionAsync(replacement).ConfigureAwait(false);
                        return;
                    }

                    RegisterSession(replacement);
                    _logger.LogInformation("Re-established RocketMQ telemetry session for {Endpoint}", expected.Endpoint);
                    return;
                }
                finally
                {
                    _gate.Release();
                }
            }
            catch (OperationCanceledException) when (_cts.IsCancellationRequested)
            {
                return;
            }
            catch (Exception exception)
            {
                _logger.LogWarning(
                    exception,
                    "Failed to re-establish RocketMQ telemetry session for {Endpoint}; retrying in {Delay}",
                    expected.Endpoint,
                    delay);
            }

            try
            {
                await Task.Delay(delay, _cts.Token).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (_cts.IsCancellationRequested)
            {
                return;
            }

            delay = TimeSpan.FromMilliseconds(Math.Min(delay.TotalMilliseconds * 2, MaximumReconnectDelay.TotalMilliseconds));
        }
    }

    private async Task DisposeSessionAsync(SessionEntry entry)
    {
        try
        {
            await entry.Session.DisposeAsync().ConfigureAwait(false);
        }
        catch (Exception exception)
        {
            _logger.LogDebug(exception, "Failed to dispose RocketMQ telemetry session for {Endpoint}", entry.Endpoint);
        }
    }

    private void TrackBackgroundTask(Task task)
    {
        lock (_backgroundGate)
        {
            _backgroundTasks.Add(task);
        }

        _ = RemoveBackgroundTaskAsync(task);
    }

    private async Task RemoveBackgroundTaskAsync(Task task)
    {
        try
        {
            await task.ConfigureAwait(false);
        }
        finally
        {
            lock (_backgroundGate)
            {
                _backgroundTasks.Remove(task);
            }
        }
    }

    private async Task HeartbeatLoopAsync(CancellationToken cancellationToken)
    {
        using var timer = new PeriodicTimer(_options.HeartbeatInterval);
        while (await timer.WaitForNextTickAsync(cancellationToken).ConfigureAwait(false))
        {
            foreach (var entry in _sessions.Values)
            {
                try
                {
                    var response = await _client.HeartbeatAsync(entry.Endpoint, new Proto.HeartbeatRequest
                    {
                        ClientType = _clientType,
                        Group = string.IsNullOrEmpty(_group) ? null : Resource(_group)
                    }, cancellationToken).ConfigureAwait(false);
                    GrpcStatus.EnsureSuccess(response.Status);
                }
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                {
                    return;
                }
                catch (Exception exception)
                {
                    _logger.LogWarning(exception, "RocketMQ heartbeat failed for {Endpoint}", entry.Endpoint);
                }
            }
        }
    }

    private Proto.Resource Resource(string name) => new()
    {
        ResourceNamespace = _options.Namespace ?? string.Empty,
        Name = name
    };

    private void ThrowIfDisposed()
    {
        ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);
    }

    private sealed record SessionEntry(Uri Endpoint, IGrpcTelemetrySession Session, Proto.Settings Settings);
}
