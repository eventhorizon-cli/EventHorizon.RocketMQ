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

using System.Threading.Channels;
using Grpc.Core;
using Proto = Apache.Rocketmq.V2;

namespace EventHorizon.RocketMQ.Grpc.Protocol.Telemetry;

/// <summary>
/// Maintains one RocketMQ Telemetry RPC control stream for a gRPC endpoint.
/// </summary>
/// <remarks>
/// This protocol session synchronizes client settings and transports server commands; it is not OpenTelemetry instrumentation.
/// </remarks>
internal sealed class GrpcTelemetrySession : IGrpcTelemetrySession
{
    private static readonly Task Never = new TaskCompletionSource(
        TaskCreationOptions.RunContinuationsAsynchronously).Task;
    private static readonly TimeSpan InitializationGracePeriod = TimeSpan.FromSeconds(3);
    private readonly AsyncDuplexStreamingCall<Proto.TelemetryCommand, Proto.TelemetryCommand> _call;
    private readonly Func<Proto.TelemetryCommand, CancellationToken, Task>? _commandHandler;
    private readonly Channel<Proto.TelemetryCommand>? _commands;
    private readonly CancellationTokenSource _cts = new();
    private readonly SemaphoreSlim _writerLock = new(1, 1);
    private readonly object _disposeGate = new();
    private Task? _reader;
    private Task? _commandDispatcher;
    private Task? _disposeTask;

    public GrpcTelemetrySession(
        AsyncDuplexStreamingCall<Proto.TelemetryCommand, Proto.TelemetryCommand> call,
        Func<Proto.TelemetryCommand, CancellationToken, Task>? commandHandler)
    {
        _call = call;
        _commandHandler = commandHandler;
        if (commandHandler is not null)
        {
            _commands = Channel.CreateBounded<Proto.TelemetryCommand>(new BoundedChannelOptions(32)
            {
                FullMode = BoundedChannelFullMode.Wait,
                SingleReader = true,
                SingleWriter = true
            });
        }
    }

    public Proto.Settings? ServerSettings { get; private set; }
    public Task Completion => _reader ?? Never;

    public async Task StartAsync(Proto.Settings settings, TimeSpan timeout, CancellationToken cancellationToken)
    {
        var ready = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        if (_commands is not null)
        {
            _commandDispatcher = DispatchCommandsAsync(_commands.Reader, _cts.Token);
        }
        _reader = ReadAsync(ready, _cts.Token);
        await WriteSettingsAsync(settings, cancellationToken).ConfigureAwait(false);

        using var timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        timeoutCts.CancelAfter(timeout + InitializationGracePeriod);
        await ready.Task.WaitAsync(timeoutCts.Token).ConfigureAwait(false);
    }

    public Task WriteSettingsAsync(Proto.Settings settings, CancellationToken cancellationToken) =>
        WriteCommandAsync(new Proto.TelemetryCommand { Settings = settings }, cancellationToken);

    public async Task WriteCommandAsync(Proto.TelemetryCommand command, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(command);
        await _writerLock.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            await _call.RequestStream.WriteAsync(command, cancellationToken)
                .ConfigureAwait(false);
        }
        finally
        {
            _writerLock.Release();
        }
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
        _commands?.Writer.TryComplete();
        _cts.Cancel();
        try
        {
            await _call.RequestStream.CompleteAsync().ConfigureAwait(false);
            if (_reader is not null)
            {
                await _reader.ConfigureAwait(false);
            }
            if (_commandDispatcher is not null)
            {
                await _commandDispatcher.ConfigureAwait(false);
            }
        }
        catch (OperationCanceledException)
        {
        }
        catch (RpcException exception) when (exception.StatusCode is StatusCode.Cancelled or StatusCode.Unavailable)
        {
            // NotifyClientTermination allows the proxy to close telemetry before local disposal.
        }
        finally
        {
            _call.Dispose();
            _writerLock.Dispose();
            _cts.Dispose();
        }
    }

    private async Task ReadAsync(TaskCompletionSource ready, CancellationToken cancellationToken)
    {
        try
        {
            while (await _call.ResponseStream.MoveNext(cancellationToken).ConfigureAwait(false))
            {
                var command = _call.ResponseStream.Current;
                if (command.CommandCase == Proto.TelemetryCommand.CommandOneofCase.Settings)
                {
                    ServerSettings = command.Settings;
                    ready.TrySetResult();
                }
                else if (command.CommandCase == Proto.TelemetryCommand.CommandOneofCase.ReconnectEndpointsCommand)
                {
                    ready.TrySetException(new RpcException(new Status(
                        StatusCode.Unavailable,
                        "RocketMQ requested telemetry reconnection.")));
                    return;
                }
                else if (_commands is not null)
                {
                    await _commands.Writer.WriteAsync(command, cancellationToken).ConfigureAwait(false);
                }
            }

            ready.TrySetException(new RpcException(new Status(StatusCode.Unavailable, "Telemetry stream closed before settings were synchronized.")));
        }
        catch (Exception exception)
        {
            ready.TrySetException(exception);
            if (!cancellationToken.IsCancellationRequested)
            {
                throw;
            }
        }
        finally
        {
            _commands?.Writer.TryComplete();
        }
    }

    private async Task DispatchCommandsAsync(ChannelReader<Proto.TelemetryCommand> commands, CancellationToken cancellationToken)
    {
        try
        {
            await foreach (var command in commands.ReadAllAsync(cancellationToken).ConfigureAwait(false))
            {
                try
                {
                    await _commandHandler!(command, cancellationToken).ConfigureAwait(false);
                }
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                {
                    return;
                }
                catch
                {
                    // Individual command handlers own their logging; keep the telemetry dispatcher alive.
                }
            }
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
        }
    }
}
