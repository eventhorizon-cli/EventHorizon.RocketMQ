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
using System.Net;
using System.Threading.Channels;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace EventHorizon.RocketMQ.Remoting.Protocol;

internal class RemotingClient : IRemotingClient
{
    private const int InboundRequestCapacity = 128;
    private const int InboundRequestLoopCount = 2;

    private readonly ILogger<RemotingClient> _logger;
    private readonly Dictionary<EndPoint, RemotingConnection> _connections;
    private readonly ConcurrentDictionary<EndPoint, SemaphoreSlim> _connectionLocks;
    private readonly ConcurrentDictionary<int, PendingRequest> _responseCallbacks;
    private readonly ConcurrentDictionary<int, ConcurrentDictionary<long, RemotingRequestHandler>>
        _requestHandlers;
    private readonly ConcurrentDictionary<RemotingConnection, Task> _receiveTasks;
    private readonly SemaphoreSlim _connectionsLock;
    private readonly IRemoteCommandSerializer _serializer;
    private readonly LegacyAclSigner _aclSigner;
    private readonly bool _useTls;
    private readonly Action<EndPoint, System.Net.Security.SslClientAuthenticationOptions>? _configureSslOptions;
    private readonly int _maximumFrameLength;
    private readonly Channel<InboundRequest> _inboundRequests;
    private readonly Task[] _inboundRequestLoopTasks;
    private readonly CancellationTokenSource _disposeCts = new();
    private readonly TaskCompletionSource _operationsDrained =
        new(TaskCreationOptions.RunContinuationsAsynchronously);
    private readonly Lazy<Task> _disposeTask;
    private long _nextHandlerRegistration;
    private int _activeOperations;
    private int _disposed;

    public RemotingClient(
        IRemoteCommandSerializer serializer,
        IOptions<RemotingClientOptions> options,
        ILogger<RemotingClient> logger)
    {
        ArgumentNullException.ThrowIfNull(serializer);
        ArgumentNullException.ThrowIfNull(options);
        _logger = logger;

        _connections = new Dictionary<EndPoint, RemotingConnection>();
        _connectionLocks = new ConcurrentDictionary<EndPoint, SemaphoreSlim>();
        _responseCallbacks = new ConcurrentDictionary<int, PendingRequest>();
        _requestHandlers = new ConcurrentDictionary<int, ConcurrentDictionary<long, RemotingRequestHandler>>();
        _receiveTasks = new ConcurrentDictionary<RemotingConnection, Task>();
        _connectionsLock = new SemaphoreSlim(1, 1);
        _serializer = serializer;
        _aclSigner = new LegacyAclSigner(options);
        _useTls = options.Value.UseTLS;
        _configureSslOptions = options.Value.ConfigureLegacySslOptions;
        _maximumFrameLength = options.Value.MaxRemotingFrameSize;
        _disposeTask = new Lazy<Task>(DisposeCoreAsync, LazyThreadSafetyMode.ExecutionAndPublication);
        _inboundRequests = Channel.CreateBounded<InboundRequest>(new BoundedChannelOptions(InboundRequestCapacity)
        {
            FullMode = BoundedChannelFullMode.Wait,
            SingleReader = false,
            SingleWriter = false
        });
        _inboundRequestLoopTasks = Enumerable.Range(0, InboundRequestLoopCount)
            .Select(_ => RunInboundRequestLoopAsync())
            .ToArray();

    }

    public async Task<RemotingCommand> InvokeAsync(
        EndPoint endPoint,
        RemotingCommand request,
        TimeSpan timeout,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(endPoint);
        ArgumentNullException.ThrowIfNull(request);
        EnterOperation();
        try
        {
            request.PrepareForRequest(oneway: false);
            _aclSigner.Sign(request);
            using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(
                cancellationToken,
                _disposeCts.Token);
            linkedCts.CancelAfter(timeout);
            var effectiveCancellationToken = linkedCts.Token;
            var pending = new PendingRequest();
            var opaque = request.Opaque;
            if (!_responseCallbacks.TryAdd(opaque, pending))
            {
                throw new InvalidOperationException(
                    $"A remoting request with opaque ID {opaque} is already pending.");
            }

            using var registration = effectiveCancellationToken.Register(() =>
            {
                if (TryRemovePending(opaque, pending))
                {
                    pending.Completion.TrySetCanceled(effectiveCancellationToken);
                }
            });

            try
            {
                effectiveCancellationToken.ThrowIfCancellationRequested();
                var connection = await ConnectAsync(endPoint, effectiveCancellationToken).ConfigureAwait(false);
                pending.SetConnection(connection);
                try
                {
                    await connection.SendAsync(request, effectiveCancellationToken).ConfigureAwait(false);
                }
                catch (Exception exception) when (!connection.IsUsable)
                {
                    await HandleConnectionFailureAsync(endPoint, connection, exception, pending)
                        .ConfigureAwait(false);
                    // A matching response can arrive before the concurrent receive failure cancels this flush.
                    if (pending.Completion.Task.IsCompletedSuccessfully)
                    {
                        return await pending.Completion.Task.ConfigureAwait(false);
                    }

                    throw;
                }

                return await pending.Completion.Task.ConfigureAwait(false);
            }
            finally
            {
                TryRemovePending(opaque, pending);
            }
        }
        finally
        {
            ExitOperation();
        }
    }

    public async Task InvokeOnewayAsync(
        EndPoint endPoint,
        RemotingCommand request,
        TimeSpan timeout,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(endPoint);
        ArgumentNullException.ThrowIfNull(request);
        EnterOperation();
        try
        {
            request.PrepareForRequest(oneway: true);
            _aclSigner.Sign(request);
            using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(
                cancellationToken,
                _disposeCts.Token);
            linkedCts.CancelAfter(timeout);
            var connection = await ConnectAsync(endPoint, linkedCts.Token).ConfigureAwait(false);
            try
            {
                await connection.SendAsync(request, linkedCts.Token).ConfigureAwait(false);
            }
            catch (Exception exception) when (!connection.IsUsable)
            {
                await HandleConnectionFailureAsync(endPoint, connection, exception).ConfigureAwait(false);
                throw;
            }
        }
        finally
        {
            ExitOperation();
        }
    }

    public IDisposable RegisterRequestHandler(int requestCode, RemotingRequestHandler handler)
    {
        ArgumentNullException.ThrowIfNull(handler);
        ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);
        var registrationId = Interlocked.Increment(ref _nextHandlerRegistration);
        AddRequestHandler(requestCode, registrationId, handler);
        if (Volatile.Read(ref _disposed) != 0)
        {
            RemoveRequestHandler(requestCode, registrationId, handler);
            throw new ObjectDisposedException(nameof(RemotingClient));
        }

        return new HandlerRegistration(() => RemoveRequestHandler(requestCode, registrationId, handler));
    }

    public ValueTask DisposeAsync() => new(_disposeTask.Value);

    private async Task DisposeCoreAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
        {
            return;
        }

        var disposedException = new ObjectDisposedException(nameof(RemotingClient));
        foreach (var entry in _responseCallbacks)
        {
            if (TryRemovePending(entry.Key, entry.Value))
            {
                entry.Value.Completion.TrySetException(disposedException);
            }
        }

        _inboundRequests.Writer.TryComplete();
        _disposeCts.Cancel();
        if (Volatile.Read(ref _activeOperations) == 0)
        {
            _operationsDrained.TrySetResult();
        }

        await _operationsDrained.Task.ConfigureAwait(false);

        RemotingConnection[] connections;
        await _connectionsLock.WaitAsync().ConfigureAwait(false);
        try
        {
            connections = _connections.Values.ToArray();
            _connections.Clear();
        }
        finally
        {
            _connectionsLock.Release();
        }

        List<Exception>? disposeExceptions = null;
        foreach (var connection in connections)
        {
            try
            {
                await connection.DisposeAsync().ConfigureAwait(false);
            }
            catch (Exception exception)
            {
                (disposeExceptions ??= []).Add(exception);
            }
        }

        try
        {
            await Task.WhenAll(_receiveTasks.Values).ConfigureAwait(false);
        }
        catch (Exception exception)
        {
            (disposeExceptions ??= []).Add(exception);
        }

        try
        {
            await Task.WhenAll(_inboundRequestLoopTasks).ConfigureAwait(false);
        }
        catch (Exception exception)
        {
            (disposeExceptions ??= []).Add(exception);
        }

        _requestHandlers.Clear();
        var connectionLocks = _connectionLocks.Values.Distinct().ToArray();
        _connectionLocks.Clear();
        foreach (var connectionLock in connectionLocks)
        {
            connectionLock.Dispose();
        }

        _connectionsLock.Dispose();
        _disposeCts.Dispose();
        if (disposeExceptions is not null)
        {
            throw new AggregateException("One or more remoting connections failed to close.", disposeExceptions);
        }
    }

    private async ValueTask<RemotingConnection> ConnectAsync(EndPoint endPoint, CancellationToken cancellationToken)
    {
        var connectionLock = await AcquireConnectionLockAsync(endPoint, cancellationToken).ConfigureAwait(false);
        var connected = false;
        try
        {
            ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);
            RemotingConnection? staleConnection = null;
            await _connectionsLock.WaitAsync(cancellationToken).ConfigureAwait(false);
            try
            {
                if (_connections.TryGetValue(endPoint, out var existingConnection))
                {
                    if (existingConnection.IsUsable)
                    {
                        connected = true;
                        return existingConnection;
                    }

                    _connections.Remove(endPoint);
                    staleConnection = existingConnection;
                }
            }
            finally
            {
                _connectionsLock.Release();
            }

            if (staleConnection is not null)
            {
                try
                {
                    await staleConnection.DisposeAsync().ConfigureAwait(false);
                }
                catch (Exception exception)
                {
                    _logger.LogDebug(exception, "Failed to dispose stale remoting connection {EndPoint}", endPoint);
                }
            }

            var connection = await RemotingConnection.ConnectAsync(
                endPoint,
                _useTls,
                _serializer,
                cancellationToken,
                _configureSslOptions,
                _maximumFrameLength).ConfigureAwait(false);
            try
            {
                await _connectionsLock.WaitAsync(cancellationToken).ConfigureAwait(false);
                try
                {
                    ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);
                    _connections[endPoint] = connection;
                }
                finally
                {
                    _connectionsLock.Release();
                }

                StartReceiveLoop(endPoint, connection);
                connected = true;
                return connection;
            }
            catch
            {
                await connection.DisposeAsync().ConfigureAwait(false);
                throw;
            }
        }
        finally
        {
            if (!connected)
            {
                TryRemoveConnectionLock(endPoint, connectionLock);
            }

            connectionLock.Release();
        }
    }

    private async ValueTask<SemaphoreSlim> AcquireConnectionLockAsync(
        EndPoint endPoint,
        CancellationToken cancellationToken)
    {
        while (true)
        {
            ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);
            var connectionLock = _connectionLocks.GetOrAdd(
                endPoint,
                static _ => new SemaphoreSlim(1, 1));
            await connectionLock.WaitAsync(cancellationToken).ConfigureAwait(false);
            if (_connectionLocks.TryGetValue(endPoint, out var current) &&
                ReferenceEquals(current, connectionLock))
            {
                return connectionLock;
            }

            connectionLock.Release();
        }
    }

    private async Task RemoveConnectionAsync(EndPoint endPoint, RemotingConnection connection)
    {
        if (Volatile.Read(ref _disposed) != 0)
        {
            return;
        }

        SemaphoreSlim connectionLock;
        try
        {
            connectionLock = await AcquireConnectionLockAsync(endPoint, CancellationToken.None).ConfigureAwait(false);
        }
        catch (ObjectDisposedException)
        {
            return;
        }

        try
        {
            var removeConnectionLock = false;
            await _connectionsLock.WaitAsync().ConfigureAwait(false);
            try
            {
                if (!_connections.TryGetValue(endPoint, out var current))
                {
                    removeConnectionLock = true;
                }
                else if (ReferenceEquals(current, connection))
                {
                    _connections.Remove(endPoint);
                    removeConnectionLock = true;
                }
            }
            finally
            {
                _connectionsLock.Release();
            }

            if (removeConnectionLock)
            {
                TryRemoveConnectionLock(endPoint, connectionLock);
            }
        }
        finally
        {
            connectionLock.Release();
        }
    }

    private void StartReceiveLoop(EndPoint endPoint, RemotingConnection connection)
    {
        var receiveTask = ReceiveAsync(endPoint, connection);
        if (!_receiveTasks.TryAdd(connection, receiveTask))
        {
            throw new InvalidOperationException("A receive loop is already running for this remoting connection.");
        }

        _ = RemoveCompletedReceiveTaskAsync(connection, receiveTask);
    }

    private async Task RemoveCompletedReceiveTaskAsync(RemotingConnection connection, Task receiveTask)
    {
        try
        {
            await receiveTask.ConfigureAwait(false);
        }
        catch (Exception exception)
        {
            _logger.LogError(exception, "The remoting receive loop stopped unexpectedly");
        }
        finally
        {
            TryRemoveReceiveTask(connection, receiveTask);
        }
    }

    private async Task ReceiveAsync(EndPoint endPoint, RemotingConnection connection)
    {
        while (true)
        {
            try
            {
                var command = await connection.ReceiveAsync().ConfigureAwait(false);
                if (command.IsResponseType())
                {
                    CompleteResponse(connection, command);
                    continue;
                }

                var inboundRequest = new InboundRequest(endPoint, connection, command);
                if (_inboundRequests.Writer.TryWrite(inboundRequest))
                {
                    continue;
                }

                if (Volatile.Read(ref _disposed) != 0)
                {
                    _logger.LogDebug(
                        "Dropping broker request {RequestCode} from {EndPoint} during shutdown",
                        command.Code,
                        endPoint);
                    continue;
                }

                if (command.IsOnewayRpc())
                {
                    _logger.LogWarning(
                        "Dropping broker request {RequestCode} from {EndPoint} because the inbound queue is full",
                        command.Code,
                        endPoint);
                    continue;
                }

                await SendResponseAsync(
                    connection,
                    command,
                    CreateResponse(
                        ResponseCodes.ResSystemBusy,
                        "The client is busy processing broker requests."),
                    _disposeCts.Token).ConfigureAwait(false);
            }
            catch (Exception exception)
            {
                if (Volatile.Read(ref _disposed) != 0 || exception is OperationCanceledException)
                {
                    _logger.LogDebug(exception, "Stopped receiving remoting commands from {EndPoint}", endPoint);
                }
                else
                {
                    _logger.LogError(exception, "Failed to receive a remoting command from {EndPoint}", endPoint);
                }

                await HandleConnectionFailureAsync(endPoint, connection, exception).ConfigureAwait(false);
                return;
            }
        }
    }

    private void CompleteResponse(RemotingConnection connection, RemotingCommand response)
    {
        _logger.LogDebug("Received remoting response {Opaque}", response.Opaque);
        if (!_responseCallbacks.TryGetValue(response.Opaque, out var pending))
        {
            _logger.LogDebug("Ignoring response for unknown opaque ID {Opaque}", response.Opaque);
            return;
        }

        if (!ReferenceEquals(pending.Connection, connection))
        {
            _logger.LogWarning("Ignoring response {Opaque} received on a stale remoting connection", response.Opaque);
            return;
        }

        if (TryRemovePending(response.Opaque, pending))
        {
            pending.Completion.TrySetResult(response);
        }
    }

    private async Task RunInboundRequestLoopAsync()
    {
        try
        {
            await foreach (var inboundRequest in _inboundRequests.Reader.ReadAllAsync(_disposeCts.Token))
            {
                await ProcessInboundRequestAsync(inboundRequest).ConfigureAwait(false);
            }
        }
        catch (OperationCanceledException) when (_disposeCts.IsCancellationRequested)
        {
        }
    }

    private async Task ProcessInboundRequestAsync(InboundRequest inboundRequest)
    {
        var request = inboundRequest.Request;
        try
        {
            if (!_requestHandlers.TryGetValue(request.Code, out var registrations))
            {
                await HandleUnsupportedRequestAsync(inboundRequest).ConfigureAwait(false);
                return;
            }

            var handlers = registrations.OrderBy(static entry => entry.Key)
                .Select(static entry => entry.Value)
                .ToArray();
            if (handlers.Length == 0)
            {
                await HandleUnsupportedRequestAsync(inboundRequest).ConfigureAwait(false);
                return;
            }

            var handled = false;
            var suppressResponse = false;
            RemotingCommand? response = null;
            try
            {
                var context = new RemotingRequestContext(
                    inboundRequest.EndPoint,
                    request,
                    _disposeCts.Token);
                foreach (var handler in handlers)
                {
                    var result = await handler(context).ConfigureAwait(false);
                    if (!result.IsHandled)
                    {
                        continue;
                    }

                    handled = true;
                    suppressResponse |= result.SuppressResponse;
                    var handlerResponse = result.Response;
                    if (handlerResponse is null)
                    {
                        continue;
                    }

                    if (response is not null)
                    {
                        _logger.LogWarning(
                            "Multiple handlers returned responses for broker request {RequestCode}; using the first",
                            request.Code);
                        continue;
                    }

                    response = handlerResponse;
                }
            }
            catch (OperationCanceledException) when (_disposeCts.IsCancellationRequested)
            {
                return;
            }
            catch (Exception exception)
            {
                handled = true;
                _logger.LogWarning(
                    exception,
                    "Broker request handler failed for request {RequestCode} from {EndPoint}",
                    request.Code,
                    inboundRequest.EndPoint);
                if (!request.IsOnewayRpc())
                {
                    response = CreateResponse(ResponseCodes.ResError, "The client request handler failed.");
                }
            }

            if (!handled)
            {
                await HandleUnsupportedRequestAsync(inboundRequest).ConfigureAwait(false);
                return;
            }

            if (request.IsOnewayRpc())
            {
                return;
            }

            if (request.Code == RequestCode.CheckTransactionState && suppressResponse && response is null)
            {
                return;
            }

            response ??= CreateResponse(
                ResponseCodes.ResError,
                "The client request handler did not provide a response.");

            await SendResponseAsync(
                inboundRequest.Connection,
                request,
                response,
                _disposeCts.Token).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (_disposeCts.IsCancellationRequested)
        {
        }
        catch (Exception exception)
        {
            _logger.LogError(
                exception,
                "Failed to process broker request {RequestCode} from {EndPoint}",
                request.Code,
                inboundRequest.EndPoint);
            await HandleConnectionFailureAsync(
                inboundRequest.EndPoint,
                inboundRequest.Connection,
                exception).ConfigureAwait(false);
        }
    }

    private async Task HandleUnsupportedRequestAsync(InboundRequest inboundRequest)
    {
        var request = inboundRequest.Request;
        if (request.Code == RequestCode.CheckTransactionState)
        {
            _logger.LogWarning(
                "Ignoring transaction-state check from {EndPoint}; no matching classic transactional producer is running",
                inboundRequest.EndPoint);
            return;
        }

        if (request.IsOnewayRpc())
        {
            _logger.LogWarning(
                "Ignoring unsupported one-way broker request {RequestCode} from {EndPoint}",
                request.Code,
                inboundRequest.EndPoint);
            return;
        }

        await SendResponseAsync(
            inboundRequest.Connection,
            request,
            CreateResponse(
                ResponseCodes.ResRequestCodeNotSupported,
                $"Broker request code {request.Code} is not supported by this client."),
            _disposeCts.Token).ConfigureAwait(false);
    }

    private static async ValueTask SendResponseAsync(
        RemotingConnection connection,
        RemotingCommand request,
        RemotingCommand response,
        CancellationToken cancellationToken)
    {
        response.Opaque = request.Opaque;
        response.MarkResponseType();
        await connection.SendAsync(response, cancellationToken).ConfigureAwait(false);
    }

    private static RemotingCommand CreateResponse(int code, string remark) => new()
    {
        Code = code,
        Remark = remark
    };

    private async Task HandleConnectionFailureAsync(
        EndPoint endPoint,
        RemotingConnection connection,
        Exception exception,
        PendingRequest? initiatingPending = null)
    {
        connection.Fault(exception);
        var pendingException = exception is OperationCanceledException
            ? new IOException(
                "The remoting connection was aborted while another request was being written.",
                exception)
            : exception;
        foreach (var entry in _responseCallbacks)
        {
            if (!ReferenceEquals(entry.Value, initiatingPending) &&
                ReferenceEquals(entry.Value.Connection, connection) &&
                TryRemovePending(entry.Key, entry.Value))
            {
                entry.Value.Completion.TrySetException(pendingException);
            }
        }

        try
        {
            await RemoveConnectionAsync(endPoint, connection).ConfigureAwait(false);
        }
        catch (Exception removeException)
        {
            _logger.LogDebug(
                removeException,
                "Failed to remove remoting connection {EndPoint} from the connection pool",
                endPoint);
        }
        try
        {
            await connection.DisposeAsync().ConfigureAwait(false);
        }
        catch (Exception disposeException)
        {
            _logger.LogDebug(disposeException, "Failed to dispose remoting connection {EndPoint}", endPoint);
        }
    }

    private void EnterOperation()
    {
        Interlocked.Increment(ref _activeOperations);
        if (Volatile.Read(ref _disposed) == 0)
        {
            return;
        }

        ExitOperation();
        throw new ObjectDisposedException(nameof(RemotingClient));
    }

    private void ExitOperation()
    {
        if (Interlocked.Decrement(ref _activeOperations) == 0 && Volatile.Read(ref _disposed) != 0)
        {
            _operationsDrained.TrySetResult();
        }
    }

    private void AddRequestHandler(int requestCode, RemotingRequestHandler handler) =>
        AddRequestHandler(requestCode, 0, handler);

    private void AddRequestHandler(int requestCode, long registrationId, RemotingRequestHandler handler)
    {
        var registrations = _requestHandlers.GetOrAdd(
            requestCode,
            static _ => new ConcurrentDictionary<long, RemotingRequestHandler>());
        if (!registrations.TryAdd(registrationId, handler))
        {
            throw new InvalidOperationException(
                $"A remoting request handler with registration ID {registrationId} already exists.");
        }
    }

    private void RemoveRequestHandler(int requestCode, long registrationId, RemotingRequestHandler handler)
    {
        if (_requestHandlers.TryGetValue(requestCode, out var registrations))
        {
            ((ICollection<KeyValuePair<long, RemotingRequestHandler>>)registrations).Remove(
                new KeyValuePair<long, RemotingRequestHandler>(registrationId, handler));
        }
    }

    private bool TryRemovePending(int opaque, PendingRequest pending) =>
        ((ICollection<KeyValuePair<int, PendingRequest>>)_responseCallbacks).Remove(
            new KeyValuePair<int, PendingRequest>(opaque, pending));

    private bool TryRemoveConnectionLock(EndPoint endPoint, SemaphoreSlim connectionLock) =>
        ((ICollection<KeyValuePair<EndPoint, SemaphoreSlim>>)_connectionLocks).Remove(
            new KeyValuePair<EndPoint, SemaphoreSlim>(endPoint, connectionLock));

    private bool TryRemoveReceiveTask(RemotingConnection connection, Task receiveTask) =>
        ((ICollection<KeyValuePair<RemotingConnection, Task>>)_receiveTasks).Remove(
            new KeyValuePair<RemotingConnection, Task>(connection, receiveTask));

    private readonly record struct InboundRequest(
        EndPoint EndPoint,
        RemotingConnection Connection,
        RemotingCommand Request);

    private sealed class HandlerRegistration(Action unregister) : IDisposable
    {
        private Action? _unregister = unregister;

        public void Dispose() => Interlocked.Exchange(ref _unregister, null)?.Invoke();
    }

    private sealed class PendingRequest
    {
        private RemotingConnection? _connection;

        public TaskCompletionSource<RemotingCommand> Completion { get; } =
            new(TaskCreationOptions.RunContinuationsAsynchronously);

        public RemotingConnection? Connection => Volatile.Read(ref _connection);

        public void SetConnection(RemotingConnection connection) => Volatile.Write(ref _connection, connection);
    }
}
