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
using EventHorizon.RocketMQ.Grpc.Protocol.Telemetry;
using Grpc.Core;
using Grpc.Net.Client;
using Microsoft.Extensions.Options;
using Proto = Apache.Rocketmq.V2;

namespace EventHorizon.RocketMQ.Grpc.Protocol;

internal sealed class RocketMQGrpcClient : IRocketMQGrpcClient
{
    private readonly GrpcClientOptions _options;
    private readonly GrpcMetadataFactory _metadata;
    private readonly IReadOnlyList<Uri> _accessPoints;
    private readonly ConcurrentDictionary<string, GrpcChannel> _channels = new(StringComparer.OrdinalIgnoreCase);
    private int _accessPointIndex;

    public RocketMQGrpcClient(IOptions<GrpcClientOptions> options, GrpcMetadataFactory metadata)
    {
        _options = options.Value;
        _metadata = metadata;
        _accessPoints = GrpcEndpoint.ParseMany(_options.Endpoint, _options.UseTLS);
        AccessPoint = _accessPoints[0];
        AccessPointProtobuf = GrpcEndpoint.ToProtobuf(_accessPoints);
    }

    public Uri AccessPoint { get; }
    public Proto.Endpoints AccessPointProtobuf { get; }

    public async Task<Proto.QueryRouteResponse> QueryRouteAsync(Proto.QueryRouteRequest request, CancellationToken cancellationToken)
    {
        return await InvokeAccessPointAsync(
            async (endpoint, deadline) =>
            {
                var call = GetClient(endpoint).QueryRouteAsync(request, Options(deadline, cancellationToken));
                return await call.ResponseAsync.ConfigureAwait(false);
            },
            cancellationToken).ConfigureAwait(false);
    }

    public async Task<Proto.SendMessageResponse> SendMessageAsync(Uri endpoint, Proto.SendMessageRequest request, TimeSpan timeout, CancellationToken cancellationToken)
    {
        var call = GetClient(endpoint).SendMessageAsync(request, Options(timeout, cancellationToken));
        return await call.ResponseAsync.ConfigureAwait(false);
    }

    public async Task<Proto.QueryAssignmentResponse> QueryAssignmentAsync(Uri endpoint, Proto.QueryAssignmentRequest request, CancellationToken cancellationToken)
    {
        if (_accessPoints.Contains(endpoint))
        {
            return await InvokeAccessPointAsync(
                async (accessPoint, deadline) =>
                {
                    var accessPointCall = GetClient(accessPoint).QueryAssignmentAsync(
                        request,
                        Options(deadline, cancellationToken));
                    return await accessPointCall.ResponseAsync.ConfigureAwait(false);
                },
                cancellationToken).ConfigureAwait(false);
        }

        var call = GetClient(endpoint).QueryAssignmentAsync(request, Options(_options.RequestTimeout, cancellationToken));
        return await call.ResponseAsync.ConfigureAwait(false);
    }

    public async Task<IReadOnlyList<Proto.ReceiveMessageResponse>> ReceiveMessageAsync(Uri endpoint, Proto.ReceiveMessageRequest request, TimeSpan timeout, CancellationToken cancellationToken)
    {
        using var call = GetClient(endpoint).ReceiveMessage(request, Options(timeout, cancellationToken));
        var responses = new List<Proto.ReceiveMessageResponse>();
        while (await call.ResponseStream.MoveNext(cancellationToken).ConfigureAwait(false))
        {
            responses.Add(call.ResponseStream.Current);
        }

        return responses;
    }

    public async Task<Proto.AckMessageResponse> AckMessageAsync(Uri endpoint, Proto.AckMessageRequest request, CancellationToken cancellationToken)
    {
        var call = GetClient(endpoint).AckMessageAsync(request, Options(_options.RequestTimeout, cancellationToken));
        return await call.ResponseAsync.ConfigureAwait(false);
    }

    public async Task<Proto.ChangeInvisibleDurationResponse> ChangeInvisibleDurationAsync(Uri endpoint, Proto.ChangeInvisibleDurationRequest request, CancellationToken cancellationToken)
    {
        var call = GetClient(endpoint).ChangeInvisibleDurationAsync(request, Options(_options.RequestTimeout, cancellationToken));
        return await call.ResponseAsync.ConfigureAwait(false);
    }

    public async Task<Proto.ForwardMessageToDeadLetterQueueResponse> ForwardToDeadLetterQueueAsync(Uri endpoint, Proto.ForwardMessageToDeadLetterQueueRequest request, CancellationToken cancellationToken)
    {
        var call = GetClient(endpoint).ForwardMessageToDeadLetterQueueAsync(request, Options(_options.RequestTimeout, cancellationToken));
        return await call.ResponseAsync.ConfigureAwait(false);
    }

    public async Task<Proto.EndTransactionResponse> EndTransactionAsync(Uri endpoint, Proto.EndTransactionRequest request, CancellationToken cancellationToken)
    {
        var call = GetClient(endpoint).EndTransactionAsync(request, Options(_options.RequestTimeout, cancellationToken));
        return await call.ResponseAsync.ConfigureAwait(false);
    }

    public async Task<Proto.HeartbeatResponse> HeartbeatAsync(Uri endpoint, Proto.HeartbeatRequest request, CancellationToken cancellationToken)
    {
        var call = GetClient(endpoint).HeartbeatAsync(request, Options(_options.RequestTimeout, cancellationToken));
        return await call.ResponseAsync.ConfigureAwait(false);
    }

    public async Task<Proto.NotifyClientTerminationResponse> NotifyTerminationAsync(Uri endpoint, Proto.NotifyClientTerminationRequest request, CancellationToken cancellationToken)
    {
        var call = GetClient(endpoint).NotifyClientTerminationAsync(request, Options(_options.RequestTimeout, cancellationToken));
        return await call.ResponseAsync.ConfigureAwait(false);
    }

    public async Task<Proto.RecallMessageResponse> RecallMessageAsync(Uri endpoint, Proto.RecallMessageRequest request, CancellationToken cancellationToken)
    {
        var call = GetClient(endpoint).RecallMessageAsync(request, Options(_options.RequestTimeout, cancellationToken));
        return await call.ResponseAsync.ConfigureAwait(false);
    }

    public async Task<Proto.SyncLiteSubscriptionResponse> SyncLiteSubscriptionAsync(
        Uri endpoint,
        Proto.SyncLiteSubscriptionRequest request,
        CancellationToken cancellationToken)
    {
        var call = GetClient(endpoint).SyncLiteSubscriptionAsync(request, Options(_options.RequestTimeout, cancellationToken));
        return await call.ResponseAsync.ConfigureAwait(false);
    }

    public async Task<IGrpcTelemetrySession> OpenTelemetryAsync(
        Uri endpoint,
        Proto.Settings settings,
        Func<Proto.TelemetryCommand, CancellationToken, Task>? commandHandler,
        CancellationToken cancellationToken)
    {
        // The operation token only bounds initialization. The session owns the stream lifetime thereafter.
        var call = GetClient(endpoint).Telemetry(_metadata.Create(), cancellationToken: CancellationToken.None);
        var session = new GrpcTelemetrySession(call, commandHandler);
        try
        {
            await session.StartAsync(settings, _options.RequestTimeout, cancellationToken).ConfigureAwait(false);
            return session;
        }
        catch
        {
            try
            {
                await session.DisposeAsync().ConfigureAwait(false);
            }
            catch
            {
                // Preserve the initialization error.
            }

            throw;
        }
    }

    public ValueTask DisposeAsync()
    {
        foreach (var channel in _channels.Values)
        {
            channel.Dispose();
        }

        _channels.Clear();
        return ValueTask.CompletedTask;
    }

    private Proto.MessagingService.MessagingServiceClient GetClient(Uri endpoint)
    {
        var channel = _channels.GetOrAdd(endpoint.AbsoluteUri, static address =>
            GrpcChannel.ForAddress(address, new GrpcChannelOptions { MaxRetryAttempts = 0 }));
        return new Proto.MessagingService.MessagingServiceClient(channel);
    }

    private CallOptions Options(TimeSpan timeout, CancellationToken cancellationToken) =>
        new(_metadata.Create(), DateTime.UtcNow.Add(timeout), cancellationToken);

    private CallOptions Options(DateTime deadline, CancellationToken cancellationToken) =>
        new(_metadata.Create(), deadline, cancellationToken);

    private async Task<T> InvokeAccessPointAsync<T>(
        Func<Uri, DateTime, Task<T>> operation,
        CancellationToken cancellationToken)
    {
        var deadline = DateTime.UtcNow.Add(_options.RequestTimeout);
        var start = ((Interlocked.Increment(ref _accessPointIndex) - 1) & int.MaxValue) % _accessPoints.Count;
        Exception? lastException = null;
        for (var index = 0; index < _accessPoints.Count; index++)
        {
            var endpoint = _accessPoints[(start + index) % _accessPoints.Count];
            try
            {
                return await operation(endpoint, deadline).ConfigureAwait(false);
            }
            catch (Exception exception) when (
                index + 1 < _accessPoints.Count &&
                IsTransientAccessPointFailure(exception, cancellationToken))
            {
                lastException = exception;
            }
        }

        throw lastException ?? new InvalidOperationException("No RocketMQ access point is available.");
    }

    private static bool IsTransientAccessPointFailure(
        Exception exception,
        CancellationToken cancellationToken)
    {
        if (cancellationToken.IsCancellationRequested)
        {
            return false;
        }

        return exception is IOException or HttpRequestException or TimeoutException ||
               exception is RpcException rpcException && rpcException.StatusCode is
                   StatusCode.Cancelled or
                   StatusCode.DeadlineExceeded or
                   StatusCode.Internal or
                   StatusCode.ResourceExhausted or
                   StatusCode.Unavailable or
                   StatusCode.Unknown;
    }
}
