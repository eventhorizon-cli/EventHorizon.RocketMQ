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

using System.Net;
using System.Net.Sockets;
using EventHorizon.RocketMQ.Grpc.Protocol;
using Grpc.Core;
using Microsoft.Extensions.Options;
using Xunit;
using Proto = Apache.Rocketmq.V2;

namespace EventHorizon.RocketMQ.Grpc.Tests.Protocol;

public sealed class RocketMQGrpcClientTests
{
    [Fact]
    public async Task QueryRouteAsync_TriesEveryAccessPointAfterTransientFailure()
    {
        var firstListener = StartListener();
        var secondListener = StartListener();
        var firstConnections = 0;
        var secondConnections = 0;
        using var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        var firstRejector = RejectConnectionsAsync(
            firstListener,
            () => Interlocked.Increment(ref firstConnections),
            cancellationTokenSource.Token);
        var secondRejector = RejectConnectionsAsync(
            secondListener,
            () => Interlocked.Increment(ref secondConnections),
            cancellationTokenSource.Token);
        await using var client = CreateClient(firstListener, secondListener);

        try
        {
            var exception = await Assert.ThrowsAsync<RpcException>(() => client.QueryRouteAsync(
                new Proto.QueryRouteRequest(),
                TestContext.Current.CancellationToken));

            Assert.True(exception.StatusCode is StatusCode.Unavailable or StatusCode.Internal);
            Assert.True(Volatile.Read(ref firstConnections) > 0);
            Assert.True(Volatile.Read(ref secondConnections) > 0);
        }
        finally
        {
            cancellationTokenSource.Cancel();
            firstListener.Stop();
            secondListener.Stop();
            await Task.WhenAll(firstRejector, secondRejector);
        }
    }

    [Fact]
    public async Task EndpointOperations_PropagateTransportFailure()
    {
        var listener = StartListener();
        using var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        var rejector = RejectConnectionsAsync(listener, static () => { }, cancellationTokenSource.Token);
        await using var client = CreateClient(listener);
        var endpoint = new Uri($"http://127.0.0.1:{((IPEndPoint)listener.LocalEndpoint).Port}");

        try
        {
            await AssertTransportFailureAsync(() => client.SendMessageAsync(
                endpoint,
                new Proto.SendMessageRequest(),
                TimeSpan.FromSeconds(1),
                TestContext.Current.CancellationToken));
            await AssertTransportFailureAsync(() => client.QueryAssignmentAsync(
                endpoint,
                new Proto.QueryAssignmentRequest(),
                TestContext.Current.CancellationToken));
            await AssertTransportFailureAsync(() => client.ReceiveMessageAsync(
                endpoint,
                new Proto.ReceiveMessageRequest(),
                TimeSpan.FromSeconds(1),
                TestContext.Current.CancellationToken));
            await AssertTransportFailureAsync(() => client.AckMessageAsync(
                endpoint,
                new Proto.AckMessageRequest(),
                TestContext.Current.CancellationToken));
            await AssertTransportFailureAsync(() => client.ChangeInvisibleDurationAsync(
                endpoint,
                new Proto.ChangeInvisibleDurationRequest(),
                TestContext.Current.CancellationToken));
            await AssertTransportFailureAsync(() => client.ForwardToDeadLetterQueueAsync(
                endpoint,
                new Proto.ForwardMessageToDeadLetterQueueRequest(),
                TestContext.Current.CancellationToken));
            await AssertTransportFailureAsync(() => client.EndTransactionAsync(
                endpoint,
                new Proto.EndTransactionRequest(),
                TestContext.Current.CancellationToken));
            await AssertTransportFailureAsync(() => client.HeartbeatAsync(
                endpoint,
                new Proto.HeartbeatRequest(),
                TestContext.Current.CancellationToken));
            await AssertTransportFailureAsync(() => client.NotifyTerminationAsync(
                endpoint,
                new Proto.NotifyClientTerminationRequest(),
                TestContext.Current.CancellationToken));
            await AssertTransportFailureAsync(() => client.RecallMessageAsync(
                endpoint,
                new Proto.RecallMessageRequest(),
                TestContext.Current.CancellationToken));
            await AssertTransportFailureAsync(() => client.SyncLiteSubscriptionAsync(
                endpoint,
                new Proto.SyncLiteSubscriptionRequest(),
                TestContext.Current.CancellationToken));
            await AssertTransportFailureAsync(() => client.OpenTelemetryAsync(
                endpoint,
                new Proto.Settings(),
                null,
                TestContext.Current.CancellationToken));
        }
        finally
        {
            cancellationTokenSource.Cancel();
            listener.Stop();
            await rejector;
        }
    }

    private static RocketMQGrpcClient CreateClient(params TcpListener[] listeners)
    {
        var options = Options.Create(new GrpcClientOptions
        {
            Endpoint = string.Join(';', listeners.Select(static listener =>
                $"127.0.0.1:{((IPEndPoint)listener.LocalEndpoint).Port}")),
            RequestTimeout = TimeSpan.FromSeconds(1)
        });
        return new RocketMQGrpcClient(options, new GrpcMetadataFactory(options, "test"));
    }

    private static TcpListener StartListener()
    {
        var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        return listener;
    }

    private static async Task AssertTransportFailureAsync<T>(Func<Task<T>> operation)
    {
        var exception = await Assert.ThrowsAsync<RpcException>(async () => await operation());

        Assert.True(exception.StatusCode is StatusCode.Unavailable or StatusCode.Internal);
    }

    private static async Task RejectConnectionsAsync(
        TcpListener listener,
        Action connectionAccepted,
        CancellationToken cancellationToken)
    {
        try
        {
            while (true)
            {
                using var connection = await listener.AcceptTcpClientAsync(cancellationToken).ConfigureAwait(false);
                connectionAccepted();
            }
        }
        catch (Exception) when (cancellationToken.IsCancellationRequested)
        {
        }
    }
}
