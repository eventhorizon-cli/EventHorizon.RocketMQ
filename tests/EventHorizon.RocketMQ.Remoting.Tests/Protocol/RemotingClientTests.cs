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

using System.Buffers;
using System.Net;
using System.Net.Security;
using System.Net.Sockets;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using EventHorizon.RocketMQ.Remoting.Protocol;
using Microsoft.Extensions.Options;
using Xunit;

namespace EventHorizon.RocketMQ.Remoting.Tests.Protocol;

public sealed class RemotingClientTests
{
    private static readonly TimeSpan OperationTimeout = TimeSpan.FromSeconds(5);

    [Fact]
    public async Task InvokeAsync_RoundTripsFragmentedResponse()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        await using var server = new LoopbackServer(cancellationToken);
        var request = CreateRequest("fragmented-request");
        var serverTask = ServeAsync();
        await using var client = CreateClient();

        var response = await client.InvokeAsync(
            server.EndPoint,
            request,
            OperationTimeout,
            cancellationToken);

        Assert.Equal(request.Opaque, response.Opaque);
        Assert.Equal("fragmented-response", Encoding.UTF8.GetString(response.Body!));
        await serverTask;

        async Task ServeAsync()
        {
            using var socket = await server.AcceptAsync();
            var connection = new CommandConnection(socket.GetStream());
            var received = await connection.ReadAsync(server.CancellationToken);
            Assert.Equal(request.Opaque, received.Opaque);
            Assert.Equal("fragmented-request", Encoding.UTF8.GetString(received.Body!));

            await connection.WriteFragmentedAsync(
                CreateResponse(received, "fragmented-response"),
                server.CancellationToken);
        }
    }

    [Fact]
    public async Task InvokeAsync_RoundTripsLargeFrameOverNetworkStream()
    {
        const int BodyLength = 512 * 1024;
        var cancellationToken = TestContext.Current.CancellationToken;
        await using var server = new LoopbackServer(cancellationToken);
        var requestBody = new byte[BodyLength];
        var responseBody = new byte[BodyLength];
        for (var index = 0; index < BodyLength; index++)
        {
            requestBody[index] = (byte)(index % 251);
            responseBody[index] = (byte)(250 - (index % 251));
        }

        var request = new RemotingCommand(
            RequestCode.GetRouteInfoByTopic,
            new TestHeader { Topic = "large-loopback" })
        {
            Body = requestBody
        };
        var serverTask = ServeAsync();
        await using var client = CreateClient();

        var response = await client.InvokeAsync(
            server.EndPoint,
            request,
            OperationTimeout,
            cancellationToken);

        Assert.Equal(request.Opaque, response.Opaque);
        Assert.Equal<byte>(responseBody, response.Body!);
        await serverTask;

        async Task ServeAsync()
        {
            using var socket = await server.AcceptAsync();
            var connection = new CommandConnection(socket.GetStream());
            var received = await connection.ReadAsync(server.CancellationToken);
            Assert.Equal(request.Opaque, received.Opaque);
            Assert.Equal<byte>(requestBody, received.Body!);
            await connection.WriteAsync(
                new RemotingCommand
                {
                    Code = ResponseCodes.ResSuccess,
                    Opaque = received.Opaque,
                    Flag = 1,
                    Body = responseBody
                },
                server.CancellationToken);
        }
    }

    [Fact]
    public async Task InvokeAsync_UsesTlsWithLocalhostSni()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        await using var server = new LoopbackServer(cancellationToken);
        using var privateKey = RSA.Create(2048);
        using var certificate = CreateLocalhostCertificate(privateKey);
        var sni = new TaskCompletionSource<string?>(TaskCreationOptions.RunContinuationsAsynchronously);
        var request = CreateRequest("tls-request");
        var serverTask = ServeAsync();
        var validationCalls = 0;
        var endPoint = server.EndPoint;
        var originalPort = endPoint.Port;
        await using var client = CreateTlsClient(
            certificate,
            () => Interlocked.Increment(ref validationCalls),
            targetHost: "localhost",
            onConfigure: configuredEndPoint =>
            {
                Assert.NotSame(endPoint, configuredEndPoint);
                Assert.IsType<IPEndPoint>(configuredEndPoint).Port = 1;
            });

        var response = await client.InvokeAsync(
            endPoint,
            request,
            OperationTimeout,
            cancellationToken);

        Assert.Equal(request.Opaque, response.Opaque);
        Assert.Equal("tls-response", Encoding.UTF8.GetString(response.Body!));
        Assert.Equal("localhost", await sni.Task.WaitAsync(OperationTimeout, cancellationToken));
        Assert.Equal(1, Volatile.Read(ref validationCalls));
        Assert.Equal(originalPort, endPoint.Port);
        await serverTask;

        async Task ServeAsync()
        {
            using var socket = await server.AcceptAsync();
            await using var sslStream = new SslStream(socket.GetStream(), leaveInnerStreamOpen: false);
            await sslStream.AuthenticateAsServerAsync(
                new SslServerAuthenticationOptions
                {
                    ClientCertificateRequired = false,
                    ServerCertificateSelectionCallback = (_, hostName) =>
                    {
                        sni.TrySetResult(hostName);
                        return certificate;
                    }
                },
                server.CancellationToken);
            var connection = new CommandConnection(sslStream);
            var received = await connection.ReadAsync(server.CancellationToken);
            Assert.Equal(request.Opaque, received.Opaque);
            Assert.Equal("tls-request", Encoding.UTF8.GetString(received.Body!));
            await connection.WriteAsync(CreateResponse(received, "tls-response"), server.CancellationToken);
        }
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task InvokeAsync_CancelsWhileTlsHandshakeIsStalled(bool callerCancellation)
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        await using var server = new LoopbackServer(cancellationToken);
        var accepted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseServer = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var serverTask = ServeAsync();
        await using var client = CreateClient(useTls: true);
        using var callerCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        var invocation = client.InvokeAsync(
            new DnsEndPoint("localhost", server.EndPoint.Port),
            CreateRequest("stalled-tls"),
            callerCancellation ? OperationTimeout : TimeSpan.FromMilliseconds(250),
            callerCts.Token);

        await accepted.Task.WaitAsync(OperationTimeout, cancellationToken);
        if (callerCancellation)
        {
            callerCts.Cancel();
        }

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => invocation);
        releaseServer.TrySetResult();
        await serverTask;

        async Task ServeAsync()
        {
            using var socket = await server.AcceptAsync();
            accepted.TrySetResult();
            await releaseServer.Task.WaitAsync(OperationTimeout, server.CancellationToken);
        }
    }

    [Fact]
    public async Task StalledTlsHandshake_DoesNotBlockDifferentEndpoint()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        await using var stalledServer = new LoopbackServer(cancellationToken);
        await using var healthyServer = new LoopbackServer(cancellationToken);
        using var privateKey = RSA.Create(2048);
        using var certificate = CreateLocalhostCertificate(privateKey);
        var stalledAccepted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseStalledServer = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var stalledServerTask = ServeStalledAsync();
        var healthyServerTask = ServeHealthyAsync();
        await using var client = CreateTlsClient(certificate, static () => { });
        using var stalledCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        var stalledInvocation = client.InvokeAsync(
            new DnsEndPoint("localhost", stalledServer.EndPoint.Port),
            CreateRequest("stalled-endpoint"),
            OperationTimeout,
            stalledCts.Token);

        try
        {
            await stalledAccepted.Task.WaitAsync(OperationTimeout, cancellationToken);
            var healthyRequest = CreateRequest("healthy-endpoint");
            var response = await client.InvokeAsync(
                new DnsEndPoint("localhost", healthyServer.EndPoint.Port),
                healthyRequest,
                OperationTimeout,
                cancellationToken);

            Assert.Equal(healthyRequest.Opaque, response.Opaque);
            Assert.Equal("healthy-response", Encoding.UTF8.GetString(response.Body!));
        }
        finally
        {
            stalledCts.Cancel();
            try
            {
                await Assert.ThrowsAnyAsync<OperationCanceledException>(() => stalledInvocation);
            }
            finally
            {
                releaseStalledServer.TrySetResult();
                await Task.WhenAll(stalledServerTask, healthyServerTask);
            }
        }

        async Task ServeStalledAsync()
        {
            using var socket = await stalledServer.AcceptAsync();
            stalledAccepted.TrySetResult();
            await releaseStalledServer.Task.WaitAsync(OperationTimeout, stalledServer.CancellationToken);
        }

        async Task ServeHealthyAsync()
        {
            using var socket = await healthyServer.AcceptAsync();
            await using var sslStream = new SslStream(socket.GetStream(), leaveInnerStreamOpen: false);
            await sslStream.AuthenticateAsServerAsync(
                new SslServerAuthenticationOptions { ServerCertificate = certificate },
                healthyServer.CancellationToken);
            var connection = new CommandConnection(sslStream);
            var request = await connection.ReadAsync(healthyServer.CancellationToken);
            await connection.WriteAsync(CreateResponse(request, "healthy-response"), healthyServer.CancellationToken);
        }
    }

    [Fact]
    public async Task DisposeAsync_CancelsStalledTlsHandshakeWithInfiniteRequestTimeout()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        await using var server = new LoopbackServer(cancellationToken);
        var accepted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseServer = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var serverTask = ServeAsync();
        var client = CreateClient(useTls: true);
        var invocation = client.InvokeAsync(
            new DnsEndPoint("localhost", server.EndPoint.Port),
            CreateRequest("dispose-stalled-tls"),
            Timeout.InfiniteTimeSpan,
            CancellationToken.None);
        Task? disposal = null;

        try
        {
            await accepted.Task.WaitAsync(OperationTimeout, cancellationToken);
            disposal = client.DisposeAsync().AsTask();
            var concurrentDisposal = client.DisposeAsync().AsTask();
            Assert.Same(disposal, concurrentDisposal);
            await disposal.WaitAsync(TimeSpan.FromSeconds(2), cancellationToken);
            var exception = await Record.ExceptionAsync(async () =>
                await invocation.WaitAsync(OperationTimeout, cancellationToken));
            Assert.True(
                exception is ObjectDisposedException or OperationCanceledException,
                $"Expected disposal or cancellation, but received {exception?.GetType().FullName ?? "no exception"}.");
        }
        finally
        {
            releaseServer.TrySetResult();
            await serverTask;
            if (disposal is not null)
            {
                await disposal.WaitAsync(OperationTimeout, cancellationToken);
            }

            try
            {
                await invocation;
            }
            catch (Exception)
            {
                // The invocation is expected to fail when disposal aborts its TLS handshake.
            }

            await client.DisposeAsync();
        }

        async Task ServeAsync()
        {
            using var socket = await server.AcceptAsync();
            accepted.TrySetResult();
            await releaseServer.Task.WaitAsync(OperationTimeout, server.CancellationToken);
        }
    }

    [Fact]
    public async Task ConcurrentRequests_PreserveFramesAndMatchOutOfOrderResponses()
    {
        const int requestCount = 16;
        var cancellationToken = TestContext.Current.CancellationToken;
        await using var server = new LoopbackServer(cancellationToken);
        var expectedBodies = Enumerable.Range(0, requestCount)
            .Select(index => $"request-{index:D2}-{new string((char)('a' + index), 4096)}")
            .ToArray();
        var requests = expectedBodies.Select(CreateRequest).ToArray();
        var serverTask = ServeAsync();
        await using var client = CreateClient();

        var responses = await Task.WhenAll(requests.Select(request => client.InvokeAsync(
            server.EndPoint,
            request,
            OperationTimeout,
            cancellationToken)));

        for (var index = 0; index < responses.Length; index++)
        {
            Assert.Equal(requests[index].Opaque, responses[index].Opaque);
            Assert.Equal($"response-{index:D2}", Encoding.UTF8.GetString(responses[index].Body!));
        }

        await serverTask;

        async Task ServeAsync()
        {
            using var socket = await server.AcceptAsync();
            var connection = new CommandConnection(socket.GetStream());
            var received = new List<RemotingCommand>(requestCount);
            for (var index = 0; index < requestCount; index++)
            {
                received.Add(await connection.ReadAsync(server.CancellationToken));
            }

            Assert.Equal(requestCount, received.Select(static request => request.Opaque).Distinct().Count());
            Assert.Equal(
                expectedBodies.Order(StringComparer.Ordinal),
                received.Select(static request => Encoding.UTF8.GetString(request.Body!))
                    .Order(StringComparer.Ordinal));

            var responseByOpaque = requests
                .Select((request, index) => (request.Opaque, Body: $"response-{index:D2}"))
                .ToDictionary(static item => item.Opaque, static item => item.Body);
            var responses = received
                .AsEnumerable()
                .Reverse()
                .Select(request => CreateResponse(request, responseByOpaque[request.Opaque]));
            await connection.WriteManyAsync(responses, server.CancellationToken);
        }
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task CanceledRequest_IsRemovedAndLateResponseDoesNotBlockNextRequest(bool callerCancellation)
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        await using var server = new LoopbackServer(cancellationToken);
        var firstReceived = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var serverTask = ServeAsync();
        await using var client = CreateClient();
        using var callerCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        var firstRequest = CreateRequest(callerCancellation ? "caller-cancel" : "timeout");
        var firstTask = client.InvokeAsync(
            server.EndPoint,
            firstRequest,
            callerCancellation ? OperationTimeout : TimeSpan.FromMilliseconds(250),
            callerCts.Token);

        await firstReceived.Task.WaitAsync(OperationTimeout, cancellationToken);
        if (callerCancellation)
        {
            callerCts.Cancel();
        }

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => firstTask);

        var secondRequest = CreateRequest("after-cancellation");
        var secondResponse = await client.InvokeAsync(
            server.EndPoint,
            secondRequest,
            OperationTimeout,
            cancellationToken);

        Assert.Equal(secondRequest.Opaque, secondResponse.Opaque);
        Assert.Equal("second-response", Encoding.UTF8.GetString(secondResponse.Body!));
        await serverTask;

        async Task ServeAsync()
        {
            using var socket = await server.AcceptAsync();
            var connection = new CommandConnection(socket.GetStream());
            var first = await connection.ReadAsync(server.CancellationToken);
            firstReceived.TrySetResult();
            var second = await connection.ReadAsync(server.CancellationToken);
            await connection.WriteManyAsync(
                [CreateResponse(first, "late-response"), CreateResponse(second, "second-response")],
                server.CancellationToken);
        }
    }

    [Fact]
    public async Task RemoteDisconnect_FailsPendingRequestAndNextRequestReconnects()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        await using var server = new LoopbackServer(cancellationToken);
        var failedRequest = CreateRequest("disconnect");
        var serverTask = ServeAsync();
        await using var client = CreateClient();

        var exception = await Record.ExceptionAsync(async () => await client.InvokeAsync(
            server.EndPoint,
            failedRequest,
            OperationTimeout,
            cancellationToken));

        Assert.NotNull(exception);
        Assert.IsNotType<OperationCanceledException>(exception);

        var retryRequest = CreateRequest("reconnect");
        var response = await client.InvokeAsync(
            server.EndPoint,
            retryRequest,
            OperationTimeout,
            cancellationToken);

        Assert.Equal(retryRequest.Opaque, response.Opaque);
        Assert.Equal("reconnected", Encoding.UTF8.GetString(response.Body!));
        await serverTask;

        async Task ServeAsync()
        {
            using (var firstSocket = await server.AcceptAsync())
            {
                var firstConnection = new CommandConnection(firstSocket.GetStream());
                var first = await firstConnection.ReadAsync(server.CancellationToken);
                Assert.Equal(failedRequest.Opaque, first.Opaque);
            }

            using var secondSocket = await server.AcceptAsync();
            var secondConnection = new CommandConnection(secondSocket.GetStream());
            var second = await secondConnection.ReadAsync(server.CancellationToken);
            await secondConnection.WriteAsync(
                CreateResponse(second, "reconnected"),
                server.CancellationToken);
        }
    }

    [Fact]
    public async Task PartialResponseFrame_FailsPendingRequestImmediately()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        await using var server = new LoopbackServer(cancellationToken);
        var serverTask = ServeAsync();
        await using var client = CreateClient();

        var exception = await Assert.ThrowsAsync<InvalidDataException>(() => client.InvokeAsync(
            server.EndPoint,
            CreateRequest("partial-response"),
            OperationTimeout,
            cancellationToken));

        Assert.Contains("middle of a frame", exception.Message);
        await serverTask;

        async Task ServeAsync()
        {
            using var socket = await server.AcceptAsync();
            var stream = socket.GetStream();
            var connection = new CommandConnection(stream);
            await connection.ReadAsync(server.CancellationToken);
            byte[] partialFrame = [0, 0, 0, 100, 1, 2, 3];
            await stream.WriteAsync(partialFrame, server.CancellationToken);
        }
    }

    [Fact]
    public async Task BrokerRequest_WithPendingOpaque_DoesNotCompletePendingInvocation()
    {
        const int BrokerRequestCode = 200050;
        var cancellationToken = TestContext.Current.CancellationToken;
        await using var server = new LoopbackServer(cancellationToken);
        var handlerInvoked = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var allowActualResponse = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var serverTask = ServeAsync();
        await using var client = CreateClient();
        using var registration = client.RegisterRequestHandler(BrokerRequestCode, context =>
        {
            handlerInvoked.TrySetResult();
            return ValueTask.FromResult(RemotingRequestResult.NoResponse);
        });
        var invocation = client.InvokeAsync(
            server.EndPoint,
            CreateRequest("pending-request"),
            OperationTimeout,
            cancellationToken);

        try
        {
            await handlerInvoked.Task.WaitAsync(OperationTimeout, cancellationToken);
            Assert.False(invocation.IsCompleted);
        }
        finally
        {
            allowActualResponse.TrySetResult();
        }

        var response = await invocation;
        Assert.Equal("actual-response", Encoding.UTF8.GetString(response.Body!));
        await serverTask;

        async Task ServeAsync()
        {
            using var socket = await server.AcceptAsync();
            var connection = new CommandConnection(socket.GetStream());
            var request = await connection.ReadAsync(server.CancellationToken);
            await connection.WriteAsync(
                CreateBrokerRequest(BrokerRequestCode, request.Opaque, oneway: true),
                server.CancellationToken);
            await handlerInvoked.Task.WaitAsync(OperationTimeout, server.CancellationToken);
            await allowActualResponse.Task.WaitAsync(OperationTimeout, server.CancellationToken);
            await connection.WriteAsync(CreateResponse(request, "actual-response"), server.CancellationToken);
        }
    }

    [Fact]
    public async Task BrokerOnewayRequest_InvokesRegisteredHandlerWithoutWritingResponse()
    {
        const int BrokerRequestCode = 1201;
        var cancellationToken = TestContext.Current.CancellationToken;
        await using var server = new LoopbackServer(cancellationToken);
        var handlerInvoked = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var serverTask = ServeAsync();
        await using var client = CreateClient();
        using var registration = client.RegisterRequestHandler(BrokerRequestCode, context =>
        {
            handlerInvoked.TrySetResult();
            return ValueTask.FromResult(RemotingRequestResult.Respond(new RemotingCommand
            {
                Code = ResponseCodes.ResSuccess
            }));
        });

        var response = await client.InvokeAsync(
            server.EndPoint,
            CreateRequest("open-connection"),
            OperationTimeout,
            cancellationToken);

        Assert.Equal("normal-response", Encoding.UTF8.GetString(response.Body!));
        await serverTask;

        async Task ServeAsync()
        {
            using var socket = await server.AcceptAsync();
            var connection = new CommandConnection(socket.GetStream());
            var request = await connection.ReadAsync(server.CancellationToken);
            await connection.WriteAsync(
                CreateBrokerRequest(BrokerRequestCode, opaque: 7001, oneway: true),
                server.CancellationToken);
            await handlerInvoked.Task.WaitAsync(OperationTimeout, server.CancellationToken);

            using var noResponseCts = CancellationTokenSource.CreateLinkedTokenSource(server.CancellationToken);
            noResponseCts.CancelAfter(TimeSpan.FromMilliseconds(250));
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() =>
                connection.ReadAsync(noResponseCts.Token));

            await connection.WriteAsync(CreateResponse(request, "normal-response"), server.CancellationToken);
        }
    }

    [Fact]
    public async Task UnknownBrokerRequest_ReturnsNotSupportedResponse()
    {
        const int UnknownRequestCode = 1202;
        const int BrokerOpaque = 7002;
        var cancellationToken = TestContext.Current.CancellationToken;
        await using var server = new LoopbackServer(cancellationToken);
        var serverTask = ServeAsync();
        await using var client = CreateClient();

        var response = await client.InvokeAsync(
            server.EndPoint,
            CreateRequest("open-connection"),
            OperationTimeout,
            cancellationToken);

        Assert.Equal("normal-response", Encoding.UTF8.GetString(response.Body!));
        await serverTask;

        async Task ServeAsync()
        {
            using var socket = await server.AcceptAsync();
            var connection = new CommandConnection(socket.GetStream());
            var request = await connection.ReadAsync(server.CancellationToken);
            await connection.WriteAsync(
                CreateBrokerRequest(UnknownRequestCode, BrokerOpaque),
                server.CancellationToken);

            var unsupportedResponse = await connection.ReadAsync(server.CancellationToken);
            Assert.Equal(ResponseCodes.ResRequestCodeNotSupported, unsupportedResponse.Code);
            Assert.Equal(BrokerOpaque, unsupportedResponse.Opaque);
            Assert.True(unsupportedResponse.IsResponseType());
            Assert.False(unsupportedResponse.IsOnewayRpc());

            await connection.WriteAsync(CreateResponse(request, "normal-response"), server.CancellationToken);
        }
    }

    [Fact]
    public async Task SyncHandlerWithoutResponse_ReturnsErrorInsteadOfTimingOut()
    {
        const int BrokerRequestCode = 1205;
        const int BrokerOpaque = 7005;
        var cancellationToken = TestContext.Current.CancellationToken;
        await using var server = new LoopbackServer(cancellationToken);
        var serverTask = ServeAsync();
        await using var client = CreateClient();
        using var registration = client.RegisterRequestHandler(
            BrokerRequestCode,
            static _ => ValueTask.FromResult(RemotingRequestResult.NoResponse));

        var response = await client.InvokeAsync(
            server.EndPoint,
            CreateRequest("open-connection"),
            OperationTimeout,
            cancellationToken);

        Assert.Equal("normal-response", Encoding.UTF8.GetString(response.Body!));
        await serverTask;

        async Task ServeAsync()
        {
            using var socket = await server.AcceptAsync();
            var connection = new CommandConnection(socket.GetStream());
            var request = await connection.ReadAsync(server.CancellationToken);
            await connection.WriteAsync(
                CreateBrokerRequest(BrokerRequestCode, BrokerOpaque),
                server.CancellationToken);

            var handlerError = await connection.ReadAsync(server.CancellationToken);
            Assert.Equal(ResponseCodes.ResError, handlerError.Code);
            Assert.Equal(BrokerOpaque, handlerError.Opaque);
            Assert.True(handlerError.IsResponseType());
            await connection.WriteAsync(CreateResponse(request, "normal-response"), server.CancellationToken);
        }
    }

    [Fact]
    public async Task SlowBrokerRequestHandler_DoesNotBlockNormalResponse()
    {
        const int BrokerRequestCode = 1203;
        var cancellationToken = TestContext.Current.CancellationToken;
        await using var server = new LoopbackServer(cancellationToken);
        var handlerEntered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseHandler = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var serverTask = ServeAsync();
        await using var client = CreateClient();
        using var registration = client.RegisterRequestHandler(BrokerRequestCode, async context =>
        {
            handlerEntered.TrySetResult();
            await releaseHandler.Task.WaitAsync(context.CancellationToken);
            return RemotingRequestResult.NoResponse;
        });
        var invocation = client.InvokeAsync(
            server.EndPoint,
            CreateRequest("normal-request"),
            OperationTimeout,
            cancellationToken);

        try
        {
            await handlerEntered.Task.WaitAsync(OperationTimeout, cancellationToken);
            var response = await invocation.WaitAsync(TimeSpan.FromSeconds(2), cancellationToken);
            Assert.Equal("normal-response", Encoding.UTF8.GetString(response.Body!));
        }
        finally
        {
            releaseHandler.TrySetResult();
        }

        await serverTask;

        async Task ServeAsync()
        {
            using var socket = await server.AcceptAsync();
            var connection = new CommandConnection(socket.GetStream());
            var request = await connection.ReadAsync(server.CancellationToken);
            await connection.WriteAsync(
                CreateBrokerRequest(BrokerRequestCode, opaque: 7003, oneway: true),
                server.CancellationToken);
            await handlerEntered.Task.WaitAsync(OperationTimeout, server.CancellationToken);
            await connection.WriteAsync(CreateResponse(request, "normal-response"), server.CancellationToken);
        }
    }

    [Fact]
    public async Task ThrowingBrokerRequestHandler_ReturnsErrorAndKeepsConnectionUsable()
    {
        const int BrokerRequestCode = 1204;
        const int BrokerOpaque = 7004;
        var cancellationToken = TestContext.Current.CancellationToken;
        await using var server = new LoopbackServer(cancellationToken);
        var serverTask = ServeAsync();
        await using var client = CreateClient();
        using var registration = client.RegisterRequestHandler(
            BrokerRequestCode,
            static _ => ValueTask.FromException<RemotingRequestResult>(
                new InvalidOperationException("handler failed")));

        var firstResponse = await client.InvokeAsync(
            server.EndPoint,
            CreateRequest("first-request"),
            OperationTimeout,
            cancellationToken);
        Assert.Equal("first-response", Encoding.UTF8.GetString(firstResponse.Body!));

        var secondResponse = await client.InvokeAsync(
            server.EndPoint,
            CreateRequest("second-request"),
            OperationTimeout,
            cancellationToken);
        Assert.Equal("second-response", Encoding.UTF8.GetString(secondResponse.Body!));
        await serverTask;

        async Task ServeAsync()
        {
            using var socket = await server.AcceptAsync();
            var connection = new CommandConnection(socket.GetStream());
            var firstRequest = await connection.ReadAsync(server.CancellationToken);
            await connection.WriteAsync(
                CreateBrokerRequest(BrokerRequestCode, BrokerOpaque),
                server.CancellationToken);

            var handlerError = await connection.ReadAsync(server.CancellationToken);
            Assert.Equal(ResponseCodes.ResError, handlerError.Code);
            Assert.Equal(BrokerOpaque, handlerError.Opaque);
            Assert.True(handlerError.IsResponseType());
            await connection.WriteAsync(
                CreateResponse(firstRequest, "first-response"),
                server.CancellationToken);

            var secondRequest = await connection.ReadAsync(server.CancellationToken);
            await connection.WriteAsync(
                CreateResponse(secondRequest, "second-response"),
                server.CancellationToken);
        }
    }

    [Fact]
    public async Task InvokeOnewayAsync_SetsOnewayFlagAndWritesCompleteFrame()
    {
        const int OnewayFlag = 1 << 1;
        var cancellationToken = TestContext.Current.CancellationToken;
        await using var server = new LoopbackServer(cancellationToken);
        var request = CreateRequest("one-way");
        var serverTask = ServeAsync();
        await using var client = CreateClient();

        await client.InvokeOnewayAsync(
            server.EndPoint,
            request,
            OperationTimeout,
            cancellationToken);

        await serverTask;

        async Task ServeAsync()
        {
            using var socket = await server.AcceptAsync();
            var connection = new CommandConnection(socket.GetStream());
            var received = await connection.ReadAsync(server.CancellationToken);
            Assert.Equal(request.Opaque, received.Opaque);
            Assert.Equal(OnewayFlag, received.Flag & OnewayFlag);
            Assert.Equal("one-way", Encoding.UTF8.GetString(received.Body!));
        }
    }

    [Fact]
    public async Task DisposeAsync_IsIdempotentAndRejectsFurtherRequests()
    {
        var client = CreateClient();

        await client.DisposeAsync();
        await client.DisposeAsync();

        await Assert.ThrowsAsync<ObjectDisposedException>(() => client.InvokeAsync(
            new IPEndPoint(IPAddress.Loopback, 1),
            CreateRequest("disposed"),
            OperationTimeout,
            TestContext.Current.CancellationToken));
    }

    private static RemotingClient CreateClient(bool useTls = false) => new(
        new RemoteCommandSerializer(),
        Options.Create(new RemotingClientOptions { UseTLS = useTls }));

    private static RemotingClient CreateTlsClient(
        X509Certificate2 trustedCertificate,
        Action onValidation,
        string? targetHost = null,
        Action<EndPoint>? onConfigure = null) => new(
        new RemoteCommandSerializer(),
        Options.Create(new RemotingClientOptions
        {
            UseTLS = true,
            ConfigureLegacySslOptions = (endPoint, sslOptions) =>
            {
                onConfigure?.Invoke(endPoint);
                if (targetHost is not null)
                {
                    sslOptions.TargetHost = targetHost;
                }

                sslOptions.RemoteCertificateValidationCallback = (_, presentedCertificate, _, _) =>
                {
                    onValidation();
                    return presentedCertificate is not null && CryptographicOperations.FixedTimeEquals(
                        presentedCertificate.GetRawCertData(),
                        trustedCertificate.RawData);
                };
            }
        }));

    private static X509Certificate2 CreateLocalhostCertificate(RSA privateKey)
    {
        var request = new CertificateRequest(
            "CN=localhost",
            privateKey,
            HashAlgorithmName.SHA256,
            RSASignaturePadding.Pkcs1);
        request.CertificateExtensions.Add(new X509BasicConstraintsExtension(false, false, 0, false));
        request.CertificateExtensions.Add(new X509KeyUsageExtension(X509KeyUsageFlags.DigitalSignature, false));
        var subjectAlternativeNames = new SubjectAlternativeNameBuilder();
        subjectAlternativeNames.AddDnsName("localhost");
        request.CertificateExtensions.Add(subjectAlternativeNames.Build());
        request.CertificateExtensions.Add(new X509SubjectKeyIdentifierExtension(request.PublicKey, false));
        return request.CreateSelfSigned(
            DateTimeOffset.UtcNow.AddMinutes(-1),
            DateTimeOffset.UtcNow.AddHours(1));
    }

    private static RemotingCommand CreateRequest(string body) => new(
        RequestCode.GetRouteInfoByTopic,
        new TestHeader { Topic = "loopback" })
    {
        Body = Encoding.UTF8.GetBytes(body)
    };

    private static RemotingCommand CreateResponse(RemotingCommand request, string body) => new()
    {
        Code = ResponseCodes.ResSuccess,
        Opaque = request.Opaque,
        Flag = 1,
        Body = Encoding.UTF8.GetBytes(body)
    };

    private static RemotingCommand CreateBrokerRequest(int code, int opaque, bool oneway = false)
    {
        var request = new RemotingCommand
        {
            Code = code,
            Opaque = opaque
        };
        if (oneway)
        {
            request.MarkOnewayRpc();
        }

        return request;
    }

    private sealed class TestHeader : CommandCustomHeader
    {
        public required string Topic { get; init; }
    }

    private sealed class CommandConnection(Stream stream)
    {
        private readonly RemoteCommandSerializer _serializer = new();
        private byte[] _buffer = new byte[1024];
        private int _bufferedLength;

        public async Task<RemotingCommand> ReadAsync(CancellationToken cancellationToken)
        {
            while (true)
            {
                var input = new ReadOnlySequence<byte>(_buffer.AsMemory(0, _bufferedLength));
                var consumed = input.Start;
                var examined = input.Start;
                if (_serializer.TryParseMessage(input, ref consumed, ref examined, out var command))
                {
                    var consumedLength = checked((int)input.Slice(0, consumed).Length);
                    _buffer.AsSpan(consumedLength, _bufferedLength - consumedLength).CopyTo(_buffer);
                    _bufferedLength -= consumedLength;
                    return command;
                }

                if (_bufferedLength == _buffer.Length)
                {
                    Array.Resize(ref _buffer, checked(_buffer.Length * 2));
                }

                var read = await stream.ReadAsync(
                    _buffer.AsMemory(_bufferedLength, _buffer.Length - _bufferedLength),
                    cancellationToken);
                if (read == 0)
                {
                    throw new EndOfStreamException("The remoting peer closed before sending a complete frame.");
                }

                _bufferedLength += read;
            }
        }

        public Task WriteAsync(RemotingCommand command, CancellationToken cancellationToken) =>
            WriteManyAsync([command], cancellationToken);

        public async Task WriteManyAsync(
            IEnumerable<RemotingCommand> commands,
            CancellationToken cancellationToken)
        {
            var output = new ArrayBufferWriter<byte>();
            foreach (var command in commands)
            {
                _serializer.WriteMessage(command, output);
            }

            await stream.WriteAsync(output.WrittenMemory, cancellationToken);
        }

        public async Task WriteFragmentedAsync(
            RemotingCommand command,
            CancellationToken cancellationToken)
        {
            var output = new ArrayBufferWriter<byte>();
            _serializer.WriteMessage(command, output);
            var remaining = output.WrittenMemory;
            foreach (var fragmentLength in new[] { 1, 2, 3 })
            {
                await stream.WriteAsync(remaining[..fragmentLength], cancellationToken);
                remaining = remaining[fragmentLength..];
                await Task.Delay(TimeSpan.FromMilliseconds(10), cancellationToken);
            }

            await stream.WriteAsync(remaining, cancellationToken);
        }
    }

    private sealed class LoopbackServer : IAsyncDisposable
    {
        private readonly TcpListener _listener = new(IPAddress.Loopback, 0);
        private readonly CancellationTokenSource _timeoutCts;
        private readonly List<TcpClient> _connections = [];
        private int _disposed;

        public LoopbackServer(CancellationToken cancellationToken)
        {
            _timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            _timeoutCts.CancelAfter(TimeSpan.FromSeconds(10));
            _listener.Start();
        }

        public IPEndPoint EndPoint => (IPEndPoint)_listener.LocalEndpoint;

        public CancellationToken CancellationToken => _timeoutCts.Token;

        public async Task<TcpClient> AcceptAsync()
        {
            var connection = await _listener.AcceptTcpClientAsync(_timeoutCts.Token);
            connection.NoDelay = true;
            lock (_connections)
            {
                _connections.Add(connection);
            }

            return connection;
        }

        public ValueTask DisposeAsync()
        {
            if (Interlocked.Exchange(ref _disposed, 1) != 0)
            {
                return ValueTask.CompletedTask;
            }

            _timeoutCts.Cancel();
            _listener.Stop();
            lock (_connections)
            {
                foreach (var connection in _connections)
                {
                    connection.Dispose();
                }

                _connections.Clear();
            }

            _timeoutCts.Dispose();
            return ValueTask.CompletedTask;
        }
    }
}
