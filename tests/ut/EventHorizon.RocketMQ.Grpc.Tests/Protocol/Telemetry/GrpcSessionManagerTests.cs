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
using EventHorizon.RocketMQ.Grpc.Protocol.Telemetry;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Moq;
using Xunit;
using Proto = Apache.Rocketmq.V2;

namespace EventHorizon.RocketMQ.Grpc.Tests.Protocol.Telemetry;

public sealed class GrpcSessionManagerTests
{
    [Fact]
    public async Task EnsureAsync_SynchronizesSettingsAndNotifiesTheServerDuringDisposal()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var endpoint = new Uri("http://127.0.0.1:8081");
        var serverSettings = new Proto.Settings { ClientType = Proto.ClientType.Producer };
        var telemetry = CreateSession(serverSettings);
        Proto.NotifyClientTerminationRequest? termination = null;
        var client = CreateClient();
        client
            .Setup(value => value.OpenTelemetryAsync(
                endpoint,
                It.IsAny<Proto.Settings>(),
                It.IsAny<Func<Proto.TelemetryCommand, CancellationToken, Task>>(),
                cancellationToken))
            .ReturnsAsync(telemetry.Session.Object);
        client
            .Setup(value => value.NotifyTerminationAsync(
                endpoint,
                It.IsAny<Proto.NotifyClientTerminationRequest>(),
                CancellationToken.None))
            .Callback<Uri, Proto.NotifyClientTerminationRequest, CancellationToken>(
                (_, request, _) => termination = request.Clone())
            .ReturnsAsync(OkTerminationResponse());
        await using var manager = CreateManager(client.Object, "orders", "tenant-a");

        await manager.EnsureAsync([endpoint], new Proto.Settings(), cancellationToken);
        await manager.SyncSettingsAsync(new Proto.Settings { ClientType = Proto.ClientType.Producer }, cancellationToken);

        Assert.Same(serverSettings, manager.GetServerSettings(endpoint));
        telemetry.Session.Verify(
            value => value.WriteSettingsAsync(
                It.IsAny<Proto.Settings>(),
                cancellationToken),
            Times.Once);

        await manager.DisposeAsync();

        Assert.Equal("orders", termination?.Group.Name);
        Assert.Equal("tenant-a", termination?.Group.ResourceNamespace);
        telemetry.Session.Verify(value => value.DisposeAsync(), Times.Once);
    }

    [Fact]
    public async Task DisposeAsync_ContinuesWhenTerminationAndSessionDisposalFail()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var endpoint = new Uri("http://127.0.0.1:8081");
        var telemetry = CreateSession(failDispose: true);
        var client = CreateClient();
        client
            .Setup(value => value.OpenTelemetryAsync(
                endpoint,
                It.IsAny<Proto.Settings>(),
                It.IsAny<Func<Proto.TelemetryCommand, CancellationToken, Task>>(),
                cancellationToken))
            .ReturnsAsync(telemetry.Session.Object);
        client
            .Setup(value => value.NotifyTerminationAsync(
                endpoint,
                It.IsAny<Proto.NotifyClientTerminationRequest>(),
                CancellationToken.None))
            .Returns(Task.FromException<Proto.NotifyClientTerminationResponse>(
                new IOException("termination unavailable")));
        await using var manager = CreateManager(client.Object, "orders", null);

        await manager.EnsureAsync([endpoint], new Proto.Settings(), cancellationToken);
        await manager.DisposeAsync();

        telemetry.Session.Verify(value => value.DisposeAsync(), Times.Once);
    }

    [Fact]
    public async Task TelemetryCommands_WriteDiagnosticResponsesAndForwardOtherCommands()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var endpoint = new Uri("http://127.0.0.1:8081");
        Proto.TelemetryCommand? writtenCommand = null;
        Proto.TelemetryCommand? forwardedCommand = null;
        Func<Proto.TelemetryCommand, CancellationToken, Task>? commandHandler = null;
        var telemetry = CreateSession(onCommand: command => writtenCommand = command.Clone());
        var client = CreateClient();
        client
            .Setup(value => value.OpenTelemetryAsync(
                endpoint,
                It.IsAny<Proto.Settings>(),
                It.IsAny<Func<Proto.TelemetryCommand, CancellationToken, Task>>(),
                cancellationToken))
            .Callback<Uri, Proto.Settings, Func<Proto.TelemetryCommand, CancellationToken, Task>?, CancellationToken>(
                (_, _, handler, _) => commandHandler = handler)
            .ReturnsAsync(telemetry.Session.Object);
        client
            .Setup(value => value.NotifyTerminationAsync(
                endpoint,
                It.IsAny<Proto.NotifyClientTerminationRequest>(),
                CancellationToken.None))
            .ReturnsAsync(OkTerminationResponse());
        await using var manager = CreateManager(
            client.Object,
            "orders",
            null,
            (_, command, _) =>
            {
                forwardedCommand = command.Clone();
                return Task.CompletedTask;
            });

        await manager.EnsureAsync([endpoint], new Proto.Settings(), cancellationToken);
        var handler = Assert.IsType<Func<Proto.TelemetryCommand, CancellationToken, Task>>(commandHandler);

        await handler(new Proto.TelemetryCommand
        {
            PrintThreadStackTraceCommand = new Proto.PrintThreadStackTraceCommand { Nonce = "stack-1" }
        }, cancellationToken);
        await handler(new Proto.TelemetryCommand
        {
            NotifyUnsubscribeLiteCommand = new Proto.NotifyUnsubscribeLiteCommand { LiteTopic = "orders" }
        }, cancellationToken);

        Assert.Equal(Proto.Code.Ok, writtenCommand?.Status.Code);
        Assert.Equal("stack-1", writtenCommand?.ThreadStackTrace.Nonce);
        Assert.Equal("orders", forwardedCommand?.NotifyUnsubscribeLiteCommand.LiteTopic);
    }

    [Fact]
    public async Task ReconnectCommand_ReplacesTheActiveTelemetrySession()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var endpoint = new Uri("http://127.0.0.1:8081");
        var first = CreateSession();
        var replacementSettings = new Proto.Settings { ClientType = Proto.ClientType.PushConsumer };
        var replacement = CreateSession(replacementSettings);
        var replacementOpened = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        Func<Proto.TelemetryCommand, CancellationToken, Task>? commandHandler = null;
        var openCalls = 0;
        var client = CreateClient();
        client
            .Setup(value => value.OpenTelemetryAsync(
                endpoint,
                It.IsAny<Proto.Settings>(),
                It.IsAny<Func<Proto.TelemetryCommand, CancellationToken, Task>>(),
                It.IsAny<CancellationToken>()))
            .Callback<Uri, Proto.Settings, Func<Proto.TelemetryCommand, CancellationToken, Task>?, CancellationToken>(
                (_, _, handler, _) => commandHandler = handler)
            .ReturnsAsync(() =>
            {
                return Interlocked.Increment(ref openCalls) == 1
                    ? first.Session.Object
                    : replacementOpened.Task.IsCompleted
                        ? throw new InvalidOperationException("Unexpected telemetry replacement.")
                        : CompleteReplacement(replacement.Session.Object, replacementOpened);
            });
        client
            .Setup(value => value.NotifyTerminationAsync(
                endpoint,
                It.IsAny<Proto.NotifyClientTerminationRequest>(),
                CancellationToken.None))
            .ReturnsAsync(OkTerminationResponse());
        await using var manager = CreateManager(client.Object, "orders", null);

        await manager.EnsureAsync([endpoint], new Proto.Settings(), cancellationToken);
        var handler = Assert.IsType<Func<Proto.TelemetryCommand, CancellationToken, Task>>(commandHandler);
        await handler(new Proto.TelemetryCommand
        {
            ReconnectEndpointsCommand = new Proto.ReconnectEndpointsCommand { Nonce = "reconnect-1" }
        }, cancellationToken);
        await replacementOpened.Task.WaitAsync(TimeSpan.FromSeconds(3), cancellationToken);

        Assert.Same(replacementSettings, manager.GetServerSettings(endpoint));
        first.Session.Verify(value => value.DisposeAsync(), Times.Once);
    }

    [Fact]
    public async Task ReconnectCommandReceivedDuringSessionOpening_IsHonoredAfterRegistration()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var endpoint = new Uri("http://127.0.0.1:8081");
        var first = CreateSession();
        var replacement = CreateSession();
        var replacementOpened = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var openCalls = 0;
        var client = CreateClient();
        client
            .Setup(value => value.OpenTelemetryAsync(
                endpoint,
                It.IsAny<Proto.Settings>(),
                It.IsAny<Func<Proto.TelemetryCommand, CancellationToken, Task>>(),
                It.IsAny<CancellationToken>()))
            .Returns<Uri, Proto.Settings, Func<Proto.TelemetryCommand, CancellationToken, Task>?, CancellationToken>(
                async (_, _, handler, token) =>
                {
                    if (Interlocked.Increment(ref openCalls) == 1)
                    {
                        var commandHandler = Assert.IsType<Func<Proto.TelemetryCommand, CancellationToken, Task>>(handler);
                        await commandHandler(new Proto.TelemetryCommand
                        {
                            ReconnectEndpointsCommand = new Proto.ReconnectEndpointsCommand { Nonce = "reconnect-queued" }
                        }, token);
                        return first.Session.Object;
                    }

                    replacementOpened.TrySetResult();
                    return replacement.Session.Object;
                });
        client
            .Setup(value => value.NotifyTerminationAsync(
                endpoint,
                It.IsAny<Proto.NotifyClientTerminationRequest>(),
                CancellationToken.None))
            .ReturnsAsync(OkTerminationResponse());
        await using var manager = CreateManager(client.Object, "orders", null);

        await manager.EnsureAsync([endpoint], new Proto.Settings(), cancellationToken);
        await replacementOpened.Task.WaitAsync(TimeSpan.FromSeconds(3), cancellationToken);

        Assert.Equal(2, Volatile.Read(ref openCalls));
        first.Session.Verify(value => value.DisposeAsync(), Times.Once);
    }

    [Fact]
    public async Task SessionFailure_ReplacesTheTelemetrySession()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var endpoint = new Uri("http://127.0.0.1:8081");
        var first = CreateSession();
        var replacement = CreateSession();
        var replacementOpened = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var openCalls = 0;
        var client = CreateClient();
        client
            .Setup(value => value.OpenTelemetryAsync(
                endpoint,
                It.IsAny<Proto.Settings>(),
                It.IsAny<Func<Proto.TelemetryCommand, CancellationToken, Task>>(),
                It.IsAny<CancellationToken>()))
            .ReturnsAsync(() =>
            {
                return Interlocked.Increment(ref openCalls) == 1
                    ? first.Session.Object
                    : CompleteReplacement(replacement.Session.Object, replacementOpened);
            });
        client
            .Setup(value => value.NotifyTerminationAsync(
                endpoint,
                It.IsAny<Proto.NotifyClientTerminationRequest>(),
                CancellationToken.None))
            .ReturnsAsync(OkTerminationResponse());
        await using var manager = CreateManager(client.Object, "orders", null);

        await manager.EnsureAsync([endpoint], new Proto.Settings(), cancellationToken);
        first.Completion.TrySetException(new IOException("stream failed"));
        await replacementOpened.Task.WaitAsync(TimeSpan.FromSeconds(3), cancellationToken);

        first.Session.Verify(value => value.DisposeAsync(), Times.Once);
    }

    [Fact]
    public async Task FailedSessionReplacement_IsCancelledWhenTheManagerIsDisposed()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var endpoint = new Uri("http://127.0.0.1:8081");
        var first = CreateSession();
        var replacementAttempted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var openCalls = 0;
        var client = CreateClient();
        client
            .Setup(value => value.OpenTelemetryAsync(
                endpoint,
                It.IsAny<Proto.Settings>(),
                It.IsAny<Func<Proto.TelemetryCommand, CancellationToken, Task>>(),
                It.IsAny<CancellationToken>()))
            .Returns<Uri, Proto.Settings, Func<Proto.TelemetryCommand, CancellationToken, Task>?, CancellationToken>(
                (_, _, _, _) =>
                {
                    if (Interlocked.Increment(ref openCalls) == 1)
                    {
                        return Task.FromResult<IGrpcTelemetrySession>(first.Session.Object);
                    }

                    replacementAttempted.TrySetResult();
                    return Task.FromException<IGrpcTelemetrySession>(new IOException("telemetry unavailable"));
                });
        client
            .Setup(value => value.NotifyTerminationAsync(
                endpoint,
                It.IsAny<Proto.NotifyClientTerminationRequest>(),
                CancellationToken.None))
            .ReturnsAsync(OkTerminationResponse());
        await using var manager = CreateManager(client.Object, "orders", null);

        await manager.EnsureAsync([endpoint], new Proto.Settings(), cancellationToken);
        first.Completion.TrySetResult();
        await replacementAttempted.Task.WaitAsync(TimeSpan.FromSeconds(3), cancellationToken);
        await manager.DisposeAsync();

        Assert.Equal(2, Volatile.Read(ref openCalls));
    }

    [Fact]
    public async Task Start_EmitsHeartbeatsForActiveSessions()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var endpoint = new Uri("http://127.0.0.1:8081");
        var heartbeatReceived = new TaskCompletionSource<Proto.HeartbeatRequest>(
            TaskCreationOptions.RunContinuationsAsynchronously);
        var telemetry = CreateSession();
        var client = CreateClient();
        client
            .Setup(value => value.OpenTelemetryAsync(
                endpoint,
                It.IsAny<Proto.Settings>(),
                It.IsAny<Func<Proto.TelemetryCommand, CancellationToken, Task>>(),
                cancellationToken))
            .ReturnsAsync(telemetry.Session.Object);
        client
            .Setup(value => value.HeartbeatAsync(
                endpoint,
                It.IsAny<Proto.HeartbeatRequest>(),
                It.IsAny<CancellationToken>()))
            .Callback<Uri, Proto.HeartbeatRequest, CancellationToken>(
                (_, request, _) => heartbeatReceived.TrySetResult(request.Clone()))
            .ReturnsAsync(new Proto.HeartbeatResponse { Status = OkStatus() });
        client
            .Setup(value => value.NotifyTerminationAsync(
                endpoint,
                It.IsAny<Proto.NotifyClientTerminationRequest>(),
                CancellationToken.None))
            .ReturnsAsync(OkTerminationResponse());
        await using var manager = CreateManager(
            client.Object,
            "orders",
            "tenant-a",
            heartbeatInterval: TimeSpan.FromMilliseconds(10));

        manager.Start();
        await manager.EnsureAsync([endpoint], new Proto.Settings(), cancellationToken);
        var heartbeat = await heartbeatReceived.Task.WaitAsync(TimeSpan.FromSeconds(3), cancellationToken);

        Assert.Equal(Proto.ClientType.Producer, heartbeat.ClientType);
        Assert.Equal("orders", heartbeat.Group.Name);
        Assert.Equal("tenant-a", heartbeat.Group.ResourceNamespace);
    }

    [Fact]
    public async Task Start_ContinuesAfterAHeartbeatFailure()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var endpoint = new Uri("http://127.0.0.1:8081");
        var telemetry = CreateSession();
        var heartbeatSucceeded = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var heartbeatAttempts = 0;
        var client = CreateClient();
        client
            .Setup(value => value.OpenTelemetryAsync(
                endpoint,
                It.IsAny<Proto.Settings>(),
                It.IsAny<Func<Proto.TelemetryCommand, CancellationToken, Task>>(),
                cancellationToken))
            .ReturnsAsync(telemetry.Session.Object);
        client
            .Setup(value => value.HeartbeatAsync(
                endpoint,
                It.IsAny<Proto.HeartbeatRequest>(),
                It.IsAny<CancellationToken>()))
            .Returns<Uri, Proto.HeartbeatRequest, CancellationToken>((_, _, _) =>
            {
                if (Interlocked.Increment(ref heartbeatAttempts) == 1)
                {
                    return Task.FromException<Proto.HeartbeatResponse>(new IOException("heartbeat unavailable"));
                }

                heartbeatSucceeded.TrySetResult();
                return Task.FromResult(new Proto.HeartbeatResponse { Status = OkStatus() });
            });
        client
            .Setup(value => value.NotifyTerminationAsync(
                endpoint,
                It.IsAny<Proto.NotifyClientTerminationRequest>(),
                CancellationToken.None))
            .ReturnsAsync(OkTerminationResponse());
        await using var manager = CreateManager(
            client.Object,
            "orders",
            null,
            heartbeatInterval: TimeSpan.FromMilliseconds(10));

        manager.Start();
        await manager.EnsureAsync([endpoint], new Proto.Settings(), cancellationToken);
        await heartbeatSucceeded.Task.WaitAsync(TimeSpan.FromSeconds(3), cancellationToken);

        Assert.True(Volatile.Read(ref heartbeatAttempts) >= 2);
    }

    [Fact]
    public async Task DisposeAsync_CancelsAnInFlightHeartbeat()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var endpoint = new Uri("http://127.0.0.1:8081");
        var telemetry = CreateSession();
        var heartbeatStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var client = CreateClient();
        client
            .Setup(value => value.OpenTelemetryAsync(
                endpoint,
                It.IsAny<Proto.Settings>(),
                It.IsAny<Func<Proto.TelemetryCommand, CancellationToken, Task>>(),
                cancellationToken))
            .ReturnsAsync(telemetry.Session.Object);
        client
            .Setup(value => value.HeartbeatAsync(
                endpoint,
                It.IsAny<Proto.HeartbeatRequest>(),
                It.IsAny<CancellationToken>()))
            .Returns<Uri, Proto.HeartbeatRequest, CancellationToken>((_, _, token) =>
            {
                heartbeatStarted.TrySetResult();
                return WaitForHeartbeatCancellationAsync(token);
            });
        client
            .Setup(value => value.NotifyTerminationAsync(
                endpoint,
                It.IsAny<Proto.NotifyClientTerminationRequest>(),
                CancellationToken.None))
            .ReturnsAsync(OkTerminationResponse());
        await using var manager = CreateManager(
            client.Object,
            "orders",
            null,
            heartbeatInterval: TimeSpan.FromMilliseconds(10));

        manager.Start();
        await manager.EnsureAsync([endpoint], new Proto.Settings(), cancellationToken);
        await heartbeatStarted.Task.WaitAsync(TimeSpan.FromSeconds(3), cancellationToken);
        await manager.DisposeAsync();
    }

    [Fact]
    public async Task GetServerSettings_ReturnsNullWhenTheEndpointHasNoSession()
    {
        var client = CreateClient();
        await using var manager = CreateManager(client.Object, "orders", null);

        Assert.Null(manager.GetServerSettings(new Uri("http://127.0.0.1:8081")));
    }

    private static Mock<IRocketMQGrpcClient> CreateClient() => new(MockBehavior.Strict);

    private static GrpcSessionManager CreateManager(
        IRocketMQGrpcClient client,
        string group,
        string? @namespace,
        Func<Uri, Proto.TelemetryCommand, CancellationToken, Task>? telemetryHandler = null,
        TimeSpan? heartbeatInterval = null)
    {
        return new GrpcSessionManager(
            client,
            Options.Create(new GrpcClientOptions
            {
                Namespace = @namespace,
                HeartbeatInterval = heartbeatInterval ?? TimeSpan.FromSeconds(30)
            }),
            Proto.ClientType.Producer,
            group,
            NullLogger<GrpcSessionManager>.Instance,
            telemetryHandler);
    }

    private static (
        Mock<IGrpcTelemetrySession> Session,
        TaskCompletionSource Completion) CreateSession(
        Proto.Settings? serverSettings = null,
        Action<Proto.TelemetryCommand>? onCommand = null,
        bool failDispose = false)
    {
        var completion = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var session = new Mock<IGrpcTelemetrySession>(MockBehavior.Strict);
        session.SetupGet(value => value.ServerSettings).Returns(serverSettings);
        session.SetupGet(value => value.Completion).Returns(completion.Task);
        session
            .Setup(value => value.WriteSettingsAsync(
                It.IsAny<Proto.Settings>(),
                It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask);
        session
            .Setup(value => value.WriteCommandAsync(
                It.IsAny<Proto.TelemetryCommand>(),
                It.IsAny<CancellationToken>()))
            .Callback<Proto.TelemetryCommand, CancellationToken>((command, _) => onCommand?.Invoke(command))
            .Returns(Task.CompletedTask);
        if (failDispose)
        {
            session
                .Setup(value => value.DisposeAsync())
                .Returns(() => new ValueTask(Task.FromException(new IOException("dispose failed"))));
        }
        else
        {
            session
                .Setup(value => value.DisposeAsync())
                .Callback(() => completion.TrySetResult())
                .Returns(ValueTask.CompletedTask);
        }

        return (session, completion);
    }

    private static IGrpcTelemetrySession CompleteReplacement(
        IGrpcTelemetrySession session,
        TaskCompletionSource replacementOpened)
    {
        replacementOpened.TrySetResult();
        return session;
    }

    private static async Task<Proto.HeartbeatResponse> WaitForHeartbeatCancellationAsync(
        CancellationToken cancellationToken)
    {
        await Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken);
        return new Proto.HeartbeatResponse { Status = OkStatus() };
    }

    private static Proto.Status OkStatus() => new() { Code = Proto.Code.Ok };

    private static Proto.NotifyClientTerminationResponse OkTerminationResponse() => new() { Status = OkStatus() };
}
