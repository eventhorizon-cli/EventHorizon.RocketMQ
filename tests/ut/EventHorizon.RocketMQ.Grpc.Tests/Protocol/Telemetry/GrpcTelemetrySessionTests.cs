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
using System.Threading.Channels;
using EventHorizon.RocketMQ.Grpc.Protocol.Telemetry;
using Grpc.Core;
using Moq;
using Xunit;
using Proto = Apache.Rocketmq.V2;

namespace EventHorizon.RocketMQ.Grpc.Tests.Protocol.Telemetry;

public sealed class GrpcTelemetrySessionTests
{
    [Fact]
    public async Task Completion_BeforeSessionStarts_IsIncomplete()
    {
        var responses = new ChannelStreamReader<Proto.TelemetryCommand>();
        var requests = CreateRequestWriter(new ConcurrentQueue<Proto.TelemetryCommand>());
        await using var session = new GrpcTelemetrySession(CreateCall(requests.Object, responses), null);

        Assert.False(session.Completion.IsCompleted);
    }

    [Fact]
    public async Task Session_ReconnectCommand_HonorsAfterInitialization()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var responses = new ChannelStreamReader<Proto.TelemetryCommand>();
        var writtenRequests = new ConcurrentQueue<Proto.TelemetryCommand>();
        var requests = CreateRequestWriter(writtenRequests);
        var commandReceived = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        using var initialization = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        var call = new AsyncDuplexStreamingCall<Proto.TelemetryCommand, Proto.TelemetryCommand>(
            requests.Object,
            responses,
            Task.FromResult(new Metadata()),
            static () => Status.DefaultSuccess,
            static () => new Metadata(),
            static () => { });
        var session = new GrpcTelemetrySession(
            call,
            (command, _) =>
            {
                if (command.CommandCase == Proto.TelemetryCommand.CommandOneofCase.RecoverOrphanedTransactionCommand)
                {
                    commandReceived.TrySetResult();
                }

                return Task.CompletedTask;
            });
        await responses.WriteAsync(new Proto.TelemetryCommand { Settings = new Proto.Settings() }, cancellationToken);

        await session.StartAsync(new Proto.Settings(), TimeSpan.FromSeconds(1), initialization.Token);
        initialization.Cancel();
        await responses.WriteAsync(new Proto.TelemetryCommand
        {
            RecoverOrphanedTransactionCommand = new Proto.RecoverOrphanedTransactionCommand()
        }, cancellationToken);
        await commandReceived.Task.WaitAsync(TimeSpan.FromSeconds(2), cancellationToken);

        Assert.False(session.Completion.IsCompleted);
        Assert.Single(writtenRequests);
        await responses.WriteAsync(new Proto.TelemetryCommand
        {
            ReconnectEndpointsCommand = new Proto.ReconnectEndpointsCommand { Nonce = "reconnect-1" }
        }, cancellationToken);
        await session.Completion.WaitAsync(TimeSpan.FromSeconds(2), cancellationToken);
        await session.DisposeAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(2), cancellationToken);
    }

    [Fact]
    public async Task StartAsync_StreamClosesBeforeSettings_FailsStartup()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var responses = new ChannelStreamReader<Proto.TelemetryCommand>();
        responses.Complete();
        var requests = CreateRequestWriter(new ConcurrentQueue<Proto.TelemetryCommand>());
        await using var session = new GrpcTelemetrySession(CreateCall(requests.Object, responses), null);

        var exception = await Assert.ThrowsAsync<RpcException>(() => session.StartAsync(
            new Proto.Settings(),
            TimeSpan.FromSeconds(1),
            cancellationToken));

        Assert.Equal(StatusCode.Unavailable, exception.StatusCode);
    }

    [Fact]
    public async Task DisposeAsync_UnavailableReaderFailure_IgnoresFailure()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var responses = new SequenceStreamReader<Proto.TelemetryCommand>(
            new Proto.TelemetryCommand { Settings = new Proto.Settings() },
            new RpcException(new Status(StatusCode.Unavailable, "telemetry closed")));
        var requests = CreateRequestWriter(new ConcurrentQueue<Proto.TelemetryCommand>());
        await using var session = new GrpcTelemetrySession(CreateCall(requests.Object, responses), null);

        await session.StartAsync(new Proto.Settings(), TimeSpan.FromSeconds(1), cancellationToken);
        var exception = await Assert.ThrowsAsync<RpcException>(() => session.Completion);
        await session.DisposeAsync();

        Assert.Equal(StatusCode.Unavailable, exception.StatusCode);
    }

    [Fact]
    public async Task DisposeAsync_CancelledReaderFailure_IgnoresFailure()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var responses = new SequenceStreamReader<Proto.TelemetryCommand>(
            new Proto.TelemetryCommand { Settings = new Proto.Settings() },
            new OperationCanceledException("telemetry cancelled"));
        var requests = CreateRequestWriter(new ConcurrentQueue<Proto.TelemetryCommand>());
        await using var session = new GrpcTelemetrySession(CreateCall(requests.Object, responses), null);

        await session.StartAsync(new Proto.Settings(), TimeSpan.FromSeconds(1), cancellationToken);
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => session.Completion);
        await session.DisposeAsync();
    }

    [Fact]
    public async Task CommandHandlerFailure_TelemetryDispatcher_DoesNotStop()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var responses = new ChannelStreamReader<Proto.TelemetryCommand>();
        var requests = CreateRequestWriter(new ConcurrentQueue<Proto.TelemetryCommand>());
        var secondCommandHandled = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var handledCommands = 0;
        await using var session = new GrpcTelemetrySession(
            CreateCall(requests.Object, responses),
            (_, _) =>
            {
                if (Interlocked.Increment(ref handledCommands) == 1)
                {
                    throw new InvalidOperationException("handler failed");
                }

                secondCommandHandled.TrySetResult();
                return Task.CompletedTask;
            });
        await responses.WriteAsync(new Proto.TelemetryCommand { Settings = new Proto.Settings() }, cancellationToken);
        await session.StartAsync(new Proto.Settings(), TimeSpan.FromSeconds(1), cancellationToken);

        await responses.WriteAsync(new Proto.TelemetryCommand
        {
            NotifyUnsubscribeLiteCommand = new Proto.NotifyUnsubscribeLiteCommand { LiteTopic = "orders" }
        }, cancellationToken);
        await responses.WriteAsync(new Proto.TelemetryCommand
        {
            NotifyUnsubscribeLiteCommand = new Proto.NotifyUnsubscribeLiteCommand { LiteTopic = "payments" }
        }, cancellationToken);
        await secondCommandHandled.Task.WaitAsync(TimeSpan.FromSeconds(2), cancellationToken);

        Assert.Equal(2, Volatile.Read(ref handledCommands));
    }

    [Fact]
    public async Task DisposeAsync_ActiveTelemetryCommandHandler_CancelsHandler()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var responses = new ChannelStreamReader<Proto.TelemetryCommand>();
        var requests = CreateRequestWriter(new ConcurrentQueue<Proto.TelemetryCommand>());
        var handlerStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var session = new GrpcTelemetrySession(
            CreateCall(requests.Object, responses),
            async (_, token) =>
            {
                handlerStarted.TrySetResult();
                await Task.Delay(Timeout.InfiniteTimeSpan, token);
            });
        await responses.WriteAsync(new Proto.TelemetryCommand { Settings = new Proto.Settings() }, cancellationToken);
        await session.StartAsync(new Proto.Settings(), TimeSpan.FromSeconds(1), cancellationToken);
        await responses.WriteAsync(new Proto.TelemetryCommand
        {
            NotifyUnsubscribeLiteCommand = new Proto.NotifyUnsubscribeLiteCommand { LiteTopic = "orders" }
        }, cancellationToken);
        await handlerStarted.Task.WaitAsync(TimeSpan.FromSeconds(2), cancellationToken);

        await session.DisposeAsync();
    }

    [Fact]
    public void DiagnosticCommands_NonceAndStatus_PreserveAndReturn()
    {
        var stack = GrpcSessionManager.CreateDiagnosticResponse(new Proto.TelemetryCommand
        {
            PrintThreadStackTraceCommand = new Proto.PrintThreadStackTraceCommand { Nonce = "stack-1" }
        });
        var verification = GrpcSessionManager.CreateDiagnosticResponse(new Proto.TelemetryCommand
        {
            VerifyMessageCommand = new Proto.VerifyMessageCommand { Nonce = "verify-1" }
        });

        Assert.NotNull(stack);
        Assert.Equal(Proto.Code.Ok, stack.Status.Code);
        Assert.Equal("stack-1", stack.ThreadStackTrace.Nonce);
        Assert.False(string.IsNullOrWhiteSpace(stack.ThreadStackTrace.ThreadStackTrace_));
        Assert.NotNull(verification);
        Assert.Equal(Proto.Code.NotImplemented, verification.Status.Code);
        Assert.Equal("verify-1", verification.VerifyMessageResult.Nonce);
        Assert.Null(GrpcSessionManager.CreateDiagnosticResponse(new Proto.TelemetryCommand
        {
            ReconnectEndpointsCommand = new Proto.ReconnectEndpointsCommand()
        }));
    }

    [Fact]
    public async Task WriteCommandAsync_TelemetryStream_SerializesDiagnosticReply()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var responses = new ChannelStreamReader<Proto.TelemetryCommand>();
        var writtenRequests = new ConcurrentQueue<Proto.TelemetryCommand>();
        var requests = CreateRequestWriter(writtenRequests);
        var call = new AsyncDuplexStreamingCall<Proto.TelemetryCommand, Proto.TelemetryCommand>(
            requests.Object,
            responses,
            Task.FromResult(new Metadata()),
            static () => Status.DefaultSuccess,
            static () => new Metadata(),
            static () => { });
        await using var session = new GrpcTelemetrySession(call, null);
        await responses.WriteAsync(new Proto.TelemetryCommand { Settings = new Proto.Settings() }, cancellationToken);
        await session.StartAsync(new Proto.Settings(), TimeSpan.FromSeconds(1), cancellationToken);
        var response = GrpcSessionManager.CreateDiagnosticResponse(new Proto.TelemetryCommand
        {
            VerifyMessageCommand = new Proto.VerifyMessageCommand { Nonce = "verify-2" }
        });

        await session.WriteCommandAsync(Assert.IsType<Proto.TelemetryCommand>(response), cancellationToken);

        Assert.Equal(2, writtenRequests.Count);
        Assert.Equal("verify-2", writtenRequests.Last().VerifyMessageResult.Nonce);
    }

    private static AsyncDuplexStreamingCall<Proto.TelemetryCommand, Proto.TelemetryCommand> CreateCall(
        IClientStreamWriter<Proto.TelemetryCommand> requests,
        IAsyncStreamReader<Proto.TelemetryCommand> responses) => new(
        requests,
        responses,
        Task.FromResult(new Metadata()),
        static () => Status.DefaultSuccess,
        static () => new Metadata(),
        static () => { });

    private static Mock<IClientStreamWriter<T>> CreateRequestWriter<T>(ConcurrentQueue<T> messages)
    {
        var writer = new Mock<IClientStreamWriter<T>>(MockBehavior.Strict);
        writer
            .Setup(value => value.WriteAsync(It.IsAny<T>(), It.IsAny<CancellationToken>()))
            .Returns<T, CancellationToken>((message, cancellationToken) =>
            {
                cancellationToken.ThrowIfCancellationRequested();
                messages.Enqueue(message);
                return Task.CompletedTask;
            });
        writer
            .Setup(value => value.CompleteAsync())
            .Returns(Task.CompletedTask);
        return writer;
    }

    private sealed class ChannelStreamReader<T> : IAsyncStreamReader<T>
    {
        private readonly Channel<T> _channel = Channel.CreateUnbounded<T>();

        public T Current { get; private set; } = default!;

        public ValueTask WriteAsync(T item, CancellationToken cancellationToken) =>
            _channel.Writer.WriteAsync(item, cancellationToken);

        public void Complete(Exception? exception = null) => _channel.Writer.TryComplete(exception);

        public async Task<bool> MoveNext(CancellationToken cancellationToken)
        {
            try
            {
                Current = await _channel.Reader.ReadAsync(cancellationToken);
                return true;
            }
            catch (ChannelClosedException)
            {
                return false;
            }
        }
    }

    private sealed class SequenceStreamReader<T>(params object[] responses) : IAsyncStreamReader<T>
    {
        private readonly Queue<object> _responses = new(responses);

        public T Current { get; private set; } = default!;

        public Task<bool> MoveNext(CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (_responses.Count == 0)
            {
                return Task.FromResult(false);
            }

            var response = _responses.Dequeue();
            if (response is Exception exception)
            {
                return Task.FromException<bool>(exception);
            }

            Current = (T)response;
            return Task.FromResult(true);
        }
    }

}
