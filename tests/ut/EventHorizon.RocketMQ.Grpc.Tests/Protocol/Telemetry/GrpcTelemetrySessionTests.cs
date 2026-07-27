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
    public async Task SessionOutlivesInitializationTokenAndHonorsReconnectCommand()
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
    public void DiagnosticCommandsPreserveNonceAndReturnExplicitStatus()
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
    public async Task WriteCommandAsyncSerializesDiagnosticReplyOnTelemetryStream()
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

}
