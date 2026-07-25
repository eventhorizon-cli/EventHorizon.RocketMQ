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

using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Net;
using System.Text;
using EventHorizon.RocketMQ.Producer;
using EventHorizon.RocketMQ.Remoting.Producer;
using EventHorizon.RocketMQ.Remoting.Producer.Transactions;
using EventHorizon.RocketMQ.Remoting.Protocol;
using EventHorizon.RocketMQ.Remoting.Protocol.Route;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Moq;
using Xunit;

namespace EventHorizon.RocketMQ.Remoting.Tests.Producer.Transactions;

public sealed class RemotingTransactionTests
{
    [Fact]
    public async Task SendTransactionAsync_SendsHalfMessageExecutesLocalTransactionAndEndsIt()
    {
        var remoting = new FakeRemotingClient();
        var localExecutions = new List<object?>();
        var producer = CreateProducer(
            remoting,
            options =>
            {
                options.LocalTransactionExecutor = (_, argument, _) =>
                {
                    localExecutions.Add(argument);
                    return ValueTask.FromResult(RemotingTransactionResolution.Commit);
                };
                options.TransactionChecker = static (_, _) =>
                    ValueTask.FromResult(RemotingTransactionResolution.Unknown);
            });
        var message = new Message("orders", "transactional"u8.ToArray());

        await producer.StartAsync(TestContext.Current.CancellationToken);
        var result = await producer.SendTransactionAsync(message, "order-42", TestContext.Current.CancellationToken);

        var halfMessage = Assert.Single(remoting.Invocations);
        Assert.Equal(RequestCode.SendMessage, halfMessage.Request.Code);
        Assert.Equal(MessageSysFlag.TransactionPreparedType, halfMessage.Request.ExtFields["sysFlag"]);
        var halfProperties = MessagePropertyCodec.Deserialize(Assert.IsType<string>(halfMessage.Request.ExtFields["properties"]));
        Assert.Equal("true", halfProperties["TRAN_MSG"]);
        Assert.Equal("tests", halfProperties["PGROUP"]);
        Assert.Single(localExecutions, "order-42");
        Assert.Equal(RemotingTransactionResolution.Commit, result.LocalTransactionResolution);

        var endTransaction = Assert.Single(remoting.OnewayInvocations);
        Assert.Equal(RequestCode.EndTransaction, endTransaction.Request.Code);
        Assert.Equal(MessageSysFlag.TransactionCommitType, endTransaction.Request.ExtFields["commitOrRollback"]);
        Assert.Equal(42L, endTransaction.Request.ExtFields["tranStateTableOffset"]);
        Assert.Equal(1234L, endTransaction.Request.ExtFields["commitLogOffset"]);
        Assert.Equal(message.Properties["UNIQ_KEY"], endTransaction.Request.ExtFields["msgId"]);
        Assert.Equal("transaction-id", endTransaction.Request.ExtFields["transactionId"]);
        Assert.Equal("broker-a", endTransaction.Request.ExtFields["bname"]);
        Assert.False(Assert.IsType<bool>(endTransaction.Request.ExtFields["fromTransactionCheck"]));
    }

    [Fact]
    public async Task SendTransactionAsync_ExecutorFailureResolvesUnknown()
    {
        var remoting = new FakeRemotingClient();
        var producer = CreateProducer(
            remoting,
            options =>
            {
                options.LocalTransactionExecutor = static (_, _, _) =>
                    throw new InvalidOperationException("local store failed");
                options.TransactionChecker = static (_, _) =>
                    ValueTask.FromResult(RemotingTransactionResolution.Unknown);
            });

        await producer.StartAsync(TestContext.Current.CancellationToken);
        var result = await producer.SendTransactionAsync(
            new Message("orders", "transactional"u8.ToArray()),
            cancellationToken: TestContext.Current.CancellationToken);

        Assert.Equal(RemotingTransactionResolution.Unknown, result.LocalTransactionResolution);
        var endTransaction = Assert.Single(remoting.OnewayInvocations);
        Assert.Equal(MessageSysFlag.TransactionNotType, endTransaction.Request.ExtFields["commitOrRollback"]);
        Assert.Contains("local transaction executor failed", endTransaction.Request.Remark, StringComparison.Ordinal);
    }

    [Fact]
    public async Task TransactionCheck_QueuesCheckerAndEndsTransactionAtCallingBroker()
    {
        var remoting = new FakeRemotingClient();
        var checkerCalled = new TaskCompletionSource<RemotingTransactionMessage>(
            TaskCreationOptions.RunContinuationsAsynchronously);
        var producer = CreateProducer(
            remoting,
            options =>
            {
                options.LocalTransactionExecutor = static (_, _, _) =>
                    ValueTask.FromResult(RemotingTransactionResolution.Commit);
                options.TransactionChecker = (message, _) =>
                {
                    checkerCalled.TrySetResult(message);
                    return ValueTask.FromResult(RemotingTransactionResolution.Rollback);
                };
            });

        await producer.StartAsync(TestContext.Current.CancellationToken);
        var request = new RemotingCommand
        {
            Code = RequestCode.CheckTransactionState,
            ExtFields = new Dictionary<string, object>
            {
                ["topic"] = "orders",
                ["tranStateTableOffset"] = "42",
                ["commitLogOffset"] = "1234",
                ["msgId"] = "message-id",
                ["transactionId"] = "transaction-id",
                ["offsetMsgId"] = CreateOffsetMessageId(1234),
                ["bname"] = "broker-a"
            },
            Body = CreateTransactionCheckMessage("orders", "message-id", "tests")
        };

        var handlerResult = await remoting.DispatchInboundAsync(
            new IPEndPoint(IPAddress.Loopback, 10911),
            request);
        var checkedMessage = await checkerCalled.Task.WaitAsync(TestContext.Current.CancellationToken);
        await WaitForAsync(() => remoting.OnewayInvocations.Count == 1, TestContext.Current.CancellationToken);

        Assert.True(handlerResult.IsHandled);
        Assert.True(handlerResult.SuppressResponse);
        Assert.Equal("orders", checkedMessage.Topic);
        Assert.Equal("transactional"u8.ToArray(), checkedMessage.Body);
        Assert.Equal("message-id", checkedMessage.MessageId);
        Assert.Equal("transaction-id", checkedMessage.TransactionId);
        var endTransaction = Assert.Single(remoting.OnewayInvocations);
        Assert.Equal(RequestCode.EndTransaction, endTransaction.Request.Code);
        Assert.Equal(MessageSysFlag.TransactionRollbackType, endTransaction.Request.ExtFields["commitOrRollback"]);
        Assert.True(Assert.IsType<bool>(endTransaction.Request.ExtFields["fromTransactionCheck"]));
        Assert.Equal(1234L, endTransaction.Request.ExtFields["commitLogOffset"]);
        Assert.Equal(42L, endTransaction.Request.ExtFields["tranStateTableOffset"]);
    }

    [Fact]
    public async Task StopAsync_UnregistersTransactionCheckHandler()
    {
        var remoting = new FakeRemotingClient();
        var producer = CreateProducer(
            remoting,
            options =>
            {
                options.LocalTransactionExecutor = static (_, _, _) =>
                    ValueTask.FromResult(RemotingTransactionResolution.Commit);
                options.TransactionChecker = static (_, _) =>
                    ValueTask.FromResult(RemotingTransactionResolution.Commit);
            });

        await producer.StartAsync(TestContext.Current.CancellationToken);
        Assert.Equal(2, remoting.RequestHandlerCount);

        await producer.StopAsync(TestContext.Current.CancellationToken);

        Assert.Equal(0, remoting.RequestHandlerCount);
    }

    private static RemotingProducer CreateProducer(
        FakeRemotingClient remoting,
        Action<RemotingProducerOptions> configure)
    {
        var routeService = new Mock<ITopicRouteService>(MockBehavior.Strict);
        routeService
            .Setup(value => value.GetAsync(
                It.IsAny<string>(),
                It.IsAny<bool>(),
                It.IsAny<CancellationToken>()))
            .Returns(Task.FromResult(CreateRoute()));
        var options = new RemotingProducerOptions { GroupName = "tests" };
        configure(options);
        return new RemotingProducer(
            Options.Create(options),
            Options.Create(new RemotingClientOptions()),
            routeService.Object,
            remoting,
            TimeProvider.System,
            NullLoggerFactory.Instance);
    }

    private static TopicRouteData CreateRoute() => new()
    {
        QueueDatas =
        [
            new QueueData { BrokerName = "broker-a", ReadQueueNums = 1, WriteQueueNums = 1, Perm = 6 }
        ],
        BrokerDatas =
        [
            new BrokerData
            {
                BrokerName = "broker-a",
                BrokerAddrs = new ConcurrentDictionary<long, string>(
                    new[] { new KeyValuePair<long, string>(0, "localhost:10911") })
            }
        ]
    };

    private static RemotingCommand SendSuccessResponse() => new()
    {
        Code = ResponseCodes.ResSuccess,
        ExtFields = new Dictionary<string, object>
        {
            ["msgId"] = CreateOffsetMessageId(1234),
            ["queueId"] = "0",
            ["queueOffset"] = "42",
            ["transactionId"] = "transaction-id",
            ["MSG_REGION"] = "DefaultRegion",
            ["TRACE_ON"] = "true"
        }
    };

    private static byte[] CreateTransactionCheckMessage(string topic, string messageId, string producerGroup)
    {
        var body = "transactional"u8.ToArray();
        var properties = Encoding.UTF8.GetBytes(
            $"UNIQ_KEY\u0001{messageId}\u0002PGROUP\u0001{producerGroup}\u0002TAGS\u0001paid\u0002");
        using var stream = new MemoryStream();
        WriteInt32(stream, 0);
        WriteInt32(stream, -626843481);
        WriteInt32(stream, 0);
        WriteInt32(stream, 0);
        WriteInt32(stream, 0);
        WriteInt64(stream, 42);
        WriteInt64(stream, 1234);
        WriteInt32(stream, 0);
        WriteInt64(stream, 1_700_000_000_000);
        stream.Write(new byte[] { 127, 0, 0, 1, 0x2A, 0x9F, 0, 0 });
        WriteInt64(stream, 1_700_000_000_100);
        stream.Write(new byte[] { 127, 0, 0, 1, 0x2A, 0x9F, 0, 0 });
        WriteInt32(stream, 0);
        WriteInt64(stream, 0);
        WriteInt32(stream, body.Length);
        stream.Write(body);
        stream.WriteByte(checked((byte)Encoding.UTF8.GetByteCount(topic)));
        stream.Write(Encoding.UTF8.GetBytes(topic));
        WriteUInt16(stream, checked((ushort)properties.Length));
        stream.Write(properties);

        var record = stream.ToArray();
        BinaryPrimitives.WriteInt32BigEndian(record, record.Length);
        return record;
    }

    private static string CreateOffsetMessageId(long offset)
    {
        Span<byte> bytes = stackalloc byte[16];
        BinaryPrimitives.WriteInt64BigEndian(bytes[8..], offset);
        return Convert.ToHexString(bytes);
    }

    private static async Task WaitForAsync(Func<bool> predicate, CancellationToken cancellationToken)
    {
        while (!predicate())
        {
            await Task.Delay(TimeSpan.FromMilliseconds(10), cancellationToken);
        }
    }

    private static void WriteInt32(Stream stream, int value)
    {
        Span<byte> bytes = stackalloc byte[sizeof(int)];
        BinaryPrimitives.WriteInt32BigEndian(bytes, value);
        stream.Write(bytes);
    }

    private static void WriteInt64(Stream stream, long value)
    {
        Span<byte> bytes = stackalloc byte[sizeof(long)];
        BinaryPrimitives.WriteInt64BigEndian(bytes, value);
        stream.Write(bytes);
    }

    private static void WriteUInt16(Stream stream, ushort value)
    {
        Span<byte> bytes = stackalloc byte[sizeof(ushort)];
        BinaryPrimitives.WriteUInt16BigEndian(bytes, value);
        stream.Write(bytes);
    }

    private sealed class FakeRemotingClient : IRemotingClient
    {
        private readonly ConcurrentDictionary<int, RemotingRequestHandler> _requestHandlers = new();

        public List<(EndPoint EndPoint, RemotingCommand Request)> Invocations { get; } = [];

        public List<(EndPoint EndPoint, RemotingCommand Request)> OnewayInvocations { get; } = [];

        public int RequestHandlerCount => _requestHandlers.Count;

        public Task<RemotingCommand> InvokeAsync(
            EndPoint endPoint,
            RemotingCommand request,
            TimeSpan timeout,
            CancellationToken cancellationToken = default)
        {
            Invocations.Add((endPoint, request));
            return Task.FromResult(SendSuccessResponse());
        }

        public Task InvokeOnewayAsync(
            EndPoint endPoint,
            RemotingCommand request,
            TimeSpan timeout,
            CancellationToken cancellationToken = default)
        {
            OnewayInvocations.Add((endPoint, request));
            return Task.CompletedTask;
        }

        public IDisposable RegisterRequestHandler(int requestCode, RemotingRequestHandler handler)
        {
            if (!_requestHandlers.TryAdd(requestCode, handler))
            {
                throw new InvalidOperationException($"Request handler {requestCode} is already registered.");
            }

            return new Registration(() => _requestHandlers.TryRemove(requestCode, out _));
        }

        public ValueTask DisposeAsync() => ValueTask.CompletedTask;

        public ValueTask<RemotingRequestResult> DispatchInboundAsync(EndPoint endPoint, RemotingCommand request)
        {
            if (!_requestHandlers.TryGetValue(request.Code, out var handler))
            {
                return ValueTask.FromResult(RemotingRequestResult.NotHandled);
            }

            return handler(new RemotingRequestContext(endPoint, request, CancellationToken.None));
        }

        private sealed class Registration(Action dispose) : IDisposable
        {
            private Action? _dispose = dispose;

            public void Dispose() => Interlocked.Exchange(ref _dispose, null)?.Invoke();
        }
    }
}
