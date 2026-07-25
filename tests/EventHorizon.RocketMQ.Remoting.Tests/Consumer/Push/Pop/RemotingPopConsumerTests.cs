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
using System.Net;
using System.Text;
using EventHorizon.RocketMQ.Consumer;
using EventHorizon.RocketMQ.Remoting.Consumer;
using EventHorizon.RocketMQ.Remoting.Consumer.Pull;
using EventHorizon.RocketMQ.Remoting.Consumer.Push.Pop;
using EventHorizon.RocketMQ.Remoting.Exceptions;
using EventHorizon.RocketMQ.Remoting.Protocol;
using Microsoft.Extensions.Options;
using Moq;
using Xunit;

namespace EventHorizon.RocketMQ.Remoting.Tests.Consumer.Push.Pop;

public sealed class RemotingPopConsumerTests
{
    [Theory]
    [InlineData(ResponseCodes.ResSuccess, RemotingPopStatus.Found)]
    [InlineData(ResponseCodes.ResPullNotFound, RemotingPopStatus.NoNewMessage)]
    [InlineData(ResponseCodes.ResPollingTimeout, RemotingPopStatus.NoNewMessage)]
    [InlineData(ResponseCodes.ResPollingFull, RemotingPopStatus.PollingFull)]
    public async Task PopAsync_UsesConfiguredRequestAndMapsBrokerStatus(
        int responseCode,
        RemotingPopStatus expectedStatus)
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var requests = new List<(EndPoint Endpoint, RemotingCommand Request)>();
        var engine = CreateEngine();
        var remoting = new Mock<IRemotingClient>(MockBehavior.Strict);
        remoting
            .Setup(value => value.InvokeAsync(
                It.IsAny<EndPoint>(),
                It.IsAny<RemotingCommand>(),
                It.IsAny<TimeSpan>(),
                It.IsAny<CancellationToken>()))
            .Returns<EndPoint, RemotingCommand, TimeSpan, CancellationToken>((endpoint, request, _, _) =>
            {
                requests.Add((endpoint, request));
                return Task.FromResult(new RemotingCommand
                {
                    Code = responseCode,
                    ExtFields = new Dictionary<string, object> { ["restNum"] = "7" }
                });
            });
        var options = new RemotingPopConsumerOptions
        {
            GroupName = "orders-consumer",
            BatchSize = 11,
            InvisibleDuration = TimeSpan.FromSeconds(45),
            LongPollingTimeout = TimeSpan.FromSeconds(9)
        };
        options.Subscribe("orders", new FilterExpression("amount > 100", FilterExpressionType.Sql));
        await using var consumer = CreateConsumer(options, engine.Object, remoting.Object);
        var queue = new RemotingPullMessageQueue("orders", 2, "broker-a", "127.0.0.1:10911");

        await consumer.StartAsync(cancellationToken);
        var result = await consumer.PopAsync(queue, cancellationToken: cancellationToken);

        Assert.Equal(expectedStatus, result.Status);
        Assert.Equal(7, result.RestNum);
        Assert.Empty(result.Messages);
        var request = Assert.Single(requests);
        Assert.Equal(10911, Assert.IsType<IPEndPoint>(request.Endpoint).Port);
        Assert.Equal(RequestCode.PopMessage, request.Request.Code);
        Assert.Equal("tenant-a%orders-consumer", request.Request.ExtFields["consumerGroup"]);
        Assert.Equal("tenant-a%orders", request.Request.ExtFields["topic"]);
        Assert.Equal(2, request.Request.ExtFields["queueId"]);
        Assert.Equal(11, request.Request.ExtFields["maxMsgNums"]);
        Assert.Equal(45_000L, request.Request.ExtFields["invisibleTime"]);
        Assert.Equal(9_000L, request.Request.ExtFields["pollTime"]);
        Assert.Equal(1_700_000_000_000L, request.Request.ExtFields["bornTime"]);
        Assert.Equal(0, request.Request.ExtFields["initMode"]);
        Assert.Equal("SQL92", request.Request.ExtFields["expType"]);
        Assert.Equal("amount > 100", request.Request.ExtFields["exp"]);
        Assert.Equal(false, request.Request.ExtFields["order"]);
        Assert.Equal("broker-a", request.Request.ExtFields["bname"]);
    }

    [Fact]
    public async Task PopAsync_RejectsResponsesAboveTheConfiguredBodyLimit()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var engine = CreateEngine();
        var remoting = new Mock<IRemotingClient>(MockBehavior.Strict);
        remoting
            .Setup(value => value.InvokeAsync(
                It.IsAny<EndPoint>(),
                It.IsAny<RemotingCommand>(),
                It.IsAny<TimeSpan>(),
                It.IsAny<CancellationToken>()))
            .ReturnsAsync(new RemotingCommand
            {
                Code = ResponseCodes.ResSuccess,
                Body = [1, 2, 3]
            });
        await using var consumer = CreateConsumer(
            new RemotingPopConsumerOptions { GroupName = "orders-consumer", MaxMessageBytes = 2 },
            engine.Object,
            remoting.Object);
        var queue = new RemotingPullMessageQueue("orders", 0, "broker-a", "127.0.0.1:10911");
        await consumer.StartAsync(cancellationToken);

        var exception = await Assert.ThrowsAsync<InvalidDataException>(() =>
            consumer.PopAsync(queue, cancellationToken: cancellationToken));

        Assert.Contains("configured limit", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task PopAsync_RejectsMessageCountAboveTheBrokerLimitBeforeInvoking()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var engine = CreateEngine();
        var remoting = new Mock<IRemotingClient>(MockBehavior.Strict);
        await using var consumer = CreateConsumer(
            new RemotingPopConsumerOptions { GroupName = "orders-consumer" },
            engine.Object,
            remoting.Object);
        var queue = new RemotingPullMessageQueue("orders", 0, "broker-a", "127.0.0.1:10911");
        await consumer.StartAsync(cancellationToken);

        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() =>
            consumer.PopAsync(queue, maxMessages: 33, cancellationToken: cancellationToken));

        remoting.VerifyNoOtherCalls();
    }

    [Fact]
    public async Task PopAsync_CapturesIssuingBrokerAddressBeforeTheQueuePreferenceChanges()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var queue = new RemotingPullMessageQueue(
            "orders",
            3,
            "broker-a",
            new Dictionary<long, string>
            {
                [0] = "127.0.0.1:10911",
                [1] = "127.0.0.1:10912"
            });
        var engine = CreateEngine();
        var remoting = new Mock<IRemotingClient>(MockBehavior.Strict);
        remoting
            .Setup(value => value.InvokeAsync(
                It.IsAny<EndPoint>(),
                It.IsAny<RemotingCommand>(),
                It.IsAny<TimeSpan>(),
                It.IsAny<CancellationToken>()))
            .Returns<EndPoint, RemotingCommand, TimeSpan, CancellationToken>((endpoint, _, _, _) =>
            {
                Assert.Equal(10911, Assert.IsType<IPEndPoint>(endpoint).Port);
                queue.SetPreferredBrokerId(1);
                return Task.FromResult(new RemotingCommand
                {
                    Code = ResponseCodes.ResSuccess,
                    Body = CreatePopMessageBody()
                });
            });
        await using var consumer = CreateConsumer(
            new RemotingPopConsumerOptions { GroupName = "orders-consumer" },
            engine.Object,
            remoting.Object);
        await consumer.StartAsync(cancellationToken);

        var result = await consumer.PopAsync(queue, cancellationToken: cancellationToken);

        Assert.Equal("127.0.0.1:10911", Assert.Single(result.Messages).Receipt.BrokerAddress);
    }

    [Fact]
    public async Task ReceiptOperations_UseIssuingBrokerAndRenewTheReceipt()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var requests = new List<(EndPoint Endpoint, RemotingCommand Request)>();
        var engine = CreateEngine();
        var remoting = new Mock<IRemotingClient>(MockBehavior.Strict);
        remoting
            .Setup(value => value.InvokeAsync(
                It.IsAny<EndPoint>(),
                It.IsAny<RemotingCommand>(),
                It.IsAny<TimeSpan>(),
                It.IsAny<CancellationToken>()))
            .Returns<EndPoint, RemotingCommand, TimeSpan, CancellationToken>((endpoint, request, _, _) =>
            {
                requests.Add((endpoint, request));
                return Task.FromResult(request.Code == RequestCode.ChangeMessageInvisibleTime
                    ? new RemotingCommand
                    {
                        Code = ResponseCodes.ResSuccess,
                        ExtFields = new Dictionary<string, object>
                        {
                            ["popTime"] = "1700000010000",
                            ["invisibleTime"] = "60000",
                            ["reviveQid"] = "7"
                        }
                    }
                    : new RemotingCommand { Code = ResponseCodes.ResSuccess });
            });
        await using var consumer = CreateConsumer(
            new RemotingPopConsumerOptions { GroupName = "orders-consumer" },
            engine.Object,
            remoting.Object);
        var receipt = new RemotingPopReceipt(
            "orders",
            "broker-a",
            "127.0.0.1:10912",
            2,
            4,
            8,
            DateTimeOffset.FromUnixTimeMilliseconds(1_700_000_000_000),
            TimeSpan.FromSeconds(30),
            3,
            "4 1700000000000 30000 3 0 broker-a 2 8");
        await consumer.StartAsync(cancellationToken);

        await consumer.AcknowledgeAsync(receipt, cancellationToken);
        var renewed = await consumer.ChangeInvisibleTimeAsync(receipt, TimeSpan.FromMinutes(1), cancellationToken);

        Assert.Equal(DateTimeOffset.FromUnixTimeMilliseconds(1_700_000_010_000), renewed.PopTime);
        Assert.Equal(TimeSpan.FromMinutes(1), renewed.InvisibleTime);
        Assert.Equal(7, renewed.ReviveQueueId);
        Assert.Equal(8, renewed.CheckpointOffset);
        Assert.Equal("8 1700000010000 60000 7 0 broker-a 2 8", renewed.ExtraInfo);
        await consumer.AcknowledgeAsync(renewed, cancellationToken);

        Assert.Equal(3, requests.Count);
        Assert.All(requests, request => Assert.Equal(10912, Assert.IsType<IPEndPoint>(request.Endpoint).Port));
        Assert.Equal(RequestCode.AckMessage, requests[0].Request.Code);
        Assert.Equal(RequestCode.ChangeMessageInvisibleTime, requests[1].Request.Code);
        Assert.Equal(RequestCode.AckMessage, requests[2].Request.Code);
        Assert.Equal("tenant-a%orders-consumer", requests[0].Request.ExtFields["consumerGroup"]);
        Assert.Equal("tenant-a%orders", requests[0].Request.ExtFields["topic"]);
        Assert.Equal(2, requests[0].Request.ExtFields["queueId"]);
        Assert.Equal(8L, requests[0].Request.ExtFields["offset"]);
        Assert.Equal("4 1700000000000 30000 3 0 broker-a 2 8", requests[0].Request.ExtFields["extraInfo"]);
        Assert.Equal(60_000L, requests[1].Request.ExtFields["invisibleTime"]);
        Assert.Equal("8 1700000010000 60000 7 0 broker-a 2 8", requests[2].Request.ExtFields["extraInfo"]);
    }

    [Fact]
    public async Task ChangeInvisibleTimeAsync_RejectsMalformedRenewalResponse()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var engine = CreateEngine();
        var remoting = new Mock<IRemotingClient>(MockBehavior.Strict);
        remoting
            .Setup(value => value.InvokeAsync(
                It.IsAny<EndPoint>(),
                It.IsAny<RemotingCommand>(),
                It.IsAny<TimeSpan>(),
                It.IsAny<CancellationToken>()))
            .ReturnsAsync(new RemotingCommand
            {
                Code = ResponseCodes.ResSuccess,
                ExtFields = new Dictionary<string, object>
                {
                    ["popTime"] = "invalid",
                    ["invisibleTime"] = "60000",
                    ["reviveQid"] = "7"
                }
            });
        await using var consumer = CreateConsumer(
            new RemotingPopConsumerOptions { GroupName = "orders-consumer" },
            engine.Object,
            remoting.Object);
        var receipt = new RemotingPopReceipt(
            "orders",
            "broker-a",
            "127.0.0.1:10912",
            2,
            4,
            8,
            DateTimeOffset.FromUnixTimeMilliseconds(1_700_000_000_000),
            TimeSpan.FromSeconds(30),
            3,
            "4 1700000000000 30000 3 0 broker-a 2 8");
        await consumer.StartAsync(cancellationToken);

        var exception = await Assert.ThrowsAsync<InvalidDataException>(() =>
            consumer.ChangeInvisibleTimeAsync(receipt, TimeSpan.FromMinutes(1), cancellationToken));

        Assert.Contains("popTime", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task AcknowledgeAsync_ThrowsWhenBrokerRejectsTheReceipt()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var engine = CreateEngine();
        var remoting = new Mock<IRemotingClient>(MockBehavior.Strict);
        remoting
            .Setup(value => value.InvokeAsync(
                It.IsAny<EndPoint>(),
                It.IsAny<RemotingCommand>(),
                It.IsAny<TimeSpan>(),
                It.IsAny<CancellationToken>()))
            .ReturnsAsync(new RemotingCommand { Code = ResponseCodes.ResNoPermission, Remark = "receipt expired" });
        await using var consumer = CreateConsumer(
            new RemotingPopConsumerOptions { GroupName = "orders-consumer" },
            engine.Object,
            remoting.Object);
        var receipt = new RemotingPopReceipt(
            "orders",
            "broker-a",
            "127.0.0.1:10912",
            2,
            4,
            8,
            DateTimeOffset.FromUnixTimeMilliseconds(1_700_000_000_000),
            TimeSpan.FromSeconds(30),
            3,
            "4 1700000000000 30000 3 0 broker-a 2 8");
        await consumer.StartAsync(cancellationToken);

        var exception = await Assert.ThrowsAsync<RemotingCommandException>(() =>
            consumer.AcknowledgeAsync(receipt, cancellationToken));

        Assert.Equal(ResponseCodes.ResNoPermission, exception.ResponseCode);
        Assert.Equal("receipt expired", exception.Message);
    }

    private static Mock<IRemotingConsumerEngine> CreateEngine()
    {
        var engine = new Mock<IRemotingConsumerEngine>(MockBehavior.Strict);
        engine.Setup(value => value.StartAsync(It.IsAny<CancellationToken>())).Returns(ValueTask.CompletedTask);
        engine.Setup(value => value.StopAsync(CancellationToken.None)).Returns(ValueTask.CompletedTask);
        engine.Setup(value => value.DisposeAsync()).Returns(ValueTask.CompletedTask);
        return engine;
    }

    private static RemotingPopConsumer CreateConsumer(
        RemotingPopConsumerOptions options,
        IRemotingConsumerEngine engine,
        IRemotingClient remotingClient)
    {
        return new RemotingPopConsumer(
            Options.Create(options),
            Options.Create(new RemotingClientOptions
            {
                Namespace = "tenant-a",
                RequestTimeout = TimeSpan.FromSeconds(3)
            }),
            engine,
            remotingClient,
            new FixedTimeProvider(DateTimeOffset.FromUnixTimeMilliseconds(1_700_000_000_000)));
    }

    private static byte[] CreatePopMessageBody()
    {
        const string topic = "orders";
        const string checkpoint = "40 1700000000000 60000 2 0 broker-a 3 42";
        var properties = Encoding.UTF8.GetBytes(
            $"UNIQ_KEY\u0001message-id\u0002POP_CK\u0001{checkpoint}\u0002");
        using var stream = new MemoryStream();
        WriteInt32(stream, 0);
        WriteInt32(stream, -626843481);
        WriteInt32(stream, 0);
        WriteInt32(stream, 3);
        WriteInt32(stream, 0);
        WriteInt64(stream, 42);
        WriteInt64(stream, 100);
        WriteInt32(stream, 0);
        WriteInt64(stream, 1_700_000_000_000);
        stream.Write([127, 0, 0, 1, 0x2A, 0x9F, 0, 0]);
        WriteInt64(stream, 1_700_000_000_001);
        stream.Write([127, 0, 0, 1, 0x2A, 0x9F, 0, 0]);
        WriteInt32(stream, 0);
        WriteInt64(stream, 0);
        WriteInt32(stream, 0);
        stream.WriteByte((byte)topic.Length);
        stream.Write(Encoding.UTF8.GetBytes(topic));
        WriteUInt16(stream, checked((ushort)properties.Length));
        stream.Write(properties);

        var record = stream.ToArray();
        BinaryPrimitives.WriteInt32BigEndian(record, record.Length);
        return record;
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

    private sealed class FixedTimeProvider(DateTimeOffset utcNow) : TimeProvider
    {
        public override DateTimeOffset GetUtcNow() => utcNow;
    }
}
