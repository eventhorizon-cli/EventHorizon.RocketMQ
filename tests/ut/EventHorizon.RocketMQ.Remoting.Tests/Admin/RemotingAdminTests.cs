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
using EventHorizon.RocketMQ.Remoting.Admin;
using EventHorizon.RocketMQ.Remoting.Consumer;
using EventHorizon.RocketMQ.Remoting.Exceptions;
using EventHorizon.RocketMQ.Remoting.Protocol;
using EventHorizon.RocketMQ.Remoting.Protocol.Route;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Moq;
using Xunit;

namespace EventHorizon.RocketMQ.Remoting.Tests.Admin;

public sealed class RemotingAdminTests
{
    [Fact]
    public async Task GetMessageQueuesAsync_RouteContainsReadOnlyBroker_ReturnsLogicalReadableQueues()
    {
        var route = CreateRoute(
            ("broker-b", 2, 2, 6, [(0L, "127.0.0.1:20911")]),
            ("broker-a", 1, 1, 6, [(0L, "127.0.0.1:10911")]),
            ("read-only", 1, 0, 4, [(0L, "127.0.0.1:30911")]),
            ("write-only", 0, 1, 2, [(0L, "127.0.0.1:40911")]));
        var routes = CreateRouteServiceMock(route);
        var remoting = new Mock<IRemotingClient>(MockBehavior.Strict);
        var admin = CreateAdmin(
            new RemotingClientOptions { Namespace = "tenant-a" },
            routes.Object,
            remoting.Object);

        var queues = await admin.GetMessageQueuesAsync("orders", TestContext.Current.CancellationToken);

        Assert.Equal(4, queues.Count);
        Assert.Equal(
            [("broker-a", 0), ("broker-b", 0), ("broker-b", 1), ("read-only", 0)],
            queues.Select(static queue => (queue.BrokerName, queue.QueueId)));
        Assert.All(queues, static queue => Assert.Equal("orders", queue.Topic));
        routes.Verify(value => value.GetAsync(
            "tenant-a%orders",
            false,
            TestContext.Current.CancellationToken), Times.Once);
    }

    [Fact]
    public async Task OffsetAndTimeOperations_MasterAndClassicHeaders_UsesMasterHeaders()
    {
        var route = CreateRoute(("broker-a", 1, 1, 6,
            [(0L, "127.0.0.1:10911"), (1L, "127.0.0.1:10912")]));
        var routes = CreateRouteServiceMock(route);
        var calls = new List<(EndPoint Endpoint, RemotingCommand Request)>();
        var remoting = new Mock<IRemotingClient>(MockBehavior.Strict);
        remoting
            .Setup(value => value.InvokeAsync(
                It.IsAny<EndPoint>(),
                It.IsAny<RemotingCommand>(),
                It.IsAny<TimeSpan>(),
                It.IsAny<CancellationToken>()))
            .Returns<EndPoint, RemotingCommand, TimeSpan, CancellationToken>((endpoint, request, _, _) =>
            {
                calls.Add((endpoint, request));
                var extFields = request.Code == RequestCode.GetEarliestMessageStoreTime
                    ? new Dictionary<string, object> { ["timestamp"] = "1767225600123" }
                    : new Dictionary<string, object> { ["offset"] = "42" };
                return Task.FromResult(new RemotingCommand { Code = ResponseCodes.ResSuccess, ExtFields = extFields });
            });
        var admin = CreateAdmin(
            new RemotingClientOptions { Namespace = "tenant-a" },
            routes.Object,
            remoting.Object);
        var queue = new RemotingConsumerQueue("orders", "broker-a", 3);
        var timestamp = DateTimeOffset.Parse("2026-01-01T00:00:00Z");

        var minimum = await admin.GetMinOffsetAsync(queue, TestContext.Current.CancellationToken);
        var maximum = await admin.GetMaxOffsetAsync(queue, committed: false, TestContext.Current.CancellationToken);
        var searched = await admin.SearchOffsetAsync(
            queue,
            timestamp,
            RemotingOffsetBoundary.Upper,
            TestContext.Current.CancellationToken);
        var earliestMessageStoreTime = await admin.GetEarliestMessageStoreTimeAsync(
            queue,
            TestContext.Current.CancellationToken);
        var consumerOffset = await admin.GetConsumerOffsetAsync(
            "orders-group",
            queue,
            TestContext.Current.CancellationToken);

        Assert.Equal(42, minimum);
        Assert.Equal(42, maximum);
        Assert.Equal(42, searched);
        Assert.Equal(DateTimeOffset.FromUnixTimeMilliseconds(1_767_225_600_123), earliestMessageStoreTime);
        Assert.Equal(42, consumerOffset);
        Assert.All(calls, static call => Assert.Equal(10911, Assert.IsType<IPEndPoint>(call.Endpoint).Port));
        Assert.Equal(
            [
                RequestCode.GetMinOffset,
                RequestCode.GetMaxOffset,
                RequestCode.SearchOffsetByTimestamp,
                RequestCode.GetEarliestMessageStoreTime,
                RequestCode.QueryConsumerOffset
            ],
            calls.Select(static call => call.Request.Code));

        var minimumRequest = calls[0].Request;
        Assert.Equal("tenant-a%orders", minimumRequest.ExtFields["topic"]);
        Assert.Equal(3, minimumRequest.ExtFields["queueId"]);
        Assert.Equal("broker-a", minimumRequest.ExtFields["bname"]);
        Assert.DoesNotContain("committed", minimumRequest.ExtFields.Keys);

        Assert.Equal(false, calls[1].Request.ExtFields["committed"]);
        Assert.Equal("upper", calls[2].Request.ExtFields["boundaryType"]);
        Assert.Equal(timestamp.ToUnixTimeMilliseconds(), calls[2].Request.ExtFields["timestamp"]);
        Assert.Equal("tenant-a%orders", calls[3].Request.ExtFields["topic"]);
        Assert.Equal(3, calls[3].Request.ExtFields["queueId"]);
        Assert.Equal("broker-a", calls[3].Request.ExtFields["bname"]);
        Assert.Equal("tenant-a%orders-group", calls[4].Request.ExtFields["consumerGroup"]);
        Assert.Equal(false, calls[4].Request.ExtFields["setZeroIfNotFound"]);
    }

    [Fact]
    public async Task GetEarliestMessageStoreTimeAsync_NoMessage_ReturnsNullAndReports()
    {
        var routes = CreateRouteServiceMock(CreateRoute(("broker-a", 1, 1, 6, [(0L, "127.0.0.1:10911")])));
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
                ExtFields = new Dictionary<string, object> { ["timestamp"] = "-1" }
            });
        var admin = CreateAdmin(new RemotingClientOptions(), routes.Object, remoting.Object);

        var timestamp = await admin.GetEarliestMessageStoreTimeAsync(
            new RemotingConsumerQueue("orders", "broker-a", 0),
            TestContext.Current.CancellationToken);

        Assert.Null(timestamp);
    }

    [Fact]
    public async Task GetEarliestMessageStoreTimeAsync_InvalidDataExceptionForOutOfRangeTimestamp_Throws()
    {
        var routes = CreateRouteServiceMock(CreateRoute(("broker-a", 1, 1, 6, [(0L, "127.0.0.1:10911")])));
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
                ExtFields = new Dictionary<string, object> { ["timestamp"] = long.MaxValue.ToString() }
            });
        var admin = CreateAdmin(new RemotingClientOptions(), routes.Object, remoting.Object);

        await Assert.ThrowsAsync<InvalidDataException>(() => admin.GetEarliestMessageStoreTimeAsync(
            new RemotingConsumerQueue("orders", "broker-a", 0),
            TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task GetEarliestMessageStoreTimeAsync_RemotingCommandExceptionForBrokerRejection_Throws()
    {
        var routes = CreateRouteServiceMock(CreateRoute(("broker-a", 1, 1, 6, [(0L, "127.0.0.1:10911")])));
        var remoting = new Mock<IRemotingClient>(MockBehavior.Strict);
        remoting
            .Setup(value => value.InvokeAsync(
                It.IsAny<EndPoint>(),
                It.IsAny<RemotingCommand>(),
                It.IsAny<TimeSpan>(),
                It.IsAny<CancellationToken>()))
            .ReturnsAsync(new RemotingCommand { Code = ResponseCodes.ResNoPermission, Remark = "denied" });
        var admin = CreateAdmin(new RemotingClientOptions(), routes.Object, remoting.Object);

        var exception = await Assert.ThrowsAsync<RemotingCommandException>(() =>
            admin.GetEarliestMessageStoreTimeAsync(
                new RemotingConsumerQueue("orders", "broker-a", 0),
                TestContext.Current.CancellationToken));

        Assert.Equal(ResponseCodes.ResNoPermission, exception.ResponseCode);
        Assert.Equal("denied", exception.Message);
    }

    [Fact]
    public async Task ViewMessageAsync_OffsetMessageId_UsesAndDecodesMessage()
    {
        const long commitLogOffset = 1234;
        var routes = new Mock<ITopicRouteService>(MockBehavior.Strict);
        EndPoint? endpoint = null;
        RemotingCommand? request = null;
        var remoting = new Mock<IRemotingClient>(MockBehavior.Strict);
        remoting
            .Setup(value => value.InvokeAsync(
                It.IsAny<EndPoint>(),
                It.IsAny<RemotingCommand>(),
                It.IsAny<TimeSpan>(),
                It.IsAny<CancellationToken>()))
            .Returns<EndPoint, RemotingCommand, TimeSpan, CancellationToken>((value, command, _, _) =>
            {
                endpoint = value;
                request = command;
                return Task.FromResult(new RemotingCommand
                {
                    Code = ResponseCodes.ResSuccess,
                    Body = CreateViewMessageRecord("tenant-a%orders", commitLogOffset)
                });
            });
        var admin = CreateAdmin(
            new RemotingClientOptions { Namespace = "tenant-a" },
            routes.Object,
            remoting.Object);
        var offsetMessageId = CreateOffsetMessageId(IPAddress.Loopback, 10911, commitLogOffset);

        var message = await admin.ViewMessageAsync("orders", offsetMessageId, TestContext.Current.CancellationToken);

        var brokerEndpoint = Assert.IsType<IPEndPoint>(endpoint);
        Assert.Equal(IPAddress.Loopback, brokerEndpoint.Address);
        Assert.Equal(10911, brokerEndpoint.Port);
        Assert.Equal(RequestCode.ViewMessageById, request!.Code);
        Assert.Equal("tenant-a%orders", request.ExtFields["topic"]);
        Assert.Equal(commitLogOffset, request.ExtFields["offset"]);
        Assert.DoesNotContain("bname", request.ExtFields.Keys);
        Assert.Equal("orders", message.Topic);
        Assert.Equal("view message"u8.ToArray(), message.Body);
        Assert.Equal("message-id", message.MessageId);
        Assert.Equal(offsetMessageId, message.OffsetMessageId);
        Assert.Equal(3, message.QueueId);
        Assert.Equal(42, message.QueueOffset);
        Assert.Equal(commitLogOffset, message.CommitLogOffset);
        Assert.Null(message.BrokerName);
        routes.VerifyNoOtherCalls();
    }

    [Fact]
    public async Task ViewMessageAsync_Ipv6EndpointAndRejection_UsesAndPreserves()
    {
        var routes = new Mock<ITopicRouteService>(MockBehavior.Strict);
        EndPoint? endpoint = null;
        var remoting = new Mock<IRemotingClient>(MockBehavior.Strict);
        remoting
            .Setup(value => value.InvokeAsync(
                It.IsAny<EndPoint>(),
                It.IsAny<RemotingCommand>(),
                It.IsAny<TimeSpan>(),
                It.IsAny<CancellationToken>()))
            .Returns<EndPoint, RemotingCommand, TimeSpan, CancellationToken>((value, _, _, _) =>
            {
                endpoint = value;
                return Task.FromResult(new RemotingCommand
                {
                    Code = ResponseCodes.ResError,
                    Remark = "can not find message by the offset"
                });
            });
        var admin = CreateAdmin(new RemotingClientOptions(), routes.Object, remoting.Object);
        var offsetMessageId = CreateOffsetMessageId(IPAddress.IPv6Loopback, 10911, 42);

        var exception = await Assert.ThrowsAsync<RemotingCommandException>(() =>
            admin.ViewMessageAsync("orders", offsetMessageId, TestContext.Current.CancellationToken));

        var brokerEndpoint = Assert.IsType<IPEndPoint>(endpoint);
        Assert.Equal(IPAddress.IPv6Loopback, brokerEndpoint.Address);
        Assert.Equal(10911, brokerEndpoint.Port);
        Assert.Equal(ResponseCodes.ResError, exception.ResponseCode);
        Assert.Equal("can not find message by the offset", exception.Message);
        routes.VerifyNoOtherCalls();
    }

    [Fact]
    public async Task ViewMessageAsync_MalformedOffsetMessageIdWithoutSendingRequest_Rejects()
    {
        var routes = new Mock<ITopicRouteService>(MockBehavior.Strict);
        var remoting = new Mock<IRemotingClient>(MockBehavior.Strict);
        var admin = CreateAdmin(new RemotingClientOptions(), routes.Object, remoting.Object);

        await Assert.ThrowsAsync<InvalidDataException>(() =>
            admin.ViewMessageAsync("orders", "not-an-offset-message-id", TestContext.Current.CancellationToken));

        remoting.VerifyNoOtherCalls();
        routes.VerifyNoOtherCalls();
    }

    [Fact]
    public async Task ViewMessageAsync_ResponseWithoutExactlyOneMessage_Rejects()
    {
        var record = CreateViewMessageRecord("orders", 1234);
        var responses = new Queue<RemotingCommand>(
        [
            new RemotingCommand { Code = ResponseCodes.ResSuccess },
            new RemotingCommand { Code = ResponseCodes.ResSuccess, Body = record.Concat(record).ToArray() }
        ]);
        var routes = new Mock<ITopicRouteService>(MockBehavior.Strict);
        var remoting = new Mock<IRemotingClient>(MockBehavior.Strict);
        remoting
            .Setup(value => value.InvokeAsync(
                It.IsAny<EndPoint>(),
                It.IsAny<RemotingCommand>(),
                It.IsAny<TimeSpan>(),
                It.IsAny<CancellationToken>()))
            .Returns(() => Task.FromResult(responses.Dequeue()));
        var admin = CreateAdmin(new RemotingClientOptions(), routes.Object, remoting.Object);
        var offsetMessageId = CreateOffsetMessageId(IPAddress.Loopback, 10911, 1234);

        await Assert.ThrowsAsync<InvalidDataException>(() =>
            admin.ViewMessageAsync("orders", offsetMessageId, TestContext.Current.CancellationToken));
        await Assert.ThrowsAsync<InvalidDataException>(() =>
            admin.ViewMessageAsync("orders", offsetMessageId, TestContext.Current.CancellationToken));

        routes.VerifyNoOtherCalls();
    }

    [Fact]
    public async Task GetConsumerOffsetAsync_NoCommittedOffset_ReturnsNull()
    {
        var routes = CreateRouteServiceMock(CreateRoute(("broker-a", 1, 1, 6, [(0L, "127.0.0.1:10911")])));
        var remoting = new Mock<IRemotingClient>(MockBehavior.Strict);
        remoting
            .Setup(value => value.InvokeAsync(
                It.IsAny<EndPoint>(),
                It.IsAny<RemotingCommand>(),
                It.IsAny<TimeSpan>(),
                It.IsAny<CancellationToken>()))
            .ReturnsAsync(new RemotingCommand { Code = ResponseCodes.ResQueryNotFound });
        var admin = CreateAdmin(new RemotingClientOptions(), routes.Object, remoting.Object);

        var offset = await admin.GetConsumerOffsetAsync(
            "orders-group",
            new RemotingConsumerQueue("orders", "broker-a", 0),
            TestContext.Current.CancellationToken);

        Assert.Null(offset);
    }

    [Fact]
    public async Task GetMinOffsetAsync_MissingMaster_RefreshesAndFallsBack()
    {
        var staleRoute = CreateRoute(("broker-b", 1, 1, 6, [(0L, "127.0.0.1:20911")]));
        var refreshedRoute = CreateRoute(("broker-a", 1, 1, 6,
            [(0L, string.Empty), (1L, "127.0.0.1:10912")]));
        var forceRefreshCalls = new List<bool>();
        var routes = new Mock<ITopicRouteService>(MockBehavior.Strict);
        routes
            .Setup(value => value.GetAsync(
                "orders",
                It.IsAny<bool>(),
                It.IsAny<CancellationToken>()))
            .Returns<string, bool, CancellationToken>((_, forceRefresh, _) =>
            {
                forceRefreshCalls.Add(forceRefresh);
                return Task.FromResult(forceRefresh ? refreshedRoute : staleRoute);
            });
        EndPoint? endpoint = null;
        var remoting = new Mock<IRemotingClient>(MockBehavior.Strict);
        remoting
            .Setup(value => value.InvokeAsync(
                It.IsAny<EndPoint>(),
                It.IsAny<RemotingCommand>(),
                It.IsAny<TimeSpan>(),
                It.IsAny<CancellationToken>()))
            .Returns<EndPoint, RemotingCommand, TimeSpan, CancellationToken>((value, _, _, _) =>
            {
                endpoint = value;
                return Task.FromResult(new RemotingCommand
                {
                    Code = ResponseCodes.ResSuccess,
                    ExtFields = new Dictionary<string, object> { ["offset"] = "7" }
                });
            });
        var admin = CreateAdmin(new RemotingClientOptions(), routes.Object, remoting.Object);

        var offset = await admin.GetMinOffsetAsync(
            new RemotingConsumerQueue("orders", "broker-a", 0),
            TestContext.Current.CancellationToken);

        Assert.Equal(7, offset);
        Assert.Equal([false, true], forceRefreshCalls);
        Assert.Equal(10912, Assert.IsType<IPEndPoint>(endpoint).Port);
    }

    [Fact]
    public async Task GetMaxOffsetAsync_RemotingCommandExceptionForBrokerRejection_Throws()
    {
        var routes = CreateRouteServiceMock(CreateRoute(("broker-a", 1, 1, 6, [(0L, "127.0.0.1:10911")])));
        var remoting = new Mock<IRemotingClient>(MockBehavior.Strict);
        remoting
            .Setup(value => value.InvokeAsync(
                It.IsAny<EndPoint>(),
                It.IsAny<RemotingCommand>(),
                It.IsAny<TimeSpan>(),
                It.IsAny<CancellationToken>()))
            .ReturnsAsync(new RemotingCommand { Code = ResponseCodes.ResNoPermission, Remark = "denied" });
        var admin = CreateAdmin(new RemotingClientOptions(), routes.Object, remoting.Object);

        var exception = await Assert.ThrowsAsync<RemotingCommandException>(() => admin.GetMaxOffsetAsync(
            new RemotingConsumerQueue("orders", "broker-a", 0),
            cancellationToken: TestContext.Current.CancellationToken));

        Assert.Equal(ResponseCodes.ResNoPermission, exception.ResponseCode);
        Assert.Equal("denied", exception.Message);
    }

    [Fact]
    public async Task AddRemotingAdmin_ProtocolSpecificAdminWithoutHostedService_RegistersAdmin()
    {
        var services = new ServiceCollection();
        services
            .AddRocketMQRemoting(options => options.NamesrvAddr = "127.0.0.1:9876")
            .AddRemotingAdmin();

        await using var provider = services.BuildServiceProvider(
            new ServiceProviderOptions { ValidateOnBuild = true });

        var admin = provider.GetRequiredService<IRemotingAdmin>();

        Assert.IsType<RemotingAdmin>(admin);
        Assert.Empty(provider.GetServices<Microsoft.Extensions.Hosting.IHostedService>());
    }

    private static RemotingAdmin CreateAdmin(
        RemotingClientOptions options,
        ITopicRouteService routeService,
        IRemotingClient remotingClient) =>
        new(Options.Create(options), routeService, remotingClient);

    private static Mock<ITopicRouteService> CreateRouteServiceMock(TopicRouteData route)
    {
        var routes = new Mock<ITopicRouteService>(MockBehavior.Strict);
        routes
            .Setup(value => value.GetAsync(
                It.IsAny<string>(),
                It.IsAny<bool>(),
                It.IsAny<CancellationToken>()))
            .ReturnsAsync(route);
        return routes;
    }

    private static TopicRouteData CreateRoute(
        params (string BrokerName, int ReadQueueCount, int WriteQueueCount, int Permission,
            (long BrokerId, string Address)[] Addresses)[] entries)
    {
        var brokers = new List<BrokerData>();
        var queues = new List<QueueData>();
        foreach (var entry in entries)
        {
            var broker = new BrokerData { BrokerName = entry.BrokerName };
            foreach (var (brokerId, address) in entry.Addresses)
            {
                broker.BrokerAddrs[brokerId] = address;
            }

            brokers.Add(broker);
            queues.Add(new QueueData
            {
                BrokerName = entry.BrokerName,
                ReadQueueNums = entry.ReadQueueCount,
                WriteQueueNums = entry.WriteQueueCount,
                Perm = entry.Permission
            });
        }

        return new TopicRouteData
        {
            QueueDatas = queues.ToArray(),
            BrokerDatas = brokers.ToArray()
        };
    }

    private static byte[] CreateViewMessageRecord(string topic, long commitLogOffset)
    {
        var body = "view message"u8.ToArray();
        var properties = Encoding.UTF8.GetBytes("UNIQ_KEY\u0001message-id\u0002TAGS\u0001created\u0002");
        using var stream = new MemoryStream();
        WriteInt32(stream, 0);
        WriteInt32(stream, -626843481);
        WriteInt32(stream, 0);
        WriteInt32(stream, 3);
        WriteInt32(stream, 0);
        WriteInt64(stream, 42);
        WriteInt64(stream, commitLogOffset);
        WriteInt32(stream, 0);
        WriteInt64(stream, 1_700_000_000_000);
        stream.Write([127, 0, 0, 1, 0, 0, 0x2A, 0x9F]);
        WriteInt64(stream, 1_700_000_000_100);
        stream.Write([127, 0, 0, 1, 0, 0, 0x2A, 0x9F]);
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

    private static string CreateOffsetMessageId(IPAddress address, int port, long offset)
    {
        var addressBytes = address.GetAddressBytes();
        var bytes = new byte[addressBytes.Length + sizeof(int) + sizeof(long)];
        addressBytes.CopyTo(bytes, 0);
        BinaryPrimitives.WriteInt32BigEndian(bytes.AsSpan(addressBytes.Length), port);
        BinaryPrimitives.WriteInt64BigEndian(bytes.AsSpan(addressBytes.Length + sizeof(int)), offset);
        return Convert.ToHexString(bytes);
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
}
