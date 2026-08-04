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

using System.Diagnostics;
using System.Net;
using EventHorizon.RocketMQ.Remoting.Consumer;
using EventHorizon.RocketMQ.Remoting.Consumer.Push;
using EventHorizon.RocketMQ.Remoting.Consumer.Push.Assignment;
using EventHorizon.RocketMQ.Remoting.Consumer.Push.Pop;
using EventHorizon.RocketMQ.Remoting.Consumer.Route;
using EventHorizon.RocketMQ.Remoting.Instrumentation;
using EventHorizon.RocketMQ.Remoting.Protocol;
using EventHorizon.RocketMQ.Remoting.Protocol.Route;
using Microsoft.Extensions.Options;
using Moq;
using Xunit;

namespace EventHorizon.RocketMQ.Remoting.Tests.Consumer.Push.Pop;

public sealed class PopWireClientTests
{
    [Fact]
    public async Task PopAsync_RouteChangesBetweenRequests_UsesCurrentBrokerAddress()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var ports = new List<int>();
        var remoting = new Mock<IRemotingClient>(MockBehavior.Strict);
        remoting
            .Setup(value => value.InvokeAsync(
                It.IsAny<EndPoint>(),
                It.Is<RemotingCommand>(request =>
                    request.Code == RequestCode.PopMessage &&
                    Equals(request.ExtFields["bname"], "broker-a") &&
                    Equals(request.ExtFields["queueId"], -1)),
                It.IsAny<TimeSpan>(),
                It.IsAny<CancellationToken>()))
            .Callback<EndPoint, RemotingCommand, TimeSpan, CancellationToken>(
                (endpoint, _, _, _) => ports.Add(GetPort(endpoint)))
            .ReturnsAsync(new RemotingCommand { Code = ResponseCodes.ResPollingTimeout });
        var operation = CreateOperation();
        var telemetry = CreateReceiveTelemetry(operation);
        var client = CreateClient(
            remoting.Object,
            telemetry.Object,
            "127.0.0.1:10911",
            "127.0.0.1:20911");

        await client.PopAsync(
            CreateAssignment(),
            FilterExpression.All,
            cancellationToken);
        await client.PopAsync(
            CreateAssignment(),
            FilterExpression.All,
            cancellationToken);

        Assert.Equal([10911, 20911], ports);
    }

    [Fact]
    public async Task PopAsync_BrokerReturnsNoMessages_RecordsEmptyReceiveWithoutSpan()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var remoting = new Mock<IRemotingClient>(MockBehavior.Strict);
        remoting
            .Setup(value => value.InvokeAsync(
                It.IsAny<EndPoint>(),
                It.Is<RemotingCommand>(request => request.Code == RequestCode.PopMessage),
                It.IsAny<TimeSpan>(),
                It.IsAny<CancellationToken>()))
            .ReturnsAsync(new RemotingCommand { Code = ResponseCodes.ResPollingTimeout });
        var operation = CreateOperation();
        var telemetry = new Mock<IRemotingRocketMQTelemetry>(MockBehavior.Strict);
        telemetry
            .Setup(value => value.StartReceive(
                "orders",
                "orders-consumer",
                0,
                It.IsAny<ActivityContext?>(),
                It.IsAny<DateTimeOffset>(),
                It.IsAny<long>(),
                null,
                false,
                It.IsAny<IEnumerable<IReadOnlyDictionary<string, string>>?>()))
            .Returns(operation.Object);
        var client = CreateClient(remoting.Object, telemetry.Object);

        var result = await client.PopAsync(
            CreateAssignment(),
            FilterExpression.All,
            cancellationToken);

        Assert.Empty(result.Messages);
        operation.Verify(value => value.Complete(), Times.Once);
        operation.Verify(value => value.Dispose(), Times.Once);
    }

    [Fact]
    public async Task PopAsync_RemotingCallFails_RecordsReceiveFailure()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var failure = new IOException("POP failed.");
        var remoting = new Mock<IRemotingClient>(MockBehavior.Strict);
        remoting
            .Setup(value => value.InvokeAsync(
                It.IsAny<EndPoint>(),
                It.IsAny<RemotingCommand>(),
                It.IsAny<TimeSpan>(),
                It.IsAny<CancellationToken>()))
            .ThrowsAsync(failure);
        var operation = CreateOperation();
        var telemetry = new Mock<IRemotingRocketMQTelemetry>(MockBehavior.Strict);
        telemetry
            .Setup(value => value.StartReceive(
                "orders",
                "orders-consumer",
                0,
                It.IsAny<ActivityContext?>(),
                It.IsAny<DateTimeOffset>(),
                It.IsAny<long>(),
                null,
                true,
                It.IsAny<IEnumerable<IReadOnlyDictionary<string, string>>?>()))
            .Returns(operation.Object);
        var client = CreateClient(remoting.Object, telemetry.Object);

        var exception = await Assert.ThrowsAsync<IOException>(() => client.PopAsync(
            CreateAssignment(),
            FilterExpression.All,
            cancellationToken));

        Assert.Same(failure, exception);
        operation.Verify(value => value.Complete(failure), Times.Once);
        operation.Verify(value => value.Dispose(), Times.Once);
    }

    [Fact]
    public async Task PopAsync_RouteResolutionFails_RecordsReceiveFailure()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var failure = new IOException("Route resolution failed.");
        var remoting = new Mock<IRemotingClient>(MockBehavior.Strict);
        var operation = CreateOperation();
        var telemetry = CreateReceiveTelemetry(operation, createActivity: true);
        var client = CreateClient(remoting.Object, telemetry.Object, CreateFailedRouteService(failure).Object);

        var exception = await Assert.ThrowsAsync<IOException>(() => client.PopAsync(
            CreateAssignment(),
            FilterExpression.All,
            cancellationToken));

        Assert.Same(failure, exception);
        operation.Verify(value => value.Complete(failure), Times.Once);
        operation.Verify(value => value.Dispose(), Times.Once);
    }

    [Fact]
    public async Task ChangeInvisibleTimeAsync_RequestSucceeds_RecordsNackSettlement()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var remoting = new Mock<IRemotingClient>(MockBehavior.Strict);
        remoting
            .Setup(value => value.InvokeAsync(
                It.IsAny<EndPoint>(),
                It.Is<RemotingCommand>(request =>
                    request.Code == RequestCode.ChangeMessageInvisibleTime &&
                    Equals(request.ExtFields["suspend"], false)),
                It.IsAny<TimeSpan>(),
                It.IsAny<CancellationToken>()))
            .ReturnsAsync(new RemotingCommand
            {
                Code = ResponseCodes.ResSuccess,
                ExtFields = new Dictionary<string, object>
                {
                    ["popTime"] = "1700000001000",
                    ["invisibleTime"] = "5000",
                    ["reviveQid"] = "3"
                }
            });
        var operation = CreateOperation();
        var telemetry = CreateSettlementTelemetry(operation, "nack");
        var client = CreateClient(remoting.Object, telemetry.Object);
        var message = CreateMessage();

        await client.ChangeInvisibleTimeAsync(
            CreateReceipt(),
            TimeSpan.FromSeconds(5),
            message,
            cancellationToken);

        operation.Verify(value => value.Complete(), Times.Once);
        operation.Verify(value => value.Dispose(), Times.Once);
    }

    [Fact]
    public async Task ChangeInvisibleTimeAsync_RouteChangesBetweenRequests_UsesCurrentBrokerAddressAndReceiptHeader()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var ports = new List<int>();
        var remoting = new Mock<IRemotingClient>(MockBehavior.Strict);
        remoting
            .Setup(value => value.InvokeAsync(
                It.IsAny<EndPoint>(),
                It.Is<RemotingCommand>(request =>
                    request.Code == RequestCode.ChangeMessageInvisibleTime &&
                    Equals(request.ExtFields["bname"], "broker-a") &&
                    Equals(request.ExtFields["queueId"], 3) &&
                    Equals(request.ExtFields["offset"], 42L) &&
                    Equals(request.ExtFields["extraInfo"], "40 1700000000000 5000 2 0 broker-a 3 42")),
                It.IsAny<TimeSpan>(),
                It.IsAny<CancellationToken>()))
            .Callback<EndPoint, RemotingCommand, TimeSpan, CancellationToken>(
                (endpoint, _, _, _) => ports.Add(GetPort(endpoint)))
            .ReturnsAsync(new RemotingCommand
            {
                Code = ResponseCodes.ResSuccess,
                ExtFields = new Dictionary<string, object>
                {
                    ["popTime"] = "1700000001000",
                    ["invisibleTime"] = "5000",
                    ["reviveQid"] = "3"
                }
            });
        var operation = CreateOperation();
        var telemetry = CreateSettlementTelemetry(operation, "nack");
        var client = CreateClient(
            remoting.Object,
            telemetry.Object,
            "127.0.0.1:10911",
            "127.0.0.1:20911");

        await client.ChangeInvisibleTimeAsync(
            CreateReceipt(),
            TimeSpan.FromSeconds(5),
            CreateMessage(),
            cancellationToken);
        await client.ChangeInvisibleTimeAsync(
            CreateReceipt(),
            TimeSpan.FromSeconds(5),
            CreateMessage(),
            cancellationToken);

        Assert.Equal([10911, 20911], ports);
    }

    [Fact]
    public async Task ChangeInvisibleTimeAsync_RemotingCallFails_RecordsNackSettlementFailure()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var failure = new IOException("Changing POP invisibility failed.");
        var remoting = new Mock<IRemotingClient>(MockBehavior.Strict);
        remoting
            .Setup(value => value.InvokeAsync(
                It.IsAny<EndPoint>(),
                It.Is<RemotingCommand>(request => request.Code == RequestCode.ChangeMessageInvisibleTime),
                It.IsAny<TimeSpan>(),
                It.IsAny<CancellationToken>()))
            .ThrowsAsync(failure);
        var operation = CreateOperation();
        var telemetry = CreateSettlementTelemetry(operation, "nack");
        var client = CreateClient(remoting.Object, telemetry.Object);
        var message = CreateMessage();

        var exception = await Assert.ThrowsAsync<IOException>(() => client.ChangeInvisibleTimeAsync(
            CreateReceipt(),
            TimeSpan.FromSeconds(5),
            message,
            cancellationToken));

        Assert.Same(failure, exception);
        operation.Verify(value => value.Complete(failure), Times.Once);
        operation.Verify(value => value.Dispose(), Times.Once);
    }

    [Fact]
    public async Task AcknowledgeAsync_RequestSucceeds_RecordsSettlement()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var remoting = new Mock<IRemotingClient>(MockBehavior.Strict);
        remoting
            .Setup(value => value.InvokeAsync(
                It.IsAny<EndPoint>(),
                It.Is<RemotingCommand>(request => request.Code == RequestCode.AckMessage),
                It.IsAny<TimeSpan>(),
                It.IsAny<CancellationToken>()))
            .ReturnsAsync(new RemotingCommand { Code = ResponseCodes.ResSuccess });
        var operation = CreateOperation();
        var telemetry = CreateSettlementTelemetry(operation, "ack");
        var client = CreateClient(remoting.Object, telemetry.Object);
        var message = CreateMessage();

        await client.AcknowledgeAsync(CreateReceipt(), message, cancellationToken);

        operation.Verify(value => value.Complete(), Times.Once);
        operation.Verify(value => value.Dispose(), Times.Once);
    }

    [Fact]
    public async Task AcknowledgeAsync_RouteChangesBetweenRequests_UsesCurrentBrokerAddressAndReceiptHeader()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var ports = new List<int>();
        var remoting = new Mock<IRemotingClient>(MockBehavior.Strict);
        remoting
            .Setup(value => value.InvokeAsync(
                It.IsAny<EndPoint>(),
                It.Is<RemotingCommand>(request =>
                    request.Code == RequestCode.AckMessage &&
                    Equals(request.ExtFields["bname"], "broker-a") &&
                    Equals(request.ExtFields["queueId"], 3) &&
                    Equals(request.ExtFields["offset"], 42L) &&
                    Equals(request.ExtFields["extraInfo"], "40 1700000000000 5000 2 0 broker-a 3 42")),
                It.IsAny<TimeSpan>(),
                It.IsAny<CancellationToken>()))
            .Callback<EndPoint, RemotingCommand, TimeSpan, CancellationToken>(
                (endpoint, _, _, _) => ports.Add(GetPort(endpoint)))
            .ReturnsAsync(new RemotingCommand { Code = ResponseCodes.ResSuccess });
        var operation = CreateOperation();
        var telemetry = CreateSettlementTelemetry(operation, "ack");
        var client = CreateClient(
            remoting.Object,
            telemetry.Object,
            "127.0.0.1:10911",
            "127.0.0.1:20911");

        await client.AcknowledgeAsync(CreateReceipt(), CreateMessage(), cancellationToken);
        await client.AcknowledgeAsync(CreateReceipt(), CreateMessage(), cancellationToken);

        Assert.Equal([10911, 20911], ports);
    }

    [Fact]
    public async Task AcknowledgeAsync_RemotingCallFails_RecordsSettlementFailure()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var failure = new IOException("Acknowledging the POP message failed.");
        var remoting = new Mock<IRemotingClient>(MockBehavior.Strict);
        remoting
            .Setup(value => value.InvokeAsync(
                It.IsAny<EndPoint>(),
                It.Is<RemotingCommand>(request => request.Code == RequestCode.AckMessage),
                It.IsAny<TimeSpan>(),
                It.IsAny<CancellationToken>()))
            .ThrowsAsync(failure);
        var operation = CreateOperation();
        var telemetry = CreateSettlementTelemetry(operation, "ack");
        var client = CreateClient(remoting.Object, telemetry.Object);
        var message = CreateMessage();

        var exception = await Assert.ThrowsAsync<IOException>(() => client.AcknowledgeAsync(
            CreateReceipt(),
            message,
            cancellationToken));

        Assert.Same(failure, exception);
        operation.Verify(value => value.Complete(failure), Times.Once);
        operation.Verify(value => value.Dispose(), Times.Once);
    }

    [Fact]
    public async Task AcknowledgeAsync_RouteResolutionFails_RecordsSettlementFailure()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var failure = new IOException("Route resolution failed.");
        var remoting = new Mock<IRemotingClient>(MockBehavior.Strict);
        var operation = CreateOperation();
        var telemetry = CreateSettlementTelemetry(operation, "ack");
        var client = CreateClient(remoting.Object, telemetry.Object, CreateFailedRouteService(failure).Object);

        var exception = await Assert.ThrowsAsync<IOException>(() => client.AcknowledgeAsync(
            CreateReceipt(),
            CreateMessage(),
            cancellationToken));

        Assert.Same(failure, exception);
        operation.Verify(value => value.Complete(failure), Times.Once);
        operation.Verify(value => value.Dispose(), Times.Once);
    }

    [Fact]
    public async Task AcknowledgeAsync_RemotingCallIsCanceled_RecordsSettlementCancellation()
    {
        using var cancellation = new CancellationTokenSource();
        await cancellation.CancelAsync();
        var canceled = new OperationCanceledException(cancellation.Token);
        var remoting = new Mock<IRemotingClient>(MockBehavior.Strict);
        remoting
            .Setup(value => value.InvokeAsync(
                It.IsAny<EndPoint>(),
                It.Is<RemotingCommand>(request => request.Code == RequestCode.AckMessage),
                It.IsAny<TimeSpan>(),
                It.IsAny<CancellationToken>()))
            .ThrowsAsync(canceled);
        var operation = CreateOperation();
        var telemetry = CreateSettlementTelemetry(operation, "ack");
        var client = CreateClient(remoting.Object, telemetry.Object);

        var exception = await Assert.ThrowsAsync<OperationCanceledException>(() => client.AcknowledgeAsync(
            CreateReceipt(),
            CreateMessage(),
            cancellation.Token));

        Assert.Same(canceled, exception);
        operation.Verify(value => value.Complete(canceled), Times.Once);
        operation.Verify(value => value.Dispose(), Times.Once);
    }

    private static PopWireClient CreateClient(
        IRemotingClient remoting,
        IRemotingRocketMQTelemetry telemetry,
        params string[] brokerAddresses) =>
        CreateClient(
            remoting,
            telemetry,
            CreateRouteService(
                brokerAddresses.Length == 0 ? ["127.0.0.1:10911"] : brokerAddresses).Object);

    private static PopWireClient CreateClient(
        IRemotingClient remoting,
        IRemotingRocketMQTelemetry telemetry,
        ITopicRouteService routes) =>
        new(
            new RemotingPushConsumerOptions
            {
                GroupName = "orders-consumer",
                PopBatchSize = 1,
                PopMaxInflightMessagesPerAssignment = 1,
                PopInvisibleDuration = TimeSpan.FromSeconds(5),
                LongPollingTimeout = TimeSpan.FromSeconds(1)
            },
            new RemotingClientOptions { RequestTimeout = TimeSpan.FromSeconds(1) },
            new RemotingConsumerRouteResolver(
                Options.Create(new RemotingClientOptions()),
                routes),
            remoting,
            TimeProvider.System,
            telemetry);

    private static Mock<ITopicRouteService> CreateRouteService(params string[] brokerAddresses)
    {
        var remainingAddresses = new Queue<string>(brokerAddresses);
        var routes = new Mock<ITopicRouteService>(MockBehavior.Strict);
        routes
            .Setup(value => value.GetAsync(
                "orders",
                It.IsAny<bool>(),
                It.IsAny<CancellationToken>()))
            .Returns(() =>
            {
                var broker = new BrokerData { BrokerName = "broker-a" };
                broker.BrokerAddrs[0] = remainingAddresses.Count > 1
                    ? remainingAddresses.Dequeue()
                    : remainingAddresses.Peek();
                return Task.FromResult(new TopicRouteData
                {
                    QueueDatas =
                    [
                        new QueueData
                        {
                            BrokerName = "broker-a",
                            ReadQueueNums = 1,
                            WriteQueueNums = 1,
                            Perm = 6
                        }
                    ],
                    BrokerDatas = [broker]
                });
            });
        return routes;
    }

    private static Mock<ITopicRouteService> CreateFailedRouteService(Exception failure)
    {
        var routes = new Mock<ITopicRouteService>(MockBehavior.Strict);
        routes
            .Setup(value => value.GetAsync(
                "orders",
                It.IsAny<bool>(),
                It.IsAny<CancellationToken>()))
            .ThrowsAsync(failure);
        return routes;
    }

    private static Mock<IRemotingRocketMQTelemetry> CreateReceiveTelemetry(
        Mock<IRemotingRocketMQTelemetryOperation> operation,
        bool createActivity = false)
    {
        var telemetry = new Mock<IRemotingRocketMQTelemetry>(MockBehavior.Strict);
        telemetry
            .Setup(value => value.StartReceive(
                "orders",
                "orders-consumer",
                0,
                It.IsAny<ActivityContext?>(),
                It.IsAny<DateTimeOffset>(),
                It.IsAny<long>(),
                null,
                createActivity,
                It.IsAny<IEnumerable<IReadOnlyDictionary<string, string>>?>()))
            .Returns(operation.Object);
        return telemetry;
    }

    private static RemotingPushAssignment CreateAssignment() => new(
        "orders",
        "broker-a",
        -1,
        RemotingPushReceiveMode.Pop,
        new Dictionary<string, string>());

    private static RemotingPopReceipt CreateReceipt() => new(
        "orders",
        "broker-a",
        3,
        40,
        42,
        DateTimeOffset.FromUnixTimeMilliseconds(1_700_000_000_000),
        TimeSpan.FromSeconds(5),
        2,
        "40 1700000000000 5000 2 0 broker-a 3 42");

    private static RemotingMessageView CreateMessage() => new(
        "orders",
        [1, 2, 3],
        "message-id",
        "offset-message-id",
        null,
        [],
        new Dictionary<string, string>(),
        1,
        null,
        3,
        "broker-a",
        42,
        100,
        DateTimeOffset.UnixEpoch,
        DateTimeOffset.UnixEpoch);

    private static Mock<IRemotingRocketMQTelemetry> CreateSettlementTelemetry(
        Mock<IRemotingRocketMQTelemetryOperation> operation,
        string operationName)
    {
        var telemetry = new Mock<IRemotingRocketMQTelemetry>(MockBehavior.Strict);
        telemetry
            .Setup(value => value.StartSettle(
                operationName,
                "orders",
                "orders-consumer",
                "message-id",
                3,
                It.Is<IReadOnlyDictionary<string, string>>(properties => properties.Count == 0)))
            .Returns(operation.Object);
        return telemetry;
    }

    private static Mock<IRemotingRocketMQTelemetryOperation> CreateOperation()
    {
        var operation = new Mock<IRemotingRocketMQTelemetryOperation>(MockBehavior.Strict);
        operation.SetupGet(value => value.Activity).Returns((Activity?)null);
        operation.Setup(value => value.Complete());
        operation.Setup(value => value.Complete(It.IsAny<Exception>()));
        operation.Setup(value => value.Dispose());
        return operation;
    }

    private static int GetPort(EndPoint endpoint) => Assert.IsType<IPEndPoint>(endpoint).Port;
}
