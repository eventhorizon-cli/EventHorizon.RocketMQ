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
using EventHorizon.RocketMQ.Remoting.Instrumentation;
using EventHorizon.RocketMQ.Remoting.Protocol;
using Moq;
using Xunit;

namespace EventHorizon.RocketMQ.Remoting.Tests.Consumer.Push.Pop;

public sealed class PopWireClientTests
{
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
            "127.0.0.1:10911",
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
            "127.0.0.1:10911",
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
        IRemotingRocketMQTelemetry telemetry) =>
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
            remoting,
            TimeProvider.System,
            telemetry);

    private static RemotingPushAssignment CreateAssignment() => new(
        "orders",
        "broker-a",
        -1,
        RemotingPushReceiveMode.Pop,
        new Dictionary<string, string>());

    private static RemotingPopReceipt CreateReceipt() => new(
        "orders",
        "broker-a",
        "127.0.0.1:10911",
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
}
