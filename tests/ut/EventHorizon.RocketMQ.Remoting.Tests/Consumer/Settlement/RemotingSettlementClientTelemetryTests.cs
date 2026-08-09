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
using System.Net;
using EventHorizon.RocketMQ.Remoting.Consumer;
using EventHorizon.RocketMQ.Remoting.Consumer.Route;
using EventHorizon.RocketMQ.Remoting.Consumer.Settlement;
using EventHorizon.RocketMQ.Remoting.Exceptions;
using EventHorizon.RocketMQ.Remoting.Instrumentation;
using EventHorizon.RocketMQ.Remoting.Protocol;
using EventHorizon.RocketMQ.Remoting.Protocol.Route;
using Microsoft.Extensions.Options;
using Moq;
using Xunit;

namespace EventHorizon.RocketMQ.Remoting.Tests.Consumer.Settlement;

public sealed class RemotingSettlementClientTelemetryTests
{
    [Fact]
    public async Task SendBackAsync_BrokerAcceptsRetry_CompletesNackTelemetry()
    {
        var operation = new Mock<IRemotingRocketMQTelemetryOperation>(MockBehavior.Strict);
        operation.Setup(value => value.Complete());
        operation.Setup(value => value.Dispose());
        var telemetry = CreateSettlementTelemetry(operation, "nack");
        var remoting = CreateRemotingClient(_ => Task.FromResult(new RemotingCommand
        {
            Code = ResponseCodes.ResSuccess
        }));
        var client = CreateClient(remoting.Object, telemetry.Object);

        await client.SendBackAsync(
            Queue,
            Message,
            delayLevel: 3,
            maxReconsumeTimes: 16,
            TestContext.Current.CancellationToken);

        operation.VerifyAll();
        telemetry.VerifyAll();
    }

    [Fact]
    public async Task SendBackAsync_DeadLetterForwardingFails_CompletesRejectTelemetryWithError()
    {
        var expected = new IOException("Broker connection failed.");
        var operation = CreateFailedOperation(expected);
        var telemetry = CreateSettlementTelemetry(operation, "reject");
        var remoting = CreateRemotingClient(_ => Task.FromException<RemotingCommand>(expected));
        var client = CreateClient(remoting.Object, telemetry.Object);

        var exception = await Assert.ThrowsAsync<IOException>(() => client.SendBackAsync(
            Queue,
            Message,
            delayLevel: -1,
            maxReconsumeTimes: 16,
            TestContext.Current.CancellationToken));

        Assert.Same(expected, exception);
        operation.VerifyAll();
        telemetry.VerifyAll();
    }

    [Fact]
    public async Task SendBackAsync_CanceledRetry_CompletesNackTelemetryWithCancellation()
    {
        using var cancellation = new CancellationTokenSource();
        cancellation.Cancel();
        var expected = new OperationCanceledException(cancellation.Token);
        var operation = CreateFailedOperation(expected);
        var telemetry = CreateSettlementTelemetry(operation, "nack");
        var remoting = CreateRemotingClient(_ => Task.FromException<RemotingCommand>(expected));
        var client = CreateClient(remoting.Object, telemetry.Object);

        var exception = await Assert.ThrowsAnyAsync<OperationCanceledException>(() => client.SendBackAsync(
            Queue,
            Message,
            delayLevel: 3,
            maxReconsumeTimes: 16,
            cancellation.Token));

        Assert.Same(expected, exception);
        operation.VerifyAll();
        telemetry.VerifyAll();
    }

    [Fact]
    public async Task SendOrderlyRetryAsync_BrokerAcceptsRetryTopicMessage_CompletesRejectTelemetry()
    {
        var operation = new Mock<IRemotingRocketMQTelemetryOperation>(MockBehavior.Strict);
        operation.Setup(value => value.Complete());
        operation.Setup(value => value.Dispose());
        var telemetry = CreateSettlementTelemetry(operation, "reject");
        var remoting = CreateRemotingClient(
            _ => Task.FromResult(new RemotingCommand { Code = ResponseCodes.ResSuccess }),
            RequestCode.SendMessage,
            request =>
            {
                Assert.Equal(7, Convert.ToInt32(request.ExtFields["flag"]));
                var properties = MessagePropertyCodec.Deserialize(
                    Assert.IsType<string>(request.ExtFields["properties"]));
                Assert.Equal("message-1", properties["ORIGIN_MESSAGE_ID"]);
            });
        var client = CreateClient(remoting.Object, telemetry.Object);

        await client.SendOrderlyRetryAsync(
            Message,
            maxReconsumeTimes: 0,
            TestContext.Current.CancellationToken);

        operation.VerifyAll();
        telemetry.VerifyAll();
    }

    [Fact]
    public async Task SendOrderlyRetryAsync_RetryTopicHasNoWritableQueue_UsesDefaultTopicRoute()
    {
        var operation = new Mock<IRemotingRocketMQTelemetryOperation>(MockBehavior.Strict);
        operation.Setup(value => value.Complete());
        operation.Setup(value => value.Dispose());
        var telemetry = CreateSettlementTelemetry(operation, "reject");
        var remoting = CreateRemotingClient(
            _ => Task.FromResult(new RemotingCommand { Code = ResponseCodes.ResSuccess }),
            RequestCode.SendMessage);
        var routes = new Mock<ITopicRouteService>(MockBehavior.Strict);
        routes
            .Setup(value => value.GetAsync(
                "%RETRY%orders-consumer",
                It.IsAny<bool>(),
                It.IsAny<CancellationToken>()))
            .ReturnsAsync(new TopicRouteData());
        routes
            .Setup(value => value.GetAsync(
                "TBW102",
                It.IsAny<bool>(),
                It.IsAny<CancellationToken>()))
            .ReturnsAsync(Route());
        var client = CreateClient(remoting.Object, telemetry.Object, routes.Object);

        await client.SendOrderlyRetryAsync(
            Message,
            maxReconsumeTimes: 0,
            TestContext.Current.CancellationToken);

        routes.VerifyAll();
        operation.VerifyAll();
        telemetry.VerifyAll();
    }

    [Fact]
    public async Task SendOrderlyRetryAsync_ThreeWireAttemptsFail_CompletesRejectTelemetryWithError()
    {
        var expected = new IOException("Broker connection failed.");
        var operation = CreateFailedOperation(expected);
        var telemetry = CreateSettlementTelemetry(operation, "reject");
        var attempts = 0;
        var remoting = CreateRemotingClient(
            _ =>
            {
                Interlocked.Increment(ref attempts);
                return Task.FromException<RemotingCommand>(expected);
            },
            RequestCode.SendMessage);
        var client = CreateClient(remoting.Object, telemetry.Object);

        var exception = await Assert.ThrowsAsync<IOException>(() => client.SendOrderlyRetryAsync(
            Message,
            maxReconsumeTimes: 0,
            TestContext.Current.CancellationToken));

        Assert.Same(expected, exception);
        Assert.Equal(3, attempts);
        operation.VerifyAll();
        telemetry.VerifyAll();
    }

    [Fact]
    public async Task SendOrderlyRetryAsync_FirstBrokerFails_RetriesAnotherBroker()
    {
        var operation = new Mock<IRemotingRocketMQTelemetryOperation>(MockBehavior.Strict);
        operation.Setup(value => value.Complete());
        operation.Setup(value => value.Dispose());
        var telemetry = CreateSettlementTelemetry(operation, "reject");
        var attempts = 0;
        var brokers = new ConcurrentQueue<string>();
        var remoting = CreateRemotingClient(
            _ => Interlocked.Increment(ref attempts) == 1
                ? Task.FromException<RemotingCommand>(new IOException("Broker connection failed."))
                : Task.FromResult(new RemotingCommand { Code = ResponseCodes.ResSuccess }),
            RequestCode.SendMessage,
            request => brokers.Enqueue(Assert.IsType<string>(request.ExtFields["bname"])));
        var routes = new Mock<ITopicRouteService>(MockBehavior.Strict);
        routes
            .Setup(value => value.GetAsync(
                It.IsAny<string>(),
                It.IsAny<bool>(),
                It.IsAny<CancellationToken>()))
            .ReturnsAsync(RouteWithMultipleBrokers());
        var client = CreateClient(remoting.Object, telemetry.Object, routes.Object);

        await client.SendOrderlyRetryAsync(
            Message,
            maxReconsumeTimes: 0,
            TestContext.Current.CancellationToken);

        Assert.Equal(2, attempts);
        var selectedBrokers = brokers.ToArray();
        Assert.Equal(2, selectedBrokers.Length);
        Assert.NotEqual(selectedBrokers[0], selectedBrokers[1]);
        operation.VerifyAll();
        telemetry.VerifyAll();
    }

    [Fact]
    public async Task SendOrderlyRetryAsync_NonRetryableBrokerRejection_DoesNotRetry()
    {
        var operation = new Mock<IRemotingRocketMQTelemetryOperation>(MockBehavior.Strict);
        operation.Setup(value => value.Complete(It.Is<RemotingCommandException>(exception =>
            exception.ResponseCode == ResponseCodes.ResRequestCodeNotSupported)));
        operation.Setup(value => value.Dispose());
        var telemetry = CreateSettlementTelemetry(operation, "reject");
        var attempts = 0;
        var remoting = CreateRemotingClient(
            _ =>
            {
                Interlocked.Increment(ref attempts);
                return Task.FromResult(new RemotingCommand
                {
                    Code = ResponseCodes.ResRequestCodeNotSupported,
                    Remark = "Unsupported request."
                });
            },
            RequestCode.SendMessage);
        var client = CreateClient(remoting.Object, telemetry.Object);

        var exception = await Assert.ThrowsAsync<RemotingCommandException>(() => client.SendOrderlyRetryAsync(
            Message,
            maxReconsumeTimes: 0,
            TestContext.Current.CancellationToken));

        Assert.Equal(ResponseCodes.ResRequestCodeNotSupported, exception.ResponseCode);
        Assert.Equal(1, attempts);
        operation.VerifyAll();
        telemetry.VerifyAll();
    }

    [Theory]
    [InlineData(ResponseCodes.ResTopicNotExist)]
    [InlineData(ResponseCodes.ResServiceNotAvailable)]
    [InlineData(ResponseCodes.ResError)]
    [InlineData(ResponseCodes.ResSystemBusy)]
    [InlineData(ResponseCodes.ResNoPermission)]
    [InlineData(ResponseCodes.ResNoBuyerId)]
    [InlineData(ResponseCodes.ResNotInCurrentUnit)]
    [InlineData(ResponseCodes.ResGoAway)]
    public async Task SendOrderlyRetryAsync_JavaRetryableBrokerRejection_Retries(int responseCode)
    {
        var operation = new Mock<IRemotingRocketMQTelemetryOperation>(MockBehavior.Strict);
        operation.Setup(value => value.Complete());
        operation.Setup(value => value.Dispose());
        var telemetry = CreateSettlementTelemetry(operation, "reject");
        var attempts = 0;
        var remoting = CreateRemotingClient(
            _ => Task.FromResult(new RemotingCommand
            {
                Code = Interlocked.Increment(ref attempts) == 1
                    ? responseCode
                    : ResponseCodes.ResSuccess
            }),
            RequestCode.SendMessage);
        var client = CreateClient(remoting.Object, telemetry.Object);

        await client.SendOrderlyRetryAsync(
            Message,
            maxReconsumeTimes: 0,
            TestContext.Current.CancellationToken);

        Assert.Equal(2, attempts);
        operation.VerifyAll();
        telemetry.VerifyAll();
    }

    [Fact]
    public async Task SendOrderlyRetryAsync_CanceledBeforeWireAttempt_CompletesRejectTelemetryWithCancellation()
    {
        using var cancellation = new CancellationTokenSource();
        cancellation.Cancel();
        var operation = new Mock<IRemotingRocketMQTelemetryOperation>(MockBehavior.Strict);
        operation.Setup(value => value.Complete(It.Is<OperationCanceledException>(exception =>
            exception.CancellationToken == cancellation.Token)));
        operation.Setup(value => value.Dispose());
        var telemetry = CreateSettlementTelemetry(operation, "reject");
        var remoting = new Mock<IRemotingClient>(MockBehavior.Strict);
        var client = CreateClient(remoting.Object, telemetry.Object);

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => client.SendOrderlyRetryAsync(
            Message,
            maxReconsumeTimes: 0,
            cancellation.Token));

        operation.VerifyAll();
        telemetry.VerifyAll();
    }

    private static RemotingConsumerQueue Queue { get; } = new("orders", "broker-a", 0);

    private static RemotingMessageView Message { get; } = new(
        "orders",
        [1, 2, 3],
        "message-1",
        "offset-message-1",
        "created",
        ["key-1"],
        new Dictionary<string, string> { ["ORIGIN_MESSAGE_ID"] = " " },
        1,
        null,
        0,
        "broker-a",
        7,
        1_000,
        DateTimeOffset.UnixEpoch,
        DateTimeOffset.UnixEpoch,
        flag: 7);

    private static Mock<IRemotingRocketMQTelemetryOperation> CreateFailedOperation(Exception expected)
    {
        var operation = new Mock<IRemotingRocketMQTelemetryOperation>(MockBehavior.Strict);
        operation.Setup(value => value.Complete(It.Is<Exception>(exception => ReferenceEquals(exception, expected))));
        operation.Setup(value => value.Dispose());
        return operation;
    }

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
                "message-1",
                0,
                Message.Properties))
            .Returns(operation.Object);
        return telemetry;
    }

    private static Mock<IRemotingClient> CreateRemotingClient(
        Func<CancellationToken, Task<RemotingCommand>> invoke,
        int requestCode = RequestCode.ConsumerSendMsgBack,
        Action<RemotingCommand>? inspect = null)
    {
        var remoting = new Mock<IRemotingClient>(MockBehavior.Strict);
        remoting
            .Setup(value => value.InvokeAsync(
                It.IsAny<EndPoint>(),
                It.Is<RemotingCommand>(request => request.Code == requestCode),
                It.IsAny<TimeSpan>(),
                It.IsAny<CancellationToken>()))
            .Returns<EndPoint, RemotingCommand, TimeSpan, CancellationToken>((_, request, _, token) =>
            {
                inspect?.Invoke(request);
                return invoke(token);
            });
        return remoting;
    }

    private static RemotingSettlementClient CreateClient(
        IRemotingClient remoting,
        IRemotingRocketMQTelemetry telemetry,
        ITopicRouteService? routeService = null)
    {
        if (routeService is null)
        {
            var routes = new Mock<ITopicRouteService>(MockBehavior.Strict);
            routes
                .Setup(value => value.GetAsync(
                    It.IsAny<string>(),
                    It.IsAny<bool>(),
                    It.IsAny<CancellationToken>()))
                .ReturnsAsync(Route());
            routeService = routes.Object;
        }

        var clientOptions = new RemotingClientOptions { RequestTimeout = TimeSpan.FromSeconds(1) };
        return new RemotingSettlementClient(
            new RemotingConsumerSettings(
                "orders-consumer",
                32,
                32 * 1024 * 1024,
                TimeSpan.FromSeconds(1)),
            clientOptions,
            new RemotingConsumerRouteResolver(Options.Create(clientOptions), routeService),
            remoting,
            TimeProvider.System,
            telemetry);
    }

    private static TopicRouteData Route() => new()
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
        BrokerDatas =
        [
            new BrokerData
            {
                BrokerName = "broker-a",
                BrokerAddrs = new ConcurrentDictionary<long, string>(
                    [new KeyValuePair<long, string>(0, "127.0.0.1:10911")])
            }
        ]
    };

    private static TopicRouteData RouteWithMultipleBrokers() => new()
    {
        QueueDatas =
        [
            new QueueData
            {
                BrokerName = "broker-a",
                ReadQueueNums = 3,
                WriteQueueNums = 3,
                Perm = 6
            },
            new QueueData
            {
                BrokerName = "broker-b",
                ReadQueueNums = 1,
                WriteQueueNums = 1,
                Perm = 6
            }
        ],
        BrokerDatas =
        [
            new BrokerData
            {
                BrokerName = "broker-a",
                BrokerAddrs = new ConcurrentDictionary<long, string>(
                    [new KeyValuePair<long, string>(0, "127.0.0.1:10911")])
            },
            new BrokerData
            {
                BrokerName = "broker-b",
                BrokerAddrs = new ConcurrentDictionary<long, string>(
                    [new KeyValuePair<long, string>(0, "127.0.0.2:10911")])
            }
        ]
    };
}
