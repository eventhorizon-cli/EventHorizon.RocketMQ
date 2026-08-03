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

using System.Net;
using EventHorizon.RocketMQ.Remoting.Consumer;
using EventHorizon.RocketMQ.Remoting.Exceptions;
using EventHorizon.RocketMQ.Remoting.Protocol;
using EventHorizon.RocketMQ.Remoting.Protocol.Route;
using Microsoft.Extensions.Options;
using Moq;
using Xunit;

namespace EventHorizon.RocketMQ.Remoting.Tests.Consumer;

public sealed class RemotingConsumerEngineTests
{
    [Theory]
    [InlineData(
        RemotingClientOptions.DefaultRemotingFrameSize,
        RemotingClientOptions.DefaultRemotingFrameSize - RemotingClientOptions.RemotingMessageFrameReserve)]
    [InlineData(64 * 1024 * 1024, 32 * 1024 * 1024)]
    public async Task PullAsync_BrokerSuggestsReplicas_UsesSuggestedReplicaWhileOffsetsStayOnPrimary(
        int maximumFrameSize,
        int expectedMaximumMessageBytes)
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var calls = new List<(EndPoint Endpoint, RemotingCommand Request, TimeSpan Timeout)>();
        var pullCount = 0;
        var remoting = new Mock<IRemotingClient>(MockBehavior.Strict);
        remoting
            .Setup(value => value.InvokeAsync(
                It.IsAny<EndPoint>(),
                It.IsAny<RemotingCommand>(),
                It.IsAny<TimeSpan>(),
                It.IsAny<CancellationToken>()))
            .Returns<EndPoint, RemotingCommand, TimeSpan, CancellationToken>((endpoint, request, timeout, _) =>
            {
                calls.Add((endpoint, request, timeout));
                if (request.Code == RequestCode.PullMessage)
                {
                    var call = Interlocked.Increment(ref pullCount);
                    return Task.FromResult(new RemotingCommand
                    {
                        Code = ResponseCodes.ResPullNotFound,
                        ExtFields = new Dictionary<string, object>
                        {
                            ["nextBeginOffset"] = "0",
                            ["suggestWhichBrokerId"] = call == 1 ? "1" : call == 2 ? "99" : "0"
                        }
                    });
                }

                return request.Code == RequestCode.QueryConsumerOffset
                    ? Task.FromResult(new RemotingCommand { Code = ResponseCodes.ResQueryNotFound })
                    : Task.FromException<RemotingCommand>(
                        new InvalidOperationException($"Unexpected request code {request.Code}."));
            });
        var routes = new Mock<ITopicRouteService>(MockBehavior.Strict);
        routes
            .Setup(value => value.GetAsync(
                It.IsAny<string>(),
                It.IsAny<bool>(),
                It.IsAny<CancellationToken>()))
            .ReturnsAsync(Route());
        await using var consumer = CreateConsumer(
            new Dictionary<string, FilterExpression>
            {
                ["orders"] = new("created")
            },
            TimeSpan.FromMilliseconds(10),
            Options.Create(new RemotingClientOptions
            {
                RequestTimeout = TimeSpan.FromSeconds(1),
                MaxRemotingFrameSize = maximumFrameSize
            }),
            routes.Object,
            remoting.Object,
            TimeProvider.System);
        await consumer.StartAsync(cancellationToken);
        var queue = Assert.Single(await consumer.GetMessageQueuesAsync("orders", cancellationToken));

        await consumer.PullAsync(queue, 0, cancellationToken: cancellationToken);
        await consumer.PullAsync(queue, 0, cancellationToken: cancellationToken);
        await consumer.PullAsync(queue, 0, cancellationToken: cancellationToken);
        Assert.Equal(-1, await consumer.GetOffsetAsync(queue, cancellationToken));

        var pulls = calls.Where(static call => call.Request.Code == RequestCode.PullMessage).ToArray();
        Assert.Equal(3, pulls.Length);
        Assert.Equal(10911, Port(pulls[0].Endpoint));
        Assert.Equal(10912, Port(pulls[1].Endpoint));
        Assert.Equal(10911, Port(pulls[2].Endpoint));
        var expectedPullTimeout = TimeSpan.FromSeconds(1) + TimeSpan.FromMilliseconds(10) + TimeSpan.FromSeconds(5);
        Assert.All(pulls, pull => Assert.Equal(expectedPullTimeout, pull.Timeout));
        var offset = Assert.Single(calls, static call =>
            call.Request.Code == RequestCode.QueryConsumerOffset);
        Assert.Equal(10911, Port(offset.Endpoint));
        Assert.All(pulls, call =>
        {
            Assert.Equal("orders-consumer", call.Request.ExtFields["consumerGroup"]);
            Assert.Equal("orders", call.Request.ExtFields["topic"]);
            Assert.Equal("created", call.Request.ExtFields["subscription"]);
            Assert.Equal(expectedMaximumMessageBytes, call.Request.ExtFields["maxMsgBytes"]);
        });
    }

    [Fact]
    public async Task PullAsync_BrokerRejectsRequest_ThrowsRemotingCommandException()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var remoting = new Mock<IRemotingClient>(MockBehavior.Strict);
        remoting
            .Setup(value => value.InvokeAsync(
                It.IsAny<EndPoint>(),
                It.IsAny<RemotingCommand>(),
                It.IsAny<TimeSpan>(),
                It.IsAny<CancellationToken>()))
            .ReturnsAsync(new RemotingCommand
            {
                Code = ResponseCodes.ResNoPermission,
                Remark = "read denied"
            });
        var routes = new Mock<ITopicRouteService>(MockBehavior.Strict);
        routes
            .Setup(value => value.GetAsync(
                "orders",
                It.IsAny<bool>(),
                It.IsAny<CancellationToken>()))
            .ReturnsAsync(Route());
        await using var consumer = CreateConsumer(
            new Dictionary<string, FilterExpression>
            {
                ["orders"] = FilterExpression.All
            },
            TimeSpan.FromSeconds(15),
            Options.Create(new RemotingClientOptions()),
            routes.Object,
            remoting.Object,
            TimeProvider.System);
        await consumer.StartAsync(cancellationToken);
        var queue = Assert.Single(await consumer.GetMessageQueuesAsync("orders", cancellationToken));

        var exception = await Assert.ThrowsAsync<RemotingCommandException>(() =>
            consumer.PullAsync(queue, 0, cancellationToken: cancellationToken));

        Assert.Equal(ResponseCodes.ResNoPermission, exception.ResponseCode);
        Assert.Equal("read denied", exception.Message);
    }

    [Fact]
    public async Task PullAsync_RouteChanges_ResolvesLatestBrokerForLogicalQueue()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var routes = new Mock<ITopicRouteService>(MockBehavior.Strict);
        routes
            .SetupSequence(value => value.GetAsync(
                "orders",
                It.IsAny<bool>(),
                It.IsAny<CancellationToken>()))
            .ReturnsAsync(Route(masterAddress: "127.0.0.1:10911"))
            .ReturnsAsync(Route(masterAddress: "127.0.0.1:20911"));
        EndPoint? endpoint = null;
        var remoting = new Mock<IRemotingClient>(MockBehavior.Strict);
        remoting
            .Setup(value => value.InvokeAsync(
                It.IsAny<EndPoint>(),
                It.Is<RemotingCommand>(request => request.Code == RequestCode.PullMessage),
                It.IsAny<TimeSpan>(),
                It.IsAny<CancellationToken>()))
            .Callback<EndPoint, RemotingCommand, TimeSpan, CancellationToken>((value, _, _, _) => endpoint = value)
            .ReturnsAsync(new RemotingCommand
            {
                Code = ResponseCodes.ResPullNotFound,
                ExtFields = new Dictionary<string, object> { ["nextBeginOffset"] = "0" }
            });
        await using var consumer = CreateConsumer(
            new Dictionary<string, FilterExpression> { ["orders"] = FilterExpression.All },
            TimeSpan.FromMilliseconds(10),
            Options.Create(new RemotingClientOptions()),
            routes.Object,
            remoting.Object,
            TimeProvider.System);
        await consumer.StartAsync(cancellationToken);
        var queue = Assert.Single(await consumer.GetMessageQueuesAsync("orders", cancellationToken));

        await consumer.PullAsync(queue, 0, cancellationToken: cancellationToken);

        Assert.Equal(20911, Port(Assert.IsAssignableFrom<EndPoint>(endpoint)));
    }

    [Fact]
    public async Task GetOffsetAsync_LatestRouteOmitsKnownBroker_UsesLastResolvedBroker()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var routes = new Mock<ITopicRouteService>(MockBehavior.Strict);
        routes
            .SetupSequence(value => value.GetAsync(
                "orders",
                It.IsAny<bool>(),
                It.IsAny<CancellationToken>()))
            .ReturnsAsync(Route())
            .ReturnsAsync(new TopicRouteData());
        EndPoint? endpoint = null;
        var remoting = new Mock<IRemotingClient>(MockBehavior.Strict);
        remoting
            .Setup(value => value.InvokeAsync(
                It.IsAny<EndPoint>(),
                It.Is<RemotingCommand>(request => request.Code == RequestCode.QueryConsumerOffset),
                It.IsAny<TimeSpan>(),
                It.IsAny<CancellationToken>()))
            .Callback<EndPoint, RemotingCommand, TimeSpan, CancellationToken>((value, _, _, _) => endpoint = value)
            .ReturnsAsync(new RemotingCommand { Code = ResponseCodes.ResQueryNotFound });
        await using var consumer = CreateConsumer(
            new Dictionary<string, FilterExpression> { ["orders"] = FilterExpression.All },
            TimeSpan.FromMilliseconds(10),
            Options.Create(new RemotingClientOptions()),
            routes.Object,
            remoting.Object,
            TimeProvider.System);
        await consumer.StartAsync(cancellationToken);
        var queue = Assert.Single(await consumer.GetMessageQueuesAsync("orders", cancellationToken));

        var offset = await consumer.GetOffsetAsync(queue, cancellationToken);

        Assert.Equal(-1, offset);
        Assert.Equal(10911, Port(Assert.IsAssignableFrom<EndPoint>(endpoint)));
    }

    [Fact]
    public async Task GetOffsetAsync_RouteLookupFails_UsesLastResolvedBroker()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var routes = new Mock<ITopicRouteService>(MockBehavior.Strict);
        routes
            .SetupSequence(value => value.GetAsync(
                "orders",
                It.IsAny<bool>(),
                It.IsAny<CancellationToken>()))
            .ReturnsAsync(Route())
            .ThrowsAsync(new RocketMQClientException("The NameServer route lookup failed."));
        EndPoint? endpoint = null;
        var remoting = new Mock<IRemotingClient>(MockBehavior.Strict);
        remoting
            .Setup(value => value.InvokeAsync(
                It.IsAny<EndPoint>(),
                It.Is<RemotingCommand>(request => request.Code == RequestCode.QueryConsumerOffset),
                It.IsAny<TimeSpan>(),
                It.IsAny<CancellationToken>()))
            .Callback<EndPoint, RemotingCommand, TimeSpan, CancellationToken>((value, _, _, _) => endpoint = value)
            .ReturnsAsync(new RemotingCommand { Code = ResponseCodes.ResQueryNotFound });
        await using var consumer = CreateConsumer(
            new Dictionary<string, FilterExpression> { ["orders"] = FilterExpression.All },
            TimeSpan.FromMilliseconds(10),
            Options.Create(new RemotingClientOptions()),
            routes.Object,
            remoting.Object,
            TimeProvider.System);
        await consumer.StartAsync(cancellationToken);
        var queue = Assert.Single(await consumer.GetMessageQueuesAsync("orders", cancellationToken));

        var offset = await consumer.GetOffsetAsync(queue, cancellationToken);

        Assert.Equal(-1, offset);
        Assert.Equal(10911, Port(Assert.IsAssignableFrom<EndPoint>(endpoint)));
    }

    private static RemotingConsumerEngine CreateConsumer(
        IReadOnlyDictionary<string, FilterExpression> subscriptions,
        TimeSpan longPollingTimeout,
        IOptions<RemotingClientOptions> clientOptions,
        ITopicRouteService routes,
        IRemotingClient remoting,
        TimeProvider timeProvider)
    {
        var settings = new RemotingConsumerSettings(
            "orders-consumer",
            subscriptions,
            32,
            32 * 1024 * 1024,
            longPollingTimeout);
        return new RemotingConsumerEngine(
            settings,
            clientOptions,
            routes,
            remoting,
            timeProvider);
    }

    private static int Port(EndPoint endpoint) => Assert.IsType<IPEndPoint>(endpoint).Port;

    private static TopicRouteData Route(string masterAddress = "127.0.0.1:10911")
    {
        var broker = new BrokerData { BrokerName = "broker-a" };
        broker.BrokerAddrs[0] = masterAddress;
        broker.BrokerAddrs[1] = "127.0.0.1:10912";
        return new TopicRouteData
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
        };
    }
}
