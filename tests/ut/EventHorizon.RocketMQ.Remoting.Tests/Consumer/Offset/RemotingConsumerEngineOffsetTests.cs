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
using EventHorizon.RocketMQ.Remoting.Consumer.Route;
using EventHorizon.RocketMQ.Remoting.Exceptions;
using EventHorizon.RocketMQ.Remoting.Protocol;
using EventHorizon.RocketMQ.Remoting.Protocol.Route;
using Microsoft.Extensions.Options;
using Moq;
using Xunit;

namespace EventHorizon.RocketMQ.Remoting.Tests.Consumer.Offset;

public sealed class RemotingConsumerEngineOffsetTests
{
    [Fact]
    public async Task GetOffsetAsync_LatestRouteOmitsKnownBroker_UsesResolverCacheEndpoint()
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
            TimeSpan.FromMilliseconds(10),
            Options.Create(new RemotingClientOptions()),
            routes.Object,
            remoting.Object,
            TimeProvider.System);
        await consumer.StartAsync(cancellationToken);
        Assert.Single(await consumer.GetMessageQueuesAsync("orders", cancellationToken));
        var queue = new RemotingConsumerQueue("orders", "broker-a", 0);

        var offset = await consumer.GetOffsetAsync(queue, cancellationToken);

        Assert.Equal(-1, offset);
        Assert.Equal(10911, Port(Assert.IsAssignableFrom<EndPoint>(endpoint)));
        routes.Verify(
            value => value.GetAsync("orders", It.IsAny<bool>(), It.IsAny<CancellationToken>()),
            Times.Exactly(2));
    }

    [Fact]
    public async Task GetOffsetAsync_NameServerLookupFailsAfterValidRoute_UsesResolverCacheEndpoint()
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
            TimeSpan.FromMilliseconds(10),
            Options.Create(new RemotingClientOptions()),
            routes.Object,
            remoting.Object,
            TimeProvider.System);
        await consumer.StartAsync(cancellationToken);
        Assert.Single(await consumer.GetMessageQueuesAsync("orders", cancellationToken));
        var queue = new RemotingConsumerQueue("orders", "broker-a", 0);

        var offset = await consumer.GetOffsetAsync(queue, cancellationToken);

        Assert.Equal(-1, offset);
        Assert.Equal(10911, Port(Assert.IsAssignableFrom<EndPoint>(endpoint)));
        routes.Verify(
            value => value.GetAsync("orders", It.IsAny<bool>(), It.IsAny<CancellationToken>()),
            Times.Exactly(2));
    }

    private static RemotingConsumerEngine CreateConsumer(
        TimeSpan longPollingTimeout,
        IOptions<RemotingClientOptions> clientOptions,
        ITopicRouteService routes,
        IRemotingClient remoting,
        TimeProvider timeProvider)
    {
        var settings = new RemotingConsumerSettings(
            "orders-consumer",
            32,
            32 * 1024 * 1024,
            longPollingTimeout);
        return new RemotingConsumerEngine(
            settings,
            clientOptions,
            new RemotingConsumerRouteResolver(clientOptions, routes),
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
