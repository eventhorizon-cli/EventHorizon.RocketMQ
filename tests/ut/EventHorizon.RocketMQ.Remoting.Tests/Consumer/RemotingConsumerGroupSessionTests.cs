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
using System.Text;
using EventHorizon.RocketMQ.Remoting.Consumer;
using EventHorizon.RocketMQ.Remoting.Protocol;
using Microsoft.Extensions.Logging.Abstractions;
using Moq;
using Xunit;

namespace EventHorizon.RocketMQ.Remoting.Tests.Consumer;

public sealed class RemotingConsumerGroupSessionTests
{
    [Fact]
    public void Constructor_NamespacedGroupAndConfiguredClientIdentity_Builds()
    {
        var remoting = new Mock<IRemotingClient>(MockBehavior.Strict);
        var session = CreateSession(remoting.Object);

        Assert.Equal("10.0.0.4@consumer-instance@unit-a", session.ClientId);
        Assert.Equal("tenant-a%orders-consumer", session.ConsumerGroup);
    }

    [Fact]
    public async Task SendHeartbeatsAsync_BrokerFailure_ContinuesAndDeduplicatesAddresses()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var invocations = new List<(EndPoint Endpoint, RemotingCommand Request, TimeSpan Timeout)>();
        var remoting = new Mock<IRemotingClient>(MockBehavior.Strict);
        remoting
            .Setup(value => value.InvokeAsync(
                It.IsAny<EndPoint>(),
                It.IsAny<RemotingCommand>(),
                It.IsAny<TimeSpan>(),
                It.IsAny<CancellationToken>()))
            .Returns<EndPoint, RemotingCommand, TimeSpan, CancellationToken>((endpoint, request, timeout, _) =>
            {
                invocations.Add((endpoint, request, timeout));
                return Task.FromResult(new RemotingCommand
                {
                    Code = Assert.IsType<IPEndPoint>(endpoint).Port == 10911
                        ? ResponseCodes.ResError
                        : ResponseCodes.ResSuccess
                });
            });
        var session = CreateSession(remoting.Object);
        byte[] heartbeat = [1, 2, 3];

        await session.SendHeartbeatsAsync(
            [
                new RemotingBrokerEndpoint("broker-a", "127.0.0.1:10911"),
                new RemotingBrokerEndpoint("broker-b", "127.0.0.1:10921"),
                new RemotingBrokerEndpoint("broker-b-replica", "127.0.0.1:10921")
            ],
            heartbeat,
            cancellationToken);

        Assert.Equal(2, invocations.Count);
        Assert.Equal([10911, 10921], invocations.Select(static value =>
            Assert.IsType<IPEndPoint>(value.Endpoint).Port));
        Assert.All(invocations, value =>
        {
            Assert.Equal(RequestCode.HeartBeat, value.Request.Code);
            Assert.Same(heartbeat, value.Request.Body);
            Assert.Equal(TimeSpan.FromSeconds(7), value.Timeout);
        });
    }

    [Fact]
    public async Task GetConsumerIdsAsync_NamespacedGroupAndBrokerName_UsesIdentity()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        (EndPoint Endpoint, RemotingCommand Request, TimeSpan Timeout)? invocation = null;
        var remoting = new Mock<IRemotingClient>(MockBehavior.Strict);
        remoting
            .Setup(value => value.InvokeAsync(
                It.IsAny<EndPoint>(),
                It.IsAny<RemotingCommand>(),
                It.IsAny<TimeSpan>(),
                It.IsAny<CancellationToken>()))
            .Returns<EndPoint, RemotingCommand, TimeSpan, CancellationToken>((endpoint, request, timeout, _) =>
            {
                invocation = (endpoint, request, timeout);
                return Task.FromResult(new RemotingCommand
                {
                    Code = ResponseCodes.ResSuccess,
                    Body = Encoding.UTF8.GetBytes(
                        """{"consumerIdList":["client-b","client-a","client-a",""]}""")
                });
            });
        var session = CreateSession(remoting.Object);
        var queue = new RemotingConsumerQueue("orders", 3, "broker-a", "127.0.0.1:10911");

        var consumerIds = await session.GetConsumerIdsAsync(queue, cancellationToken);

        Assert.Equal(["client-a", "client-b"], consumerIds);
        var captured = Assert.NotNull(invocation);
        Assert.Equal(10911, Assert.IsType<IPEndPoint>(captured.Endpoint).Port);
        Assert.Equal(TimeSpan.FromSeconds(7), captured.Timeout);
        Assert.Equal(RequestCode.GetConsumerListByGroup, captured.Request.Code);
        Assert.Equal("tenant-a%orders-consumer", captured.Request.ExtFields["consumerGroup"]);
        Assert.Equal("broker-a", captured.Request.ExtFields["bname"]);
    }

    [Fact]
    public async Task UnregisterAsync_BrokerFailure_ContinuesWithGroupIdentity()
    {
        var invocations = new List<(EndPoint Endpoint, RemotingCommand Request, CancellationToken Token)>();
        var remoting = new Mock<IRemotingClient>(MockBehavior.Strict);
        remoting
            .Setup(value => value.InvokeAsync(
                It.IsAny<EndPoint>(),
                It.IsAny<RemotingCommand>(),
                It.IsAny<TimeSpan>(),
                It.IsAny<CancellationToken>()))
            .Returns<EndPoint, RemotingCommand, TimeSpan, CancellationToken>((endpoint, request, _, token) =>
            {
                invocations.Add((endpoint, request, token));
                return Assert.IsType<IPEndPoint>(endpoint).Port == 10911
                    ? Task.FromException<RemotingCommand>(new IOException("expected broker failure"))
                    : Task.FromResult(new RemotingCommand { Code = ResponseCodes.ResSuccess });
            });
        var session = CreateSession(remoting.Object);

        await session.UnregisterAsync(
            [
                new RemotingBrokerEndpoint("broker-a", "127.0.0.1:10911"),
                new RemotingBrokerEndpoint("broker-b", "127.0.0.1:10921"),
                new RemotingBrokerEndpoint("broker-b-replica", "127.0.0.1:10921")
            ]);

        Assert.Equal(2, invocations.Count);
        Assert.All(invocations, value =>
        {
            Assert.Equal(CancellationToken.None, value.Token);
            Assert.Equal(RequestCode.UnregisterClient, value.Request.Code);
            Assert.Equal("10.0.0.4@consumer-instance@unit-a", value.Request.ExtFields["clientID"]);
            Assert.Equal("tenant-a%orders-consumer", value.Request.ExtFields["consumerGroup"]);
        });
        Assert.Equal("broker-a", invocations[0].Request.ExtFields["bname"]);
        Assert.Equal("broker-b", invocations[1].Request.ExtFields["bname"]);
    }

    private static RemotingConsumerGroupSession CreateSession(IRemotingClient remoting) =>
        new(
            remoting,
            new RemotingClientOptions
            {
                ClientIP = "10.0.0.4",
                InstanceName = "consumer-instance",
                UnitName = "unit-a",
                Namespace = "tenant-a",
                RequestTimeout = TimeSpan.FromSeconds(7)
            },
            "orders-consumer",
            NullLogger<RemotingConsumerGroupSession>.Instance);
}
