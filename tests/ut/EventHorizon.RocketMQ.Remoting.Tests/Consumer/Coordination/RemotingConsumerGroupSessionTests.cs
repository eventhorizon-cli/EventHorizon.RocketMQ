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
using EventHorizon.RocketMQ.Remoting.Consumer.Coordination;
using EventHorizon.RocketMQ.Remoting.Consumer.Coordination.Rebalance;
using EventHorizon.RocketMQ.Remoting.Consumer.Route;
using EventHorizon.RocketMQ.Remoting.Protocol;
using EventHorizon.RocketMQ.Remoting.Tests.Consumer.Coordination.Rebalance;
using Microsoft.Extensions.Logging.Abstractions;
using Moq;
using Xunit;

namespace EventHorizon.RocketMQ.Remoting.Tests.Consumer.Coordination;

public sealed class RemotingConsumerGroupSessionTests
{
    [Fact]
    public void AddKnownBrokers_DuplicateAddress_ReplacesExistingEndpoint()
    {
        var session = CreateSession();

        session.AddKnownBroker(new RemotingBrokerEndpoint("broker-a", "127.0.0.1:10911"));
        session.AddKnownBrokers(
        [
            new RemotingBrokerEndpoint("broker-b", "127.0.0.1:10921"),
            new RemotingBrokerEndpoint("broker-a-primary", "127.0.0.1:10911")
        ]);

        Assert.Equal(2, session.KnownBrokers.Count);
        Assert.Equal(
            "broker-a-primary",
            Assert.Single(session.KnownBrokers, value => value.Address == "127.0.0.1:10911").BrokerName);
        Assert.Contains(
            session.KnownBrokers,
            value => value == new RemotingBrokerEndpoint("broker-b", "127.0.0.1:10921"));
    }

    [Fact]
    public void BumpSubscriptionVersion_ExistingVersion_IncrementsVersion()
    {
        var session = CreateSession(subscriptionVersion: 41);

        Assert.Equal(41, session.SubscriptionVersion);

        session.BumpSubscriptionVersion();
        session.BumpSubscriptionVersion();

        Assert.Equal(43, session.SubscriptionVersion);
    }

    [Fact]
    public async Task RegisterRebalance_FirstRegistration_RegistersConsumerGroupCallback()
    {
        var rebalanceService = new TestRemotingRebalanceService();
        var session = CreateSession(rebalanceService);
        var calls = 0;

        session.RegisterRebalance(_ =>
        {
            Interlocked.Increment(ref calls);
            return Task.FromResult(true);
        });

        Assert.Equal(1, rebalanceService.ActiveRegistrations);
        Assert.True(await rebalanceService.RebalanceAsync(session.ConsumerGroup, CancellationToken.None));
        Assert.Equal(1, Volatile.Read(ref calls));
    }

    [Fact]
    public void RegisterRebalance_SecondRegistration_ThrowsAndKeepsOriginalRegistration()
    {
        var rebalanceService = new TestRemotingRebalanceService();
        var session = CreateSession(rebalanceService);

        session.RegisterRebalance(_ => Task.FromResult(true));

        var exception = Assert.Throws<InvalidOperationException>(() =>
            session.RegisterRebalance(_ => Task.FromResult(true)));

        Assert.Contains("rebalance registration", exception.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Equal(1, rebalanceService.ActiveRegistrations);
    }

    [Fact]
    public async Task StopRebalanceAsync_RegisteredParticipant_DisposesRegistrationAndIsIdempotent()
    {
        var rebalanceService = new TestRemotingRebalanceService();
        var session = CreateSession(rebalanceService);
        session.RegisterRebalance(_ => Task.FromResult(true));

        await session.StopRebalanceAsync();
        await session.StopRebalanceAsync();

        Assert.Equal(0, rebalanceService.ActiveRegistrations);
    }

    [Fact]
    public async Task UnregisterAsync_KnownBrokers_DeduplicatesAddressesAndUnregistersEachBroker()
    {
        var invocations = new List<(EndPoint Endpoint, RemotingCommand Request)>();
        var remoting = new Mock<IRemotingClient>(MockBehavior.Strict);
        remoting
            .Setup(value => value.InvokeAsync(
                It.IsAny<EndPoint>(),
                It.IsAny<RemotingCommand>(),
                It.IsAny<TimeSpan>(),
                It.IsAny<CancellationToken>()))
            .Callback<EndPoint, RemotingCommand, TimeSpan, CancellationToken>(
                (endpoint, request, _, _) => invocations.Add((endpoint, request)))
            .ReturnsAsync(new RemotingCommand { Code = ResponseCodes.ResSuccess });
        var session = CreateSession(remoting: remoting.Object);
        session.AddKnownBrokers(
        [
            new RemotingBrokerEndpoint("broker-a", "127.0.0.1:10911"),
            new RemotingBrokerEndpoint("broker-b", "127.0.0.1:10921"),
            new RemotingBrokerEndpoint("broker-b-replica", "127.0.0.1:10921")
        ]);

        await session.UnregisterAsync();

        Assert.Equal(2, invocations.Count);
        Assert.Equal(
            [10911, 10921],
            invocations.Select(static value => Assert.IsType<IPEndPoint>(value.Endpoint).Port).Order());
        Assert.All(invocations, value => Assert.Equal(RequestCode.UnregisterClient, value.Request.Code));
    }

    private static RemotingConsumerGroupSession CreateSession(
        IRemotingRebalanceService? rebalanceService = null,
        IRemotingClient? remoting = null,
        long subscriptionVersion = 0)
    {
        var client = new RemotingConsumerGroupClient(
            remoting ?? new Mock<IRemotingClient>(MockBehavior.Strict).Object,
            new RemotingClientOptions
            {
                ClientIP = "10.0.0.4",
                InstanceName = "consumer-instance",
                UnitName = "unit-a",
                Namespace = "tenant-a",
                RequestTimeout = TimeSpan.FromSeconds(7)
            },
            "orders-consumer",
            NullLogger<RemotingConsumerGroupClient>.Instance);

        return new RemotingConsumerGroupSession(
            client,
            rebalanceService ?? new TestRemotingRebalanceService(),
            subscriptionVersion);
    }
}
