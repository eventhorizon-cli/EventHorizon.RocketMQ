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

using System.Text;
using EventHorizon.RocketMQ.Grpc.Producer;
using EventHorizon.RocketMQ.IntegrationTestInfrastructure;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace EventHorizon.RocketMQ.Grpc.IntegrationTests;

[Collection(RocketMQMultiBrokerCollection.Name)]
[Trait("Topology", "MultiBroker")]
public sealed class RocketMQMultiBrokerIntegrationTests
{
    private readonly RocketMQMultiBrokerClusterProxyContainerFixture _fixture;

    public RocketMQMultiBrokerIntegrationTests(RocketMQMultiBrokerClusterProxyContainerFixture fixture)
    {
        _fixture = fixture;
    }

    [Fact]
    [Trait("Category", "Integration")]
    public async Task Producer_AllBrokersThroughProxy_RoutesMessages()
    {
        const int messageCount = 24;
        var cancellationToken = TestContext.Current.CancellationToken;
        var services = new ServiceCollection();
        services
            .AddRocketMQGrpc(options => options.Endpoint = _fixture.GrpcEndpoints)
            .AddGrpcProducer();

        await using var provider = services.BuildServiceProvider(new ServiceProviderOptions { ValidateOnBuild = true });
        var producer = provider.GetRequiredService<IGrpcProducer>();
        await producer.StartAsync(cancellationToken);
        try
        {
            for (var index = 0; index < messageCount; index++)
            {
                var receipt = await producer.SendAsync(
                    new Message(
                        RocketMQMultiBrokerClusterProxyContainerFixture.TestTopic,
                        Encoding.UTF8.GetBytes($"grpc-multi-broker-{index}-{Guid.NewGuid():N}")),
                    cancellationToken);
                Assert.NotEmpty(receipt.MessageId);
            }

            var offsets = await _fixture.WaitForMessagesOnAllBrokersAsync(
                RocketMQMultiBrokerClusterProxyContainerFixture.TestTopic,
                TimeSpan.FromSeconds(15),
                cancellationToken);
            Assert.True(
                offsets.TryGetValue(RocketMQMultiBrokerClusterProxyContainerFixture.BrokerAName, out var brokerAOffset) &&
                brokerAOffset > 0,
                $"No gRPC message reached {RocketMQMultiBrokerClusterProxyContainerFixture.BrokerAName}. Offsets: {FormatOffsets(offsets)}");
            Assert.True(
                offsets.TryGetValue(RocketMQMultiBrokerClusterProxyContainerFixture.BrokerBName, out var brokerBOffset) &&
                brokerBOffset > 0,
                $"No gRPC message reached {RocketMQMultiBrokerClusterProxyContainerFixture.BrokerBName}. Offsets: {FormatOffsets(offsets)}");
            Assert.True(
                offsets.TryGetValue(RocketMQMultiBrokerClusterProxyContainerFixture.BrokerCName, out var brokerCOffset) &&
                brokerCOffset > 0,
                $"No gRPC message reached {RocketMQMultiBrokerClusterProxyContainerFixture.BrokerCName}. Offsets: {FormatOffsets(offsets)}");
        }
        finally
        {
            await producer.StopAsync(CancellationToken.None);
        }
    }

    [Fact]
    [Trait("Category", "Integration")]
    public async Task Producer_ProxyAUnavailable_FallsBackToProxyB()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var services = new ServiceCollection();
        services
            .AddRocketMQGrpc(options => options.Endpoint = _fixture.GrpcEndpoints)
            .AddGrpcProducer();

        await using var provider = services.BuildServiceProvider(new ServiceProviderOptions { ValidateOnBuild = true });
        var producer = provider.GetRequiredService<IGrpcProducer>();
        var proxyAStopped = false;
        try
        {
            var beforeOffsets = await _fixture.GetBrokerMessageOffsetsAsync(
                RocketMQMultiBrokerClusterProxyContainerFixture.TestTopic,
                cancellationToken);
            var beforeTotalOffset = beforeOffsets.Values.Sum();

            proxyAStopped = true;
            await _fixture.StopProxyAAsync(CancellationToken.None);
            await producer.StartAsync(cancellationToken);
            var receipt = await producer.SendAsync(
                new Message(
                    RocketMQMultiBrokerClusterProxyContainerFixture.TestTopic,
                    Encoding.UTF8.GetBytes($"grpc-proxy-failover-{Guid.NewGuid():N}")),
                cancellationToken);

            Assert.NotEmpty(receipt.MessageId);
            var offsets = beforeOffsets;
            var deadline = DateTimeOffset.UtcNow + TimeSpan.FromSeconds(15);
            do
            {
                offsets = await _fixture.GetBrokerMessageOffsetsAsync(
                    RocketMQMultiBrokerClusterProxyContainerFixture.TestTopic,
                    cancellationToken);
                if (offsets.Values.Sum() > beforeTotalOffset)
                {
                    break;
                }

                await Task.Delay(TimeSpan.FromMilliseconds(250), cancellationToken);
            }
            while (DateTimeOffset.UtcNow < deadline);

            Assert.True(
                offsets.Values.Sum() > beforeTotalOffset,
                $"The failover message was not stored by any Broker. Offsets: {FormatOffsets(offsets)}");
        }
        finally
        {
            try
            {
                await producer.StopAsync(CancellationToken.None);
            }
            finally
            {
                if (proxyAStopped)
                {
                    await _fixture.RestoreProxyAAsync(CancellationToken.None);
                }
            }
        }
    }

    private static string FormatOffsets(IReadOnlyDictionary<string, long> offsets) =>
        string.Join(", ", offsets.Select(static entry => $"{entry.Key}={entry.Value}"));
}
