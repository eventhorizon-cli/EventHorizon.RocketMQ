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
    private readonly RocketMQMultiBrokerGrpcContainerFixture _fixture;

    public RocketMQMultiBrokerIntegrationTests(RocketMQMultiBrokerGrpcContainerFixture fixture)
    {
        _fixture = fixture;
    }

    [Fact]
    [Trait("Category", "Integration")]
    public async Task ProducerRoutesMessagesThroughProxyToAllBrokers()
    {
        const int messageCount = 24;
        var cancellationToken = TestContext.Current.CancellationToken;
        var services = new ServiceCollection();
        services
            .AddRocketMQGrpc(options => options.Endpoint = _fixture.GrpcEndpoint)
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
                        RocketMQMultiBrokerGrpcContainerFixture.TestTopic,
                        Encoding.UTF8.GetBytes($"grpc-multi-broker-{index}-{Guid.NewGuid():N}")),
                    cancellationToken);
                Assert.NotEmpty(receipt.MessageId);
            }

            var offsets = await _fixture.WaitForMessagesOnAllBrokersAsync(
                RocketMQMultiBrokerGrpcContainerFixture.TestTopic,
                TimeSpan.FromSeconds(15),
                cancellationToken);
            Assert.True(
                offsets.TryGetValue(RocketMQMultiBrokerGrpcContainerFixture.BrokerAName, out var brokerAOffset) &&
                brokerAOffset > 0,
                $"No gRPC message reached {RocketMQMultiBrokerGrpcContainerFixture.BrokerAName}. Offsets: {FormatOffsets(offsets)}");
            Assert.True(
                offsets.TryGetValue(RocketMQMultiBrokerGrpcContainerFixture.BrokerBName, out var brokerBOffset) &&
                brokerBOffset > 0,
                $"No gRPC message reached {RocketMQMultiBrokerGrpcContainerFixture.BrokerBName}. Offsets: {FormatOffsets(offsets)}");
            Assert.True(
                offsets.TryGetValue(RocketMQMultiBrokerGrpcContainerFixture.BrokerCName, out var brokerCOffset) &&
                brokerCOffset > 0,
                $"No gRPC message reached {RocketMQMultiBrokerGrpcContainerFixture.BrokerCName}. Offsets: {FormatOffsets(offsets)}");
        }
        finally
        {
            await producer.StopAsync(CancellationToken.None);
        }
    }

    private static string FormatOffsets(IReadOnlyDictionary<string, long> offsets) =>
        string.Join(", ", offsets.Select(static entry => $"{entry.Key}={entry.Value}"));
}
