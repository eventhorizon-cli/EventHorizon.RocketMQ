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
using EventHorizon.RocketMQ.IntegrationTestInfrastructure;
using EventHorizon.RocketMQ.Remoting.Producer;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace EventHorizon.RocketMQ.Remoting.IntegrationTests;

[Collection(RocketMQMultiBrokerCollection.Name)]
[Trait("Topology", "MultiBroker")]
public sealed class RocketMQMultiBrokerIntegrationTests
{
    private readonly RocketMQMultiBrokerRemotingContainerFixture _fixture;

    public RocketMQMultiBrokerIntegrationTests(RocketMQMultiBrokerRemotingContainerFixture fixture)
    {
        _fixture = fixture;
    }

    [Fact]
    [Trait("Category", "Integration")]
    public async Task ProducerDiscoversAndSendsToAllBrokers()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var services = new ServiceCollection();
        services
            .AddRocketMQRemoting(options => options.NamesrvAddr = _fixture.NameServerAddress)
            .AddRemotingProducer(options => options.GroupName = "remoting-multi-broker-producer-it");

        await using var provider = services.BuildServiceProvider(new ServiceProviderOptions { ValidateOnBuild = true });
        var producer = provider.GetRequiredService<IRemotingProducer>();
        await producer.StartAsync(cancellationToken);
        try
        {
            var queues = await producer.GetPublishMessageQueuesAsync(
                RocketMQMultiBrokerRemotingContainerFixture.TestTopic,
                cancellationToken);
            var brokerNames = queues.Select(queue => queue.BrokerName).Distinct(StringComparer.Ordinal).ToArray();
            Assert.Equal(
                new[]
                {
                    RocketMQMultiBrokerRemotingContainerFixture.BrokerAName,
                    RocketMQMultiBrokerRemotingContainerFixture.BrokerBName,
                    RocketMQMultiBrokerRemotingContainerFixture.BrokerCName
                },
                brokerNames.OrderBy(static brokerName => brokerName, StringComparer.Ordinal).ToArray());

            foreach (var brokerName in brokerNames)
            {
                var queue = queues.First(candidate => candidate.BrokerName == brokerName);
                var body = $"remoting-multi-broker-{brokerName}-{Guid.NewGuid():N}";
                var result = await producer.SendAsync(
                    new Message(RocketMQMultiBrokerRemotingContainerFixture.TestTopic, Encoding.UTF8.GetBytes(body)),
                    queue,
                    cancellationToken);

                Assert.Equal(RemotingSendStatus.SendOk, result.Status);
                Assert.Equal(brokerName, result.MessageQueue.BrokerName);
                Assert.Equal(queue.QueueId, result.MessageQueue.QueueId);
            }
        }
        finally
        {
            await producer.StopAsync(CancellationToken.None);
        }
    }
}
