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
using EventHorizon.RocketMQ.Grpc.Consumer;
using EventHorizon.RocketMQ.Grpc.Consumer.Simple;
using EventHorizon.RocketMQ.Grpc.Producer;
using EventHorizon.RocketMQ.IntegrationTestInfrastructure;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace EventHorizon.RocketMQ.Grpc.IntegrationTests;

[Collection(RocketMQLocalProxyCollection.Name)]
[Trait("Topology", "LocalProxy")]
public sealed class RocketMQLocalProxyIntegrationTests
{
    private readonly RocketMQLocalProxyContainerFixture _fixture;

    public RocketMQLocalProxyIntegrationTests(RocketMQLocalProxyContainerFixture fixture)
    {
        _fixture = fixture;
    }

    [Fact]
    [Trait("Category", "Integration")]
    public async Task LocalProxyProducerAndSimpleConsumer_MessageRoundTrip_Completes()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var consumerGroup = $"grpc-local-proxy-simple-consumer-{Guid.NewGuid():N}";
        await _fixture.CreateConsumerGroupAsync(consumerGroup, cancellationToken);
        var services = new ServiceCollection();
        services
            .AddRocketMQGrpc(options => options.Endpoint = _fixture.GrpcEndpoint)
            .AddGrpcProducer()
            .AddGrpcSimpleConsumer(options =>
            {
                options.GroupName = consumerGroup;
                options.AwaitDuration = TimeSpan.FromSeconds(3);
                options.Subscribe(RocketMQLocalProxyContainerFixture.TestTopic);
            });

        await using var provider = services.BuildServiceProvider(new ServiceProviderOptions { ValidateOnBuild = true });
        var producer = provider.GetRequiredService<IGrpcProducer>();
        var consumer = provider.GetRequiredService<IGrpcSimpleConsumer>();
        await producer.StartAsync(cancellationToken);
        await consumer.StartAsync(cancellationToken);
        try
        {
            var body = $"grpc-local-proxy-{Guid.NewGuid():N}";
            var receipt = await producer.SendAsync(
                new Message(
                    RocketMQLocalProxyContainerFixture.TestTopic,
                    Encoding.UTF8.GetBytes(body)),
                cancellationToken);
            Assert.NotEmpty(receipt.MessageId);

            GrpcMessageView? received = null;
            for (var attempt = 0; attempt < 10 && received is null; attempt++)
            {
                var messages = await consumer.ReceiveAsync(16, cancellationToken: cancellationToken);
                received = messages.FirstOrDefault(message => Encoding.UTF8.GetString(message.Body) == body);
                foreach (var message in messages)
                {
                    await consumer.AckAsync(message, cancellationToken);
                }
            }

            Assert.NotNull(received);
            Assert.Equal(body, Encoding.UTF8.GetString(received.Body));
            Assert.Equal(receipt.MessageId, received.MessageId);
        }
        finally
        {
            await consumer.StopAsync(CancellationToken.None);
            await producer.StopAsync(CancellationToken.None);
        }
    }
}
