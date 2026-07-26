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
using System.Text;
using EventHorizon.RocketMQ.Grpc.Consumer;
using EventHorizon.RocketMQ.Grpc.Consumer.Push;
using EventHorizon.RocketMQ.Grpc.Producer;
using EventHorizon.RocketMQ.IntegrationTestInfrastructure;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace EventHorizon.RocketMQ.Grpc.IntegrationTests;

[Collection(RocketMQCollection.Name)]
public sealed class RocketMQMessageTypesIntegrationTests
{
    private readonly RocketMQContainerFixture _fixture;

    public RocketMQMessageTypesIntegrationTests(RocketMQContainerFixture fixture)
    {
        _fixture = fixture;
    }

    [Fact]
    [Trait("Category", "Integration")]
    public async Task GrpcFifoMessagesRemainOrderedWithConcurrentWorkers()
    {
        const int messageCount = 8;
        var cancellationToken = TestContext.Current.CancellationToken;
        var tag = $"fifo-{Guid.NewGuid():N}";
        var messageGroup = $"account-{Guid.NewGuid():N}";
        var received = new ConcurrentQueue<int>();
        var allReceived = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var services = new ServiceCollection();
        services
            .AddRocketMQGrpc(options => options.Endpoint = _fixture.GrpcEndpoint)
            .AddGrpcProducer(options => options.Topics.Add(RocketMQContainerFixture.FifoTopic))
            .AddGrpcPushConsumer(options =>
            {
                options.GroupName = "grpc-fifo-consumer-it";
                options.MaxConcurrency = 4;
                options.Subscribe(RocketMQContainerFixture.FifoTopic, new FilterExpression(tag));
                options.MessageHandler = (message, _) =>
                {
                    if (message.Tag == tag && int.TryParse(Encoding.UTF8.GetString(message.Body), out var sequence))
                    {
                        received.Enqueue(sequence);
                        if (received.Count == messageCount)
                        {
                            allReceived.TrySetResult();
                        }
                    }

                    return ValueTask.FromResult(ConsumeResult.Success);
                };
            });

        await using var provider = services.BuildServiceProvider(new ServiceProviderOptions { ValidateOnBuild = true });
        var producer = provider.GetRequiredService<IGrpcProducer>();
        var consumer = provider.GetRequiredService<IGrpcPushConsumer>();
        await producer.StartAsync(cancellationToken);
        await consumer.StartAsync(cancellationToken);
        try
        {
            for (var sequence = 0; sequence < messageCount; sequence++)
            {
                await producer.SendAsync(new Message(
                    RocketMQContainerFixture.FifoTopic,
                    Encoding.UTF8.GetBytes(sequence.ToString()))
                {
                    Tag = tag,
                    MessageGroup = messageGroup
                }, cancellationToken);
            }

            await allReceived.Task.WaitAsync(TimeSpan.FromSeconds(30), cancellationToken);
            Assert.Equal(Enumerable.Range(0, messageCount), received);
        }
        finally
        {
            await consumer.StopAsync(CancellationToken.None);
            await producer.StopAsync(CancellationToken.None);
        }
    }
}
