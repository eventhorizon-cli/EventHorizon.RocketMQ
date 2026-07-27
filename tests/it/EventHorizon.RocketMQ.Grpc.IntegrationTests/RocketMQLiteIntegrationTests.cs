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
using EventHorizon.RocketMQ.Grpc.Consumer.Lite;
using EventHorizon.RocketMQ.Grpc.Producer;
using EventHorizon.RocketMQ.IntegrationTestInfrastructure;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace EventHorizon.RocketMQ.Grpc.IntegrationTests;

public sealed class RocketMQLiteIntegrationTests(RocketMQContainerFixtureRegistry registry)
{

    [Fact]
    [Trait("Category", "Integration")]
    public async Task GrpcLitePushConsumerDispatchesLiteMessages()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var fixture = await registry.GetFixtureAsync(cancellationToken);
        var scope = await fixture.CreateTestScopeAsync(RocketMQTestTopicType.Lite, cancellationToken);
        var consumerGroup = await scope.CreateLiteConsumerGroupAsync("grpc-lite-push-consumer", cancellationToken);
        var liteTopic = $"lite-{Guid.NewGuid():N}";
        var expected = $"grpc-lite-{Guid.NewGuid():N}";
        var consumed = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
        var services = new ServiceCollection();
        services
            .AddRocketMQGrpc(options => options.Endpoint = fixture.GrpcEndpoint)
            .AddGrpcProducer(options => options.Topics.Add(scope.Topic))
            .AddGrpcLitePushConsumer(options =>
            {
                options.GroupName = consumerGroup;
                options.BindTopic = scope.Topic;
                options.LiteTopics.Add(liteTopic);
                options.LongPollingTimeout = TimeSpan.FromSeconds(3);
                options.MessageHandler = (message, _) =>
                {
                    if (message.LiteTopic == liteTopic && Encoding.UTF8.GetString(message.Body) == expected)
                    {
                        consumed.TrySetResult(message.MessageId);
                    }

                    return ValueTask.FromResult(ConsumeResult.Success);
                };
            });

        await using var provider = services.BuildServiceProvider(new ServiceProviderOptions { ValidateOnBuild = true });
        var producer = provider.GetRequiredService<IGrpcProducer>();
        var consumer = provider.GetRequiredService<IGrpcLitePushConsumer>();
        await producer.StartAsync(cancellationToken);
        await consumer.StartAsync(cancellationToken);
        try
        {
            var receipt = await producer.SendAsync(new Message(
                scope.Topic,
                Encoding.UTF8.GetBytes(expected))
            {
                LiteTopic = liteTopic
            }, cancellationToken);

            var messageId = await consumed.Task.WaitAsync(TimeSpan.FromSeconds(30), cancellationToken);
            Assert.Equal(receipt.MessageId, messageId);
        }
        finally
        {
            await consumer.StopAsync(CancellationToken.None);
            await producer.StopAsync(CancellationToken.None);
        }
    }
}
