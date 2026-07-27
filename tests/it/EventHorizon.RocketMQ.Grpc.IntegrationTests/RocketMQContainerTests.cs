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
using EventHorizon.RocketMQ.Grpc.Consumer.Push;
using EventHorizon.RocketMQ.Grpc.Consumer.Simple;
using EventHorizon.RocketMQ.Grpc.Producer;
using EventHorizon.RocketMQ.Grpc.Producer.Transactions;
using EventHorizon.RocketMQ.IntegrationTestInfrastructure;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace EventHorizon.RocketMQ.Grpc.IntegrationTests;

public sealed class RocketMQContainerTests(RocketMQContainerFixtureRegistry registry)
{

    [Fact]
    [Trait("Category", "Integration")]
    public async Task GrpcProducerAndSimpleConsumerRoundTrip()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var fixture = await registry.GetFixtureAsync(cancellationToken);
        var scope = await fixture.CreateTestScopeAsync(RocketMQTestTopicType.Normal, cancellationToken);
        var consumerGroup = scope.CreateConsumerGroupName("grpc-simple-consumer");
        var services = new ServiceCollection();
        services
            .AddRocketMQGrpc(options => options.Endpoint = fixture.GrpcEndpoint)
            .AddGrpcProducer()
            .AddGrpcSimpleConsumer(options =>
            {
                options.GroupName = consumerGroup;
                options.AwaitDuration = TimeSpan.FromSeconds(3);
                options.Subscribe(scope.Topic, new FilterExpression("grpc-simple"));
            });

        await using var provider = services.BuildServiceProvider(new ServiceProviderOptions { ValidateOnBuild = true });
        var producer = provider.GetRequiredService<IGrpcProducer>();
        var consumer = provider.GetRequiredService<IGrpcSimpleConsumer>();
        await producer.StartAsync(cancellationToken);
        await consumer.StartAsync(cancellationToken);
        try
        {
            var body = $"grpc-simple-{Guid.NewGuid():N}";
            var receipt = await producer.SendAsync(new Message(
                scope.Topic,
                Encoding.UTF8.GetBytes(body))
            {
                Tag = "grpc-simple"
            }, cancellationToken);
            Assert.NotEmpty(receipt.MessageId);
            Assert.NotEmpty(receipt.Endpoints);

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
            Assert.Equal(receipt.MessageId, received.MessageId);
        }
        finally
        {
            await consumer.StopAsync(CancellationToken.None);
            await producer.StopAsync(CancellationToken.None);
        }
    }

    [Fact]
    [Trait("Category", "Integration")]
    public async Task GrpcTransactionCommitMakesMessageVisible()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var fixture = await registry.GetFixtureAsync(cancellationToken);
        var scope = await fixture.CreateTestScopeAsync(RocketMQTestTopicType.Transaction, cancellationToken);
        var consumerGroup = scope.CreateConsumerGroupName("grpc-transaction-consumer");
        var services = new ServiceCollection();
        services
            .AddRocketMQGrpc(options => options.Endpoint = fixture.GrpcEndpoint)
            .AddGrpcProducer(options =>
            {
                options.Topics.Add(scope.Topic);
                options.TransactionChecker = static (_, _) =>
                    ValueTask.FromResult(TransactionResolution.Unknown);
            })
            .AddGrpcSimpleConsumer(options =>
            {
                options.GroupName = consumerGroup;
                options.AwaitDuration = TimeSpan.FromSeconds(3);
                options.Subscribe(scope.Topic, new FilterExpression("transaction"));
            });

        await using var provider = services.BuildServiceProvider(new ServiceProviderOptions { ValidateOnBuild = true });
        var producer = provider.GetRequiredService<IGrpcProducer>();
        var consumer = provider.GetRequiredService<IGrpcSimpleConsumer>();
        await producer.StartAsync(cancellationToken);
        await consumer.StartAsync(cancellationToken);
        try
        {
            var body = $"transaction-{Guid.NewGuid():N}";
            var transaction = await producer.SendTransactionAsync(new Message(
                scope.Topic,
                Encoding.UTF8.GetBytes(body))
            {
                Tag = "transaction"
            }, cancellationToken);
            await transaction.CommitAsync(cancellationToken);

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
            Assert.Equal(transaction.MessageId, received.MessageId);
        }
        finally
        {
            await consumer.StopAsync(CancellationToken.None);
            await producer.StopAsync(CancellationToken.None);
        }
    }

    [Fact]
    [Trait("Category", "Integration")]
    public async Task GrpcPushConsumerDispatchesAndAcknowledgesMessage()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var fixture = await registry.GetFixtureAsync(cancellationToken);
        var scope = await fixture.CreateTestScopeAsync(RocketMQTestTopicType.Normal, cancellationToken);
        var consumerGroup = scope.CreateConsumerGroupName("grpc-push-consumer");
        var consumed = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
        var expected = $"grpc-push-{Guid.NewGuid():N}";
        var services = new ServiceCollection();
        services
            .AddRocketMQGrpc(options => options.Endpoint = fixture.GrpcEndpoint)
            .AddGrpcProducer()
            .AddGrpcPushConsumer(options =>
            {
                options.GroupName = consumerGroup;
                options.LongPollingTimeout = TimeSpan.FromSeconds(3);
                options.Subscribe(scope.Topic, new FilterExpression("grpc-push"));
                options.MessageHandler = (message, _) =>
                {
                    var body = Encoding.UTF8.GetString(message.Body);
                    if (body == expected)
                    {
                        consumed.TrySetResult(message.MessageId);
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
            var sent = await producer.SendAsync(new Message(
                scope.Topic,
                Encoding.UTF8.GetBytes(expected))
            {
                Tag = "grpc-push"
            }, cancellationToken);
            var messageId = await consumed.Task.WaitAsync(TimeSpan.FromSeconds(30), cancellationToken);
            Assert.Equal(sent.MessageId, messageId);
        }
        finally
        {
            await consumer.StopAsync(CancellationToken.None);
            await producer.StopAsync(CancellationToken.None);
        }
    }
}
