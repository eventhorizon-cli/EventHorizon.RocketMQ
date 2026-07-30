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
using EventHorizon.RocketMQ.Grpc.Consumer.Push;
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
        services.AddSingleton(new LiteMessageObservation(liteTopic, expected, consumed));
        services
            .AddRocketMQGrpc(options => options.Endpoint = fixture.GrpcEndpoint)
            .AddGrpcProducer(options => options.Topics.Add(scope.Topic))
            .AddGrpcLitePushConsumer<LiteMessageHandler>(ServiceLifetime.Singleton, options =>
            {
                options.GroupName = consumerGroup;
                options.BindTopic = scope.Topic;
                options.LiteTopics.Add(liteTopic);
                options.LongPollingTimeout = TimeSpan.FromSeconds(3);
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

    [Fact]
    [Trait("Category", "Integration")]
    public async Task GrpcLitePushConsumersInTheSameGroupDispatchTheirOwnLiteTopics()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var fixture = await registry.GetFixtureAsync(cancellationToken);
        var scope = await fixture.CreateTestScopeAsync(RocketMQTestTopicType.Lite, cancellationToken);
        var consumerGroup = await scope.CreateLiteConsumerGroupAsync(
            "grpc-multiple-lite-push-consumers",
            cancellationToken);
        var firstLiteTopic = $"lite-first-{Guid.NewGuid():N}";
        var secondLiteTopic = $"lite-second-{Guid.NewGuid():N}";
        var firstBody = $"grpc-lite-first-{Guid.NewGuid():N}";
        var secondBody = $"grpc-lite-second-{Guid.NewGuid():N}";
        var firstConsumed = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
        var secondConsumed = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
        var unexpectedDelivery = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
        var services = new ServiceCollection();
        services.AddSingleton(new MultipleLiteMessageObservation(
            firstLiteTopic,
            secondLiteTopic,
            firstBody,
            secondBody,
            firstConsumed,
            secondConsumed,
            unexpectedDelivery));
        var rocketMQ = services
            .AddRocketMQGrpc(options => options.Endpoint = fixture.GrpcEndpoint)
            .AddGrpcProducer(options => options.Topics.Add(scope.Topic));
        rocketMQ.AddGrpcLitePushConsumer<FirstLiteMessageHandler>(ServiceLifetime.Singleton, options =>
        {
            options.GroupName = consumerGroup;
            options.BindTopic = scope.Topic;
            options.LiteTopics.Add(firstLiteTopic);
            options.LongPollingTimeout = TimeSpan.FromSeconds(3);
        });
        rocketMQ.AddGrpcLitePushConsumer<SecondLiteMessageHandler>(ServiceLifetime.Singleton, options =>
        {
            options.GroupName = consumerGroup;
            options.BindTopic = scope.Topic;
            options.LiteTopics.Add(secondLiteTopic);
            options.LongPollingTimeout = TimeSpan.FromSeconds(3);
        });

        await using var provider = services.BuildServiceProvider(
            new ServiceProviderOptions { ValidateOnBuild = true });
        var producer = provider.GetRequiredService<IGrpcProducer>();
        var consumers = provider.GetServices<IGrpcLitePushConsumer>().ToArray();
        Assert.Equal(2, consumers.Length);
        var startedConsumers = new List<IGrpcLitePushConsumer>();

        await producer.StartAsync(cancellationToken);
        try
        {
            foreach (var consumer in consumers)
            {
                await consumer.StartAsync(cancellationToken);
                startedConsumers.Add(consumer);
            }

            var firstReceipt = await producer.SendAsync(new Message(
                scope.Topic,
                Encoding.UTF8.GetBytes(firstBody))
            {
                LiteTopic = firstLiteTopic
            }, cancellationToken);
            var secondReceipt = await producer.SendAsync(new Message(
                scope.Topic,
                Encoding.UTF8.GetBytes(secondBody))
            {
                LiteTopic = secondLiteTopic
            }, cancellationToken);

            var expectedDeliveries = Task.WhenAll(firstConsumed.Task, secondConsumed.Task);
            var firstCompletion = await Task.WhenAny(expectedDeliveries, unexpectedDelivery.Task)
                .WaitAsync(TimeSpan.FromSeconds(30), cancellationToken);
            if (ReferenceEquals(firstCompletion, unexpectedDelivery.Task))
            {
                Assert.Fail(await unexpectedDelivery.Task);
            }

            var messageIds = await expectedDeliveries;
            Assert.Equal(firstReceipt.MessageId, messageIds[0]);
            Assert.Equal(secondReceipt.MessageId, messageIds[1]);
            await Task.Delay(TimeSpan.FromSeconds(1), cancellationToken);
            if (unexpectedDelivery.Task.IsCompletedSuccessfully)
            {
                Assert.Fail(await unexpectedDelivery.Task);
            }
        }
        finally
        {
            foreach (var consumer in startedConsumers.AsEnumerable().Reverse())
            {
                await consumer.StopAsync(CancellationToken.None);
            }

            await producer.StopAsync(CancellationToken.None);
        }
    }

    private sealed record LiteMessageObservation(
        string LiteTopic,
        string ExpectedBody,
        TaskCompletionSource<string> Consumed);

    private sealed class LiteMessageHandler(LiteMessageObservation observation) : IGrpcPushMessageHandler
    {
        public ValueTask<ConsumeResult> HandleAsync(
            GrpcMessageView message,
            CancellationToken cancellationToken)
        {
            if (message.LiteTopic == observation.LiteTopic &&
                Encoding.UTF8.GetString(message.Body) == observation.ExpectedBody)
            {
                observation.Consumed.TrySetResult(message.MessageId);
            }

            return ValueTask.FromResult(ConsumeResult.Success);
        }
    }

    private sealed class MultipleLiteMessageObservation(
        string firstLiteTopic,
        string secondLiteTopic,
        string firstBody,
        string secondBody,
        TaskCompletionSource<string> firstConsumed,
        TaskCompletionSource<string> secondConsumed,
        TaskCompletionSource<string> unexpectedDelivery)
    {
        public ValueTask<ConsumeResult> Handle(GrpcMessageView message, bool firstConsumer)
        {
            var expectedLiteTopic = firstConsumer ? firstLiteTopic : secondLiteTopic;
            var expectedBody = firstConsumer ? firstBody : secondBody;
            if (!string.Equals(message.LiteTopic, expectedLiteTopic, StringComparison.Ordinal) ||
                !string.Equals(Encoding.UTF8.GetString(message.Body), expectedBody, StringComparison.Ordinal))
            {
                var consumerName = firstConsumer ? "first" : "second";
                unexpectedDelivery.TrySetResult(
                    $"The {consumerName} LitePush consumer received LiteTopic '{message.LiteTopic}'.");
                return ValueTask.FromResult(ConsumeResult.Success);
            }

            (firstConsumer ? firstConsumed : secondConsumed).TrySetResult(message.MessageId);
            return ValueTask.FromResult(ConsumeResult.Success);
        }
    }

    private sealed class FirstLiteMessageHandler(MultipleLiteMessageObservation observation)
        : IGrpcPushMessageHandler
    {
        public ValueTask<ConsumeResult> HandleAsync(
            GrpcMessageView message,
            CancellationToken cancellationToken) =>
            observation.Handle(message, true);
    }

    private sealed class SecondLiteMessageHandler(MultipleLiteMessageObservation observation)
        : IGrpcPushMessageHandler
    {
        public ValueTask<ConsumeResult> HandleAsync(
            GrpcMessageView message,
            CancellationToken cancellationToken) =>
            observation.Handle(message, false);
    }
}
