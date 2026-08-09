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
using System.Diagnostics;
using System.Globalization;
using System.Text;
using EventHorizon.RocketMQ.Grpc.Consumer;
using EventHorizon.RocketMQ.Grpc.Consumer.LitePush;
using EventHorizon.RocketMQ.Grpc.Consumer.Push;
using EventHorizon.RocketMQ.Grpc.Producer;
using EventHorizon.RocketMQ.IntegrationTestInfrastructure;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace EventHorizon.RocketMQ.Grpc.IntegrationTests;

public sealed class RocketMQDeadLetterIntegrationTests(RocketMQSingleBrokerContainerFixtureRegistry registry)
{
    [Fact]
    [Trait("Category", "Integration")]
    public async Task GrpcPushConsumer_FifoFailureExhaustsRetries_ForwardsToDeadLetterQueue()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var fixture = await registry.GetFixtureAsync(cancellationToken);
        var scope = await fixture.CreateTestScopeAsync(RocketMQTestTopicType.Fifo, cancellationToken);
        var consumerGroup = await scope.CreateOrderedConsumerGroupAsync(
            "grpc-push-dlq-consumer",
            retryMaxTimes: 1,
            cancellationToken: cancellationToken);
        var handled = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var expected = $"grpc-dlq-{Guid.NewGuid():N}";
        var observation = new DeadLetterObservation(expected, handled);
        var services = new ServiceCollection();
        services.AddSingleton(observation);
        services
            .AddRocketMQGrpc(options => options.Endpoint = fixture.GrpcEndpoint)
            .AddGrpcProducer()
            .AddGrpcPushConsumer<DeadLetterMessageHandler>(ServiceLifetime.Singleton, options =>
            {
                options.GroupName = consumerGroup;
                options.MaxDeliveryAttempts = 2;
                options.RetryDelay = TimeSpan.FromMilliseconds(100);
                options.LongPollingTimeout = TimeSpan.FromSeconds(1);
                options.Subscribe(scope.Topic, new FilterExpression("grpc-dlq"));
            });

        await using var provider = services.BuildServiceProvider(new ServiceProviderOptions { ValidateOnBuild = true });
        var producer = provider.GetRequiredService<IGrpcProducer>();
        var consumer = provider.GetRequiredService<IGrpcPushConsumer>();
        await producer.StartAsync(cancellationToken);
        await consumer.StartAsync(cancellationToken);
        try
        {
            await producer.SendAsync(new Message(
                scope.Topic,
                Encoding.UTF8.GetBytes(expected))
            {
                Tag = "grpc-dlq",
                MessageGroup = $"grpc-dlq-{Guid.NewGuid():N}"
            }, cancellationToken);
            await handled.Task.WaitAsync(TimeSpan.FromSeconds(10), cancellationToken);
            await AssertDeadLetterMessageAsync(fixture, consumerGroup, cancellationToken);
            await consumer.StopAsync(CancellationToken.None);
            AssertOneRetryBeforeDeadLetter(observation, RetryOwnership.Local);
        }
        finally
        {
            await consumer.StopAsync(CancellationToken.None);
            await producer.StopAsync(CancellationToken.None);
        }
    }

    [Fact]
    [Trait("Category", "Integration")]
    public async Task GrpcPushConsumer_NonFifoFailureExhaustsRetries_MovesToDeadLetterQueue()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var fixture = await registry.GetFixtureAsync(cancellationToken);
        var scope = await fixture.CreateTestScopeAsync(RocketMQTestTopicType.Normal, cancellationToken);
        var consumerGroup = await scope.CreateConsumerGroupAsync(
            "grpc-push-non-fifo-dlq-consumer",
            retryMaxTimes: 1,
            cancellationToken: cancellationToken);
        var handled = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var expected = $"grpc-non-fifo-dlq-{Guid.NewGuid():N}";
        var observation = new DeadLetterObservation(expected, handled);
        var services = new ServiceCollection();
        services.AddSingleton(observation);
        services
            .AddRocketMQGrpc(options => options.Endpoint = fixture.GrpcEndpoint)
            .AddGrpcProducer()
            .AddGrpcPushConsumer<DeadLetterMessageHandler>(ServiceLifetime.Singleton, options =>
            {
                options.GroupName = consumerGroup;
                options.MaxDeliveryAttempts = 2;
                options.RetryDelay = TimeSpan.FromMilliseconds(100);
                options.LongPollingTimeout = TimeSpan.FromSeconds(1);
                options.Subscribe(scope.Topic, new FilterExpression("grpc-non-fifo-dlq"));
            });

        await using var provider = services.BuildServiceProvider(new ServiceProviderOptions { ValidateOnBuild = true });
        var producer = provider.GetRequiredService<IGrpcProducer>();
        var consumer = provider.GetRequiredService<IGrpcPushConsumer>();
        await producer.StartAsync(cancellationToken);
        await consumer.StartAsync(cancellationToken);
        try
        {
            await producer.SendAsync(new Message(
                scope.Topic,
                Encoding.UTF8.GetBytes(expected))
            {
                Tag = "grpc-non-fifo-dlq"
            }, cancellationToken);
            await handled.Task.WaitAsync(TimeSpan.FromSeconds(10), cancellationToken);
            await AssertDeadLetterMessageAsync(fixture, consumerGroup, cancellationToken);
            await consumer.StopAsync(CancellationToken.None);
            AssertOneRetryBeforeDeadLetter(observation, RetryOwnership.Service);
        }
        finally
        {
            await consumer.StopAsync(CancellationToken.None);
            await producer.StopAsync(CancellationToken.None);
        }
    }

    [Fact]
    [Trait("Category", "Integration")]
    public async Task GrpcLitePushConsumer_NonFifoFailureExhaustsRetries_MovesToDeadLetterQueue()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var fixture = await registry.GetFixtureAsync(cancellationToken);
        var scope = await fixture.CreateTestScopeAsync(RocketMQTestTopicType.Lite, cancellationToken);
        var consumerGroup = await scope.CreateLiteConsumerGroupAsync(
            "grpc-lite-push-dlq-consumer",
            retryMaxTimes: 1,
            cancellationToken: cancellationToken);
        var liteTopic = $"grpc-lite-dlq-{Guid.NewGuid():N}";
        var handled = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var expected = $"grpc-lite-dlq-{Guid.NewGuid():N}";
        var observation = new DeadLetterObservation(expected, handled);
        var services = new ServiceCollection();
        services.AddSingleton(observation);
        services
            .AddRocketMQGrpc(options => options.Endpoint = fixture.GrpcEndpoint)
            .AddGrpcProducer(options => options.Topics.Add(scope.Topic))
            .AddGrpcLitePushConsumer<DeadLetterMessageHandler>(ServiceLifetime.Singleton, options =>
            {
                options.GroupName = consumerGroup;
                options.BindTopic = scope.Topic;
                options.LiteTopics.Add(liteTopic);
                options.MaxConcurrency = 1;
                options.BatchSize = 1;
                options.MaxDeliveryAttempts = 2;
                options.RetryDelay = TimeSpan.FromMilliseconds(100);
                options.LongPollingTimeout = TimeSpan.FromSeconds(1);
            });

        await using var provider = services.BuildServiceProvider(new ServiceProviderOptions { ValidateOnBuild = true });
        var producer = provider.GetRequiredService<IGrpcProducer>();
        var consumer = provider.GetRequiredService<IGrpcLitePushConsumer>();
        await producer.StartAsync(cancellationToken);
        await consumer.StartAsync(cancellationToken);
        try
        {
            await producer.SendAsync(new Message(
                scope.Topic,
                Encoding.UTF8.GetBytes(expected))
            {
                LiteTopic = liteTopic
            }, cancellationToken);
            await handled.Task.WaitAsync(TimeSpan.FromSeconds(10), cancellationToken);
            await AssertDeadLetterMessageAsync(fixture, consumerGroup, cancellationToken);
            await consumer.StopAsync(CancellationToken.None);
            AssertOneRetryBeforeDeadLetter(observation, RetryOwnership.Service);
        }
        finally
        {
            await consumer.StopAsync(CancellationToken.None);
            await producer.StopAsync(CancellationToken.None);
        }
    }

    [Fact]
    [Trait("Category", "Integration")]
    public async Task GrpcLitePushConsumer_FifoFailureExhaustsRetries_MovesToDeadLetterQueue()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var fixture = await registry.GetFixtureAsync(cancellationToken);
        var scope = await fixture.CreateTestScopeAsync(RocketMQTestTopicType.Lite, cancellationToken);
        var consumerGroup = await scope.CreateOrderedLiteConsumerGroupAsync(
            "grpc-lite-fifo-dlq-consumer",
            retryMaxTimes: 1,
            cancellationToken: cancellationToken);
        var liteTopic = $"grpc-lite-fifo-dlq-{Guid.NewGuid():N}";
        var handled = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var expected = $"grpc-lite-fifo-dlq-{Guid.NewGuid():N}";
        var observation = new DeadLetterObservation(expected, handled);
        var services = new ServiceCollection();
        services.AddSingleton(observation);
        services
            .AddRocketMQGrpc(options => options.Endpoint = fixture.GrpcEndpoint)
            .AddGrpcProducer(options => options.Topics.Add(scope.Topic))
            .AddGrpcLitePushConsumer<DeadLetterMessageHandler>(ServiceLifetime.Singleton, options =>
            {
                options.GroupName = consumerGroup;
                options.BindTopic = scope.Topic;
                options.LiteTopics.Add(liteTopic);
                options.MaxConcurrency = 1;
                options.BatchSize = 1;
                options.MaxDeliveryAttempts = 2;
                options.RetryDelay = TimeSpan.FromMilliseconds(100);
                options.LongPollingTimeout = TimeSpan.FromSeconds(1);
            });

        await using var provider = services.BuildServiceProvider(new ServiceProviderOptions { ValidateOnBuild = true });
        var producer = provider.GetRequiredService<IGrpcProducer>();
        var consumer = provider.GetRequiredService<IGrpcLitePushConsumer>();
        await producer.StartAsync(cancellationToken);
        await consumer.StartAsync(cancellationToken);
        try
        {
            await producer.SendAsync(new Message(
                scope.Topic,
                Encoding.UTF8.GetBytes(expected))
            {
                LiteTopic = liteTopic
            }, cancellationToken);
            await handled.Task.WaitAsync(TimeSpan.FromSeconds(10), cancellationToken);
            await AssertDeadLetterMessageAsync(fixture, consumerGroup, cancellationToken);
            await consumer.StopAsync(CancellationToken.None);
            AssertOneRetryBeforeDeadLetter(observation, RetryOwnership.Local);
        }
        finally
        {
            await consumer.StopAsync(CancellationToken.None);
            await producer.StopAsync(CancellationToken.None);
        }
    }

    private static async Task AssertDeadLetterMessageAsync(
        RocketMQSingleBrokerContainerFixture fixture,
        string consumerGroup,
        CancellationToken cancellationToken)
    {
        var deadLetterTopic = $"%DLQ%{consumerGroup}";
        var deadline = DateTimeOffset.UtcNow + TimeSpan.FromSeconds(10);
        var status = string.Empty;
        do
        {
            status = await fixture.GetTopicStatusAsync(deadLetterTopic, cancellationToken);
            if (HasMessage(status))
            {
                return;
            }

            await Task.Delay(TimeSpan.FromMilliseconds(100), cancellationToken);
        }
        while (DateTimeOffset.UtcNow < deadline);

        Assert.Fail($"Dead-letter topic did not receive the message. Topic status: {status}");
    }

    private static void AssertOneRetryBeforeDeadLetter(
        DeadLetterObservation observation,
        RetryOwnership retryOwnership)
    {
        var deliveries = observation.Deliveries;
        Assert.True(
            deliveries.Count >= 2,
            $"Expected two failed handler calls, observed {observation.DeliveryCount}: " +
            string.Join(", ", deliveries.Select(static delivery =>
                $"attempt={delivery.DeliveryAttempt},message={delivery.MessageId},queue={delivery.QueueId},handle={delivery.ReceiptHandle}")));
        Assert.Equal(deliveries[0].MessageId, deliveries[1].MessageId);
        Assert.True(
            Stopwatch.GetElapsedTime(deliveries[0].Timestamp, deliveries[1].Timestamp) >=
            TimeSpan.FromMilliseconds(50),
            "The second handler call arrived before the configured retry interval.");

        if (retryOwnership == RetryOwnership.Local)
        {
            Assert.Equal(deliveries[0].ReceiptHandle, deliveries[1].ReceiptHandle);
            Assert.Equal(deliveries[0].DeliveryAttempt + 1, deliveries[1].DeliveryAttempt);
        }
        else
        {
            Assert.NotEqual(deliveries[0].ReceiptHandle, deliveries[1].ReceiptHandle);
            Assert.True(deliveries[1].DeliveryAttempt > deliveries[0].DeliveryAttempt);
        }
    }

    private static bool HasMessage(string status)
    {
        foreach (var line in status.Split('\n'))
        {
            var columns = line.Split((char[]?)null, StringSplitOptions.RemoveEmptyEntries);
            if (columns.Length >= 4 &&
                string.Equals(columns[0], "broker-a", StringComparison.Ordinal) &&
                long.TryParse(columns[3], NumberStyles.Integer, CultureInfo.InvariantCulture, out var maxOffset) &&
                maxOffset > 0)
            {
                return true;
            }
        }

        return false;
    }

    private sealed class DeadLetterObservation(string expectedBody, TaskCompletionSource handled)
    {
        private readonly ConcurrentQueue<ObservedDelivery> _deliveries = [];
        private int _deliveryCount;

        public string ExpectedBody { get; } = expectedBody;

        public TaskCompletionSource Handled { get; } = handled;

        public int DeliveryCount => Volatile.Read(ref _deliveryCount);

        public IReadOnlyList<ObservedDelivery> Deliveries => _deliveries.ToArray();

        public int RecordDelivery(GrpcMessageView message)
        {
            _deliveries.Enqueue(new ObservedDelivery(
                message.DeliveryAttempt,
                message.MessageId,
                message.QueueId,
                message.ReceiptHandle,
                Stopwatch.GetTimestamp()));
            var deliveryCount = Interlocked.Increment(ref _deliveryCount);
            if (deliveryCount == 2)
            {
                Handled.TrySetResult();
            }

            return deliveryCount;
        }
    }

    private sealed record ObservedDelivery(
        int DeliveryAttempt,
        string MessageId,
        int QueueId,
        string ReceiptHandle,
        long Timestamp);

    private sealed class DeadLetterMessageHandler(DeadLetterObservation observation) : IGrpcPushMessageHandler
    {
        public ValueTask<ConsumeResult> HandleAsync(
            GrpcMessageView message,
            CancellationToken cancellationToken)
        {
            if (Encoding.UTF8.GetString(message.Body) == observation.ExpectedBody)
            {
                // A duplicate may still arrive around service-owned DLQ progression or the Proxy's asynchronous
                // acknowledgement after client forwarding. Succeed after the configured retry so this test isolates
                // the one-retry policy from at-least-once settlement behavior.
                var deliveryCount = observation.RecordDelivery(message);
                return ValueTask.FromResult(deliveryCount <= 2 ? ConsumeResult.Failure : ConsumeResult.Success);
            }

            return ValueTask.FromResult(ConsumeResult.Success);
        }
    }

    private enum RetryOwnership
    {
        Local,
        Service
    }
}
