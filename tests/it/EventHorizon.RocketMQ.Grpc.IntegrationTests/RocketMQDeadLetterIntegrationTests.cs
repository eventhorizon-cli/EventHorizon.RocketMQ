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
        var consumerGroup = await scope.CreateConsumerGroupAsync(
            "grpc-push-dlq-consumer",
            retryMaxTimes: 1,
            cancellationToken);
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
            AssertSingleRetry(observation, requireAttemptIncrement: true);
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
            cancellationToken);
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
            AssertSingleRetry(observation, requireAttemptIncrement: true);
        }
        finally
        {
            await consumer.StopAsync(CancellationToken.None);
            await producer.StopAsync(CancellationToken.None);
        }
    }

    [Fact]
    [Trait("Category", "Integration")]
    public async Task GrpcLitePushConsumer_FailureExhaustsRetries_MovesToDeadLetterQueue()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var fixture = await registry.GetFixtureAsync(cancellationToken);
        var scope = await fixture.CreateTestScopeAsync(RocketMQTestTopicType.Lite, cancellationToken);
        // Released Proxy 5.5.0 yields one Lite redelivery with this setting. The assertions below pin the resulting
        // two handler calls by requiring both the configured retry interval and a new receipt handle.
        var consumerGroup = await scope.CreateLiteConsumerGroupAsync(
            "grpc-lite-push-dlq-consumer",
            retryMaxTimes: 0,
            cancellationToken);
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
            AssertSingleRetry(observation, requireAttemptIncrement: false);
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

    private static void AssertSingleRetry(
        DeadLetterObservation observation,
        bool requireAttemptIncrement)
    {
        Assert.Equal(2, observation.DeliveryCount);
        var deliveries = observation.Deliveries;
        Assert.Equal(2, deliveries.Count);
        Assert.True(
            Stopwatch.GetElapsedTime(deliveries[0].Timestamp, deliveries[1].Timestamp) >=
            TimeSpan.FromMilliseconds(500),
            "The second handler call arrived before the configured retry interval.");
        if (requireAttemptIncrement)
        {
            Assert.True(
                deliveries[1].DeliveryAttempt > deliveries[0].DeliveryAttempt,
                $"Expected one redelivery with an increased attempt, but observed " +
                $"[{deliveries[0].DeliveryAttempt}, {deliveries[1].DeliveryAttempt}].");
        }
        else
        {
            Assert.NotEqual(deliveries[0].ReceiptHandle, deliveries[1].ReceiptHandle);
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

        public void RecordDelivery(GrpcMessageView message)
        {
            _deliveries.Enqueue(new ObservedDelivery(
                message.DeliveryAttempt,
                message.ReceiptHandle,
                Stopwatch.GetTimestamp()));
            Interlocked.Increment(ref _deliveryCount);
            Handled.TrySetResult();
        }
    }

    private sealed record ObservedDelivery(
        int DeliveryAttempt,
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
                observation.RecordDelivery(message);
                return ValueTask.FromResult(ConsumeResult.Failure);
            }

            return ValueTask.FromResult(ConsumeResult.Success);
        }
    }
}
