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

using System.Globalization;
using System.Text;
using EventHorizon.RocketMQ.Grpc.Consumer;
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
    public async Task GrpcPushConsumer_DeadLetterQueue_ForwardsMessage()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var fixture = await registry.GetFixtureAsync(cancellationToken);
        var scope = await fixture.CreateTestScopeAsync(RocketMQTestTopicType.Normal, cancellationToken);
        var consumerGroup = scope.CreateConsumerGroupName("grpc-push-dlq-consumer");
        var handled = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var expected = $"grpc-dlq-{Guid.NewGuid():N}";
        var services = new ServiceCollection();
        services.AddSingleton(new DeadLetterObservation(expected, handled));
        services
            .AddRocketMQGrpc(options => options.Endpoint = fixture.GrpcEndpoint)
            .AddGrpcProducer()
            .AddGrpcPushConsumer<DeadLetterMessageHandler>(ServiceLifetime.Singleton, options =>
            {
                options.GroupName = consumerGroup;
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
                Tag = "grpc-dlq"
            }, cancellationToken);
            await handled.Task.WaitAsync(TimeSpan.FromSeconds(20), cancellationToken);

            var deadLetterTopic = $"%DLQ%{consumerGroup}";
            var status = string.Empty;
            for (var attempt = 0; attempt < 20; attempt++)
            {
                status = await fixture.GetTopicStatusAsync(deadLetterTopic, cancellationToken);
                if (HasMessage(status))
                {
                    return;
                }

                await Task.Delay(TimeSpan.FromMilliseconds(500), cancellationToken);
            }

            Assert.Fail($"Dead-letter topic did not receive the message. Topic status: {status}");
        }
        finally
        {
            await consumer.StopAsync(CancellationToken.None);
            await producer.StopAsync(CancellationToken.None);
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

    private sealed record DeadLetterObservation(
        string ExpectedBody,
        TaskCompletionSource Handled);

    private sealed class DeadLetterMessageHandler(DeadLetterObservation observation) : IGrpcPushMessageHandler
    {
        public ValueTask<ConsumeResult> HandleAsync(
            GrpcMessageView message,
            CancellationToken cancellationToken)
        {
            if (Encoding.UTF8.GetString(message.Body) == observation.ExpectedBody)
            {
                observation.Handled.TrySetResult();
                return ValueTask.FromResult(ConsumeResult.DeadLetter);
            }

            return ValueTask.FromResult(ConsumeResult.Success);
        }
    }
}
