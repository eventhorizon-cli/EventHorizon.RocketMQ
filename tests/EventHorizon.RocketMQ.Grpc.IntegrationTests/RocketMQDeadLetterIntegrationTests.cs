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

[Collection(RocketMQCollection.Name)]
public sealed class RocketMQDeadLetterIntegrationTests
{
    private const string ConsumerGroup = "grpc-push-dlq-consumer-it";
    private readonly RocketMQContainerFixture _fixture;

    public RocketMQDeadLetterIntegrationTests(RocketMQContainerFixture fixture)
    {
        _fixture = fixture;
    }

    [Fact]
    [Trait("Category", "Integration")]
    public async Task GrpcPushConsumerForwardsMessageToDeadLetterQueue()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var handled = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var expected = $"grpc-dlq-{Guid.NewGuid():N}";
        var services = new ServiceCollection();
        services
            .AddRocketMQGrpc(options => options.Endpoint = _fixture.GrpcEndpoint)
            .AddGrpcProducer()
            .AddGrpcPushConsumer(options =>
            {
                options.GroupName = ConsumerGroup;
                options.Subscribe(RocketMQContainerFixture.TestTopic, new FilterExpression("grpc-dlq"));
                options.MessageHandler = (message, _) =>
                {
                    if (Encoding.UTF8.GetString(message.Body) == expected)
                    {
                        handled.TrySetResult();
                        return ValueTask.FromResult(ConsumeResult.DeadLetter);
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
            await producer.SendAsync(new Message(
                RocketMQContainerFixture.TestTopic,
                Encoding.UTF8.GetBytes(expected))
            {
                Tag = "grpc-dlq"
            }, cancellationToken);
            await handled.Task.WaitAsync(TimeSpan.FromSeconds(20), cancellationToken);

            var deadLetterTopic = $"%DLQ%{ConsumerGroup}";
            var status = string.Empty;
            for (var attempt = 0; attempt < 20; attempt++)
            {
                status = await _fixture.GetTopicStatusAsync(deadLetterTopic, cancellationToken);
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
}
