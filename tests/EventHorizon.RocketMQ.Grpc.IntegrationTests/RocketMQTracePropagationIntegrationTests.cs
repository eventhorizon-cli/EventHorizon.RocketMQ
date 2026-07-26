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

using System.Diagnostics;
using System.Text;
using EventHorizon.RocketMQ.Grpc;
using EventHorizon.RocketMQ.Grpc.Consumer;
using EventHorizon.RocketMQ.Grpc.Consumer.Push;
using EventHorizon.RocketMQ.Grpc.Producer;
using EventHorizon.RocketMQ.IntegrationTestInfrastructure;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace EventHorizon.RocketMQ.Grpc.IntegrationTests;

[Collection(RocketMQCollection.Name)]
public sealed class RocketMQTracePropagationIntegrationTests(RocketMQContainerFixture fixture)
{
    [Fact]
    [Trait("Category", "Integration")]
    public async Task PushConsumerLinksProducerTraceAfterBrokerRoundTrip()
    {
        using var listener = CreateActivityListener();
        var cancellationToken = TestContext.Current.CancellationToken;
        var tag = $"grpc-trace-propagation-{Guid.NewGuid():N}";
        var body = $"grpc-trace-propagation-{Guid.NewGuid():N}";
        var received = new TaskCompletionSource<TraceObservation>(TaskCreationOptions.RunContinuationsAsynchronously);
        var services = new ServiceCollection();
        services
            .AddRocketMQGrpc(options => options.Endpoint = fixture.GrpcEndpoint)
            .AddGrpcProducer()
            .AddGrpcPushConsumer(options =>
            {
                options.GroupName = "grpc-trace-propagation-consumer-it";
                options.LongPollingTimeout = TimeSpan.FromSeconds(3);
                options.Subscribe(RocketMQContainerFixture.TestTopic, new FilterExpression(tag));
                options.MessageHandler = (message, _) =>
                {
                    if (Encoding.UTF8.GetString(message.Body) == body)
                    {
                        received.TrySetResult(new TraceObservation(
                            Activity.Current?.Links.Select(static link => link.Context.TraceId).ToArray() ?? []));
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
            using var producerActivity = new Activity("grpc-trace-propagation-test").Start();
            var producerTraceId = producerActivity.TraceId;
            await producer.SendAsync(
                new Message(RocketMQContainerFixture.TestTopic, Encoding.UTF8.GetBytes(body)) { Tag = tag },
                cancellationToken);

            var observed = await received.Task.WaitAsync(TimeSpan.FromSeconds(30), cancellationToken);
            Assert.Contains(producerTraceId, observed.LinkedTraceIds);
        }
        finally
        {
            await consumer.StopAsync(CancellationToken.None);
            await producer.StopAsync(CancellationToken.None);
        }
    }

    private static ActivityListener CreateActivityListener()
    {
        var listener = new ActivityListener
        {
            ShouldListenTo = static source => source.Name == GrpcRocketMQInstrumentation.ActivitySourceName,
            Sample = static (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllDataAndRecorded
        };
        ActivitySource.AddActivityListener(listener);
        return listener;
    }

    private sealed record TraceObservation(IReadOnlyList<ActivityTraceId> LinkedTraceIds);
}
