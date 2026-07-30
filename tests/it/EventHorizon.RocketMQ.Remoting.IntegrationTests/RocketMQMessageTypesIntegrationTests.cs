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
using EventHorizon.RocketMQ.IntegrationTestInfrastructure;
using EventHorizon.RocketMQ.Remoting.Consumer;
using EventHorizon.RocketMQ.Remoting.Consumer.Push;
using EventHorizon.RocketMQ.Remoting.Producer;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace EventHorizon.RocketMQ.Remoting.IntegrationTests;

public sealed class RocketMQMessageTypesIntegrationTests(RocketMQContainerFixtureRegistry registry)
{
    [Fact]
    [Trait("Category", "Integration")]
    public async Task KeyedClientRegistrationCombinesRemotingDelayProducerAndConsumer()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var fixture = await registry.GetFixtureAsync(cancellationToken);
        var scope = await fixture.CreateTestScopeAsync(RocketMQTestTopicType.Delay, cancellationToken);
        var consumerGroup = scope.CreateConsumerGroupName("remoting-delay-consumer");
        var tag = $"delay-{Guid.NewGuid():N}";
        var body = $"remoting-delay-{Guid.NewGuid():N}";
        var received = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var services = new ServiceCollection();
        services
            .AddRocketMQRemoting("remoting-delay", options =>
            {
                options.NamesrvAddr = fixture.NameServerAddress;
            })
            .AddRemotingProducer(options => options.GroupName = scope.CreateProducerGroupName("remoting-delay-producer"))
            .AddTestRemotingPushConsumer<RocketMQMessageTypesIntegrationTests>(options =>
            {
                options.GroupName = consumerGroup;
                options.LongPollingTimeout = TimeSpan.FromSeconds(1);
                options.Subscribe(scope.Topic, new FilterExpression(tag));
            }, (messages, _, _) =>
                {
                    var message = Assert.Single(messages);
                    if (Encoding.UTF8.GetString(message.Body) == body)
                    {
                        received.TrySetResult();
                    }

                    return ValueTask.FromResult(ConsumeResult.Success);
                });

        await using var provider = services.BuildServiceProvider(new ServiceProviderOptions { ValidateOnBuild = true });
        var producer = provider.GetRequiredKeyedService<IRemotingProducer>("remoting-delay");
        var consumer = provider.GetRequiredKeyedService<IRemotingPushConsumer>("remoting-delay");
        await producer.StartAsync(cancellationToken);
        await consumer.StartAsync(cancellationToken);
        try
        {
            var stopwatch = Stopwatch.StartNew();
            await producer.SendAsync(new Message(
                scope.Topic,
                Encoding.UTF8.GetBytes(body))
            {
                Tag = tag,
                DeliveryTimestamp = DateTimeOffset.UtcNow.AddSeconds(3)
            }, cancellationToken);

            await received.Task.WaitAsync(TimeSpan.FromSeconds(30), cancellationToken);
            Assert.True(stopwatch.Elapsed >= TimeSpan.FromSeconds(2),
                $"Delayed message arrived after only {stopwatch.Elapsed}.");
        }
        finally
        {
            await consumer.StopAsync(CancellationToken.None);
            await producer.StopAsync(CancellationToken.None);
        }
    }
}
