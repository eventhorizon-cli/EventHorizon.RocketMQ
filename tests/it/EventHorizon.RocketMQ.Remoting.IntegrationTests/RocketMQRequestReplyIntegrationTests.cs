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
using EventHorizon.RocketMQ.IntegrationTestInfrastructure;
using EventHorizon.RocketMQ.Remoting.Consumer;
using EventHorizon.RocketMQ.Remoting.Consumer.Push;
using EventHorizon.RocketMQ.Remoting.Producer;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace EventHorizon.RocketMQ.Remoting.IntegrationTests;

public sealed class RocketMQRequestReplyIntegrationTests(RocketMQContainerFixtureRegistry registry)
{
    [Fact]
    [Trait("Category", "Integration")]
    public async Task ProducerReceivesReplyFromPushConsumer()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var fixture = await registry.GetFixtureAsync(cancellationToken);
        var scope = await fixture.CreateTestScopeAsync(RocketMQTestTopicType.Normal, cancellationToken);
        var consumerGroup = scope.CreateConsumerGroupName("request-reply-consumer");
        var suffix = Guid.NewGuid().ToString("N");
        var requestBody = $"request-{suffix}";
        var replyBody = $"reply-{suffix}";
        var tag = $"request-reply-{suffix}";
        IRemotingProducer? producer = null;
        var services = new ServiceCollection();
        services
            .AddRocketMQRemoting(options =>
            {
                options.NamesrvAddr = fixture.NameServerAddress;
                options.InstanceName = $"request-reply-{suffix}";
            })
            .AddRemotingProducer(options => options.GroupName = scope.CreateProducerGroupName("request-reply-producer"))
            .AddTestRemotingPushConsumer<RocketMQRequestReplyIntegrationTests>(options =>
            {
                options.GroupName = consumerGroup;
                options.LongPollingTimeout = TimeSpan.FromSeconds(1);
                options.Subscribe(scope.Topic, new FilterExpression(tag));
            }, async (messages, _, token) =>
                {
                    var message = Assert.Single(messages);
                    if (!string.Equals(Encoding.UTF8.GetString(message.Body), requestBody, StringComparison.Ordinal))
                    {
                        return ConsumeResult.Success;
                    }

                    var responder = producer ?? throw new InvalidOperationException("The reply producer is unavailable.");
                    var reply = RemotingReply.FromRequestProperties(message.Properties, Encoding.UTF8.GetBytes(replyBody));
                    await responder.SendReplyAsync(reply, token).ConfigureAwait(false);
                    return ConsumeResult.Success;
                });

        await using var provider = services.BuildServiceProvider(new ServiceProviderOptions { ValidateOnBuild = true });
        producer = provider.GetRequiredService<IRemotingProducer>();
        var consumer = provider.GetRequiredService<IRemotingPushConsumer>();
        await producer.StartAsync(cancellationToken);
        await consumer.StartAsync(cancellationToken);
        try
        {
            var reply = await producer.RequestAsync(
                new Message(scope.Topic, Encoding.UTF8.GetBytes(requestBody)) { Tag = tag },
                TimeSpan.FromSeconds(30),
                cancellationToken);

            Assert.Equal(replyBody, Encoding.UTF8.GetString(reply.Body));
            Assert.Equal($"DefaultCluster_REPLY_TOPIC", reply.Topic);
            Assert.NotEmpty(reply.CorrelationId);
        }
        finally
        {
            await consumer.StopAsync(CancellationToken.None);
            await producer.StopAsync(CancellationToken.None);
        }
    }
}
