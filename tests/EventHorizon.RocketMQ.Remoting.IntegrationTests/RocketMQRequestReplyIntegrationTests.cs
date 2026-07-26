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

[Collection(RocketMQCollection.Name)]
public sealed class RocketMQRequestReplyIntegrationTests
{
    private readonly RocketMQContainerFixture _fixture;

    public RocketMQRequestReplyIntegrationTests(RocketMQContainerFixture fixture)
    {
        _fixture = fixture;
    }

    [Fact]
    [Trait("Category", "Integration")]
    public async Task ProducerReceivesReplyFromPushConsumer()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var suffix = Guid.NewGuid().ToString("N");
        var requestBody = $"request-{suffix}";
        var replyBody = $"reply-{suffix}";
        var tag = $"request-reply-{suffix}";
        IRemotingProducer? producer = null;
        var services = new ServiceCollection();
        services
            .AddRocketMQRemoting(options =>
            {
                options.NamesrvAddr = _fixture.NameServerAddress;
                options.InstanceName = $"request-reply-{suffix}";
            })
            .AddRemotingProducer(options => options.GroupName = $"request-reply-producer-{suffix}")
            .AddRemotingPushConsumer(options =>
            {
                options.GroupName = $"request-reply-consumer-{suffix}";
                options.LongPollingTimeout = TimeSpan.FromSeconds(1);
                options.Subscribe(RocketMQContainerFixture.TestTopic, new FilterExpression(tag));
                options.MessageHandler = async (messages, token) =>
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
                };
            });

        await using var provider = services.BuildServiceProvider(new ServiceProviderOptions { ValidateOnBuild = true });
        producer = provider.GetRequiredService<IRemotingProducer>();
        var consumer = provider.GetRequiredService<IRemotingPushConsumer>();
        await producer.StartAsync(cancellationToken);
        await consumer.StartAsync(cancellationToken);
        try
        {
            var reply = await producer.RequestAsync(
                new Message(RocketMQContainerFixture.TestTopic, Encoding.UTF8.GetBytes(requestBody)) { Tag = tag },
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
