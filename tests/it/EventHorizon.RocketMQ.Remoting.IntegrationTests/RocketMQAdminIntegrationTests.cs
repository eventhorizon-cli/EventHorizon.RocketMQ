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
using EventHorizon.RocketMQ.Remoting.Admin;
using EventHorizon.RocketMQ.Remoting.Consumer;
using EventHorizon.RocketMQ.Remoting.Consumer.Pull.Lite;
using EventHorizon.RocketMQ.Remoting.Producer;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace EventHorizon.RocketMQ.Remoting.IntegrationTests;

public sealed class RocketMQAdminIntegrationTests(RocketMQSingleBrokerContainerFixtureRegistry registry)
{
    [Fact]
    [Trait("Category", "Integration")]
    public async Task AdminWorkflow_ReadableQueueIsDiscovered_ReadsOffsetsAndConsumerCommit()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var fixture = await registry.GetFixtureAsync(cancellationToken);
        var scope = await fixture.CreateTestScopeAsync(RocketMQTestTopicType.Normal, cancellationToken);
        var suffix = Guid.NewGuid().ToString("N");
        var group = scope.CreateConsumerGroupName("remoting-admin-consumer");
        var tag = $"remoting-admin-{suffix}";
        var body = $"admin-offset-{suffix}";
        var services = new ServiceCollection();
        services
            .AddRocketMQRemoting(options =>
            {
                options.NamesrvAddr = fixture.NameServerAddress;
            })
            .AddRemotingAdmin()
            .AddRemotingProducer(options => options.GroupName = scope.CreateProducerGroupName("remoting-admin-producer"))
            .AddRemotingLitePullConsumer(options =>
            {
                options.GroupName = group;
                options.EnableAutoCommit = false;
                options.InitialPosition = ConsumeFromPosition.End;
                options.LongPollingTimeout = TimeSpan.FromSeconds(1);
                options.PollTimeout = TimeSpan.FromSeconds(2);
                options.Subscribe(scope.Topic, new FilterExpression(tag));
            });

        await using var provider = services.BuildServiceProvider(
            new ServiceProviderOptions { ValidateOnBuild = true });
        var admin = provider.GetRequiredService<IRemotingAdmin>();
        var producer = provider.GetRequiredService<IRemotingProducer>();
        var consumer = provider.GetRequiredService<IRemotingLitePullConsumer>();
        await producer.StartAsync(cancellationToken);
        await consumer.StartAsync(cancellationToken);
        try
        {
            var adminQueues = await admin.GetMessageQueuesAsync(
                scope.Topic,
                cancellationToken);
            Assert.NotEmpty(adminQueues);

            var pullQueues = await consumer.GetMessageQueuesAsync(
                scope.Topic,
                cancellationToken);

            var sent = await producer.SendAsync(
                new Message(scope.Topic, Encoding.UTF8.GetBytes(body)) { Tag = tag },
                cancellationToken);
            var viewed = await admin.ViewMessageAsync(
                scope.Topic,
                sent.OffsetMessageId,
                cancellationToken);

            Assert.Equal(scope.Topic, viewed.Topic);
            Assert.Equal(body, Encoding.UTF8.GetString(viewed.Body));
            Assert.Equal(tag, viewed.Tag);
            Assert.Equal(sent.OffsetMessageId, viewed.OffsetMessageId);
            Assert.Equal(sent.MessageQueue.QueueId, viewed.QueueId);
            Assert.Equal(sent.QueueOffset, viewed.QueueOffset);
            Assert.Null(viewed.BrokerName);

            var adminQueue = Assert.Single(adminQueues, queue =>
                queue.BrokerName == sent.MessageQueue.BrokerName && queue.QueueId == sent.MessageQueue.QueueId);
            var pullQueue = Assert.Single(pullQueues, queue =>
                queue.BrokerName == sent.MessageQueue.BrokerName && queue.QueueId == sent.MessageQueue.QueueId);
            Assert.Null(await admin.GetConsumerOffsetAsync(group, adminQueue, cancellationToken));

            RemotingMessageView? matching = null;
            for (var attempt = 0; attempt < 10 && matching is null; attempt++)
            {
                var messages = await consumer.PollAsync(cancellationToken: cancellationToken);
                matching = messages.SingleOrDefault(message => Encoding.UTF8.GetString(message.Body) == body);
            }

            var message = Assert.IsType<RemotingMessageView>(matching);
            var minimum = await admin.GetMinOffsetAsync(adminQueue, cancellationToken);
            var earliestMessageStoreTime = await admin.GetEarliestMessageStoreTimeAsync(adminQueue, cancellationToken);
            var maximum = await admin.GetMaxOffsetAsync(adminQueue, cancellationToken: cancellationToken);
            var lowerTimestampOffset = await admin.SearchOffsetAsync(
                adminQueue,
                DateTimeOffset.UtcNow.AddMinutes(-1),
                RemotingOffsetBoundary.Lower,
                cancellationToken);
            var upperTimestampOffset = await admin.SearchOffsetAsync(
                adminQueue,
                DateTimeOffset.UtcNow.AddMinutes(-1),
                RemotingOffsetBoundary.Upper,
                cancellationToken);

            Assert.True(minimum <= sent.QueueOffset);
            Assert.NotNull(earliestMessageStoreTime);
            Assert.True(earliestMessageStoreTime.Value <= DateTimeOffset.UtcNow.AddMinutes(1));
            Assert.True(maximum > sent.QueueOffset);
            Assert.InRange(lowerTimestampOffset, minimum, maximum);
            Assert.InRange(upperTimestampOffset, minimum, maximum);
            await consumer.CommitAsync(pullQueue, cancellationToken);
            Assert.Equal(
                message.QueueOffset + 1,
                await admin.GetConsumerOffsetAsync(group, adminQueue, cancellationToken));
        }
        finally
        {
            await consumer.StopAsync(CancellationToken.None);
            await producer.StopAsync(CancellationToken.None);
        }
    }
}
