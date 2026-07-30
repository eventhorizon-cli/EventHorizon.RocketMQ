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
using EventHorizon.RocketMQ.Remoting.Producer.Transactions;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace EventHorizon.RocketMQ.Remoting.IntegrationTests;

public sealed class RocketMQTransactionRecallIntegrationTests(RocketMQContainerFixtureRegistry registry)
{
    [Fact]
    [Trait("Category", "Integration")]
    public async Task CommittedTransactionBecomesVisibleToPushConsumer()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var fixture = await registry.GetFixtureAsync(cancellationToken);
        var scope = await fixture.CreateTestScopeAsync(RocketMQTestTopicType.Transaction, cancellationToken);
        var consumerGroup = scope.CreateConsumerGroupName("remoting-transaction-consumer");
        var suffix = Guid.NewGuid().ToString("N");
        var tag = $"remoting-transaction-{suffix}";
        var body = $"transaction-{suffix}";
        var received = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
        var services = new ServiceCollection();
        services
            .AddRocketMQRemoting(options =>
            {
                options.NamesrvAddr = fixture.NameServerAddress;
                options.InstanceName = $"remoting-transaction-{suffix}";
            })
            .AddRemotingProducer(options =>
            {
                options.GroupName = scope.CreateProducerGroupName("remoting-transaction-producer");
                options.LocalTransactionExecutor = static (_, _, _) =>
                    ValueTask.FromResult(RemotingTransactionResolution.Commit);
                options.TransactionChecker = static (_, _) =>
                    ValueTask.FromResult(RemotingTransactionResolution.Unknown);
            })
            .AddTestRemotingPushConsumer<RocketMQTransactionRecallIntegrationTests>(options =>
            {
                options.GroupName = consumerGroup;
                options.LongPollingTimeout = TimeSpan.FromSeconds(1);
                options.Subscribe(scope.Topic, new FilterExpression(tag));
            }, (messages, _, _) =>
                {
                    var message = Assert.Single(messages);
                    if (string.Equals(Encoding.UTF8.GetString(message.Body), body, StringComparison.Ordinal))
                    {
                        received.TrySetResult(message.MessageId);
                    }

                    return ValueTask.FromResult(ConsumeResult.Success);
                });

        await using var provider = services.BuildServiceProvider(new ServiceProviderOptions { ValidateOnBuild = true });
        var producer = provider.GetRequiredService<IRemotingProducer>();
        var consumer = provider.GetRequiredService<IRemotingPushConsumer>();
        await producer.StartAsync(cancellationToken);
        await consumer.StartAsync(cancellationToken);
        try
        {
            var transaction = await producer.SendTransactionAsync(
                new Message(scope.Topic, Encoding.UTF8.GetBytes(body)) { Tag = tag },
                cancellationToken: cancellationToken);

            Assert.Equal(RemotingTransactionResolution.Commit, transaction.LocalTransactionResolution);
            Assert.Equal(RemotingSendStatus.SendOk, transaction.SendResult.Status);
            await received.Task.WaitAsync(TimeSpan.FromSeconds(30), cancellationToken);
        }
        finally
        {
            await consumer.StopAsync(CancellationToken.None);
            await producer.StopAsync(CancellationToken.None);
        }
    }

    [Fact]
    [Trait("Category", "Integration")]
    public async Task RolledBackTransactionRemainsInvisibleToPushConsumer()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var fixture = await registry.GetFixtureAsync(cancellationToken);
        var scope = await fixture.CreateTestScopeAsync(RocketMQTestTopicType.Transaction, cancellationToken);
        var consumerGroup = scope.CreateConsumerGroupName("remoting-transaction-rollback-consumer");
        var suffix = Guid.NewGuid().ToString("N");
        var tag = $"remoting-transaction-rollback-{suffix}";
        var body = $"transaction-rollback-{suffix}";
        var delivered = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
        var services = new ServiceCollection();
        services
            .AddRocketMQRemoting(options =>
            {
                options.NamesrvAddr = fixture.NameServerAddress;
                options.InstanceName = $"remoting-transaction-rollback-{suffix}";
            })
            .AddRemotingProducer(options =>
            {
                options.GroupName = scope.CreateProducerGroupName("remoting-transaction-rollback-producer");
                options.LocalTransactionExecutor = static (_, _, _) =>
                    ValueTask.FromResult(RemotingTransactionResolution.Rollback);
                options.TransactionChecker = static (_, _) =>
                    ValueTask.FromResult(RemotingTransactionResolution.Rollback);
            })
            .AddTestRemotingPushConsumer<RocketMQTransactionRecallIntegrationTests>(options =>
            {
                options.GroupName = consumerGroup;
                options.InitialPosition = ConsumeFromPosition.Beginning;
                options.LongPollingTimeout = TimeSpan.FromSeconds(1);
                options.Subscribe(scope.Topic, new FilterExpression(tag));
            }, (messages, _, _) =>
                {
                    var message = Assert.Single(messages);
                    if (string.Equals(Encoding.UTF8.GetString(message.Body), body, StringComparison.Ordinal))
                    {
                        delivered.TrySetResult(message.MessageId);
                    }

                    return ValueTask.FromResult(ConsumeResult.Success);
                });

        await using var provider = services.BuildServiceProvider(new ServiceProviderOptions { ValidateOnBuild = true });
        var producer = provider.GetRequiredService<IRemotingProducer>();
        var consumer = provider.GetRequiredService<IRemotingPushConsumer>();
        await producer.StartAsync(cancellationToken);
        await consumer.StartAsync(cancellationToken);
        try
        {
            var transaction = await producer.SendTransactionAsync(
                new Message(scope.Topic, Encoding.UTF8.GetBytes(body)) { Tag = tag },
                cancellationToken: cancellationToken);

            Assert.Equal(RemotingTransactionResolution.Rollback, transaction.LocalTransactionResolution);
            Assert.Equal(RemotingSendStatus.SendOk, transaction.SendResult.Status);

            await Task.Delay(TimeSpan.FromSeconds(3), cancellationToken);
            Assert.False(delivered.Task.IsCompleted, "A rolled-back transaction message was delivered to the consumer.");
        }
        finally
        {
            await consumer.StopAsync(CancellationToken.None);
            await producer.StopAsync(CancellationToken.None);
        }
    }

    [Fact]
    [Trait("Category", "Integration")]
    public async Task RecallHandleCancelsDelayedMessageBeforeDelivery()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var fixture = await registry.GetFixtureAsync(cancellationToken);
        var scope = await fixture.CreateTestScopeAsync(RocketMQTestTopicType.Delay, cancellationToken);
        var consumerGroup = scope.CreateConsumerGroupName("remoting-recall-consumer");
        var suffix = Guid.NewGuid().ToString("N");
        var tag = $"remoting-recall-{suffix}";
        var body = $"recall-{suffix}";
        var delivered = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
        var deliveryTimestamp = DateTimeOffset.UtcNow.AddSeconds(10);
        var services = new ServiceCollection();
        services
            .AddRocketMQRemoting(options =>
            {
                options.NamesrvAddr = fixture.NameServerAddress;
                options.InstanceName = $"remoting-recall-{suffix}";
            })
            .AddRemotingProducer(options => options.GroupName = scope.CreateProducerGroupName("remoting-recall-producer"))
            .AddTestRemotingPushConsumer<RocketMQTransactionRecallIntegrationTests>(options =>
            {
                options.GroupName = consumerGroup;
                options.LongPollingTimeout = TimeSpan.FromSeconds(1);
                options.Subscribe(scope.Topic, new FilterExpression(tag));
            }, (messages, _, _) =>
                {
                    var message = Assert.Single(messages);
                    if (string.Equals(Encoding.UTF8.GetString(message.Body), body, StringComparison.Ordinal))
                    {
                        delivered.TrySetResult(message.MessageId);
                    }

                    return ValueTask.FromResult(ConsumeResult.Success);
                });

        await using var provider = services.BuildServiceProvider(new ServiceProviderOptions { ValidateOnBuild = true });
        var producer = provider.GetRequiredService<IRemotingProducer>();
        var consumer = provider.GetRequiredService<IRemotingPushConsumer>();
        await producer.StartAsync(cancellationToken);
        await consumer.StartAsync(cancellationToken);
        try
        {
            var sent = await producer.SendAsync(
                new Message(scope.Topic, Encoding.UTF8.GetBytes(body))
                {
                    Tag = tag,
                    DeliveryTimestamp = deliveryTimestamp
                },
                cancellationToken);

            Assert.Equal(RemotingSendStatus.SendOk, sent.Status);
            var recallHandle = Assert.IsType<string>(sent.RecallHandle);
            var recalledMessageId = await producer.RecallAsync(
                scope.Topic,
                recallHandle,
                cancellationToken);

            Assert.NotEmpty(recalledMessageId);
            Assert.True(
                DateTimeOffset.UtcNow < deliveryTimestamp,
                "The broker accepted the recall only after the delayed message was due for delivery.");

            var delayUntilAfterDelivery = deliveryTimestamp.AddSeconds(3) - DateTimeOffset.UtcNow;
            if (delayUntilAfterDelivery > TimeSpan.Zero)
            {
                await Task.Delay(delayUntilAfterDelivery, cancellationToken);
            }

            Assert.False(delivered.Task.IsCompleted, "A recalled delayed message was delivered to the consumer.");
        }
        finally
        {
            await consumer.StopAsync(CancellationToken.None);
            await producer.StopAsync(CancellationToken.None);
        }
    }
}
