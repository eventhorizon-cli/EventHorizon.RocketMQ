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
using EventHorizon.RocketMQ.Grpc.Consumer;
using EventHorizon.RocketMQ.Grpc.Consumer.Simple;
using EventHorizon.RocketMQ.Grpc.Producer;
using EventHorizon.RocketMQ.Grpc.Producer.Transactions;
using EventHorizon.RocketMQ.IntegrationTestInfrastructure;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace EventHorizon.RocketMQ.Grpc.IntegrationTests;

[Collection(RocketMQCollection.Name)]
public sealed class RocketMQTransactionIntegrationTests
{
    private readonly RocketMQContainerFixture _fixture;

    public RocketMQTransactionIntegrationTests(RocketMQContainerFixture fixture)
    {
        _fixture = fixture;
    }

    [Fact]
    [Trait("Category", "Integration")]
    public async Task GrpcTransactionRollbackKeepsMessageInvisible()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var services = new ServiceCollection();
        services
            .AddRocketMQGrpc(options => options.Endpoint = _fixture.GrpcEndpoint)
            .AddGrpcProducer(options =>
            {
                options.Topics.Add(RocketMQContainerFixture.TransactionTopic);
                options.TransactionChecker = static (_, _) =>
                    ValueTask.FromResult(TransactionResolution.Rollback);
            })
            .AddGrpcSimpleConsumer(options =>
            {
                options.GroupName = "grpc-transaction-rollback-consumer-it";
                options.AwaitDuration = TimeSpan.FromSeconds(5);
                options.Subscribe(
                    RocketMQContainerFixture.TransactionTopic,
                    new FilterExpression("transaction-rollback"));
            });

        await using var provider = services.BuildServiceProvider(new ServiceProviderOptions { ValidateOnBuild = true });
        var producer = provider.GetRequiredService<IGrpcProducer>();
        var consumer = provider.GetRequiredService<IGrpcSimpleConsumer>();
        await producer.StartAsync(cancellationToken);
        await consumer.StartAsync(cancellationToken);
        try
        {
            var body = $"transaction-rollback-{Guid.NewGuid():N}";
            var transaction = await producer.SendTransactionAsync(new Message(
                RocketMQContainerFixture.TransactionTopic,
                Encoding.UTF8.GetBytes(body))
            {
                Tag = "transaction-rollback"
            }, cancellationToken);
            await transaction.RollbackAsync(cancellationToken);

            for (var attempt = 0; attempt < 3; attempt++)
            {
                var messages = await consumer.ReceiveAsync(16, cancellationToken: cancellationToken);
                Assert.DoesNotContain(messages, message => Encoding.UTF8.GetString(message.Body) == body);
                foreach (var message in messages)
                {
                    await consumer.AckAsync(message, cancellationToken);
                }
            }
        }
        finally
        {
            await consumer.StopAsync(CancellationToken.None);
            await producer.StopAsync(CancellationToken.None);
        }
    }
}
