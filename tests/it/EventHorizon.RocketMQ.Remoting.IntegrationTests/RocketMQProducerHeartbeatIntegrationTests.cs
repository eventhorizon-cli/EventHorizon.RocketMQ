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
using System.Text.Json;
using EventHorizon.RocketMQ.IntegrationTestInfrastructure;
using EventHorizon.RocketMQ.Remoting.Consumer;
using EventHorizon.RocketMQ.Remoting.Consumer.Push;
using EventHorizon.RocketMQ.Remoting.Producer;
using EventHorizon.RocketMQ.Remoting.Producer.Transactions;
using EventHorizon.RocketMQ.Remoting.Protocol;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Xunit;

namespace EventHorizon.RocketMQ.Remoting.IntegrationTests;

public sealed class RocketMQProducerHeartbeatIntegrationTests(RocketMQSingleBrokerContainerFixtureRegistry registry)
{
    private const int GetAllProducerInfoRequestCode = 328;
    private static readonly TimeSpan RequestTimeout = TimeSpan.FromSeconds(5);
    private static readonly TimeSpan HeartbeatRenewalTimeout = TimeSpan.FromSeconds(5);
    private static readonly JsonSerializerOptions ProducerInfoJsonOptions = new(JsonSerializerDefaults.Web)
    {
        PropertyNameCaseInsensitive = true
    };

    [Fact]
    [Trait("Category", "Integration")]
    public async Task ProducerOperations_BrokerRegistryHeartbeat_RefreshesAndUnregisters()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var fixture = await registry.GetFixtureAsync(cancellationToken);
        var scope = await fixture.CreateTestScopeAsync(RocketMQTestTopicType.Normal, cancellationToken);
        var delayScope = await fixture.CreateTestScopeAsync(RocketMQTestTopicType.Delay, cancellationToken);
        var transactionScope = await fixture.CreateTestScopeAsync(RocketMQTestTopicType.Transaction, cancellationToken);
        var suffix = Guid.NewGuid().ToString("N");
        var producerGroup = scope.CreateProducerGroupName($"heartbeat-operations-{suffix}");
        var instanceName = $"heartbeat-operations-{suffix}";
        var services = new ServiceCollection();
        services
            .AddRocketMQRemoting(options =>
            {
                options.NamesrvAddr = fixture.NameServerAddress;
                options.InstanceName = instanceName;
                options.HeartbeatBrokerInterval = TimeSpan.FromMilliseconds(250);
            })
            .AddRemotingProducer(options =>
            {
                options.GroupName = producerGroup;
                options.LocalTransactionExecutor = static (_, _, _) =>
                    ValueTask.FromResult(RemotingTransactionResolution.Rollback);
                options.TransactionChecker = static (_, _) =>
                    ValueTask.FromResult(RemotingTransactionResolution.Rollback);
            });

        await using var provider = services.BuildServiceProvider(new ServiceProviderOptions { ValidateOnBuild = true });
        var producer = provider.GetRequiredService<IRemotingProducer>();
        var clientId = GetClientId(provider);
        var stopped = false;
        await producer.StartAsync(cancellationToken);
        try
        {
            var queues = await producer.GetPublishMessageQueuesAsync(scope.Topic, cancellationToken);
            Assert.NotEmpty(queues);

            var sendResult = await producer.SendAsync(
                new Message(scope.Topic, Encoding.UTF8.GetBytes($"heartbeat-send-{suffix}")),
                cancellationToken);
            Assert.Equal(RemotingSendStatus.SendOk, sendResult.Status);

            var batchResult = await producer.SendAsync(
                [
                    new Message(scope.Topic, Encoding.UTF8.GetBytes($"heartbeat-batch-a-{suffix}")),
                    new Message(scope.Topic, Encoding.UTF8.GetBytes($"heartbeat-batch-b-{suffix}"))
                ],
                cancellationToken);
            Assert.Equal(RemotingSendStatus.SendOk, batchResult.Status);

            await producer.SendOnewayAsync(
                new Message(scope.Topic, Encoding.UTF8.GetBytes($"heartbeat-oneway-{suffix}")),
                cancellationToken);

            var transaction = await producer.SendTransactionAsync(
                new Message(transactionScope.Topic, Encoding.UTF8.GetBytes($"heartbeat-transaction-{suffix}")),
                cancellationToken: cancellationToken);
            Assert.Equal(RemotingTransactionResolution.Rollback, transaction.LocalTransactionResolution);
            Assert.Equal(RemotingSendStatus.SendOk, transaction.SendResult.Status);

            var delayed = await producer.SendAsync(
                new Message(delayScope.Topic, Encoding.UTF8.GetBytes($"heartbeat-recall-{suffix}"))
                {
                    DeliveryTimestamp = DateTimeOffset.UtcNow.AddMinutes(5)
                },
                cancellationToken);
            Assert.Equal(RemotingSendStatus.SendOk, delayed.Status);
            var recallHandle = Assert.IsType<string>(delayed.RecallHandle);
            var recalledMessageId = await producer.RecallAsync(delayScope.Topic, recallHandle, cancellationToken);
            Assert.NotEmpty(recalledMessageId);

            var registered = await WaitForRegistrationAsync(
                provider,
                fixture.BrokerAddress,
                producerGroup,
                clientId,
                HeartbeatRenewalTimeout,
                cancellationToken);
            var renewed = await WaitForRegistrationAsync(
                provider,
                fixture.BrokerAddress,
                producerGroup,
                clientId,
                HeartbeatRenewalTimeout,
                registration => registration.LastUpdateTimestamp > registered.LastUpdateTimestamp,
                cancellationToken);
            Assert.True(renewed.LastUpdateTimestamp > registered.LastUpdateTimestamp);

            await producer.StopAsync(CancellationToken.None);
            stopped = true;
            await WaitForMissingRegistrationAsync(
                provider,
                fixture.BrokerAddress,
                producerGroup,
                clientId,
                HeartbeatRenewalTimeout,
                cancellationToken);
        }
        finally
        {
            if (!stopped)
            {
                await producer.StopAsync(CancellationToken.None);
            }
        }
    }

    [Fact]
    [Trait("Category", "Integration")]
    public async Task ProducerRequestReply_BrokerRegistryHeartbeat_RefreshesAndUnregisters()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var fixture = await registry.GetFixtureAsync(cancellationToken);
        var scope = await fixture.CreateTestScopeAsync(RocketMQTestTopicType.Normal, cancellationToken);
        var suffix = Guid.NewGuid().ToString("N");
        var producerGroup = scope.CreateProducerGroupName($"heartbeat-reply-producer-{suffix}");
        var consumerGroup = scope.CreateConsumerGroupName($"heartbeat-reply-consumer-{suffix}");
        var instanceName = $"heartbeat-reply-{suffix}";
        var requestBody = $"heartbeat-request-{suffix}";
        var replyBody = $"heartbeat-reply-{suffix}";
        var tag = $"heartbeat-reply-tag-{suffix}";
        IRemotingProducer? producer = null;
        var services = new ServiceCollection();
        services
            .AddRocketMQRemoting(options =>
            {
                options.NamesrvAddr = fixture.NameServerAddress;
                options.InstanceName = instanceName;
                options.HeartbeatBrokerInterval = TimeSpan.FromMilliseconds(250);
            })
            .AddRemotingProducer(options => options.GroupName = producerGroup)
            .AddRemotingPushConsumerWithTestHandler<RocketMQProducerHeartbeatIntegrationTests>(options =>
            {
                options.GroupName = consumerGroup;
                options.InitialPosition = ConsumeFromPosition.Beginning;
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
        var clientId = GetClientId(provider);
        var stopped = false;
        await producer.StartAsync(cancellationToken);
        await consumer.StartAsync(cancellationToken);
        try
        {
            var reply = await producer.RequestAsync(
                new Message(scope.Topic, Encoding.UTF8.GetBytes(requestBody)) { Tag = tag },
                TimeSpan.FromSeconds(30),
                cancellationToken);
            Assert.Equal(replyBody, Encoding.UTF8.GetString(reply.Body));

            var registered = await WaitForRegistrationAsync(
                provider,
                fixture.BrokerAddress,
                producerGroup,
                clientId,
                HeartbeatRenewalTimeout,
                cancellationToken);
            var renewed = await WaitForRegistrationAsync(
                provider,
                fixture.BrokerAddress,
                producerGroup,
                clientId,
                HeartbeatRenewalTimeout,
                registration => registration.LastUpdateTimestamp > registered.LastUpdateTimestamp,
                cancellationToken);
            Assert.True(renewed.LastUpdateTimestamp > registered.LastUpdateTimestamp);

            await producer.StopAsync(CancellationToken.None);
            stopped = true;
            await WaitForMissingRegistrationAsync(
                provider,
                fixture.BrokerAddress,
                producerGroup,
                clientId,
                HeartbeatRenewalTimeout,
                cancellationToken);
        }
        finally
        {
            await consumer.StopAsync(CancellationToken.None);
            if (!stopped)
            {
                await producer.StopAsync(CancellationToken.None);
            }
        }
    }

    [Fact]
    [Trait("Category", "Integration")]
    public async Task ProducerHeartbeat_AfterExplicitUnregister_ReRegistersOnNextInterval()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var fixture = await registry.GetFixtureAsync(cancellationToken);
        var scope = await fixture.CreateTestScopeAsync(RocketMQTestTopicType.Normal, cancellationToken);
        var suffix = Guid.NewGuid().ToString("N");
        var producerGroup = scope.CreateProducerGroupName($"heartbeat-recovery-{suffix}");
        var instanceName = $"heartbeat-recovery-{suffix}";
        var services = new ServiceCollection();
        services
            .AddRocketMQRemoting(options =>
            {
                options.NamesrvAddr = fixture.NameServerAddress;
                options.InstanceName = instanceName;
                options.HeartbeatBrokerInterval = TimeSpan.FromSeconds(2);
            })
            .AddRemotingProducer(options => options.GroupName = producerGroup);

        await using var provider = services.BuildServiceProvider(new ServiceProviderOptions { ValidateOnBuild = true });
        var producer = provider.GetRequiredService<IRemotingProducer>();
        var clientId = GetClientId(provider);
        var stopped = false;
        await producer.StartAsync(cancellationToken);
        try
        {
            var queues = await producer.GetPublishMessageQueuesAsync(scope.Topic, cancellationToken);
            Assert.NotEmpty(queues);
            var sendResult = await producer.SendAsync(
                new Message(scope.Topic, Encoding.UTF8.GetBytes($"heartbeat-recovery-{suffix}")),
                cancellationToken);
            Assert.Equal(RemotingSendStatus.SendOk, sendResult.Status);

            var registered = await WaitForRegistrationAsync(
                provider,
                fixture.BrokerAddress,
                producerGroup,
                clientId,
                HeartbeatRenewalTimeout,
                cancellationToken);
            var heartbeatRenewed = await WaitForRegistrationAsync(
                provider,
                fixture.BrokerAddress,
                producerGroup,
                clientId,
                HeartbeatRenewalTimeout,
                registration => registration.LastUpdateTimestamp > registered.LastUpdateTimestamp,
                cancellationToken);

            await UnregisterProducerAsync(
                provider,
                fixture.BrokerAddress,
                producerGroup,
                clientId,
                queues[0].BrokerName,
                cancellationToken);
            await WaitForMissingRegistrationAsync(
                provider,
                fixture.BrokerAddress,
                producerGroup,
                clientId,
                TimeSpan.FromSeconds(1),
                cancellationToken);

            var reRegistered = await WaitForRegistrationAsync(
                provider,
                fixture.BrokerAddress,
                producerGroup,
                clientId,
                TimeSpan.FromSeconds(5),
                cancellationToken);
            Assert.True(reRegistered.LastUpdateTimestamp > heartbeatRenewed.LastUpdateTimestamp);

            await producer.StopAsync(CancellationToken.None);
            stopped = true;
            await WaitForMissingRegistrationAsync(
                provider,
                fixture.BrokerAddress,
                producerGroup,
                clientId,
                HeartbeatRenewalTimeout,
                cancellationToken);
        }
        finally
        {
            if (!stopped)
            {
                await producer.StopAsync(CancellationToken.None);
            }
        }
    }

    private static string GetClientId(IServiceProvider provider)
    {
        var producerRole = provider
            .GetServices<RemotingRocketMQRoleRegistration>()
            .Single(registration => registration.RoleKey.Role == RemotingRocketMQRole.Producer)
            .RoleKey;
        return RemotingRocketMQRegistration.GetClientOptions(provider, producerRole).Value.BuildRemotingClientId();
    }

    private static async Task UnregisterProducerAsync(
        IServiceProvider provider,
        string brokerAddress,
        string producerGroup,
        string clientId,
        string brokerName,
        CancellationToken cancellationToken)
    {
        var client = provider
            .GetRequiredKeyedService<RemotingClientRegistry>(Options.DefaultName)
            .SharedClient;
        var response = await client.InvokeAsync(
            EndpointParser.Parse(brokerAddress),
            new RemotingCommand(RequestCode.UnregisterClient, new UnregisterClientRequestHeader
            {
                ClientID = clientId,
                ProducerGroup = producerGroup,
                Bname = brokerName
            }),
            RequestTimeout,
            cancellationToken).ConfigureAwait(false);
        Assert.Equal(ResponseCodes.ResSuccess, response.Code);
    }

    private static async Task<ProducerRegistration> WaitForRegistrationAsync(
        IServiceProvider provider,
        string brokerAddress,
        string producerGroup,
        string clientId,
        TimeSpan timeout,
        CancellationToken cancellationToken) =>
        await WaitForRegistrationAsync(
            provider,
            brokerAddress,
            producerGroup,
            clientId,
            timeout,
            static _ => true,
            cancellationToken).ConfigureAwait(false);

    private static async Task<ProducerRegistration> WaitForRegistrationAsync(
        IServiceProvider provider,
        string brokerAddress,
        string producerGroup,
        string clientId,
        TimeSpan timeout,
        Func<ProducerRegistration, bool> predicate,
        CancellationToken cancellationToken)
    {
        var stopwatch = Stopwatch.StartNew();
        while (stopwatch.Elapsed < timeout)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var registration = await GetProducerRegistrationAsync(
                provider,
                brokerAddress,
                producerGroup,
                clientId,
                cancellationToken).ConfigureAwait(false);
            if (registration is not null && predicate(registration))
            {
                return registration;
            }

            await Task.Delay(TimeSpan.FromMilliseconds(50), cancellationToken).ConfigureAwait(false);
        }

        throw new TimeoutException(
            $"Producer '{clientId}' in group '{producerGroup}' did not satisfy the Broker registration predicate " +
            $"within {timeout}.");
    }

    private static async Task WaitForMissingRegistrationAsync(
        IServiceProvider provider,
        string brokerAddress,
        string producerGroup,
        string clientId,
        TimeSpan timeout,
        CancellationToken cancellationToken)
    {
        var stopwatch = Stopwatch.StartNew();
        while (stopwatch.Elapsed < timeout)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var registration = await GetProducerRegistrationAsync(
                provider,
                brokerAddress,
                producerGroup,
                clientId,
                cancellationToken).ConfigureAwait(false);
            if (registration is null)
            {
                return;
            }

            await Task.Delay(TimeSpan.FromMilliseconds(50), cancellationToken).ConfigureAwait(false);
        }

        throw new TimeoutException(
            $"Producer '{clientId}' in group '{producerGroup}' remained registered in group '{producerGroup}' " +
            $"after {timeout}.");
    }

    private static async Task<ProducerRegistration?> GetProducerRegistrationAsync(
        IServiceProvider provider,
        string brokerAddress,
        string producerGroup,
        string clientId,
        CancellationToken cancellationToken)
    {
        var client = provider
            .GetRequiredKeyedService<RemotingClientRegistry>(Options.DefaultName)
            .SharedClient;
        var response = await client.InvokeAsync(
            EndpointParser.Parse(brokerAddress),
            // Apache RocketMQ rocketmq-all-5.5.1 uses GET_ALL_PRODUCER_INFO (328) and ProducerTableInfo.data.
            new RemotingCommand(GetAllProducerInfoRequestCode),
            RequestTimeout,
            cancellationToken).ConfigureAwait(false);
        Assert.Equal(ResponseCodes.ResSuccess, response.Code);
        if (response.Body is not { Length: > 0 } body)
        {
            return null;
        }

        var table = JsonSerializer.Deserialize<ProducerTableInfoDocument>(body, ProducerInfoJsonOptions);
        if (table?.Data is null || !table.Data.TryGetValue(producerGroup, out var producers))
        {
            return null;
        }

        var producer = producers.FirstOrDefault(item =>
            string.Equals(item.ClientId, clientId, StringComparison.Ordinal));
        return producer?.ClientId is { } matchedClientId
            ? new ProducerRegistration(matchedClientId, producer.LastUpdateTimestamp)
            : null;
    }

    private sealed class ProducerTableInfoDocument
    {
        public Dictionary<string, List<ProducerInfoDocument>>? Data { get; init; }
    }

    private sealed class ProducerInfoDocument
    {
        public string? ClientId { get; init; }

        public long LastUpdateTimestamp { get; init; }
    }

    private sealed record ProducerRegistration(string ClientId, long LastUpdateTimestamp);
}
