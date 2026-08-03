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

using System.Reflection;
using EventHorizon.RocketMQ.Grpc.Consumer;
using EventHorizon.RocketMQ.Grpc.Consumer.Lite;
using EventHorizon.RocketMQ.Grpc.Consumer.Push;
using EventHorizon.RocketMQ.Grpc.Consumer.Simple;
using EventHorizon.RocketMQ.Grpc.Instrumentation;
using EventHorizon.RocketMQ.Grpc.Producer;
using EventHorizon.RocketMQ.Grpc.Producer.Transactions;
using EventHorizon.RocketMQ.Grpc.Protocol;
using EventHorizon.RocketMQ.Grpc.Protocol.Route;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using Xunit;

namespace EventHorizon.RocketMQ.Grpc.Tests;

public sealed class DependencyInjectionTests
{
    [Fact]
    public void AddRocketMQGrpc_TelemetryWithTheDefaultServiceProvider_RegistersTelemetry()
    {
        var services = new ServiceCollection();
        services.AddRocketMQGrpc(options => options.Endpoint = "127.0.0.1:8081");

        using var provider = services.BuildServiceProvider(new ServiceProviderOptions { ValidateOnBuild = true });

        Assert.NotNull(provider.GetRequiredService<IGrpcRocketMQTelemetry>());
    }

    [Fact]
    public void AddRocketMQGrpc_BlankEndpoint_Rejects()
    {
        var services = new ServiceCollection();
        services.AddRocketMQGrpc(options => options.Endpoint = " ");
        using var provider = services.BuildServiceProvider();

        var exception = Assert.Throws<OptionsValidationException>(
            () => provider.GetRequiredService<IOptions<GrpcClientOptions>>().Value);

        Assert.Contains("gRPC proxy endpoint", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void AddRocketMQGrpc_RegistrationNameContainingNullCharacter_Rejects()
    {
        var services = new ServiceCollection();

        var exception = Assert.Throws<ArgumentException>(() =>
            services.AddRocketMQGrpc("orders\0archive", options => options.Endpoint = "127.0.0.1:8081"));

        Assert.Equal("registrationName", exception.ParamName);
        Assert.Contains("null character", exception.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task AddGrpcProducer_ProtocolSpecificProducerAsSingleton_RegistersSingletonProducer()
    {
        var services = new ServiceCollection();
        services
            .AddRocketMQGrpc(options => options.Endpoint = "127.0.0.1:8081")
            .AddGrpcProducer(options =>
            {
                options.Topics.Add("orders");
                options.TransactionChecker = static (_, _) =>
                    ValueTask.FromResult(TransactionResolution.Unknown);
            });

        await using var provider = services.BuildServiceProvider();

        var producer = provider.GetRequiredService<IGrpcProducer>();

        Assert.IsType<GrpcProducer>(producer);
        Assert.Same(producer, provider.GetRequiredService<IGrpcProducer>());
    }

    [Fact]
    public void AddRocketMQGrpc_IncompleteCredentials_Rejects()
    {
        var services = new ServiceCollection();
        services.AddRocketMQGrpc(options =>
        {
            options.Endpoint = "127.0.0.1:8081";
            options.AccessKey = "access-key";
        });
        using var provider = services.BuildServiceProvider();

        var exception = Assert.Throws<OptionsValidationException>(
            () => provider.GetRequiredService<IOptions<GrpcClientOptions>>().Value);

        Assert.Contains("access key and an access secret", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void AddRocketMQGrpc_SecurityTokenWithoutCredentials_Rejects()
    {
        var services = new ServiceCollection();
        services.AddRocketMQGrpc(options =>
        {
            options.Endpoint = "127.0.0.1:8081";
            options.SecurityToken = "security-token";
        });
        using var provider = services.BuildServiceProvider();

        var exception = Assert.Throws<OptionsValidationException>(
            () => provider.GetRequiredService<IOptions<GrpcClientOptions>>().Value);

        Assert.Contains("security token", exception.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task GrpcSimpleConsumer_BeforeStartup_CanSubscribe()
    {
        var services = new ServiceCollection();
        services
            .AddRocketMQGrpc(options => options.Endpoint = "127.0.0.1:8081")
            .AddGrpcSimpleConsumer(options => options.GroupName = "orders");
        await using var provider = services.BuildServiceProvider(
            new ServiceProviderOptions { ValidateOnBuild = true });
        var consumer = provider.GetRequiredService<IGrpcSimpleConsumer>();

        await consumer.SubscribeAsync(
            "orders",
            new FilterExpression("created"),
            TestContext.Current.CancellationToken);

        Assert.NotNull(consumer);
    }

    [Fact]
    public async Task AddGrpcSimpleConsumer_MultipleConsumersWithOptionsAndLifecycles_RegistersWithIsolatedOptions()
    {
        var services = new ServiceCollection();
        services
            .AddRocketMQGrpc(options => options.Endpoint = "127.0.0.1:8081")
            .AddGrpcSimpleConsumer(options =>
            {
                options.GroupName = "orders-consumer";
                options.AwaitDuration = TimeSpan.FromSeconds(11);
                options.InvisibleDuration = TimeSpan.FromSeconds(21);
                options.Subscribe("orders", new FilterExpression("created"));
            })
            .AddGrpcSimpleConsumer(options =>
            {
                options.GroupName = "audit-consumer";
                options.AwaitDuration = TimeSpan.FromSeconds(12);
                options.InvisibleDuration = TimeSpan.FromSeconds(22);
                options.Subscribe("audit", new FilterExpression("recorded"));
            });
        await using var provider = services.BuildServiceProvider(
            new ServiceProviderOptions { ValidateOnBuild = true });

        var consumers = provider.GetServices<IGrpcSimpleConsumer>().ToArray();
        var hostedServices = provider.GetServices<IHostedService>()
            .OfType<GrpcSimpleConsumerHostedService>()
            .ToArray();

        Assert.Equal(2, consumers.Length);
        Assert.NotSame(consumers[0], consumers[1]);
        Assert.Same(consumers[^1], provider.GetRequiredService<IGrpcSimpleConsumer>());
        Assert.Equal(2, hostedServices.Length);

        var consumerOptions = consumers.Select(GetSimpleConsumerOptions).ToArray();
        var ordersOptions = Assert.Single(consumerOptions, options => options.GroupName == "orders-consumer");
        var auditOptions = Assert.Single(consumerOptions, options => options.GroupName == "audit-consumer");
        Assert.Equal(TimeSpan.FromSeconds(11), ordersOptions.AwaitDuration);
        Assert.Equal(TimeSpan.FromSeconds(21), ordersOptions.InvisibleDuration);
        Assert.Equal(new FilterExpression("created"), ordersOptions.Subscriptions["orders"]);
        Assert.Equal(TimeSpan.FromSeconds(12), auditOptions.AwaitDuration);
        Assert.Equal(TimeSpan.FromSeconds(22), auditOptions.InvisibleDuration);
        Assert.Equal(new FilterExpression("recorded"), auditOptions.Subscriptions["audit"]);

        var hostedConsumers = hostedServices.Select(GetSimpleConsumer).ToArray();
        Assert.NotSame(hostedConsumers[0], hostedConsumers[1]);
        Assert.Contains(consumers[0], hostedConsumers);
        Assert.Contains(consumers[1], hostedConsumers);
    }

    [Fact]
    public async Task AddGrpcPushConsumer_ProtocolSpecificConsumerAndLifecycle_RegistersConsumerAndLifecycle()
    {
        var services = new ServiceCollection();
        services
            .AddRocketMQGrpc(options => options.Endpoint = "127.0.0.1:8081")
            .AddGrpcPushConsumer<TestGrpcPushMessageHandler>(ServiceLifetime.Singleton, options =>
            {
                options.GroupName = "orders";
                options.Subscribe("orders");
            });
        await using var provider = services.BuildServiceProvider();

        var consumer = provider.GetRequiredService<IGrpcPushConsumer>();
        var hostedService = Assert.Single(provider.GetServices<IHostedService>());

        Assert.IsType<GrpcPushConsumer>(consumer);
        Assert.IsType<GrpcPushConsumerHostedService>(hostedService);
        Assert.Same(consumer, provider.GetRequiredService<IGrpcPushConsumer>());
    }

    [Fact]
    public async Task AddGrpcPushConsumer_MultipleConsumersWithOptionsAndLifecycles_RegistersWithIsolatedOptions()
    {
        var services = new ServiceCollection();
        services
            .AddRocketMQGrpc(options => options.Endpoint = "127.0.0.1:8081")
            .AddGrpcPushConsumer<TestGrpcPushMessageHandler>(ServiceLifetime.Singleton, options =>
            {
                options.GroupName = "orders-consumer";
                options.MaxConcurrency = 2;
                options.Subscribe("orders", new FilterExpression("created"));
            })
            .AddGrpcPushConsumer<TestGrpcPushMessageHandler>(ServiceLifetime.Singleton, options =>
            {
                options.GroupName = "audit-consumer";
                options.MaxConcurrency = 3;
                options.Subscribe("audit", new FilterExpression("recorded"));
            });
        await using var provider = services.BuildServiceProvider(
            new ServiceProviderOptions { ValidateOnBuild = true });

        var consumers = provider.GetServices<IGrpcPushConsumer>().ToArray();
        var hostedServices = provider.GetServices<IHostedService>()
            .OfType<GrpcPushConsumerHostedService>()
            .ToArray();

        Assert.Equal(2, consumers.Length);
        Assert.All(consumers, consumer => Assert.IsType<GrpcPushConsumer>(consumer));
        Assert.NotSame(consumers[0], consumers[1]);
        Assert.Equal(2, hostedServices.Length);

        var consumerOptions = consumers.Select(GetPushConsumerOptions).ToArray();
        var ordersOptions = Assert.Single(consumerOptions, options => options.GroupName == "orders-consumer");
        var auditOptions = Assert.Single(consumerOptions, options => options.GroupName == "audit-consumer");
        Assert.Equal(2, ordersOptions.MaxConcurrency);
        Assert.Equal(new FilterExpression("created"), Assert.Single(ordersOptions.Subscriptions).Value);
        Assert.Equal("orders", Assert.Single(ordersOptions.Subscriptions).Key);
        Assert.Equal(3, auditOptions.MaxConcurrency);
        Assert.Equal(new FilterExpression("recorded"), Assert.Single(auditOptions.Subscriptions).Value);
        Assert.Equal("audit", Assert.Single(auditOptions.Subscriptions).Key);

        var handlers = consumers.Select(GetPushMessageHandler).Select(handler => handler.Target).ToArray();
        Assert.All(handlers, handler => Assert.IsType<TestGrpcPushMessageHandler>(handler));
        Assert.NotSame(handlers[0], handlers[1]);

        var hostedConsumers = hostedServices.Select(GetPushConsumer).ToArray();
        Assert.NotSame(hostedConsumers[0], hostedConsumers[1]);
        Assert.Contains(consumers[0], hostedConsumers);
        Assert.Contains(consumers[1], hostedConsumers);
    }

    [Fact]
    public async Task AddGrpcPushConsumer_OptionsFromRegistrationNameMatchingLegacyDerivedOptionsName_Isolates()
    {
        var services = new ServiceCollection();
        services
            .AddRocketMQGrpc("primary", options => options.Endpoint = "127.0.0.1:8081")
            .AddGrpcPushConsumer<TestGrpcPushMessageHandler>(ServiceLifetime.Singleton, options =>
            {
                options.GroupName = "orders-consumer";
                options.Subscribe("orders");
            })
            .AddGrpcPushConsumer<TestGrpcPushMessageHandler>(ServiceLifetime.Singleton, options =>
            {
                options.GroupName = "audit-consumer";
                options.Subscribe("audit");
            });
        services
            .AddRocketMQGrpc(
                "primary:grpc-push-consumer:2",
                options => options.Endpoint = "127.0.0.1:8082")
            .AddGrpcPushConsumer<TestGrpcPushMessageHandler>(ServiceLifetime.Singleton, options =>
            {
                options.GroupName = "payments-consumer";
                options.Subscribe("payments");
            });
        await using var provider = services.BuildServiceProvider(
            new ServiceProviderOptions { ValidateOnBuild = true });

        var primary = provider.GetKeyedServices<IGrpcPushConsumer>("primary")
            .Select(GetPushConsumerOptions)
            .ToArray();
        var colliding = Assert.Single(
            provider.GetKeyedServices<IGrpcPushConsumer>("primary:grpc-push-consumer:2"));

        Assert.Collection(
            primary,
            options =>
            {
                Assert.Equal("orders-consumer", options.GroupName);
                Assert.Equal("orders", Assert.Single(options.Subscriptions).Key);
            },
            options =>
            {
                Assert.Equal("audit-consumer", options.GroupName);
                Assert.Equal("audit", Assert.Single(options.Subscriptions).Key);
            });
        var collidingOptions = GetPushConsumerOptions(colliding);
        Assert.Equal("payments-consumer", collidingOptions.GroupName);
        Assert.Equal("payments", Assert.Single(collidingOptions.Subscriptions).Key);
    }

    [Fact]
    public async Task AddGrpcPushConsumer_MultipleConsumersWithMatchingGroupAndSubscriptions_RegistersMatchingConsumers()
    {
        var services = new ServiceCollection();
        services
            .AddRocketMQGrpc(options => options.Endpoint = "127.0.0.1:8081")
            .AddGrpcPushConsumer<TestGrpcPushMessageHandler>(ServiceLifetime.Singleton, options =>
            {
                options.GroupName = "orders-consumer";
                options.Subscribe("orders", new FilterExpression("created"));
            })
            .AddGrpcPushConsumer<TestGrpcPushMessageHandler>(ServiceLifetime.Singleton, options =>
            {
                options.GroupName = "orders-consumer";
                options.Subscribe("orders", new FilterExpression("created"));
            });
        await using var provider = services.BuildServiceProvider(
            new ServiceProviderOptions { ValidateOnBuild = true });

        var consumers = provider.GetServices<IGrpcPushConsumer>().ToArray();
        Assert.Equal(2, consumers.Length);
        Assert.NotSame(consumers[0], consumers[1]);

        var consumerOptions = consumers.Select(GetPushConsumerOptions).ToArray();
        Assert.All(consumerOptions, options =>
        {
            Assert.Equal("orders-consumer", options.GroupName);
            var subscription = Assert.Single(options.Subscriptions);
            Assert.Equal("orders", subscription.Key);
            Assert.Equal(new FilterExpression("created"), subscription.Value);
        });
        var handlers = consumers.Select(GetPushMessageHandler).Select(handler => handler.Target).ToArray();
        Assert.All(handlers, handler => Assert.IsType<TestGrpcPushMessageHandler>(handler));
        Assert.NotSame(handlers[0], handlers[1]);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task AddGrpcPushConsumer_RuntimeSubscriptionChangesWhenGroupHasLocalPeer_Rejects(bool subscribe)
    {
        var services = new ServiceCollection();
        services
            .AddRocketMQGrpc(options => options.Endpoint = "127.0.0.1:8081")
            .AddGrpcPushConsumer<TestGrpcPushMessageHandler>(ServiceLifetime.Singleton, options =>
            {
                options.GroupName = "orders-consumer";
                options.Subscribe("orders");
            })
            .AddGrpcPushConsumer<TestGrpcPushMessageHandler>(ServiceLifetime.Singleton, options =>
            {
                options.GroupName = "orders-consumer";
                options.Subscribe("orders");
            });
        await using var provider = services.BuildServiceProvider(
            new ServiceProviderOptions { ValidateOnBuild = true });
        var consumer = provider.GetServices<IGrpcPushConsumer>().First();

        var exception = subscribe
            ? await Assert.ThrowsAsync<InvalidOperationException>(() =>
                consumer.SubscribeAsync(
                    "payments",
                    cancellationToken: TestContext.Current.CancellationToken))
            : await Assert.ThrowsAsync<InvalidOperationException>(() =>
                consumer.UnsubscribeAsync("orders", TestContext.Current.CancellationToken));

        Assert.Contains("same consumer group", exception.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("restart", exception.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task AddGrpcPushConsumer_RuntimeSubscriptionChangesForDifferentGroups_AllowsRuntimeSubscriptionChanges()
    {
        var services = new ServiceCollection();
        services
            .AddRocketMQGrpc(options => options.Endpoint = "127.0.0.1:8081")
            .AddGrpcPushConsumer<TestGrpcPushMessageHandler>(ServiceLifetime.Singleton, options =>
            {
                options.GroupName = "orders-consumer";
                options.Subscribe("orders");
            })
            .AddGrpcPushConsumer<TestGrpcPushMessageHandler>(ServiceLifetime.Singleton, options =>
            {
                options.GroupName = "audit-consumer";
                options.Subscribe("audit");
            });
        await using var provider = services.BuildServiceProvider(
            new ServiceProviderOptions { ValidateOnBuild = true });
        var consumer = provider.GetServices<IGrpcPushConsumer>().First();

        await consumer.SubscribeAsync(
            "payments",
            cancellationToken: TestContext.Current.CancellationToken);
        await consumer.UnsubscribeAsync("orders", TestContext.Current.CancellationToken);
    }

    [Fact]
    public async Task AddGrpcPushConsumer_DistinctClientsAndSharedPool_UsesDistinctClientsAndSharedPool()
    {
        var services = new ServiceCollection();
        services
            .AddRocketMQGrpc(options => options.Endpoint = "127.0.0.1:8081")
            .AddGrpcPushConsumer<TestGrpcPushMessageHandler>(ServiceLifetime.Singleton, options =>
            {
                options.GroupName = "orders-consumer";
                options.Subscribe("orders");
            })
            .AddGrpcPushConsumer<TestGrpcPushMessageHandler>(ServiceLifetime.Singleton, options =>
            {
                options.GroupName = "orders-consumer";
                options.Subscribe("orders");
            });
        await using var provider = services.BuildServiceProvider(
            new ServiceProviderOptions { ValidateOnBuild = true });
        var firstKey = new GrpcRocketMQRoleKey(
            Options.DefaultName,
            GrpcRocketMQRole.PushConsumer,
            InstanceIndex: 0);
        var secondKey = new GrpcRocketMQRoleKey(
            Options.DefaultName,
            GrpcRocketMQRole.PushConsumer,
            InstanceIndex: 1);
        var firstClient = provider.GetRequiredKeyedService<IRocketMQGrpcClient>(firstKey);
        var secondClient = provider.GetRequiredKeyedService<IRocketMQGrpcClient>(secondKey);
        var firstMetadata = provider.GetRequiredKeyedService<GrpcMetadataFactory>(firstKey);
        var secondMetadata = provider.GetRequiredKeyedService<GrpcMetadataFactory>(secondKey);
        var sharedPool = provider.GetRequiredKeyedService<GrpcChannelPool>(Options.DefaultName);

        Assert.NotSame(firstClient, secondClient);
        Assert.NotEqual(GetClientId(firstMetadata), GetClientId(secondMetadata));
        Assert.Contains("default-grpc-push-consumer", GetClientId(firstMetadata), StringComparison.Ordinal);
        Assert.Contains("default-grpc-push-consumer-2", GetClientId(secondMetadata), StringComparison.Ordinal);
        Assert.Same(sharedPool, GetChannelPool(firstClient));
        Assert.Same(sharedPool, GetChannelPool(secondClient));
    }

    [Fact]
    public async Task AddGrpcLitePushConsumer_MultipleKeyedConsumersWithOptionsAndLifecycles_RegistersWithIsolatedOptions()
    {
        var services = new ServiceCollection();
        services
            .AddRocketMQGrpc("warehouse", options => options.Endpoint = "127.0.0.1:8081")
            .AddGrpcLitePushConsumer<TestGrpcPushMessageHandler>(ServiceLifetime.Singleton, options =>
            {
                options.GroupName = "orders-consumer";
                options.BindTopic = "orders";
                options.LiteTopics.Add("priority-orders");
                options.MaxConcurrency = 2;
                options.SubscriptionSyncInterval = TimeSpan.FromSeconds(11);
            })
            .AddGrpcLitePushConsumer<TestGrpcPushMessageHandler>(ServiceLifetime.Singleton, options =>
            {
                options.GroupName = "audit-consumer";
                options.BindTopic = "audit";
                options.LiteTopics.Add("security-audit");
                options.MaxConcurrency = 3;
                options.SubscriptionSyncInterval = TimeSpan.FromSeconds(12);
            });
        await using var provider = services.BuildServiceProvider(
            new ServiceProviderOptions { ValidateOnBuild = true });

        var consumers = provider.GetKeyedServices<IGrpcLitePushConsumer>("warehouse").ToArray();
        var hostedServices = provider.GetServices<IHostedService>()
            .OfType<GrpcLitePushConsumerHostedService>()
            .ToArray();

        Assert.Equal(2, consumers.Length);
        Assert.NotSame(consumers[0], consumers[1]);
        Assert.Same(
            consumers[^1],
            provider.GetRequiredKeyedService<IGrpcLitePushConsumer>("warehouse"));
        Assert.Equal(2, hostedServices.Length);

        var consumerOptions = consumers.Select(GetLitePushConsumerOptions).ToArray();
        var ordersOptions = Assert.Single(consumerOptions, options => options.GroupName == "orders-consumer");
        var auditOptions = Assert.Single(consumerOptions, options => options.GroupName == "audit-consumer");
        Assert.Equal("orders", ordersOptions.BindTopic);
        Assert.Equal(["priority-orders"], ordersOptions.LiteTopics);
        Assert.Equal(2, ordersOptions.MaxConcurrency);
        Assert.Equal(TimeSpan.FromSeconds(11), ordersOptions.SubscriptionSyncInterval);
        Assert.Equal("audit", auditOptions.BindTopic);
        Assert.Equal(["security-audit"], auditOptions.LiteTopics);
        Assert.Equal(3, auditOptions.MaxConcurrency);
        Assert.Equal(TimeSpan.FromSeconds(12), auditOptions.SubscriptionSyncInterval);

        var hostedConsumers = hostedServices.Select(GetLitePushConsumer).ToArray();
        Assert.NotSame(hostedConsumers[0], hostedConsumers[1]);
        Assert.Contains(consumers[0], hostedConsumers);
        Assert.Contains(consumers[1], hostedConsumers);
    }

    [Fact]
    public async Task AddGrpcLitePushConsumer_DifferentLiteTopicsForSameGroupAndBindTopic_AllowsDistinctTopics()
    {
        var services = new ServiceCollection();
        services
            .AddRocketMQGrpc(options => options.Endpoint = "127.0.0.1:8081")
            .AddGrpcLitePushConsumer<TestGrpcPushMessageHandler>(ServiceLifetime.Singleton, options =>
            {
                options.GroupName = "orders-consumer";
                options.BindTopic = "orders";
                options.LiteTopics.Add("orders-us");
            })
            .AddGrpcLitePushConsumer<TestGrpcPushMessageHandler>(ServiceLifetime.Singleton, options =>
            {
                options.GroupName = "orders-consumer";
                options.BindTopic = "orders";
                options.LiteTopics.Add("orders-eu");
            });
        await using var provider = services.BuildServiceProvider(
            new ServiceProviderOptions { ValidateOnBuild = true });

        var consumers = provider.GetServices<IGrpcLitePushConsumer>().ToArray();

        Assert.Equal(2, consumers.Length);
        var consumerOptions = consumers.Select(GetLitePushConsumerOptions).ToArray();
        Assert.Contains(consumerOptions, options => options.LiteTopics.SetEquals(["orders-us"]));
        Assert.Contains(consumerOptions, options => options.LiteTopics.SetEquals(["orders-eu"]));
    }

    [Fact]
    public void AddGrpcLitePushConsumer_DifferentBindTopicsForSameGroup_Rejects()
    {
        var services = new ServiceCollection();
        services
            .AddRocketMQGrpc(options => options.Endpoint = "127.0.0.1:8081")
            .AddGrpcLitePushConsumer<TestGrpcPushMessageHandler>(ServiceLifetime.Singleton, options =>
            {
                options.GroupName = "orders-consumer";
                options.BindTopic = "orders";
            })
            .AddGrpcLitePushConsumer<TestGrpcPushMessageHandler>(ServiceLifetime.Singleton, options =>
            {
                options.GroupName = "orders-consumer";
                options.BindTopic = "audit";
            });
        using var provider = services.BuildServiceProvider();

        var exception = Assert.Throws<OptionsValidationException>(
            () => provider.GetServices<IGrpcLitePushConsumer>().ToArray());

        Assert.Contains("same consumer group", exception.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("same bind topic", exception.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task NamedGrpcClientRegistrations_DistinctChannelPools_UseSeparatePools()
    {
        var services = new ServiceCollection();
        services
            .AddRocketMQGrpc("east", options => options.Endpoint = "127.0.0.1:8081")
            .AddGrpcSimpleConsumer(options => options.GroupName = "east-consumer");
        services
            .AddRocketMQGrpc("west", options => options.Endpoint = "127.0.0.1:8081")
            .AddGrpcSimpleConsumer(options => options.GroupName = "west-consumer");
        await using var provider = services.BuildServiceProvider(
            new ServiceProviderOptions { ValidateOnBuild = true });

        var eastPool = provider.GetRequiredKeyedService<GrpcChannelPool>("east");
        var westPool = provider.GetRequiredKeyedService<GrpcChannelPool>("west");

        Assert.NotSame(eastPool, westPool);
    }

    [Theory]
    [InlineData("payments", "created")]
    [InlineData("orders", "updated")]
    public void AddGrpcPushConsumer_MismatchedSubscriptionsForSameGroup_Rejects(
        string secondTopic,
        string secondFilter)
    {
        var services = new ServiceCollection();
        services
            .AddRocketMQGrpc(options => options.Endpoint = "127.0.0.1:8081")
            .AddGrpcPushConsumer<TestGrpcPushMessageHandler>(ServiceLifetime.Singleton, options =>
            {
                options.GroupName = "orders-consumer";
                options.Subscribe("orders", new FilterExpression("created"));
            })
            .AddGrpcPushConsumer<TestGrpcPushMessageHandler>(ServiceLifetime.Singleton, options =>
            {
                options.GroupName = "orders-consumer";
                options.Subscribe(secondTopic, new FilterExpression(secondFilter));
            });
        using var provider = services.BuildServiceProvider();

        var exception = Assert.Throws<OptionsValidationException>(
            () => provider.GetServices<IGrpcPushConsumer>().ToArray());

        Assert.Contains("same consumer group", exception.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("identical topic subscriptions", exception.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void AddGrpcPushConsumer_NonPositiveConsumeTimeout_Rejects()
    {
        var services = new ServiceCollection();
        services
            .AddRocketMQGrpc(options => options.Endpoint = "127.0.0.1:8081")
            .AddGrpcPushConsumer<TestGrpcPushMessageHandler>(ServiceLifetime.Singleton, options =>
            {
                options.GroupName = "orders";
                options.Subscribe("orders");
                options.ConsumeTimeout = TimeSpan.Zero;
            });
        using var provider = services.BuildServiceProvider();

        var exception = Assert.Throws<OptionsValidationException>(
            () => provider.GetRequiredService<IGrpcPushConsumer>());

        Assert.Contains("Consume timeout", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void AddGrpcLitePushConsumer_NonPositiveConsumeTimeout_Rejects()
    {
        var services = new ServiceCollection();
        services
            .AddRocketMQGrpc(options => options.Endpoint = "127.0.0.1:8081")
            .AddGrpcLitePushConsumer<TestGrpcPushMessageHandler>(ServiceLifetime.Singleton, options =>
            {
                options.GroupName = "orders";
                options.BindTopic = "orders";
                options.ConsumeTimeout = TimeSpan.Zero;
            });
        using var provider = services.BuildServiceProvider();

        var exception = Assert.Throws<OptionsValidationException>(
            () => provider.GetRequiredService<IGrpcLitePushConsumer>());

        Assert.Contains("Consume timeout", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void AddRocketMQGrpc_NonPositiveRequestTimeout_Rejects()
    {
        var services = new ServiceCollection();
        services.AddRocketMQGrpc(options =>
        {
            options.Endpoint = "127.0.0.1:8081";
            options.RequestTimeout = TimeSpan.Zero;
        });
        using var provider = services.BuildServiceProvider();

        var exception = Assert.Throws<OptionsValidationException>(
            () => provider.GetRequiredService<IOptions<GrpcClientOptions>>().Value);

        Assert.Contains("Request timeout", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void AddRocketMQGrpc_NonPositiveRouteCacheDuration_Rejects()
    {
        var services = new ServiceCollection();
        services.AddRocketMQGrpc(options =>
        {
            options.Endpoint = "127.0.0.1:8081";
            options.RouteCacheDuration = TimeSpan.Zero;
        });
        using var provider = services.BuildServiceProvider();

        var exception = Assert.Throws<OptionsValidationException>(
            () => provider.GetRequiredService<IOptions<GrpcClientOptions>>().Value);

        Assert.Contains("Route cache duration", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void AddRocketMQGrpc_NonPositiveHeartbeatInterval_Rejects()
    {
        var services = new ServiceCollection();
        services.AddRocketMQGrpc(options =>
        {
            options.Endpoint = "127.0.0.1:8081";
            options.HeartbeatInterval = TimeSpan.Zero;
        });
        using var provider = services.BuildServiceProvider();

        var exception = Assert.Throws<OptionsValidationException>(
            () => provider.GetRequiredService<IOptions<GrpcClientOptions>>().Value);

        Assert.Contains("Heartbeat interval", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void AddRocketMQGrpc_RegisteredTimeProvider_DoesNotReplace()
    {
        var services = new ServiceCollection();
        var timeProvider = TimeProvider.System;
        services.AddSingleton(timeProvider);
        services.AddRocketMQGrpc(options => options.Endpoint = "127.0.0.1:8081");
        using var provider = services.BuildServiceProvider();

        Assert.Same(timeProvider, provider.GetRequiredService<TimeProvider>());
    }

    [Fact]
    public void AddGrpcProducer_RoleForDefaultClientRegistration_RejectsAndDuplicate()
    {
        var services = new ServiceCollection();
        var builder = services
            .AddRocketMQGrpc(options => options.Endpoint = "127.0.0.1:8081")
            .AddGrpcProducer();

        var exception = Assert.Throws<InvalidOperationException>(() => builder.AddGrpcProducer());

        Assert.Contains("already registered", exception.Message, StringComparison.Ordinal);
        Assert.Contains("default client registration", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task GrpcRoles_ClientIdentitiesAndRouteServices_UseDistinctIdentitiesAndRoutes()
    {
        var services = new ServiceCollection();
        services
            .AddRocketMQGrpc(options => options.Endpoint = "127.0.0.1:8081")
            .AddGrpcProducer()
            .AddGrpcSimpleConsumer(options =>
            {
                options.GroupName = "consumer";
                options.Subscribe("orders");
            });

        await using var provider = services.BuildServiceProvider(
            new ServiceProviderOptions { ValidateOnBuild = true });
        var producerKey = new GrpcRocketMQRoleKey(Options.DefaultName, GrpcRocketMQRole.Producer);
        var consumerKey = new GrpcRocketMQRoleKey(Options.DefaultName, GrpcRocketMQRole.SimpleConsumer);
        var producerClient = provider.GetRequiredKeyedService<IRocketMQGrpcClient>(producerKey);
        var consumerClient = provider.GetRequiredKeyedService<IRocketMQGrpcClient>(consumerKey);
        var producerMetadata = provider.GetRequiredKeyedService<GrpcMetadataFactory>(producerKey);
        var consumerMetadata = provider.GetRequiredKeyedService<GrpcMetadataFactory>(consumerKey);
        var producerRoutes = provider.GetRequiredKeyedService<IGrpcRouteService>(producerKey);
        var consumerRoutes = provider.GetRequiredKeyedService<IGrpcRouteService>(consumerKey);

        Assert.NotSame(producerClient, consumerClient);
        var producerClientId = GetClientId(producerMetadata);
        var consumerClientId = GetClientId(consumerMetadata);
        Assert.NotEqual(producerClientId, consumerClientId);
        Assert.Contains("default-grpc-producer", producerClientId, StringComparison.Ordinal);
        Assert.Contains("default-grpc-simple-consumer", consumerClientId, StringComparison.Ordinal);
        Assert.NotSame(producerRoutes, consumerRoutes);
        Assert.Same(producerClient, GetRouteClient(producerRoutes));
        Assert.Same(consumerClient, GetRouteClient(consumerRoutes));
    }

    [Fact]
    public async Task KeyedClientRegistrations_IndependentOptionsAndLifecycle_RegisterMultipleGrpcProducers()
    {
        var services = new ServiceCollection();
        services
            .AddRocketMQGrpc("east", options => options.Endpoint = "127.0.0.1:8081")
            .AddGrpcProducer(options => options.SendMsgTimeout = TimeSpan.FromSeconds(11));
        services
            .AddRocketMQGrpc("west", options => options.Endpoint = "127.0.0.1:8082")
            .AddGrpcProducer(options => options.SendMsgTimeout = TimeSpan.FromSeconds(12));

        await using var provider = services.BuildServiceProvider(
            new ServiceProviderOptions { ValidateOnBuild = true });

        var east = provider.GetRequiredKeyedService<IGrpcProducer>("east");
        var west = provider.GetRequiredKeyedService<IGrpcProducer>("west");
        var eastClient = provider.GetRequiredKeyedService<IRocketMQGrpcClient>(
            new GrpcRocketMQRoleKey("east", GrpcRocketMQRole.Producer));
        var westClient = provider.GetRequiredKeyedService<IRocketMQGrpcClient>(
            new GrpcRocketMQRoleKey("west", GrpcRocketMQRole.Producer));
        var eastMetadata = provider.GetRequiredKeyedService<GrpcMetadataFactory>(
            new GrpcRocketMQRoleKey("east", GrpcRocketMQRole.Producer));
        var westMetadata = provider.GetRequiredKeyedService<GrpcMetadataFactory>(
            new GrpcRocketMQRoleKey("west", GrpcRocketMQRole.Producer));
        var grpcOptions = provider.GetRequiredService<IOptionsMonitor<GrpcProducerOptions>>();
        var hostedServices = provider.GetServices<IHostedService>().ToArray();

        Assert.IsType<GrpcProducer>(east);
        Assert.IsType<GrpcProducer>(west);
        Assert.NotSame(east, west);
        Assert.NotEqual(GetClientId(eastMetadata), GetClientId(westMetadata));
        Assert.Equal(new Uri("http://127.0.0.1:8081"), eastClient.AccessPoint);
        Assert.Equal(new Uri("http://127.0.0.1:8082"), westClient.AccessPoint);
        Assert.Equal(TimeSpan.FromSeconds(11), grpcOptions.Get("east").SendMsgTimeout);
        Assert.Equal(TimeSpan.FromSeconds(12), grpcOptions.Get("west").SendMsgTimeout);
        Assert.Equal(2, hostedServices.Length);

        foreach (var hostedService in hostedServices)
        {
            await hostedService.StartAsync(CancellationToken.None);
        }

        foreach (var hostedService in hostedServices.Reverse())
        {
            await hostedService.StopAsync(CancellationToken.None);
        }
    }

    private static IRocketMQGrpcClient GetRouteClient(IGrpcRouteService routes)
    {
        var concrete = Assert.IsType<GrpcRouteService>(routes);
        var field = typeof(GrpcRouteService).GetField("_client", BindingFlags.Instance | BindingFlags.NonPublic);
        return Assert.IsAssignableFrom<IRocketMQGrpcClient>(field?.GetValue(concrete));
    }

    private static GrpcChannelPool GetChannelPool(IRocketMQGrpcClient client)
    {
        var concrete = Assert.IsType<RocketMQGrpcClient>(client);
        var field = typeof(RocketMQGrpcClient).GetField(
            "_channelPool",
            BindingFlags.Instance | BindingFlags.NonPublic);
        return Assert.IsType<GrpcChannelPool>(field?.GetValue(concrete));
    }

    private static GrpcSimpleConsumerOptions GetSimpleConsumerOptions(IGrpcSimpleConsumer consumer)
    {
        var concrete = Assert.IsType<GrpcSimpleConsumer>(consumer);
        var field = typeof(GrpcSimpleConsumer).GetField("_options", BindingFlags.Instance | BindingFlags.NonPublic);
        return Assert.IsType<GrpcSimpleConsumerOptions>(field?.GetValue(concrete));
    }

    private static IGrpcSimpleConsumer GetSimpleConsumer(GrpcSimpleConsumerHostedService hostedService)
    {
        var field = typeof(GrpcSimpleConsumerHostedService)
            .GetFields(BindingFlags.Instance | BindingFlags.NonPublic)
            .Single(field => field.FieldType == typeof(IGrpcSimpleConsumer));
        return Assert.IsAssignableFrom<IGrpcSimpleConsumer>(field.GetValue(hostedService));
    }

    private static GrpcPushConsumerOptions GetPushConsumerOptions(IGrpcPushConsumer consumer)
    {
        var concrete = Assert.IsType<GrpcPushConsumer>(consumer);
        var field = typeof(GrpcPushConsumer).GetField("_options", BindingFlags.Instance | BindingFlags.NonPublic);
        return Assert.IsType<GrpcPushConsumerOptions>(field?.GetValue(concrete));
    }

    private static IGrpcPushConsumer GetPushConsumer(GrpcPushConsumerHostedService hostedService)
    {
        var field = typeof(GrpcPushConsumerHostedService)
            .GetFields(BindingFlags.Instance | BindingFlags.NonPublic)
            .Single(field => field.FieldType == typeof(IGrpcPushConsumer));
        return Assert.IsAssignableFrom<IGrpcPushConsumer>(field.GetValue(hostedService));
    }

    private static Func<GrpcMessageView, CancellationToken, ValueTask<ConsumeResult>> GetPushMessageHandler(
        IGrpcPushConsumer consumer)
    {
        var concrete = Assert.IsType<GrpcPushConsumer>(consumer);
        var field = typeof(GrpcPushConsumer).GetField(
            "_messageHandler",
            BindingFlags.Instance | BindingFlags.NonPublic);
        return Assert.IsType<Func<GrpcMessageView, CancellationToken, ValueTask<ConsumeResult>>>(
            field?.GetValue(concrete));
    }

    private static GrpcLitePushConsumerOptions GetLitePushConsumerOptions(IGrpcLitePushConsumer consumer)
    {
        var concrete = Assert.IsType<GrpcLitePushConsumer>(consumer);
        var subscriptionsField = typeof(GrpcLitePushConsumer).GetField(
            "_subscriptions",
            BindingFlags.Instance | BindingFlags.NonPublic);
        var subscriptions = Assert.IsType<GrpcLiteSubscriptionManager>(subscriptionsField?.GetValue(concrete));
        var optionsField = typeof(GrpcLiteSubscriptionManager).GetField(
            "_options",
            BindingFlags.Instance | BindingFlags.NonPublic);
        return Assert.IsType<GrpcLitePushConsumerOptions>(optionsField?.GetValue(subscriptions));
    }

    private static IGrpcLitePushConsumer GetLitePushConsumer(GrpcLitePushConsumerHostedService hostedService)
    {
        var field = typeof(GrpcLitePushConsumerHostedService)
            .GetFields(BindingFlags.Instance | BindingFlags.NonPublic)
            .Single(field => field.FieldType == typeof(IGrpcLitePushConsumer));
        return Assert.IsAssignableFrom<IGrpcLitePushConsumer>(field.GetValue(hostedService));
    }

    private static string GetClientId(GrpcMetadataFactory metadata) =>
        metadata.Create().Single(entry =>
            string.Equals(entry.Key, "x-mq-client-id", StringComparison.Ordinal)).Value;

    private sealed class TestGrpcPushMessageHandler : IGrpcPushMessageHandler
    {
        public ValueTask<ConsumeResult> HandleAsync(
            GrpcMessageView message,
            CancellationToken cancellationToken) =>
            ValueTask.FromResult(ConsumeResult.Success);
    }
}
