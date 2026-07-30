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
using EventHorizon.RocketMQ.Remoting.Consumer;
using EventHorizon.RocketMQ.Remoting.Consumer.Pull;
using EventHorizon.RocketMQ.Remoting.Consumer.Pull.Lite;
using EventHorizon.RocketMQ.Remoting.Consumer.Push;
using EventHorizon.RocketMQ.Remoting.Consumer.Push.Pop;
using EventHorizon.RocketMQ.Remoting.Instrumentation;
using EventHorizon.RocketMQ.Remoting.Producer;
using EventHorizon.RocketMQ.Remoting.Protocol;
using EventHorizon.RocketMQ.Remoting.Protocol.Route;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using Xunit;

namespace EventHorizon.RocketMQ.Remoting.Tests;

public sealed class DependencyInjectionTests
{
    [Fact]
    public void AddRocketMQRemoting_RegistersTelemetryWithTheDefaultServiceProvider()
    {
        var services = new ServiceCollection();
        services.AddRocketMQRemoting(options => options.NamesrvAddr = "127.0.0.1:9876");

        using var provider = services.BuildServiceProvider(new ServiceProviderOptions { ValidateOnBuild = true });

        Assert.NotNull(provider.GetRequiredService<IRemotingRocketMQTelemetry>());
    }

    [Fact]
    public async Task AddRocketMQRemoting_ValidatesClientOptions()
    {
        var services = new ServiceCollection();
        services.AddRocketMQRemoting(options =>
        {
            options.NamesrvAddr = " ";
            options.AccessKey = "access-key";
            options.RequestTimeout = TimeSpan.Zero;
            options.PollNameServerInterval = TimeSpan.Zero;
            options.NameServerRequestTimeout = TimeSpan.Zero;
            options.HeartbeatBrokerInterval = TimeSpan.Zero;
            options.MaxRemotingFrameSize = RemotingClientOptions.MinimumRemotingFrameSize - 1;
        });
        await using var provider = services.BuildServiceProvider();

        var exception = Assert.Throws<OptionsValidationException>(
            () => provider.GetRequiredService<IOptions<RemotingClientOptions>>().Value);

        Assert.Contains("NameServer address", exception.Message, StringComparison.Ordinal);
        Assert.Contains("access key and an access secret", exception.Message, StringComparison.Ordinal);
        Assert.Contains("Request timeout", exception.Message, StringComparison.Ordinal);
        Assert.Contains("NameServer polling interval", exception.Message, StringComparison.Ordinal);
        Assert.Contains("NameServer request timeout", exception.Message, StringComparison.Ordinal);
        Assert.Contains("Broker heartbeat interval", exception.Message, StringComparison.Ordinal);
        Assert.Contains("frame size", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void AddRocketMQRemoting_RejectsSecurityTokenWithoutCredentials()
    {
        var services = new ServiceCollection();
        services.AddRocketMQRemoting(options =>
        {
            options.NamesrvAddr = "127.0.0.1:9876";
            options.SecurityToken = "security-token";
        });
        using var provider = services.BuildServiceProvider();

        var exception = Assert.Throws<OptionsValidationException>(
            () => provider.GetRequiredService<IOptions<RemotingClientOptions>>().Value);

        Assert.Contains("security token", exception.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task AddRocketMQRemoting_PreservesRegisteredTimeProvider()
    {
        var services = new ServiceCollection();
        var timeProvider = new TestTimeProvider();
        services.AddSingleton<TimeProvider>(timeProvider);
        var builder = services.AddRocketMQRemoting(options => options.NamesrvAddr = "127.0.0.1:9876");
        await using var provider = services.BuildServiceProvider();

        Assert.IsType<RemotingRocketMQBuilder>(builder);
        Assert.Same(timeProvider, provider.GetRequiredService<TimeProvider>());
    }

    [Fact]
    public void AddRemotingProducer_RejectsDuplicateRoleForRegistrationName()
    {
        var builder = new ServiceCollection()
            .AddRocketMQRemoting("orders", options => options.NamesrvAddr = "127.0.0.1:9876")
            .AddRemotingProducer();

        var exception = Assert.Throws<InvalidOperationException>(() => builder.AddRemotingProducer());

        Assert.Contains("already registered", exception.Message, StringComparison.Ordinal);
        Assert.Contains("registration name 'orders'", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void AddRemotingAdmin_RejectsDuplicateRole()
    {
        var builder = new ServiceCollection()
            .AddRocketMQRemoting(options => options.NamesrvAddr = "127.0.0.1:9876")
            .AddRemotingAdmin();

        var exception = Assert.Throws<InvalidOperationException>(() => builder.AddRemotingAdmin());

        Assert.Contains("already registered", exception.Message, StringComparison.Ordinal);
        Assert.Contains("default client registration", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task AddRemotingProducer_RejectsMessageSizeAboveClassicFrameBudget()
    {
        var services = new ServiceCollection();
        services
            .AddRocketMQRemoting(options =>
            {
                options.NamesrvAddr = "127.0.0.1:9876";
                options.MaxRemotingFrameSize = 1024 * 1024;
            })
            .AddRemotingProducer(options =>
            {
                options.GroupName = "orders";
                options.MaxMessageSize = 1024 * 1024;
            });
        await using var provider = services.BuildServiceProvider();

        var exception = Assert.Throws<OptionsValidationException>(
            provider.GetRequiredService<IRemotingProducer>);

        Assert.Contains("command headers", exception.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task AddRemotingProducer_RegistersProducerAndHostedLifecycle()
    {
        var services = new ServiceCollection();
        services
            .AddRocketMQRemoting(options =>
            {
                options.NamesrvAddr = "localhost:9876";
            })
            .AddRemotingProducer(options => options.GroupName = "orders");

        await using var provider = services.BuildServiceProvider();

        var producer = provider.GetRequiredService<IRemotingProducer>();
        var hostedService = provider.GetServices<IHostedService>().Single();
        var options = provider.GetRequiredService<IOptions<RemotingProducerOptions>>().Value;

        Assert.Equal("orders", options.GroupName);
        Assert.IsType<RemotingProducerHostedService>(hostedService);
        await hostedService.StartAsync(CancellationToken.None);
        await hostedService.StopAsync(CancellationToken.None);
        Assert.Same(producer, provider.GetRequiredService<IRemotingProducer>());
    }

    [Fact]
    public async Task AddRemotingPullConsumer_RegistersProtocolSpecificConsumerAndLifecycle()
    {
        var services = new ServiceCollection();
        services
            .AddRocketMQRemoting(options =>
            {
                options.NamesrvAddr = "127.0.0.1:9876";
            })
            .AddRemotingPullConsumer(options =>
            {
                options.GroupName = "orders";
                options.Subscribe("orders");
            });
        await using var provider = services.BuildServiceProvider();

        var consumer = provider.GetRequiredService<IRemotingPullConsumer>();
        var hostedService = Assert.Single(provider.GetServices<IHostedService>());

        Assert.IsType<RemotingPullConsumer>(consumer);
        Assert.IsType<RemotingPullConsumerHostedService>(hostedService);
        Assert.Same(consumer, provider.GetRequiredService<IRemotingPullConsumer>());
    }

    [Fact]
    public async Task AddRemotingLitePullConsumer_RegistersProtocolSpecificConsumerAndLifecycle()
    {
        var services = new ServiceCollection();
        services
            .AddRocketMQRemoting(options =>
            {
                options.NamesrvAddr = "127.0.0.1:9876";
            })
            .AddRemotingLitePullConsumer(options => options.GroupName = "orders");
        await using var provider = services.BuildServiceProvider();

        var consumer = provider.GetRequiredService<IRemotingLitePullConsumer>();
        var hostedService = Assert.Single(provider.GetServices<IHostedService>());
        var options = provider.GetRequiredService<IOptions<RemotingLitePullConsumerOptions>>().Value;

        Assert.Equal("orders", options.GroupName);
        Assert.IsType<RemotingLitePullConsumer>(consumer);
        Assert.IsType<RemotingLitePullConsumerHostedService>(hostedService);
        Assert.Same(consumer, provider.GetRequiredService<IRemotingLitePullConsumer>());
        await hostedService.StartAsync(CancellationToken.None);
        await hostedService.StopAsync(CancellationToken.None);
    }

    [Fact]
    public async Task AddRemotingLitePullConsumer_RejectsTimestampOffsetWithoutTimestamp()
    {
        var services = new ServiceCollection();
        services
            .AddRocketMQRemoting(options =>
            {
                options.NamesrvAddr = "127.0.0.1:9876";
            })
            .AddRemotingLitePullConsumer(options =>
            {
                options.GroupName = "orders";
                options.InitialOffset = QueryOffsetPolicy.Timestamp;
            });
        await using var provider = services.BuildServiceProvider();

        var exception = Assert.Throws<OptionsValidationException>(
            provider.GetRequiredService<IRemotingLitePullConsumer>);

        Assert.Contains("initial offset timestamp", exception.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task AddRemotingPushConsumer_RegistersProtocolSpecificConsumerAndLifecycle()
    {
        var services = new ServiceCollection();
        services
            .AddRocketMQRemoting(options =>
            {
                options.NamesrvAddr = "127.0.0.1:9876";
            })
            .AddRemotingPushConsumer<TestRemotingPushMessageHandler>(ServiceLifetime.Singleton, options =>
            {
                options.GroupName = "orders";
                options.Subscribe("orders");
            });
        await using var provider = services.BuildServiceProvider();

        var consumer = provider.GetRequiredService<IRemotingPushConsumer>();
        var hostedService = Assert.Single(provider.GetServices<IHostedService>());

        Assert.IsType<RemotingPushConsumer>(consumer);
        Assert.IsType<RemotingPushConsumerHostedService>(hostedService);
        Assert.Same(consumer, provider.GetRequiredService<IRemotingPushConsumer>());
    }

    [Fact]
    public async Task AddRemotingPushConsumer_RegistersMultipleConsumersWithIsolatedOptionsAndLifecycles()
    {
        var services = new ServiceCollection();
        var rocketMQ = services.AddRocketMQRemoting(options =>
        {
            options.NamesrvAddr = "127.0.0.1:9876";
        });
        rocketMQ.AddRemotingPushConsumer<TestRemotingPushMessageHandler>(ServiceLifetime.Singleton, options =>
        {
            options.GroupName = "orders-consumer";
            options.MaxConcurrency = 1;
            options.Subscribe("orders", new FilterExpression("created"));
        });
        rocketMQ.AddRemotingPushConsumer<TestRemotingPushMessageHandler>(ServiceLifetime.Singleton, options =>
        {
            options.GroupName = "payments-consumer";
            options.MaxConcurrency = 2;
            options.Subscribe("payments", new FilterExpression("captured"));
        });

        await using var provider = services.BuildServiceProvider(
            new ServiceProviderOptions { ValidateOnBuild = true });
        var consumers = provider.GetServices<IRemotingPushConsumer>().ToArray();
        var hostedServices = provider.GetServices<IHostedService>()
            .OfType<RemotingPushConsumerHostedService>()
            .ToArray();

        Assert.Equal(2, consumers.Length);
        Assert.NotSame(consumers[0], consumers[1]);
        Assert.Same(consumers[^1], provider.GetRequiredService<IRemotingPushConsumer>());
        Assert.Same(
            GetGroupMemberClient(consumers[0]),
            GetGroupMemberClient(consumers[1]));
        Assert.Same(
            GetSingleInstanceField<IRemotingRebalanceService>(consumers[0]),
            GetSingleInstanceField<IRemotingRebalanceService>(consumers[1]));
        Assert.Same(
            GetSingleInstanceField<ITopicRouteService>(consumers[0]),
            GetSingleInstanceField<ITopicRouteService>(consumers[1]));
        Assert.All(consumers, consumer => Assert.Same(
            GetGroupMemberClient(consumer),
            GetSingleInstanceField<IRemotingClient>(
                GetSingleInstanceField<IRemotingConsumerEngine>(consumer))));
        var optionsByGroup = consumers
            .Select(GetPushConsumerOptions)
            .ToDictionary(options => options.GroupName, StringComparer.Ordinal);
        var ordersOptions = optionsByGroup["orders-consumer"];
        var paymentsOptions = optionsByGroup["payments-consumer"];

        Assert.Equal(1, ordersOptions.MaxConcurrency);
        var ordersSubscription = Assert.Single(ordersOptions.Subscriptions);
        Assert.Equal("orders", ordersSubscription.Key);
        Assert.Equal("created", ordersSubscription.Value.Expression);

        Assert.Equal(2, paymentsOptions.MaxConcurrency);
        var paymentsSubscription = Assert.Single(paymentsOptions.Subscriptions);
        Assert.Equal("payments", paymentsSubscription.Key);
        Assert.Equal("captured", paymentsSubscription.Value.Expression);
        Assert.All(consumers, consumer =>
            Assert.IsType<TestRemotingPushMessageHandler>(GetPushMessageHandler(consumer).Target));
        Assert.NotSame(GetPushMessageHandler(consumers[0]).Target, GetPushMessageHandler(consumers[1]).Target);

        Assert.Equal(2, hostedServices.Length);
        var lifecycleConsumers = hostedServices.Select(GetHostedConsumer).ToArray();
        Assert.All(consumers, consumer => Assert.Contains(consumer, lifecycleConsumers));
        Assert.NotSame(lifecycleConsumers[0], lifecycleConsumers[1]);
    }

    [Theory]
    [InlineData((int)RemotingRocketMQRole.PullConsumer, "primary:remoting-pull-consumer:2")]
    [InlineData((int)RemotingRocketMQRole.LitePullConsumer, "primary:remoting-lite-pull-consumer:2")]
    [InlineData((int)RemotingRocketMQRole.PushConsumer, "primary:remoting-push-consumer:2")]
    [InlineData((int)RemotingRocketMQRole.PopConsumer, "primary:remoting-pop-consumer:2")]
    public async Task AddRemotingConsumer_IsolatesOptionsFromARegistrationNamedLikeADerivedRoleOptionsName(
        int roleValue,
        string collidingRegistrationName)
    {
        var role = (RemotingRocketMQRole)roleValue;
        var services = new ServiceCollection();
        var primary = services.AddRocketMQRemoting(
            "primary",
            options => options.NamesrvAddr = "127.0.0.1:9876");
        AddRepeatableConsumer(primary, role, "primary-first", "primary-first-topic");
        AddRepeatableConsumer(primary, role, "primary-second", "primary-second-topic");
        var colliding = services.AddRocketMQRemoting(
            collidingRegistrationName,
            options => options.NamesrvAddr = "127.0.0.1:19876");
        AddRepeatableConsumer(colliding, role, "colliding", "colliding-topic");

        await using var provider = services.BuildServiceProvider(
            new ServiceProviderOptions { ValidateOnBuild = true });

        var primaryOptions = GetRepeatableConsumerSettings(provider, role, "primary");
        var collidingOptions = GetRepeatableConsumerSettings(provider, role, collidingRegistrationName);

        Assert.Equal(["primary-first", "primary-second"], primaryOptions.Select(static options => options.GroupName));
        Assert.Equal("colliding", Assert.Single(collidingOptions).GroupName);
        Assert.Equal(
            ["primary-first-topic", "primary-second-topic"],
            primaryOptions.Select(static options => Assert.Single(options.Subscriptions).Key));
        Assert.Equal("colliding-topic", Assert.Single(Assert.Single(collidingOptions).Subscriptions).Key);
    }

    [Fact]
    public void AddRocketMQRemoting_RejectsRegistrationNameContainingNullCharacter()
    {
        var services = new ServiceCollection();

        var exception = Assert.Throws<ArgumentException>(() => services.AddRocketMQRemoting(
            "invalid\0registration",
            options => options.NamesrvAddr = "127.0.0.1:9876"));

        Assert.Equal("registrationName", exception.ParamName);
    }

    [Fact]
    public async Task AddRemotingPushConsumer_RegistersMultipleConsumersWithMatchingGroupAndSubscriptions()
    {
        var services = new ServiceCollection();
        services
            .AddRocketMQRemoting(options => options.NamesrvAddr = "127.0.0.1:9876")
            .AddRemotingPushConsumer<TestRemotingPushMessageHandler>(ServiceLifetime.Singleton, options =>
            {
                options.GroupName = "orders-consumer";
                options.MaxConcurrency = 1;
                options.Subscribe("orders", new FilterExpression("created"));
            })
            .AddRemotingPushConsumer<TestRemotingPushMessageHandler>(ServiceLifetime.Singleton, options =>
            {
                options.GroupName = "orders-consumer";
                options.MaxConcurrency = 2;
                options.Subscribe("orders", new FilterExpression("created"));
            });
        await using var provider = services.BuildServiceProvider(
            new ServiceProviderOptions { ValidateOnBuild = true });

        var consumers = provider.GetServices<IRemotingPushConsumer>().ToArray();

        Assert.Equal(2, consumers.Length);
        Assert.NotSame(consumers[0], consumers[1]);
        Assert.NotSame(
            GetGroupMemberClient(consumers[0]),
            GetGroupMemberClient(consumers[1]));
        Assert.NotSame(
            GetSingleInstanceField<IRemotingRebalanceService>(consumers[0]),
            GetSingleInstanceField<IRemotingRebalanceService>(consumers[1]));
        var clientIds = consumers
            .Select(GetSingleInstanceField<RemotingClientOptions>)
            .Select(static options => options.BuildRemotingClientId())
            .ToArray();
        Assert.NotEqual(clientIds[0], clientIds[1]);
        var consumerOptions = consumers.Select(GetPushConsumerOptions).ToArray();
        Assert.All(consumerOptions, options =>
        {
            Assert.Equal("orders-consumer", options.GroupName);
            var subscription = Assert.Single(options.Subscriptions);
            Assert.Equal("orders", subscription.Key);
            Assert.Equal(new FilterExpression("created"), subscription.Value);
        });
        Assert.Equal([1, 2], consumerOptions.Select(options => options.MaxConcurrency));
        Assert.All(consumers, consumer =>
            Assert.IsType<TestRemotingPushMessageHandler>(GetPushMessageHandler(consumer).Target));
        Assert.NotSame(GetPushMessageHandler(consumers[0]).Target, GetPushMessageHandler(consumers[1]).Target);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task AddRemotingPushConsumer_SameGroupMembersRejectRuntimeSubscriptionChanges(bool subscribe)
    {
        var services = new ServiceCollection();
        services
            .AddRocketMQRemoting(options => options.NamesrvAddr = "127.0.0.1:9876")
            .AddRemotingPushConsumer<TestRemotingPushMessageHandler>(ServiceLifetime.Singleton, options =>
            {
                options.GroupName = "orders-consumer";
                options.Subscribe("orders");
            })
            .AddRemotingPushConsumer<TestRemotingPushMessageHandler>(ServiceLifetime.Singleton, options =>
            {
                options.GroupName = "orders-consumer";
                options.Subscribe("orders");
            });
        await using var provider = services.BuildServiceProvider();
        var consumer = provider.GetServices<IRemotingPushConsumer>().First();

        var exception = await Assert.ThrowsAsync<InvalidOperationException>(() => subscribe
            ? consumer.SubscribeAsync("payments", cancellationToken: TestContext.Current.CancellationToken)
            : consumer.UnsubscribeAsync("orders", TestContext.Current.CancellationToken));

        Assert.Contains("same consumer group", exception.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("restart", exception.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task AddRemotingPushConsumer_DifferentGroupMembersAllowRuntimeSubscriptionChanges()
    {
        var services = new ServiceCollection();
        services
            .AddRocketMQRemoting(options => options.NamesrvAddr = "127.0.0.1:9876")
            .AddRemotingPushConsumer<TestRemotingPushMessageHandler>(ServiceLifetime.Singleton, options =>
            {
                options.GroupName = "orders-consumer";
                options.Subscribe("orders");
            })
            .AddRemotingPushConsumer<TestRemotingPushMessageHandler>(ServiceLifetime.Singleton, options =>
            {
                options.GroupName = "payments-consumer";
                options.Subscribe("payments");
            });
        await using var provider = services.BuildServiceProvider();
        var consumers = provider.GetServices<IRemotingPushConsumer>().ToArray();

        await consumers[0].SubscribeAsync("audit", cancellationToken: TestContext.Current.CancellationToken);
        await consumers[1].UnsubscribeAsync("payments", TestContext.Current.CancellationToken);
    }

    [Theory]
    [InlineData("payments", "created")]
    [InlineData("orders", "updated")]
    public void AddRemotingPushConsumer_RejectsMismatchedSubscriptionsForSameGroup(
        string secondTopic,
        string secondFilter)
    {
        var services = new ServiceCollection();
        services
            .AddRocketMQRemoting(options => options.NamesrvAddr = "127.0.0.1:9876")
            .AddRemotingPushConsumer<TestRemotingPushMessageHandler>(ServiceLifetime.Singleton, options =>
            {
                options.GroupName = "orders-consumer";
                options.Subscribe("orders", new FilterExpression("created"));
            })
            .AddRemotingPushConsumer<TestRemotingPushMessageHandler>(ServiceLifetime.Singleton, options =>
            {
                options.GroupName = "orders-consumer";
                options.Subscribe(secondTopic, new FilterExpression(secondFilter));
            });
        using var provider = services.BuildServiceProvider();

        var exception = Assert.Throws<OptionsValidationException>(
            () => provider.GetServices<IRemotingPushConsumer>().ToArray());

        Assert.Contains("same consumer group", exception.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("identical topic subscriptions", exception.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Theory]
    [InlineData(ConsumerMode.Broadcasting, ConsumeFromPosition.End, "consumer mode")]
    [InlineData(ConsumerMode.Clustering, ConsumeFromPosition.Beginning, "initial consume position")]
    public void AddRemotingPushConsumer_RejectsMismatchedBrokerGroupSettings(
        ConsumerMode secondMode,
        ConsumeFromPosition secondPosition,
        string expectedFailure)
    {
        var services = new ServiceCollection();
        services
            .AddRocketMQRemoting(options => options.NamesrvAddr = "127.0.0.1:9876")
            .AddRemotingPushConsumer<TestRemotingPushMessageHandler>(ServiceLifetime.Singleton, options =>
            {
                options.GroupName = "orders-consumer";
                options.Subscribe("orders");
            })
            .AddRemotingPushConsumer<TestRemotingPushMessageHandler>(ServiceLifetime.Singleton, options =>
            {
                options.GroupName = "orders-consumer";
                options.ConsumerMode = secondMode;
                options.InitialPosition = secondPosition;
                options.Subscribe("orders");
            });
        using var provider = services.BuildServiceProvider();

        var exception = Assert.Throws<OptionsValidationException>(
            () => provider.GetServices<IRemotingPushConsumer>().ToArray());

        Assert.Contains("same consumer group", exception.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains(expectedFailure, exception.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task AddRemotingPushConsumer_RejectsMismatchedOrderlyModeForSameGroupWithinOneRegistration()
    {
        var services = new ServiceCollection();
        services
            .AddRocketMQRemoting(options => options.NamesrvAddr = "127.0.0.1:9876")
            .AddRemotingPushConsumer<TestRemotingPushMessageHandler>(ServiceLifetime.Singleton, options =>
            {
                options.GroupName = "orders-consumer";
                options.Subscribe("orders");
            })
            .AddRemotingPushConsumer<TestRemotingPushMessageHandler>(ServiceLifetime.Singleton, options =>
            {
                options.GroupName = "orders-consumer";
                options.ConsumeOrderly = true;
                options.Subscribe("orders");
            });
        await using var provider = services.BuildServiceProvider();

        var exception = Assert.Throws<OptionsValidationException>(
            () => provider.GetServices<IRemotingPushConsumer>().ToArray());

        Assert.Contains("same consumer group", exception.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("orderly mode", exception.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void AddRemotingLitePullConsumer_RejectsMismatchedSubscriptionsForSameGroup()
    {
        var services = new ServiceCollection();
        services
            .AddRocketMQRemoting(options => options.NamesrvAddr = "127.0.0.1:9876")
            .AddRemotingLitePullConsumer(options =>
            {
                options.GroupName = "orders";
                options.Subscribe("orders");
            })
            .AddRemotingLitePullConsumer(options =>
            {
                options.GroupName = "orders";
                options.Subscribe("payments");
            });
        using var provider = services.BuildServiceProvider();

        var exception = Assert.Throws<OptionsValidationException>(
            () => provider.GetServices<IRemotingLitePullConsumer>().ToArray());

        Assert.Contains("same consumer group", exception.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("identical topic subscriptions", exception.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void AddRemotingLitePullConsumer_RejectsMismatchedInitialOffsetForSameGroup()
    {
        var services = new ServiceCollection();
        services
            .AddRocketMQRemoting(options => options.NamesrvAddr = "127.0.0.1:9876")
            .AddRemotingLitePullConsumer(options =>
            {
                options.GroupName = "orders";
                options.Subscribe("orders");
            })
            .AddRemotingLitePullConsumer(options =>
            {
                options.GroupName = "orders";
                options.InitialOffset = QueryOffsetPolicy.Beginning;
                options.Subscribe("orders");
            });
        using var provider = services.BuildServiceProvider();

        var exception = Assert.Throws<OptionsValidationException>(
            () => provider.GetServices<IRemotingLitePullConsumer>().ToArray());

        Assert.Contains("same consumer group", exception.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("initial offset policy", exception.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task AddRemotingLitePullConsumer_SameGroupMembersRejectRuntimeSubscriptionChanges(bool subscribe)
    {
        var services = new ServiceCollection();
        services
            .AddRocketMQRemoting(options => options.NamesrvAddr = "127.0.0.1:9876")
            .AddRemotingLitePullConsumer(options =>
            {
                options.GroupName = "orders-consumer";
                options.Subscribe("orders");
            })
            .AddRemotingLitePullConsumer(options =>
            {
                options.GroupName = "orders-consumer";
                options.Subscribe("orders");
            });
        await using var provider = services.BuildServiceProvider();
        var consumer = provider.GetServices<IRemotingLitePullConsumer>().First();

        var exception = await Assert.ThrowsAsync<InvalidOperationException>(() => subscribe
            ? consumer.SubscribeAsync("payments", cancellationToken: TestContext.Current.CancellationToken)
            : consumer.UnsubscribeAsync("orders", TestContext.Current.CancellationToken));

        Assert.Contains("same consumer group", exception.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("restart", exception.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task AddRemotingLitePullConsumer_DifferentGroupMembersAllowRuntimeSubscriptionChanges()
    {
        var services = new ServiceCollection();
        services
            .AddRocketMQRemoting(options => options.NamesrvAddr = "127.0.0.1:9876")
            .AddRemotingLitePullConsumer(options =>
            {
                options.GroupName = "orders-consumer";
                options.Subscribe("orders");
            })
            .AddRemotingLitePullConsumer(options =>
            {
                options.GroupName = "payments-consumer";
                options.Subscribe("payments");
            });
        await using var provider = services.BuildServiceProvider();
        var consumers = provider.GetServices<IRemotingLitePullConsumer>().ToArray();

        await consumers[0].SubscribeAsync("audit", cancellationToken: TestContext.Current.CancellationToken);
        await consumers[1].UnsubscribeAsync("payments", TestContext.Current.CancellationToken);
    }

    [Fact]
    public async Task AddRemotingLitePullConsumer_IsolatesRepeatedGroupMembersWhileSharingDifferentGroups()
    {
        var services = new ServiceCollection();
        services
            .AddRocketMQRemoting(options => options.NamesrvAddr = "127.0.0.1:9876")
            .AddRemotingLitePullConsumer(options =>
            {
                options.GroupName = "orders";
                options.Subscribe("orders");
            })
            .AddRemotingLitePullConsumer(options =>
            {
                options.GroupName = "payments";
                options.Subscribe("payments");
            })
            .AddRemotingLitePullConsumer(options =>
            {
                options.GroupName = "orders";
                options.Subscribe("orders");
            });
        await using var provider = services.BuildServiceProvider(
            new ServiceProviderOptions { ValidateOnBuild = true });

        var consumers = provider.GetServices<IRemotingLitePullConsumer>().ToArray();
        var clients = consumers.Select(GetGroupMemberClient).ToArray();
        var hostedConsumers = provider.GetServices<IHostedService>()
            .OfType<RemotingLitePullConsumerHostedService>()
            .Select(GetSingleInstanceField<IRemotingLitePullConsumer>)
            .ToArray();

        Assert.Equal(3, consumers.Length);
        Assert.Same(clients[0], clients[1]);
        Assert.NotSame(clients[0], clients[2]);
        var rebalanceServices = consumers.Select(GetSingleInstanceField<IRemotingRebalanceService>).ToArray();
        Assert.Same(rebalanceServices[0], rebalanceServices[1]);
        Assert.NotSame(rebalanceServices[0], rebalanceServices[2]);
        Assert.All(consumers, consumer => Assert.Same(
            GetGroupMemberClient(consumer),
            GetSingleInstanceField<IRemotingClient>(
                GetSingleInstanceField<IRemotingConsumerEngine>(consumer))));
        Assert.All(consumers, consumer => Assert.Contains(consumer, hostedConsumers));
    }

    [Fact]
    public async Task AddRemotingConsumers_ShareTransportAcrossPushAndLitePullInDifferentGroups()
    {
        var services = new ServiceCollection();
        services
            .AddRocketMQRemoting(options => options.NamesrvAddr = "127.0.0.1:9876")
            .AddRemotingLitePullConsumer(options =>
            {
                options.GroupName = "orders-lite";
                options.Subscribe("orders");
            })
            .AddRemotingPushConsumer<TestRemotingPushMessageHandler>(ServiceLifetime.Singleton, options =>
            {
                options.GroupName = "payments-push";
                options.Subscribe("payments");
            });
        await using var provider = services.BuildServiceProvider(
            new ServiceProviderOptions { ValidateOnBuild = true });

        var litePull = provider.GetRequiredService<IRemotingLitePullConsumer>();
        var push = provider.GetRequiredService<IRemotingPushConsumer>();

        Assert.Same(
            GetGroupMemberClient(litePull),
            GetGroupMemberClient(push));
        Assert.Same(
            GetSingleInstanceField<IRemotingRebalanceService>(litePull),
            GetSingleInstanceField<IRemotingRebalanceService>(push));
    }

    [Fact]
    public void AddRemotingConsumers_RejectsPushAndLitePullInTheSameGroup()
    {
        var services = new ServiceCollection();
        services
            .AddRocketMQRemoting(options => options.NamesrvAddr = "127.0.0.1:9876")
            .AddRemotingLitePullConsumer(options =>
            {
                options.GroupName = "orders";
                options.Subscribe("orders");
            })
            .AddRemotingPushConsumer<TestRemotingPushMessageHandler>(ServiceLifetime.Singleton, options =>
            {
                options.GroupName = "orders";
                options.Subscribe("orders");
            });
        using var provider = services.BuildServiceProvider();

        var exception = Assert.Throws<OptionsValidationException>(
            provider.GetRequiredService<IRemotingPushConsumer>);

        Assert.Contains("same consumer group", exception.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("consumption types differ", exception.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task AddRemotingPassiveConsumers_RegistersMultipleInstancesAndLifecycles()
    {
        var services = new ServiceCollection();
        services
            .AddRocketMQRemoting(options => options.NamesrvAddr = "127.0.0.1:9876")
            .AddRemotingPullConsumer(options =>
            {
                options.GroupName = "orders";
                options.Subscribe("orders");
            })
            .AddRemotingPullConsumer(options =>
            {
                options.GroupName = "payments";
                options.Subscribe("payments");
            })
            .AddRemotingPopConsumer(options => options.GroupName = "orders")
            .AddRemotingPopConsumer(options => options.GroupName = "payments");
        await using var provider = services.BuildServiceProvider(
            new ServiceProviderOptions { ValidateOnBuild = true });

        var pullConsumers = provider.GetServices<IRemotingPullConsumer>().ToArray();
        var popConsumers = provider.GetServices<IRemotingPopConsumer>().ToArray();
        var hostedServices = provider.GetServices<IHostedService>().ToArray();

        Assert.Equal(2, pullConsumers.Length);
        Assert.Equal(2, popConsumers.Length);
        Assert.All(pullConsumers, consumer => Assert.Contains(
            hostedServices.OfType<RemotingPullConsumerHostedService>()
                .Select(GetSingleInstanceField<IRemotingPullConsumer>),
            candidate => ReferenceEquals(candidate, consumer)));
        Assert.All(popConsumers, consumer => Assert.Contains(
            hostedServices.OfType<RemotingPopConsumerHostedService>()
                .Select(GetSingleInstanceField<IRemotingPopConsumer>),
            candidate => ReferenceEquals(candidate, consumer)));
    }

    [Fact]
    public async Task AddRemotingPushConsumer_KeyedRegistrationEnumeratesAllConsumers()
    {
        var services = new ServiceCollection();
        services
            .AddRocketMQRemoting("east", options => options.NamesrvAddr = "127.0.0.1:9876")
            .AddRemotingPushConsumer<TestRemotingPushMessageHandler>(ServiceLifetime.Singleton, options =>
            {
                options.GroupName = "orders";
                options.Subscribe("orders");
            })
            .AddRemotingPushConsumer<TestRemotingPushMessageHandler>(ServiceLifetime.Singleton, options =>
            {
                options.GroupName = "payments";
                options.Subscribe("payments");
            });
        await using var provider = services.BuildServiceProvider(
            new ServiceProviderOptions { ValidateOnBuild = true });

        var consumers = provider.GetKeyedServices<IRemotingPushConsumer>("east").ToArray();
        var hostedConsumers = provider.GetServices<IHostedService>()
            .OfType<RemotingPushConsumerHostedService>()
            .Select(GetHostedConsumer)
            .ToArray();

        Assert.Equal(2, consumers.Length);
        Assert.Same(consumers[^1], provider.GetRequiredKeyedService<IRemotingPushConsumer>("east"));
        Assert.All(consumers, consumer => Assert.Contains(consumer, hostedConsumers));
    }

    [Fact]
    public async Task AddRemotingPopConsumer_RegistersProtocolSpecificConsumerAndLifecycle()
    {
        var services = new ServiceCollection();
        services
            .AddRocketMQRemoting(options => options.NamesrvAddr = "127.0.0.1:9876")
            .AddRemotingPopConsumer(options => options.GroupName = "orders");
        await using var provider = services.BuildServiceProvider();

        var consumer = provider.GetRequiredService<IRemotingPopConsumer>();
        var hostedService = Assert.Single(provider.GetServices<IHostedService>());
        var options = provider.GetRequiredService<IOptions<RemotingPopConsumerOptions>>().Value;

        Assert.Equal("orders", options.GroupName);
        Assert.IsType<RemotingPopConsumer>(consumer);
        Assert.IsType<RemotingPopConsumerHostedService>(hostedService);
        Assert.Same(consumer, provider.GetRequiredService<IRemotingPopConsumer>());
        await hostedService.StartAsync(CancellationToken.None);
        await hostedService.StopAsync(CancellationToken.None);
    }

    [Fact]
    public async Task AddRemotingPopConsumer_RejectsNonPositiveInvisibleDuration()
    {
        var services = new ServiceCollection();
        services
            .AddRocketMQRemoting(options => options.NamesrvAddr = "127.0.0.1:9876")
            .AddRemotingPopConsumer(options =>
            {
                options.GroupName = "orders";
                options.InvisibleDuration = TimeSpan.Zero;
            });
        await using var provider = services.BuildServiceProvider();

        var exception = Assert.Throws<OptionsValidationException>(
            provider.GetRequiredService<IRemotingPopConsumer>);

        Assert.Contains("Invisible duration", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task AddRemotingPopConsumer_RejectsBatchSizeAboveTheBrokerLimit()
    {
        var services = new ServiceCollection();
        services
            .AddRocketMQRemoting(options => options.NamesrvAddr = "127.0.0.1:9876")
            .AddRemotingPopConsumer(options =>
            {
                options.GroupName = "orders";
                options.BatchSize = 33;
            });
        await using var provider = services.BuildServiceProvider();

        var exception = Assert.Throws<OptionsValidationException>(
            provider.GetRequiredService<IRemotingPopConsumer>);

        Assert.Contains("POP batch size", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task AddRemotingPushConsumer_RejectsTimestampPositionWithoutTimestamp()
    {
        var services = new ServiceCollection();
        services
            .AddRocketMQRemoting(options =>
            {
                options.NamesrvAddr = "127.0.0.1:9876";
            })
            .AddRemotingPushConsumer<TestRemotingPushMessageHandler>(ServiceLifetime.Singleton, options =>
            {
                options.GroupName = "orders";
                options.InitialPosition = ConsumeFromPosition.Timestamp;
                options.Subscribe("orders");
            });

        await using var provider = services.BuildServiceProvider();

        var exception = Assert.Throws<OptionsValidationException>(
            provider.GetRequiredService<IRemotingPushConsumer>);
        Assert.Contains("consume timestamp", exception.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task AddRemotingPushConsumer_RejectsNonPositiveConsumeTimeout()
    {
        var services = new ServiceCollection();
        services
            .AddRocketMQRemoting(options =>
            {
                options.NamesrvAddr = "127.0.0.1:9876";
            })
            .AddRemotingPushConsumer<TestRemotingPushMessageHandler>(ServiceLifetime.Singleton, options =>
            {
                options.GroupName = "orders";
                options.ConsumeTimeout = TimeSpan.Zero;
                options.Subscribe("orders");
            });

        await using var provider = services.BuildServiceProvider();

        var exception = Assert.Throws<OptionsValidationException>(
            provider.GetRequiredService<IRemotingPushConsumer>);
        Assert.Contains("consume timeout", exception.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task AddRemotingPushConsumer_RejectsUnknownInitialPosition()
    {
        var services = new ServiceCollection();
        services
            .AddRocketMQRemoting(options =>
            {
                options.NamesrvAddr = "127.0.0.1:9876";
            })
            .AddRemotingPushConsumer<TestRemotingPushMessageHandler>(ServiceLifetime.Singleton, options =>
            {
                options.GroupName = "orders";
                options.InitialPosition = (ConsumeFromPosition)int.MaxValue;
                options.Subscribe("orders");
            });

        await using var provider = services.BuildServiceProvider();

        var exception = Assert.Throws<OptionsValidationException>(
            provider.GetRequiredService<IRemotingPushConsumer>);
        Assert.Contains("initial consume position", exception.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task AddRemotingPushConsumer_RejectsUnknownConsumerMode()
    {
        var services = new ServiceCollection();
        services
            .AddRocketMQRemoting(options =>
            {
                options.NamesrvAddr = "127.0.0.1:9876";
            })
            .AddRemotingPushConsumer<TestRemotingPushMessageHandler>(ServiceLifetime.Singleton, options =>
            {
                options.GroupName = "orders";
                options.ConsumerMode = (ConsumerMode)int.MaxValue;
                options.Subscribe("orders");
            });

        await using var provider = services.BuildServiceProvider();

        var exception = Assert.Throws<OptionsValidationException>(
            provider.GetRequiredService<IRemotingPushConsumer>);
        Assert.Contains("consumer mode", exception.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task AddRemotingPushConsumer_RejectsInvalidLocalOffsetStorePath()
    {
        var services = new ServiceCollection();
        services
            .AddRocketMQRemoting(options =>
            {
                options.NamesrvAddr = "127.0.0.1:9876";
            })
            .AddRemotingPushConsumer<TestRemotingPushMessageHandler>(ServiceLifetime.Singleton, options =>
            {
                options.GroupName = "orders";
                options.ConsumerMode = ConsumerMode.Broadcasting;
                options.LocalOffsetStorePath = "invalid\0path";
                options.Subscribe("orders");
            });

        await using var provider = services.BuildServiceProvider();

        var exception = Assert.Throws<OptionsValidationException>(
            provider.GetRequiredService<IRemotingPushConsumer>);
        Assert.Contains("offset store path", exception.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task KeyedClientRegistrationsRegisterIndependentRemotingProducersAndOptions()
    {
        var services = new ServiceCollection();
        services
            .AddRocketMQRemoting("east", options =>
            {
                options.NamesrvAddr = "127.0.0.1:19876";
                options.InstanceName = "east-client";
                options.UnitName = "east-unit";
            })
            .AddRemotingProducer(options => options.SendMsgTimeout = TimeSpan.FromSeconds(11));
        services
            .AddRocketMQRemoting("west", options =>
            {
                options.NamesrvAddr = "127.0.0.1:29876";
                options.InstanceName = "west-client";
            })
            .AddRemotingProducer(options => options.SendMsgTimeout = TimeSpan.FromSeconds(12));

        await using var provider = services.BuildServiceProvider(
            new ServiceProviderOptions { ValidateOnBuild = true });
        var east = provider.GetRequiredKeyedService<IRemotingProducer>("east");
        var west = provider.GetRequiredKeyedService<IRemotingProducer>("west");
        var eastClientOptions = provider.GetRequiredKeyedService<IOptions<RemotingClientOptions>>(
            new RemotingRocketMQRoleKey("east", RemotingRocketMQRole.Producer)).Value;
        var westClientOptions = provider.GetRequiredKeyedService<IOptions<RemotingClientOptions>>(
            new RemotingRocketMQRoleKey("west", RemotingRocketMQRole.Producer)).Value;
        var producerOptions = provider.GetRequiredService<IOptionsMonitor<RemotingProducerOptions>>();
        var hostedServices = provider.GetServices<IHostedService>().ToArray();

        Assert.IsType<RemotingProducer>(east);
        Assert.IsType<RemotingProducer>(west);
        Assert.NotSame(east, west);
        Assert.Equal("127.0.0.1:19876", eastClientOptions.NamesrvAddr);
        Assert.Equal("127.0.0.1:29876", westClientOptions.NamesrvAddr);
        Assert.Equal("east-client@east-remoting-producer", eastClientOptions.InstanceName);
        Assert.Equal("west-client@west-remoting-producer", westClientOptions.InstanceName);
        Assert.Equal("127.0.0.1@east-client@east-remoting-producer@east-unit", eastClientOptions.BuildRemotingClientId());
        Assert.Equal(TimeSpan.FromSeconds(11), producerOptions.Get("east").SendMsgTimeout);
        Assert.Equal(TimeSpan.FromSeconds(12), producerOptions.Get("west").SendMsgTimeout);
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

    private static RemotingPushConsumerOptions GetPushConsumerOptions(IRemotingPushConsumer consumer)
    {
        var implementation = Assert.IsType<RemotingPushConsumer>(consumer);
        return GetSingleInstanceField<RemotingPushConsumerOptions>(implementation);
    }

    private static void AddRepeatableConsumer(
        RemotingRocketMQBuilder builder,
        RemotingRocketMQRole role,
        string groupName,
        string topic)
    {
        switch (role)
        {
            case RemotingRocketMQRole.PullConsumer:
                builder.AddRemotingPullConsumer(options =>
                {
                    options.GroupName = groupName;
                    options.Subscribe(topic);
                });
                return;
            case RemotingRocketMQRole.LitePullConsumer:
                builder.AddRemotingLitePullConsumer(options =>
                {
                    options.GroupName = groupName;
                    options.Subscribe(topic);
                });
                return;
            case RemotingRocketMQRole.PushConsumer:
                builder.AddRemotingPushConsumer<TestRemotingPushMessageHandler>(ServiceLifetime.Singleton, options =>
                {
                    options.GroupName = groupName;
                    options.Subscribe(topic);
                });
                return;
            case RemotingRocketMQRole.PopConsumer:
                builder.AddRemotingPopConsumer(options =>
                {
                    options.GroupName = groupName;
                    options.Subscribe(topic);
                });
                return;
            default:
                throw new ArgumentOutOfRangeException(nameof(role), role, "The role is not a repeatable consumer.");
        }
    }

    private static RemotingConsumerSettings[] GetRepeatableConsumerSettings(
        IServiceProvider provider,
        RemotingRocketMQRole role,
        string registrationName) =>
        role switch
        {
            RemotingRocketMQRole.PullConsumer => provider
                .GetKeyedServices<IRemotingPullConsumer>(registrationName)
                .Select(GetConsumerSettings)
                .ToArray(),
            RemotingRocketMQRole.LitePullConsumer => provider
                .GetKeyedServices<IRemotingLitePullConsumer>(registrationName)
                .Select(GetConsumerSettings)
                .ToArray(),
            RemotingRocketMQRole.PushConsumer => provider
                .GetKeyedServices<IRemotingPushConsumer>(registrationName)
                .Select(GetConsumerSettings)
                .ToArray(),
            RemotingRocketMQRole.PopConsumer => provider
                .GetKeyedServices<IRemotingPopConsumer>(registrationName)
                .Select(GetConsumerSettings)
                .ToArray(),
            _ => throw new ArgumentOutOfRangeException(nameof(role), role, "The role is not a repeatable consumer.")
        };

    private static RemotingConsumerSettings GetConsumerSettings(object consumer) =>
        GetSingleInstanceField<RemotingConsumerSettings>(
            GetSingleInstanceField<IRemotingConsumerEngine>(consumer));

    private static IRemotingPushConsumer GetHostedConsumer(RemotingPushConsumerHostedService hostedService)
    {
        return GetSingleInstanceField<IRemotingPushConsumer>(hostedService);
    }

    private static Func<
        IReadOnlyList<RemotingMessageView>,
        RemotingPushConsumeContext,
        CancellationToken,
        ValueTask<ConsumeResult>> GetPushMessageHandler(IRemotingPushConsumer consumer)
    {
        var implementation = Assert.IsType<RemotingPushConsumer>(consumer);
        return GetSingleInstanceField<Func<
            IReadOnlyList<RemotingMessageView>,
            RemotingPushConsumeContext,
            CancellationToken,
            ValueTask<ConsumeResult>>>(implementation);
    }

    private static IRemotingClient GetGroupMemberClient(object consumer) =>
        GetSingleInstanceField<IRemotingClient>(
            GetSingleInstanceField<RemotingConsumerGroupSession>(consumer));

    private static TField GetSingleInstanceField<TField>(object instance)
    {
        var field = Assert.Single(instance.GetType()
            .GetFields(BindingFlags.Instance | BindingFlags.NonPublic),
            candidate => typeof(TField).IsAssignableFrom(candidate.FieldType));
        return Assert.IsAssignableFrom<TField>(field.GetValue(instance));
    }

    private sealed class TestRemotingPushMessageHandler : IRemotingPushMessageHandler
    {
        public ValueTask<ConsumeResult> HandleAsync(
            IReadOnlyList<RemotingMessageView> messages,
            RemotingPushConsumeContext context,
            CancellationToken cancellationToken) =>
            ValueTask.FromResult(ConsumeResult.Success);
    }

    private sealed class TestTimeProvider : TimeProvider;
}
