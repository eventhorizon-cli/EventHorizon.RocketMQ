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

using EventHorizon.RocketMQ.Remoting.Consumer;
using EventHorizon.RocketMQ.Remoting.Consumer.Pull;
using EventHorizon.RocketMQ.Remoting.Consumer.Pull.Lite;
using EventHorizon.RocketMQ.Remoting.Consumer.Push;
using EventHorizon.RocketMQ.Remoting.Consumer.Push.Pop;
using EventHorizon.RocketMQ.Remoting.Instrumentation;
using EventHorizon.RocketMQ.Remoting.Producer;
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
            .AddRemotingPushConsumer(options =>
            {
                options.GroupName = "orders";
                options.Subscribe("orders");
                options.MessageHandler = static (_, _) => ValueTask.FromResult(ConsumeResult.Success);
            });
        await using var provider = services.BuildServiceProvider();

        var consumer = provider.GetRequiredService<IRemotingPushConsumer>();
        var hostedService = Assert.Single(provider.GetServices<IHostedService>());

        Assert.IsType<RemotingPushConsumer>(consumer);
        Assert.IsType<RemotingPushConsumerHostedService>(hostedService);
        Assert.Same(consumer, provider.GetRequiredService<IRemotingPushConsumer>());
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
            .AddRemotingPushConsumer(options =>
            {
                options.GroupName = "orders";
                options.InitialPosition = ConsumeFromPosition.Timestamp;
                options.Subscribe("orders");
                options.MessageHandler = static (_, _) => ValueTask.FromResult(ConsumeResult.Success);
            });

        await using var provider = services.BuildServiceProvider();

        var exception = Assert.Throws<OptionsValidationException>(
            provider.GetRequiredService<IRemotingPushConsumer>);
        Assert.Contains("consume timestamp", exception.Message, StringComparison.OrdinalIgnoreCase);
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
            .AddRemotingPushConsumer(options =>
            {
                options.GroupName = "orders";
                options.InitialPosition = (ConsumeFromPosition)int.MaxValue;
                options.Subscribe("orders");
                options.MessageHandler = static (_, _) => ValueTask.FromResult(ConsumeResult.Success);
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
            .AddRemotingPushConsumer(options =>
            {
                options.GroupName = "orders";
                options.ConsumerMode = (ConsumerMode)int.MaxValue;
                options.Subscribe("orders");
                options.MessageHandler = static (_, _) => ValueTask.FromResult(ConsumeResult.Success);
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
            .AddRemotingPushConsumer(options =>
            {
                options.GroupName = "orders";
                options.ConsumerMode = ConsumerMode.Broadcasting;
                options.LocalOffsetStorePath = "invalid\0path";
                options.Subscribe("orders");
                options.MessageHandler = static (_, _) => ValueTask.FromResult(ConsumeResult.Success);
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

    private sealed class TestTimeProvider : TimeProvider;
}
