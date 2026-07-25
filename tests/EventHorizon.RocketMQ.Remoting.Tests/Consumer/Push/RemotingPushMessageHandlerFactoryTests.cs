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

using EventHorizon.RocketMQ.Consumer;
using EventHorizon.RocketMQ.Remoting.Consumer;
using EventHorizon.RocketMQ.Remoting.Consumer.Push;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Xunit;

namespace EventHorizon.RocketMQ.Remoting.Tests.Consumer.Push;

public sealed class RemotingPushMessageHandlerFactoryTests
{
    [Theory]
    [InlineData(ServiceLifetime.Singleton, 1, 0, 1)]
    [InlineData(ServiceLifetime.Scoped, 2, 2, 2)]
    [InlineData(ServiceLifetime.Transient, 2, 2, 2)]
    public async Task Create_UsesConfiguredLifetimeAndDisposesHandlers(
        ServiceLifetime lifetime,
        int expectedCreatedCount,
        int expectedDisposedBeforeProviderDisposal,
        int expectedDisposedAfterProviderDisposal)
    {
        var services = new ServiceCollection();
        var tracker = new MessageHandlerTracker();
        services.AddSingleton(tracker);
        var roleKey = new RemotingRocketMQRoleKey("orders", RemotingRocketMQRole.PushConsumer);
        RemotingPushMessageHandlerFactory.Register<TrackingMessageHandler>(services, roleKey, lifetime);

        var provider = services.BuildServiceProvider(new ServiceProviderOptions { ValidateScopes = true });
        try
        {
            var handleAsync = RemotingPushMessageHandlerFactory.Create(provider, roleKey, lifetime);

            Assert.Equal(ConsumeResult.Success, await handleAsync(CreateMessage(), TestContext.Current.CancellationToken));
            Assert.Equal(ConsumeResult.Success, await handleAsync(CreateMessage(), TestContext.Current.CancellationToken));

            Assert.Equal(expectedCreatedCount, tracker.Created.Count);
            Assert.Equal(expectedDisposedBeforeProviderDisposal, tracker.Disposed.Count);
            Assert.Equal(2, tracker.Handled.Count);
            if (lifetime == ServiceLifetime.Singleton)
            {
                Assert.Same(tracker.Handled[0], tracker.Handled[1]);
            }
            else
            {
                Assert.NotSame(tracker.Handled[0], tracker.Handled[1]);
            }
        }
        finally
        {
            await provider.DisposeAsync();
        }

        Assert.Equal(expectedDisposedAfterProviderDisposal, tracker.Disposed.Count);
    }

    [Fact]
    public async Task AddRemotingPushConsumer_GenericHandlerRegistersAndInvokesTypedHandler()
    {
        var services = new ServiceCollection();
        var tracker = new MessageHandlerTracker();
        services.AddSingleton(tracker);
        services
            .AddRocketMQRemoting(options => options.NamesrvAddr = "127.0.0.1:9876")
            .AddRemotingPushConsumer<TrackingMessageHandler>(
                ServiceLifetime.Transient,
                ConfigurePushConsumer);
        await using var provider = services.BuildServiceProvider(new ServiceProviderOptions { ValidateScopes = true });
        var roleKey = new RemotingRocketMQRoleKey(Options.DefaultName, RemotingRocketMQRole.PushConsumer);

        var handleAsync = RemotingPushMessageHandlerFactory.Create(provider, roleKey, ServiceLifetime.Transient);

        Assert.Equal(ConsumeResult.Success, await handleAsync(CreateMessage(), TestContext.Current.CancellationToken));
        Assert.Single(tracker.Created);
        Assert.Single(tracker.Handled);
        Assert.Single(tracker.Disposed);
    }

    [Fact]
    public async Task AddRemotingPushConsumer_GenericHandlerIsConstructedByServiceProviderWithLogger()
    {
        var services = new ServiceCollection();
        var logger = NullLogger<LoggerInjectingMessageHandler>.Instance;
        var tracker = new LoggerInjectingMessageHandlerTracker();
        services.AddSingleton<ILogger<LoggerInjectingMessageHandler>>(logger);
        services.AddSingleton(tracker);
        services
            .AddRocketMQRemoting(options => options.NamesrvAddr = "127.0.0.1:9876")
            .AddRemotingPushConsumer<LoggerInjectingMessageHandler>(
                ServiceLifetime.Singleton,
                ConfigurePushConsumer);
        await using var provider = services.BuildServiceProvider(new ServiceProviderOptions { ValidateScopes = true });
        var roleKey = new RemotingRocketMQRoleKey(Options.DefaultName, RemotingRocketMQRole.PushConsumer);

        var consumer = provider.GetRequiredService<IRemotingPushConsumer>();

        Assert.IsType<RemotingPushConsumer>(consumer);
        Assert.Same(logger, tracker.Logger);
        Assert.IsType<LoggerInjectingMessageHandler>(
            provider.GetRequiredKeyedService<IRemotingPushMessageHandler>(roleKey));
    }

    [Fact]
    public async Task AddRemotingPushConsumer_SingletonHandlerCannotConsumeScopedDependency()
    {
        var services = new ServiceCollection();
        services.AddSingleton<ScopedDependencyTracker>();
        services.AddScoped<ScopedDependency>();
        services
            .AddRocketMQRemoting(options => options.NamesrvAddr = "127.0.0.1:9876")
            .AddRemotingPushConsumer<SingletonHandlerWithScopedDependency>(
                ServiceLifetime.Singleton,
                ConfigurePushConsumer);
        await using var provider = services.BuildServiceProvider(new ServiceProviderOptions { ValidateScopes = true });

        var exception = Assert.Throws<InvalidOperationException>(
            provider.GetRequiredService<IRemotingPushConsumer>);

        Assert.Contains("Cannot consume scoped service", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task AddRemotingPushConsumer_ScopedHandlerConsumesAndDisposesScopedDependency()
    {
        var services = new ServiceCollection();
        var tracker = new ScopedDependencyTracker();
        services.AddSingleton(tracker);
        services.AddScoped<ScopedDependency>();
        services
            .AddRocketMQRemoting(options => options.NamesrvAddr = "127.0.0.1:9876")
            .AddRemotingPushConsumer<ScopedHandlerWithScopedDependency>(
                ServiceLifetime.Scoped,
                ConfigurePushConsumer);
        await using var provider = services.BuildServiceProvider(new ServiceProviderOptions { ValidateScopes = true });
        var roleKey = new RemotingRocketMQRoleKey(Options.DefaultName, RemotingRocketMQRole.PushConsumer);

        var consumer = provider.GetRequiredService<IRemotingPushConsumer>();
        var handleAsync = RemotingPushMessageHandlerFactory.Create(provider, roleKey, ServiceLifetime.Scoped);

        Assert.IsType<RemotingPushConsumer>(consumer);
        Assert.Empty(tracker.Created);
        Assert.Equal(ConsumeResult.Success, await handleAsync(CreateMessage(), TestContext.Current.CancellationToken));
        var dependency = Assert.Single(tracker.Created);
        Assert.Same(dependency, Assert.Single(tracker.Injected));
        Assert.Same(dependency, Assert.Single(tracker.Handled));
        Assert.Same(dependency, Assert.Single(tracker.Disposed));
    }

    [Fact]
    public async Task AddRemotingPushConsumer_TypedHandlerRejectsDelegateHandler()
    {
        var services = new ServiceCollection();
        services
            .AddRocketMQRemoting(options => options.NamesrvAddr = "127.0.0.1:9876")
            .AddRemotingPushConsumer<TrackingMessageHandler>(
                ServiceLifetime.Singleton,
                options =>
                {
                    ConfigurePushConsumer(options);
                    options.MessageHandler = static (_, _) => ValueTask.FromResult(ConsumeResult.Success);
                });
        await using var provider = services.BuildServiceProvider();

        var exception = Assert.Throws<OptionsValidationException>(
            provider.GetRequiredService<IRemotingPushConsumer>);

        Assert.Contains("MessageHandler cannot be configured", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task AddRemotingPushConsumer_TypedHandlersAreIsolatedByRegistrationName()
    {
        var services = new ServiceCollection();
        var tracker = new MessageHandlerTracker();
        services.AddSingleton(tracker);
        services
            .AddRocketMQRemoting("east", options => options.NamesrvAddr = "127.0.0.1:19876")
            .AddRemotingPushConsumer<TrackingMessageHandler>(
                ServiceLifetime.Singleton,
                ConfigurePushConsumer);
        services
            .AddRocketMQRemoting("west", options => options.NamesrvAddr = "127.0.0.1:29876")
            .AddRemotingPushConsumer<TrackingMessageHandler>(
                ServiceLifetime.Singleton,
                ConfigurePushConsumer);
        await using var provider = services.BuildServiceProvider(new ServiceProviderOptions { ValidateScopes = true });
        var eastRoleKey = new RemotingRocketMQRoleKey("east", RemotingRocketMQRole.PushConsumer);
        var westRoleKey = new RemotingRocketMQRoleKey("west", RemotingRocketMQRole.PushConsumer);

        var east = provider.GetRequiredKeyedService<IRemotingPushMessageHandler>(eastRoleKey);
        var west = provider.GetRequiredKeyedService<IRemotingPushMessageHandler>(westRoleKey);

        Assert.IsType<TrackingMessageHandler>(east);
        Assert.IsType<TrackingMessageHandler>(west);
        Assert.NotSame(east, west);
    }

    private static void ConfigurePushConsumer(RemotingPushConsumerOptions options)
    {
        options.GroupName = "orders";
        options.Subscribe("orders");
    }

    private static RemotingMessageView CreateMessage() =>
        new(
            "orders",
            [],
            "message-id",
            "offset-message-id",
            null,
            [],
            new Dictionary<string, string>(),
            1,
            null,
            0,
            "broker-a",
            0,
            0,
            DateTimeOffset.UnixEpoch,
            DateTimeOffset.UnixEpoch);

    private sealed class MessageHandlerTracker
    {
        public List<TrackingMessageHandler> Created { get; } = [];

        public List<TrackingMessageHandler> Handled { get; } = [];

        public List<TrackingMessageHandler> Disposed { get; } = [];
    }

    private sealed class TrackingMessageHandler : IRemotingPushMessageHandler, IAsyncDisposable
    {
        private readonly MessageHandlerTracker _tracker;

        public TrackingMessageHandler(MessageHandlerTracker tracker)
        {
            _tracker = tracker;
            _tracker.Created.Add(this);
        }

        public ValueTask<ConsumeResult> HandleAsync(RemotingMessageView message, CancellationToken cancellationToken)
        {
            _tracker.Handled.Add(this);
            return ValueTask.FromResult(ConsumeResult.Success);
        }

        public ValueTask DisposeAsync()
        {
            _tracker.Disposed.Add(this);
            return ValueTask.CompletedTask;
        }
    }

    private sealed class LoggerInjectingMessageHandler : IRemotingPushMessageHandler
    {
        public LoggerInjectingMessageHandler(
            ILogger<LoggerInjectingMessageHandler> logger,
            LoggerInjectingMessageHandlerTracker tracker)
        {
            tracker.Logger = logger;
        }

        public ValueTask<ConsumeResult> HandleAsync(RemotingMessageView message, CancellationToken cancellationToken) =>
            ValueTask.FromResult(ConsumeResult.Success);
    }

    private sealed class LoggerInjectingMessageHandlerTracker
    {
        public ILogger<LoggerInjectingMessageHandler>? Logger { get; set; }
    }

    private sealed class SingletonHandlerWithScopedDependency(ScopedDependency dependency) : IRemotingPushMessageHandler
    {
        public ValueTask<ConsumeResult> HandleAsync(RemotingMessageView message, CancellationToken cancellationToken)
        {
            ArgumentNullException.ThrowIfNull(dependency);
            return ValueTask.FromResult(ConsumeResult.Success);
        }
    }

    private sealed class ScopedHandlerWithScopedDependency : IRemotingPushMessageHandler
    {
        private readonly ScopedDependency _dependency;
        private readonly ScopedDependencyTracker _tracker;

        public ScopedHandlerWithScopedDependency(ScopedDependency dependency, ScopedDependencyTracker tracker)
        {
            _dependency = dependency;
            _tracker = tracker;
            _tracker.Injected.Add(_dependency);
        }

        public ValueTask<ConsumeResult> HandleAsync(RemotingMessageView message, CancellationToken cancellationToken)
        {
            _tracker.Handled.Add(_dependency);
            return ValueTask.FromResult(ConsumeResult.Success);
        }
    }

    private sealed class ScopedDependency : IAsyncDisposable
    {
        private readonly ScopedDependencyTracker _tracker;

        public ScopedDependency(ScopedDependencyTracker tracker)
        {
            _tracker = tracker;
            _tracker.Created.Add(this);
        }

        public ValueTask DisposeAsync()
        {
            _tracker.Disposed.Add(this);
            return ValueTask.CompletedTask;
        }
    }

    private sealed class ScopedDependencyTracker
    {
        public List<ScopedDependency> Created { get; } = [];

        public List<ScopedDependency> Injected { get; } = [];

        public List<ScopedDependency> Handled { get; } = [];

        public List<ScopedDependency> Disposed { get; } = [];
    }
}
