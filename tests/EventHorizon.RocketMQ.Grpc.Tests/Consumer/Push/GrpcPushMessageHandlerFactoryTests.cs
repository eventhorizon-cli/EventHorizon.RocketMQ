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
using EventHorizon.RocketMQ.Grpc.Consumer;
using EventHorizon.RocketMQ.Grpc.Consumer.Lite;
using EventHorizon.RocketMQ.Grpc.Consumer.Push;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Xunit;

namespace EventHorizon.RocketMQ.Grpc.Tests.Consumer.Push;

public sealed class GrpcPushMessageHandlerFactoryTests
{
    [Fact]
    public async Task Create_SingletonHandlerReusesInstanceAndDisposesWithProvider()
    {
        var services = new ServiceCollection();
        services.AddSingleton<HandlerLifecycleTracker>();
        var roleKey = new GrpcRocketMQRoleKey("singleton", GrpcRocketMQRole.PushConsumer);
        GrpcPushMessageHandlerFactory.Register<TrackingHandler>(services, roleKey, ServiceLifetime.Singleton);

        HandlerLifecycleTracker tracker;
        await using (var provider = services.BuildServiceProvider(new ServiceProviderOptions { ValidateScopes = true }))
        {
            tracker = provider.GetRequiredService<HandlerLifecycleTracker>();
            var handler = GrpcPushMessageHandlerFactory.Create(provider, roleKey, ServiceLifetime.Singleton);

            Assert.Equal(ConsumeResult.Success, await handler(CreateMessage(), TestContext.Current.CancellationToken));
            Assert.Equal(ConsumeResult.Success, await handler(CreateMessage(), TestContext.Current.CancellationToken));

            Assert.Single(tracker.Created);
            Assert.Equal(2, tracker.Handled.Count);
            Assert.Empty(tracker.Disposed);
        }

        Assert.Equal(tracker.Created, tracker.Disposed);
    }

    [Fact]
    public async Task Create_ScopedHandlerCreatesAndDisposesAnInstancePerMessage()
    {
        var services = new ServiceCollection();
        services.AddSingleton<HandlerLifecycleTracker>();
        var roleKey = new GrpcRocketMQRoleKey("scoped", GrpcRocketMQRole.PushConsumer);
        GrpcPushMessageHandlerFactory.Register<TrackingHandler>(services, roleKey, ServiceLifetime.Scoped);

        await using var provider = services.BuildServiceProvider(new ServiceProviderOptions { ValidateScopes = true });
        var tracker = provider.GetRequiredService<HandlerLifecycleTracker>();
        var handler = GrpcPushMessageHandlerFactory.Create(provider, roleKey, ServiceLifetime.Scoped);

        Assert.Equal(ConsumeResult.Success, await handler(CreateMessage(), TestContext.Current.CancellationToken));
        Assert.Equal(ConsumeResult.Success, await handler(CreateMessage(), TestContext.Current.CancellationToken));

        Assert.Equal(2, tracker.Created.Count);
        Assert.Equal(tracker.Created, tracker.Handled);
        Assert.Equal(tracker.Created, tracker.Disposed);
    }

    [Fact]
    public async Task Create_TransientHandlerCreatesAndDisposesAnInstancePerMessage()
    {
        var services = new ServiceCollection();
        services.AddSingleton<HandlerLifecycleTracker>();
        var roleKey = new GrpcRocketMQRoleKey("transient", GrpcRocketMQRole.PushConsumer);
        GrpcPushMessageHandlerFactory.Register<TrackingHandler>(services, roleKey, ServiceLifetime.Transient);

        await using var provider = services.BuildServiceProvider(new ServiceProviderOptions { ValidateScopes = true });
        var tracker = provider.GetRequiredService<HandlerLifecycleTracker>();
        var handler = GrpcPushMessageHandlerFactory.Create(provider, roleKey, ServiceLifetime.Transient);

        Assert.Equal(ConsumeResult.Success, await handler(CreateMessage(), TestContext.Current.CancellationToken));
        Assert.Equal(ConsumeResult.Success, await handler(CreateMessage(), TestContext.Current.CancellationToken));

        Assert.Equal(2, tracker.Created.Count);
        Assert.Equal(tracker.Created, tracker.Handled);
        Assert.Equal(tracker.Created, tracker.Disposed);
    }

    [Fact]
    public async Task AddGrpcPushAndLitePushConsumer_RegisterTypedHandlersForTheirRoles()
    {
        var services = new ServiceCollection();
        services
            .AddRocketMQGrpc(options => options.Endpoint = "127.0.0.1:8081")
            .AddGrpcPushConsumer<PushHandler>(ServiceLifetime.Singleton, ConfigurePushConsumer)
            .AddGrpcLitePushConsumer<LitePushHandler>(ServiceLifetime.Singleton, ConfigureLitePushConsumer);

        await using var provider = services.BuildServiceProvider(new ServiceProviderOptions { ValidateOnBuild = true });
        var pushRoleKey = new GrpcRocketMQRoleKey(Options.DefaultName, GrpcRocketMQRole.PushConsumer);
        var litePushRoleKey = new GrpcRocketMQRoleKey(Options.DefaultName, GrpcRocketMQRole.LitePushConsumer);
        var pushHandler = provider.GetRequiredKeyedService<IGrpcPushMessageHandler>(pushRoleKey);
        var litePushHandler = provider.GetRequiredKeyedService<IGrpcPushMessageHandler>(litePushRoleKey);

        Assert.IsType<PushHandler>(pushHandler);
        Assert.IsType<LitePushHandler>(litePushHandler);
        Assert.IsType<GrpcPushConsumer>(provider.GetRequiredService<IGrpcPushConsumer>());
        Assert.IsType<GrpcLitePushConsumer>(provider.GetRequiredService<IGrpcLitePushConsumer>());
    }

    [Fact]
    public async Task AddGrpcPushConsumer_TypedHandlerIsConstructedByServiceProviderWithLogger()
    {
        var services = new ServiceCollection();
        var logger = NullLogger<LoggerInjectingPushHandler>.Instance;
        services.AddSingleton<ILogger<LoggerInjectingPushHandler>>(logger);
        services
            .AddRocketMQGrpc(options => options.Endpoint = "127.0.0.1:8081")
            .AddGrpcPushConsumer<LoggerInjectingPushHandler>(ServiceLifetime.Singleton, ConfigurePushConsumer);

        await using var provider = services.BuildServiceProvider(new ServiceProviderOptions { ValidateOnBuild = true });
        var consumer = provider.GetRequiredService<IGrpcPushConsumer>();
        var handler = provider.GetRequiredKeyedService<IGrpcPushMessageHandler>(
            new GrpcRocketMQRoleKey(Options.DefaultName, GrpcRocketMQRole.PushConsumer));

        Assert.IsType<GrpcPushConsumer>(consumer);
        Assert.Same(logger, Assert.IsType<LoggerInjectingPushHandler>(handler).Logger);
    }

    [Fact]
    public async Task AddGrpcPushConsumer_SingletonHandlerCannotConsumeScopedDependency()
    {
        var services = new ServiceCollection();
        services.AddSingleton<ScopedDependencyTracker>();
        services.AddScoped<ScopedDependency>();
        services
            .AddRocketMQGrpc(options => options.Endpoint = "127.0.0.1:8081")
            .AddGrpcPushConsumer<SingletonHandlerWithScopedDependency>(ServiceLifetime.Singleton, ConfigurePushConsumer);

        await using var provider = services.BuildServiceProvider(new ServiceProviderOptions { ValidateScopes = true });

        var exception = Assert.Throws<InvalidOperationException>(() => provider.GetRequiredService<IGrpcPushConsumer>());

        Assert.Contains("Cannot consume scoped service", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task AddGrpcPushConsumer_ScopedHandlerConsumesAndDisposesScopedDependencyPerInvocation()
    {
        var services = new ServiceCollection();
        services.AddSingleton<ScopedDependencyTracker>();
        services.AddScoped<ScopedDependency>();
        services
            .AddRocketMQGrpc(options => options.Endpoint = "127.0.0.1:8081")
            .AddGrpcPushConsumer<ScopedHandlerWithScopedDependency>(ServiceLifetime.Scoped, ConfigurePushConsumer);

        await using var provider = services.BuildServiceProvider(new ServiceProviderOptions { ValidateScopes = true });
        var consumer = provider.GetRequiredService<IGrpcPushConsumer>();
        var tracker = provider.GetRequiredService<ScopedDependencyTracker>();
        var roleKey = new GrpcRocketMQRoleKey(Options.DefaultName, GrpcRocketMQRole.PushConsumer);
        var handler = GrpcPushMessageHandlerFactory.Create(provider, roleKey, ServiceLifetime.Scoped);

        Assert.IsType<GrpcPushConsumer>(consumer);
        Assert.Equal(ConsumeResult.Success, await handler(CreateMessage(), TestContext.Current.CancellationToken));
        Assert.Single(tracker.Created);
        Assert.Equal(tracker.Created, tracker.Injected);
        Assert.Equal(tracker.Created, tracker.Handled);
        Assert.Equal(tracker.Created, tracker.Disposed);
    }

    [Fact]
    public async Task AddGrpcPushConsumer_TypedHandlerRejectsDelegateConfiguration()
    {
        var services = new ServiceCollection();
        services
            .AddRocketMQGrpc(options => options.Endpoint = "127.0.0.1:8081")
            .AddGrpcPushConsumer<PushHandler>(ServiceLifetime.Singleton, options =>
            {
                ConfigurePushConsumer(options);
                options.MessageHandler = static (_, _) => ValueTask.FromResult(ConsumeResult.Success);
            });
        await using var provider = services.BuildServiceProvider();

        var exception = Assert.Throws<OptionsValidationException>(
            () => provider.GetRequiredService<IGrpcPushConsumer>());

        Assert.Contains("MessageHandler cannot be configured", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task AddGrpcLitePushConsumer_TypedHandlerRejectsDelegateConfiguration()
    {
        var services = new ServiceCollection();
        services
            .AddRocketMQGrpc(options => options.Endpoint = "127.0.0.1:8081")
            .AddGrpcLitePushConsumer<LitePushHandler>(ServiceLifetime.Singleton, options =>
            {
                ConfigureLitePushConsumer(options);
                options.MessageHandler = static (_, _) => ValueTask.FromResult(ConsumeResult.Success);
            });
        await using var provider = services.BuildServiceProvider();

        var exception = Assert.Throws<OptionsValidationException>(
            () => provider.GetRequiredService<IGrpcLitePushConsumer>());

        Assert.Contains("MessageHandler cannot be configured", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task NamedProfiles_IsolateTypedPushHandlersByRoleKey()
    {
        var services = new ServiceCollection();
        services
            .AddRocketMQGrpc("east", options => options.Endpoint = "127.0.0.1:8081")
            .AddGrpcPushConsumer<EastPushHandler>(ServiceLifetime.Singleton, ConfigurePushConsumer);
        services
            .AddRocketMQGrpc("west", options => options.Endpoint = "127.0.0.1:8082")
            .AddGrpcPushConsumer<WestPushHandler>(ServiceLifetime.Singleton, ConfigurePushConsumer);

        await using var provider = services.BuildServiceProvider(new ServiceProviderOptions { ValidateOnBuild = true });
        var eastHandler = provider.GetRequiredKeyedService<IGrpcPushMessageHandler>(
            new GrpcRocketMQRoleKey("east", GrpcRocketMQRole.PushConsumer));
        var westHandler = provider.GetRequiredKeyedService<IGrpcPushMessageHandler>(
            new GrpcRocketMQRoleKey("west", GrpcRocketMQRole.PushConsumer));

        Assert.IsType<EastPushHandler>(eastHandler);
        Assert.IsType<WestPushHandler>(westHandler);
        Assert.IsType<GrpcPushConsumer>(provider.GetRequiredKeyedService<IGrpcPushConsumer>("east"));
        Assert.IsType<GrpcPushConsumer>(provider.GetRequiredKeyedService<IGrpcPushConsumer>("west"));
    }

    private static void ConfigurePushConsumer(GrpcPushConsumerOptions options)
    {
        options.GroupName = "handler-tests";
        options.Subscribe("orders");
    }

    private static void ConfigureLitePushConsumer(GrpcLitePushConsumerOptions options)
    {
        options.GroupName = "handler-tests";
        options.BindTopic = "orders";
        options.LiteTopics.Add("orders-lite");
    }

    private static GrpcMessageView CreateMessage() => new(
        "orders",
        [],
        "message-id",
        "receipt-handle",
        null,
        [],
        new Dictionary<string, string>(),
        1,
        null,
        null,
        null,
        0,
        "broker-a",
        null,
        null,
        null,
        false,
        null,
        new Uri("http://127.0.0.1:8081"));

    private sealed class HandlerLifecycleTracker
    {
        public List<Guid> Created { get; } = [];

        public List<Guid> Handled { get; } = [];

        public List<Guid> Disposed { get; } = [];
    }

    private sealed class TrackingHandler : IGrpcPushMessageHandler, IAsyncDisposable
    {
        private readonly Guid _id = Guid.NewGuid();
        private readonly HandlerLifecycleTracker _tracker;

        public TrackingHandler(HandlerLifecycleTracker tracker)
        {
            _tracker = tracker;
            _tracker.Created.Add(_id);
        }

        public ValueTask<ConsumeResult> HandleAsync(GrpcMessageView message, CancellationToken cancellationToken)
        {
            _tracker.Handled.Add(_id);
            return ValueTask.FromResult(ConsumeResult.Success);
        }

        public ValueTask DisposeAsync()
        {
            _tracker.Disposed.Add(_id);
            return ValueTask.CompletedTask;
        }
    }

    private sealed class PushHandler : IGrpcPushMessageHandler
    {
        public ValueTask<ConsumeResult> HandleAsync(GrpcMessageView message, CancellationToken cancellationToken) =>
            ValueTask.FromResult(ConsumeResult.Success);
    }

    private sealed class LitePushHandler : IGrpcPushMessageHandler
    {
        public ValueTask<ConsumeResult> HandleAsync(GrpcMessageView message, CancellationToken cancellationToken) =>
            ValueTask.FromResult(ConsumeResult.Success);
    }

    private sealed class LoggerInjectingPushHandler(ILogger<LoggerInjectingPushHandler> logger)
        : IGrpcPushMessageHandler
    {
        public ILogger<LoggerInjectingPushHandler> Logger { get; } = logger;

        public ValueTask<ConsumeResult> HandleAsync(GrpcMessageView message, CancellationToken cancellationToken) =>
            ValueTask.FromResult(ConsumeResult.Success);
    }

    private sealed class SingletonHandlerWithScopedDependency : IGrpcPushMessageHandler
    {
        public SingletonHandlerWithScopedDependency(ScopedDependency dependency)
        {
            ArgumentNullException.ThrowIfNull(dependency);
        }

        public ValueTask<ConsumeResult> HandleAsync(GrpcMessageView message, CancellationToken cancellationToken) =>
            ValueTask.FromResult(ConsumeResult.Success);
    }

    private sealed class ScopedHandlerWithScopedDependency : IGrpcPushMessageHandler
    {
        private readonly ScopedDependency _dependency;
        private readonly ScopedDependencyTracker _tracker;

        public ScopedHandlerWithScopedDependency(ScopedDependency dependency, ScopedDependencyTracker tracker)
        {
            _dependency = dependency;
            _tracker = tracker;
            _tracker.Injected.Add(_dependency.Id);
        }

        public ValueTask<ConsumeResult> HandleAsync(GrpcMessageView message, CancellationToken cancellationToken)
        {
            _tracker.Handled.Add(_dependency.Id);
            return ValueTask.FromResult(ConsumeResult.Success);
        }
    }

    private sealed class ScopedDependency : IAsyncDisposable
    {
        private readonly ScopedDependencyTracker _tracker;

        public ScopedDependency(ScopedDependencyTracker tracker)
        {
            _tracker = tracker;
            _tracker.Created.Add(Id);
        }

        public Guid Id { get; } = Guid.NewGuid();

        public ValueTask DisposeAsync()
        {
            _tracker.Disposed.Add(Id);
            return ValueTask.CompletedTask;
        }
    }

    private sealed class ScopedDependencyTracker
    {
        public List<Guid> Created { get; } = [];

        public List<Guid> Injected { get; } = [];

        public List<Guid> Handled { get; } = [];

        public List<Guid> Disposed { get; } = [];
    }

    private sealed class EastPushHandler : IGrpcPushMessageHandler
    {
        public ValueTask<ConsumeResult> HandleAsync(GrpcMessageView message, CancellationToken cancellationToken) =>
            ValueTask.FromResult(ConsumeResult.Success);
    }

    private sealed class WestPushHandler : IGrpcPushMessageHandler
    {
        public ValueTask<ConsumeResult> HandleAsync(GrpcMessageView message, CancellationToken cancellationToken) =>
            ValueTask.FromResult(ConsumeResult.Success);
    }
}
