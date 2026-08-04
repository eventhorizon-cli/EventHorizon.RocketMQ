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

using EventHorizon.RocketMQ.Grpc.Consumer;
using EventHorizon.RocketMQ.Grpc.Consumer.LitePush;
using EventHorizon.RocketMQ.Grpc.Consumer.Push;
using EventHorizon.RocketMQ.Grpc.Consumer.Simple;
using EventHorizon.RocketMQ.Grpc.Producer;
using EventHorizon.RocketMQ.Grpc.Protocol;
using EventHorizon.RocketMQ.Grpc.Protocol.Route;
using EventHorizon.RocketMQ.Grpc.Protocol.Telemetry;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Proto = Apache.Rocketmq.V2;

namespace EventHorizon.RocketMQ.Grpc;

/// <summary>
/// Provides methods for adding producer and consumer roles to a RocketMQ gRPC client registration.
/// </summary>
public static class GrpcRocketMQBuilderExtensions
{
    /// <summary>
    /// Adds a gRPC producer role with default producer options.
    /// </summary>
    /// <param name="builder">The builder for the RocketMQ gRPC client registration.</param>
    /// <returns>The same builder so that additional roles can be configured.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="builder"/> is <see langword="null"/>.</exception>
    /// <exception cref="InvalidOperationException">
    /// A producer is already registered for this client registration.
    /// </exception>
    public static GrpcRocketMQBuilder AddGrpcProducer(this GrpcRocketMQBuilder builder) =>
        AddGrpcProducer(builder, static _ => { });

    /// <summary>
    /// Adds a configured gRPC producer role.
    /// </summary>
    /// <param name="builder">The builder for the RocketMQ gRPC client registration.</param>
    /// <param name="configure">The delegate used to configure the producer.</param>
    /// <returns>The same builder so that additional roles can be configured.</returns>
    /// <exception cref="ArgumentNullException">
    /// <paramref name="builder"/> or <paramref name="configure"/> is <see langword="null"/>.
    /// </exception>
    /// <exception cref="InvalidOperationException">
    /// A producer is already registered for this client registration.
    /// </exception>
    public static GrpcRocketMQBuilder AddGrpcProducer(
        this GrpcRocketMQBuilder builder,
        Action<GrpcProducerOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(configure);
        var registration = GrpcRocketMQRegistration.RegisterRole(builder, GrpcRocketMQRole.Producer);
        var roleKey = registration.RoleKey;
        RegisterGrpcTransportServices(builder.Services, roleKey);

        builder.Services.AddOptions<GrpcProducerOptions>(registration.RoleOptionsName)
            .Configure(configure)
            .Validate(static options => options.MaxMessageSize > 0, "Maximum message size must be positive.")
            .Validate(static options => options.SendMsgTimeout > TimeSpan.Zero, "Send timeout must be positive.")
            .Validate(
                static options => options.Topics.All(static topic => !string.IsNullOrWhiteSpace(topic)),
                "Publishing topics cannot be blank.")
            .Validate(
                static options => options.MaxConcurrentTransactionChecks > 0,
                "Maximum concurrent transaction checks must be positive.");
        GrpcRocketMQRegistration.AddRoleSingleton<IGrpcProducer>(
            builder,
            provider => CreateGrpcProducer(provider, registration));
        builder.Services.AddSingleton<IHostedService>(provider =>
            ActivatorUtilities.CreateInstance<GrpcProducerHostedService>(
                provider,
                GrpcRocketMQRegistration.GetRoleService<IGrpcProducer>(provider, builder.RegistrationName)));
        return builder;
    }

    /// <summary>
    /// Adds a configured gRPC simple-consumer role.
    /// </summary>
    /// <param name="builder">The builder for the RocketMQ gRPC client registration.</param>
    /// <param name="configure">The delegate used to configure the simple consumer.</param>
    /// <returns>The same builder so that additional roles can be configured.</returns>
    /// <exception cref="ArgumentNullException">
    /// <paramref name="builder"/> or <paramref name="configure"/> is <see langword="null"/>.
    /// </exception>
    public static GrpcRocketMQBuilder AddGrpcSimpleConsumer(
        this GrpcRocketMQBuilder builder,
        Action<GrpcSimpleConsumerOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(configure);
        var registration = GrpcRocketMQRegistration.RegisterRole(builder, GrpcRocketMQRole.SimpleConsumer);
        var roleKey = registration.RoleKey;
        RegisterGrpcTransportServices(builder.Services, roleKey);

        builder.Services.AddOptions<GrpcSimpleConsumerOptions>(registration.RoleOptionsName)
            .Configure(configure)
            .Validate(
                static options => !string.IsNullOrWhiteSpace(options.GroupName),
                "Consumer group name is required.")
            .Validate(static options => options.AwaitDuration > TimeSpan.Zero, "Await duration must be positive.")
            .Validate(
                static options => options.InvisibleDuration > TimeSpan.Zero,
                "Invisible duration must be positive.");
        var getConsumer = GrpcRocketMQRegistration.AddConsumerSingleton<IGrpcSimpleConsumer>(
            builder,
            provider => CreateGrpcSimpleConsumer(provider, registration));
        builder.Services.AddSingleton<IHostedService>(provider =>
            ActivatorUtilities.CreateInstance<GrpcSimpleConsumerHostedService>(
                provider,
                getConsumer(provider)));
        return builder;
    }

    /// <summary>
    /// Adds a configured gRPC push-consumer role with a dependency-injected message handler.
    /// </summary>
    /// <typeparam name="TMessageHandler">The type that processes received messages.</typeparam>
    /// <param name="builder">The builder for the RocketMQ gRPC client registration.</param>
    /// <param name="handlerLifetime">The dependency-injection lifetime used for the message handler.</param>
    /// <param name="configure">The delegate used to configure the push consumer.</param>
    /// <returns>The same builder so that additional roles can be configured.</returns>
    /// <exception cref="ArgumentNullException">
    /// <paramref name="builder"/> or <paramref name="configure"/> is <see langword="null"/>.
    /// </exception>
    /// <exception cref="ArgumentOutOfRangeException">
    /// <paramref name="handlerLifetime"/> is not a supported service lifetime.
    /// </exception>
    public static GrpcRocketMQBuilder AddGrpcPushConsumer<TMessageHandler>(
        this GrpcRocketMQBuilder builder,
        ServiceLifetime handlerLifetime,
        Action<GrpcPushConsumerOptions> configure)
        where TMessageHandler : class, IGrpcPushMessageHandler
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(configure);
        ValidateMessageHandlerLifetime(handlerLifetime);

        return AddGrpcPushConsumerCore<TMessageHandler>(builder, handlerLifetime, configure);
    }

    /// <summary>
    /// Adds a configured gRPC Lite Push consumer role with a dependency-injected message handler.
    /// </summary>
    /// <typeparam name="TMessageHandler">The type that processes received messages.</typeparam>
    /// <param name="builder">The builder for the RocketMQ gRPC client registration.</param>
    /// <param name="handlerLifetime">The dependency-injection lifetime used for the message handler.</param>
    /// <param name="configure">The delegate used to configure the Lite Push consumer.</param>
    /// <returns>The same builder so that additional roles can be configured.</returns>
    /// <exception cref="ArgumentNullException">
    /// <paramref name="builder"/> or <paramref name="configure"/> is <see langword="null"/>.
    /// </exception>
    /// <exception cref="ArgumentOutOfRangeException">
    /// <paramref name="handlerLifetime"/> is not a supported service lifetime.
    /// </exception>
    public static GrpcRocketMQBuilder AddGrpcLitePushConsumer<TMessageHandler>(
        this GrpcRocketMQBuilder builder,
        ServiceLifetime handlerLifetime,
        Action<GrpcLitePushConsumerOptions> configure)
        where TMessageHandler : class, IGrpcPushMessageHandler
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(configure);
        ValidateMessageHandlerLifetime(handlerLifetime);

        return AddGrpcLitePushConsumerCore<TMessageHandler>(builder, handlerLifetime, configure);
    }

    private static GrpcRocketMQBuilder AddGrpcPushConsumerCore<TMessageHandler>(
        GrpcRocketMQBuilder builder,
        ServiceLifetime handlerLifetime,
        Action<GrpcPushConsumerOptions> configure)
        where TMessageHandler : class, IGrpcPushMessageHandler
    {
        var registration = GrpcRocketMQRegistration.RegisterRole(builder, GrpcRocketMQRole.PushConsumer);
        var roleKey = registration.RoleKey;
        RegisterGrpcTransportServices(builder.Services, roleKey);
        GrpcPushMessageHandlerFactory.Register<TMessageHandler>(builder.Services, roleKey, handlerLifetime);

        builder.Services.AddOptions<GrpcPushConsumerOptions>(registration.RoleOptionsName)
            .Configure(configure)
            .Validate(
                static value => !string.IsNullOrWhiteSpace(value.GroupName),
                "Consumer group name is required.")
            .Validate(static value => value.MaxConcurrency > 0, "Maximum concurrency must be positive.")
            .Validate(static value => value.BatchSize > 0, "Batch size must be positive.")
            .Validate(static value => value.MaxCachedMessages > 0, "Maximum cached message count must be positive.")
            .Validate(
                static value => value.MaxCachedMessageBytes > 0,
                "Maximum cached message bytes must be positive.")
            .Validate(static value => value.MaxDeliveryAttempts > 0, "Maximum delivery attempts must be positive.")
            .Validate(
                static value => value.InvisibleDuration > TimeSpan.Zero,
                "Invisible duration must be positive.")
            .Validate(static value => value.ConsumeTimeout > TimeSpan.Zero, "Consume timeout must be positive.")
            .Validate(
                static value => value.LongPollingTimeout > TimeSpan.Zero,
                "Long polling timeout must be positive.")
            .Validate(static value => value.RetryDelay > TimeSpan.Zero, "Retry delay must be positive.");

        var getConsumer = GrpcRocketMQRegistration.AddConsumerSingleton<IGrpcPushConsumer>(
            builder,
            provider => CreateGrpcPushConsumer(provider, registration, handlerLifetime));
        builder.Services.AddSingleton<IHostedService>(provider =>
            ActivatorUtilities.CreateInstance<GrpcPushConsumerHostedService>(
                provider,
                getConsumer(provider)));
        return builder;
    }

    private static GrpcRocketMQBuilder AddGrpcLitePushConsumerCore<TMessageHandler>(
        GrpcRocketMQBuilder builder,
        ServiceLifetime handlerLifetime,
        Action<GrpcLitePushConsumerOptions> configure)
        where TMessageHandler : class, IGrpcPushMessageHandler
    {
        var registration = GrpcRocketMQRegistration.RegisterRole(builder, GrpcRocketMQRole.LitePushConsumer);
        var roleKey = registration.RoleKey;
        RegisterGrpcTransportServices(builder.Services, roleKey);
        GrpcPushMessageHandlerFactory.Register<TMessageHandler>(builder.Services, roleKey, handlerLifetime);

        builder.Services.AddOptions<GrpcLitePushConsumerOptions>(registration.RoleOptionsName)
            .Configure(configure)
            .Validate(
                static value => !string.IsNullOrWhiteSpace(value.GroupName),
                "Consumer group name is required.")
            .Validate(static value => !string.IsNullOrWhiteSpace(value.BindTopic), "Bind topic is required.")
            .Validate(
                static value => value.Subscriptions.Count == 0,
                "Lite Push consumers use BindTopic and cannot use standard topic subscriptions.")
            .Validate(
                static value => value.LiteTopics.All(static topic => !string.IsNullOrWhiteSpace(topic)),
                "LiteTopic names cannot be blank.")
            .Validate(static value => value.MaxConcurrency > 0, "Maximum concurrency must be positive.")
            .Validate(static value => value.BatchSize > 0, "Batch size must be positive.")
            .Validate(static value => value.MaxCachedMessages > 0, "Maximum cached message count must be positive.")
            .Validate(
                static value => value.MaxCachedMessageBytes > 0,
                "Maximum cached message bytes must be positive.")
            .Validate(static value => value.MaxDeliveryAttempts > 0, "Maximum delivery attempts must be positive.")
            .Validate(
                static value => value.InvisibleDuration > TimeSpan.Zero,
                "Invisible duration must be positive.")
            .Validate(static value => value.ConsumeTimeout > TimeSpan.Zero, "Consume timeout must be positive.")
            .Validate(
                static value => value.LongPollingTimeout > TimeSpan.Zero,
                "Long polling timeout must be positive.")
            .Validate(static value => value.RetryDelay > TimeSpan.Zero, "Retry delay must be positive.")
            .Validate(
                static value => value.SubscriptionSyncInterval > TimeSpan.Zero,
                "Lite subscription sync interval must be positive.");

        var getConsumer = GrpcRocketMQRegistration.AddConsumerSingleton<IGrpcLitePushConsumer>(
            builder,
            provider => CreateGrpcLitePushConsumer(provider, registration, handlerLifetime));
        builder.Services.AddSingleton<IHostedService>(provider =>
            ActivatorUtilities.CreateInstance<GrpcLitePushConsumerHostedService>(
                provider,
                getConsumer(provider)));
        return builder;
    }

    private static void ValidateMessageHandlerLifetime(ServiceLifetime handlerLifetime)
    {
        if (!Enum.IsDefined(handlerLifetime))
        {
            throw new ArgumentOutOfRangeException(
                nameof(handlerLifetime),
                handlerLifetime,
                "Unsupported message handler lifetime.");
        }
    }

    private static IGrpcProducer CreateGrpcProducer(
        IServiceProvider provider,
        GrpcRocketMQRoleRegistration registration)
    {
        var roleKey = registration.RoleKey;
        return ActivatorUtilities.CreateInstance<GrpcProducer>(
            provider,
            GrpcRocketMQRegistration.GetNamedOptions<GrpcProducerOptions>(provider, registration.RoleOptionsName),
            GrpcRocketMQRegistration.GetClientOptions(provider, roleKey),
            provider.GetRequiredKeyedService<IRocketMQGrpcClient>(roleKey),
            provider.GetRequiredKeyedService<IGrpcRouteService>(roleKey),
            provider.GetRequiredService<ILogger<GrpcProducer>>(),
            provider.GetRequiredService<ILogger<GrpcSessionManager>>());
    }

    private static IGrpcSimpleConsumer CreateGrpcSimpleConsumer(
        IServiceProvider provider,
        GrpcRocketMQRoleRegistration registration)
    {
        var roleKey = registration.RoleKey;
        var options = GrpcRocketMQRegistration.GetNamedOptions<GrpcSimpleConsumerOptions>(
            provider,
            registration.RoleOptionsName);
        var engine = CreateGrpcReceiveConsumerEngine(
            provider,
            roleKey,
            options.Value.GroupName,
            options.Value.Subscriptions,
            Proto.ClientType.SimpleConsumer,
            options.Value.AwaitDuration);
        return ActivatorUtilities.CreateInstance<GrpcSimpleConsumer>(provider, options, engine);
    }

    private static IGrpcPushConsumer CreateGrpcPushConsumer(
        IServiceProvider provider,
        GrpcRocketMQRoleRegistration registration,
        ServiceLifetime handlerLifetime)
    {
        var roleKey = registration.RoleKey;
        var options = GrpcRocketMQRegistration.GetNamedOptions<GrpcPushConsumerOptions>(
            provider,
            registration.RoleOptionsName);
        var hasLocalGroupPeer = GrpcConsumerGroupCompatibilityValidator.ValidatePushConsumerSubscriptions(
            provider,
            registration,
            options.Value);
        var engine = CreateGrpcReceiveConsumerEngine(
            provider,
            roleKey,
            options.Value.GroupName,
            options.Value.Subscriptions,
            Proto.ClientType.PushConsumer,
            options.Value.LongPollingTimeout);
        var logger = provider.GetRequiredService<ILogger<GrpcPushConsumer>>();
        return ActivatorUtilities.CreateInstance<GrpcPushConsumer>(
            provider,
            options,
            engine,
            logger,
            GrpcPushMessageHandlerFactory.Create(provider, roleKey, handlerLifetime),
            hasLocalGroupPeer);
    }

    private static IGrpcLitePushConsumer CreateGrpcLitePushConsumer(
        IServiceProvider provider,
        GrpcRocketMQRoleRegistration registration,
        ServiceLifetime handlerLifetime)
    {
        var roleKey = registration.RoleKey;
        var options = GrpcRocketMQRegistration.GetNamedOptions<GrpcLitePushConsumerOptions>(
            provider,
            registration.RoleOptionsName);
        GrpcConsumerGroupCompatibilityValidator.ValidateLitePushConsumerBindTopic(
            provider,
            registration,
            options.Value);
        var clientOptions = GrpcRocketMQRegistration.GetClientOptions(provider, roleKey);
        var client = provider.GetRequiredKeyedService<IRocketMQGrpcClient>(roleKey);
        var routes = provider.GetRequiredKeyedService<IGrpcRouteService>(roleKey);
        var managerLogger = provider.GetRequiredService<ILogger<GrpcLiteSubscriptionManager>>();
        var subscriptions = new Dictionary<string, FilterExpression>(StringComparer.Ordinal)
        {
            [options.Value.BindTopic] = FilterExpression.All
        };
        var manager = ActivatorUtilities.CreateInstance<GrpcLiteSubscriptionManager>(
            provider,
            options,
            clientOptions,
            client,
            routes,
            managerLogger);
        var engine = CreateGrpcReceiveConsumerEngine(
            provider,
            roleKey,
            options.Value.GroupName,
            subscriptions,
            Proto.ClientType.LitePushConsumer,
            options.Value.LongPollingTimeout,
            manager.HandleTelemetryCommandAsync);
        var pushOptions = Options.Create<GrpcPushConsumerOptions>(options.Value);
        var pushLogger = provider.GetRequiredService<ILogger<GrpcPushConsumer>>();
        var dispatcher = ActivatorUtilities.CreateInstance<GrpcPushConsumer>(
            provider,
            pushOptions,
            engine,
            pushLogger,
            GrpcPushMessageHandlerFactory.Create(provider, roleKey, handlerLifetime),
            false);
        return ActivatorUtilities.CreateInstance<GrpcLitePushConsumer>(provider, dispatcher, manager);
    }

    private static IGrpcReceiveConsumerEngine CreateGrpcReceiveConsumerEngine(
        IServiceProvider provider,
        GrpcRocketMQRoleKey roleKey,
        string groupName,
        IReadOnlyDictionary<string, FilterExpression> subscriptions,
        Proto.ClientType clientType,
        TimeSpan longPollingTimeout,
        Func<Uri, Proto.TelemetryCommand, CancellationToken, Task>? telemetryHandler = null)
    {
        var logger = provider.GetRequiredService<ILogger<GrpcReceiveConsumerEngine>>();
        var sessionLogger = provider.GetRequiredService<ILogger<GrpcSessionManager>>();
        if (telemetryHandler is null)
        {
            return ActivatorUtilities.CreateInstance<GrpcReceiveConsumerEngine>(
                provider,
                provider.GetRequiredKeyedService<IRocketMQGrpcClient>(roleKey),
                provider.GetRequiredKeyedService<IGrpcRouteService>(roleKey),
                GrpcRocketMQRegistration.GetClientOptions(provider, roleKey),
                groupName,
                subscriptions,
                clientType,
                longPollingTimeout,
                logger,
                sessionLogger);
        }

        return ActivatorUtilities.CreateInstance<GrpcReceiveConsumerEngine>(
            provider,
            provider.GetRequiredKeyedService<IRocketMQGrpcClient>(roleKey),
            provider.GetRequiredKeyedService<IGrpcRouteService>(roleKey),
            GrpcRocketMQRegistration.GetClientOptions(provider, roleKey),
            groupName,
            subscriptions,
            clientType,
            longPollingTimeout,
            logger,
            sessionLogger,
            telemetryHandler);
    }

    private static void RegisterGrpcTransportServices(
        IServiceCollection services,
        GrpcRocketMQRoleKey roleKey)
    {
        services.TryAddKeyedSingleton<GrpcChannelPool>(roleKey.OptionsName);
        services.AddKeyedSingleton<GrpcMetadataFactory>(roleKey, (provider, _) =>
            new GrpcMetadataFactory(
                GrpcRocketMQRegistration.GetClientOptions(provider, roleKey),
                roleKey.LogicalClientName));
        services.AddKeyedSingleton<IRocketMQGrpcClient>(roleKey, (provider, _) =>
            new RocketMQGrpcClient(
                GrpcRocketMQRegistration.GetClientOptions(provider, roleKey),
                provider.GetRequiredKeyedService<GrpcMetadataFactory>(roleKey),
                provider.GetRequiredKeyedService<GrpcChannelPool>(roleKey.OptionsName)));
        services.AddKeyedSingleton<IGrpcRouteService>(roleKey, (provider, _) =>
            new GrpcRouteService(
                provider.GetRequiredKeyedService<IRocketMQGrpcClient>(roleKey),
                GrpcRocketMQRegistration.GetClientOptions(provider, roleKey),
                provider.GetRequiredService<TimeProvider>()));
    }
}
