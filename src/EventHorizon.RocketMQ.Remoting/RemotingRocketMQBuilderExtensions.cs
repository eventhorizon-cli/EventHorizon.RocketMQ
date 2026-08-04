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

using EventHorizon.RocketMQ.Remoting.Admin;
using EventHorizon.RocketMQ.Remoting.Consumer;
using EventHorizon.RocketMQ.Remoting.Consumer.Coordination.Rebalance;
using EventHorizon.RocketMQ.Remoting.Consumer.LitePull;
using EventHorizon.RocketMQ.Remoting.Consumer.Push;
using EventHorizon.RocketMQ.Remoting.Consumer.Push.Pull.Offset;
using EventHorizon.RocketMQ.Remoting.Consumer.Route;
using EventHorizon.RocketMQ.Remoting.Producer;
using EventHorizon.RocketMQ.Remoting.Protocol;
using EventHorizon.RocketMQ.Remoting.Protocol.Route;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;

namespace EventHorizon.RocketMQ.Remoting;

/// <summary>
/// Provides role-registration methods for <see cref="RemotingRocketMQBuilder"/>.
/// </summary>
public static class RemotingRocketMQBuilderExtensions
{
    /// <summary>
    /// Adds a read-only classic remoting administration role.
    /// </summary>
    /// <param name="builder">The classic remoting client builder.</param>
    /// <returns>The same builder so that additional roles can be registered.</returns>
    public static RemotingRocketMQBuilder AddRemotingAdmin(this RemotingRocketMQBuilder builder)
    {
        ArgumentNullException.ThrowIfNull(builder);
        var roleKey = RemotingRocketMQRegistration.RegisterRole(builder, RemotingRocketMQRole.Admin).RoleKey;
        RegisterRemotingTransportServices(builder.Services, roleKey);
        RemotingRocketMQRegistration.AddRoleSingleton<IRemotingAdmin>(
            builder,
            provider => CreateRemotingAdmin(provider, roleKey));
        return builder;
    }

    /// <summary>
    /// Adds a classic remoting producer role with the default producer options.
    /// </summary>
    /// <param name="builder">The classic remoting client builder.</param>
    /// <returns>The same builder so that additional roles can be registered.</returns>
    public static RemotingRocketMQBuilder AddRemotingProducer(this RemotingRocketMQBuilder builder) =>
        AddRemotingProducer(builder, static _ => { });

    /// <summary>
    /// Adds and configures a classic remoting producer role.
    /// </summary>
    /// <param name="builder">The classic remoting client builder.</param>
    /// <param name="configure">The delegate used to configure the producer.</param>
    /// <returns>The same builder so that additional roles can be registered.</returns>
    public static RemotingRocketMQBuilder AddRemotingProducer(
        this RemotingRocketMQBuilder builder,
        Action<RemotingProducerOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(configure);
        var roleRegistration = RemotingRocketMQRegistration.RegisterRole(builder, RemotingRocketMQRole.Producer);
        var roleKey = roleRegistration.RoleKey;
        RegisterRemotingTransportServices(builder.Services, roleKey);

        builder.Services.AddOptions<RemotingProducerOptions>(roleRegistration.RoleOptionsName)
            .Configure(configure)
            .Validate(
                static options => !string.IsNullOrWhiteSpace(options.GroupName),
                "Producer group name is required.")
            .Validate(
                static options => options.DefaultTopicQueueNums > 0,
                "Default topic queue count must be positive.")
            .Validate(static options => options.MaxMessageSize > 0, "Maximum message size must be positive.")
            .Validate(static options => options.SendMsgTimeout > TimeSpan.Zero, "Send timeout must be positive.")
            .Validate(
                static options => options.CompressMsgBodyOverHowmuch > 0,
                "Compression threshold must be positive.")
            .Validate(
                static options => options.MaxConcurrentTransactionChecks > 0,
                "Maximum concurrent transaction checks must be positive.")
            .Validate<IOptionsMonitor<RemotingClientOptions>>(
                (options, clientOptions) =>
                    IsWithinLegacyRemotingFrameBudget(
                        options.MaxMessageSize,
                        clientOptions.Get(builder.OptionsName)),
                $"Maximum message size must leave {RemotingClientOptions.RemotingMessageFrameReserve} bytes " +
                "in the classic remoting frame for framing and command headers.");
        RemotingRocketMQRegistration.AddRoleSingleton<IRemotingProducer>(
            builder,
            provider => CreateRemotingProducer(provider, roleRegistration));
        builder.Services.AddSingleton<IHostedService>(provider =>
            ActivatorUtilities.CreateInstance<RemotingProducerHostedService>(
                provider,
                RemotingRocketMQRegistration.GetRoleService<IRemotingProducer>(provider, builder.RegistrationName)));
        return builder;
    }

    /// <summary>
    /// Adds and configures a classic remoting Lite Pull consumer role.
    /// </summary>
    /// <param name="builder">The classic remoting client builder.</param>
    /// <param name="configure">The delegate used to configure the Lite Pull consumer.</param>
    /// <returns>The same builder so that additional roles can be registered.</returns>
    public static RemotingRocketMQBuilder AddRemotingLitePullConsumer(
        this RemotingRocketMQBuilder builder,
        Action<RemotingLitePullConsumerOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(configure);
        var roleRegistration = RemotingRocketMQRegistration.RegisterRole(
            builder,
            RemotingRocketMQRole.LitePullConsumer);
        var roleKey = roleRegistration.RoleKey;
        RegisterRemotingTransportServices(builder.Services, roleKey);

        builder.Services.AddOptions<RemotingLitePullConsumerOptions>(roleRegistration.RoleOptionsName)
            .Configure(configure)
            .Validate(
                static options => !string.IsNullOrWhiteSpace(options.GroupName),
                "Consumer group name is required.")
            .Validate(static options => options.PullBatchSize > 0, "PULL batch size must be positive.")
            .Validate(
                static options => options.MaxCachedMessages > 0,
                "Maximum cached message count must be positive.")
            .Validate(
                static options => options.MaxCachedMessageBytes > 0,
                "Maximum cached message bytes must be positive.")
            .Validate(static options => options.MaxMessageBytes > 0, "Maximum pull response size must be positive.")
            .Validate(
                static options => options.LongPollingTimeout > TimeSpan.Zero,
                "Long polling timeout must be positive.")
            .Validate(
                static options => options.PollTimeout >= TimeSpan.Zero,
                "Poll timeout cannot be negative.")
            .Validate(
                static options => options.AutoCommitInterval > TimeSpan.Zero,
                "Auto-commit interval must be positive.")
            .Validate(static options => Enum.IsDefined(options.InitialPosition), "Initial consume position is invalid.")
            .Validate(
                static options => options.InitialPosition != ConsumeFromPosition.Timestamp ||
                                  options.ConsumeTimestamp.HasValue,
                "A consume timestamp is required when the initial consume position is Timestamp.");
        var getConsumer = RemotingRocketMQRegistration.AddConsumerSingleton<IRemotingLitePullConsumer>(
            builder,
            provider => CreateRemotingLitePullConsumer(provider, roleRegistration));
        builder.Services.AddSingleton<IHostedService>(provider =>
            ActivatorUtilities.CreateInstance<RemotingLitePullConsumerHostedService>(
                provider,
                getConsumer(provider)));
        return builder;
    }

    /// <summary>
    /// Adds and configures a classic remoting push-consumer role with a dependency-injected message handler.
    /// </summary>
    /// <typeparam name="TMessageHandler">The type that processes received messages.</typeparam>
    /// <param name="builder">The classic remoting client builder.</param>
    /// <param name="handlerLifetime">The dependency-injection lifetime used for the message handler.</param>
    /// <param name="configure">The delegate used to configure the push consumer.</param>
    /// <returns>The same builder so that additional roles can be registered.</returns>
    /// <exception cref="ArgumentNullException">
    /// <paramref name="builder"/> or <paramref name="configure"/> is <see langword="null"/>.
    /// </exception>
    /// <exception cref="ArgumentOutOfRangeException">
    /// <paramref name="handlerLifetime"/> is not a supported service lifetime.
    /// </exception>
    public static RemotingRocketMQBuilder AddRemotingPushConsumer<TMessageHandler>(
        this RemotingRocketMQBuilder builder,
        ServiceLifetime handlerLifetime,
        Action<RemotingPushConsumerOptions> configure)
        where TMessageHandler : class, IRemotingPushMessageHandler
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(configure);
        ValidateMessageHandlerLifetime(handlerLifetime);

        var roleRegistration = RemotingRocketMQRegistration.RegisterRole(builder, RemotingRocketMQRole.PushConsumer);
        var roleKey = roleRegistration.RoleKey;
        RegisterRemotingTransportServices(builder.Services, roleKey);
        RemotingPushMessageHandlerFactory.Register<TMessageHandler>(builder.Services, roleKey, handlerLifetime);

        var options = builder.Services.AddOptions<RemotingPushConsumerOptions>(roleRegistration.RoleOptionsName)
            .Configure(configure)
            .Validate(static value => !string.IsNullOrWhiteSpace(value.GroupName), "Consumer group name is required.")
            .Validate(static value => value.MaxConcurrency > 0, "Maximum concurrency must be positive.")
            .Validate(static value => value.PullBatchSize > 0, "PULL batch size must be positive.")
            .Validate(
                static value => value.PopBatchSize is > 0 and <= RemotingPushConsumerOptions.MaximumPopBatchSize,
                "POP batch size must be between 1 and 32.")
            .Validate(
                static value => value.PopInvisibleDuration >= RemotingPushConsumerOptions.MinimumPopInvisibleDuration &&
                                value.PopInvisibleDuration <= RemotingPushConsumerOptions.MaximumPopInvisibleDuration,
                "POP invisible duration must be between 5 seconds and 5 minutes.")
            .Validate(
                static value => value.PopMaxInflightMessagesPerAssignment > 0,
                "Maximum in-flight POP message count must be positive.")
            .Validate(
                static value => value.ConsumeMessageBatchSize > 0,
                "Consume message batch size must be positive.")
            .Validate(
                static value => value.PullMaxCachedMessages > 0,
                "Maximum cached PULL message count must be positive.")
            .Validate(
                static value => value.PullMaxCachedMessageBytes > 0,
                "Maximum cached PULL message bytes must be positive.")
            .Validate(static value => value.MaxMessageBytes > 0, "Maximum pull response size must be positive.")
            .Validate(static value => value.MaxDeliveryAttempts > 0, "Maximum delivery attempts must be positive.")
            .Validate(static value => value.LongPollingTimeout > TimeSpan.Zero, "Long polling timeout must be positive.")
            .Validate(static value => value.RetryDelay > TimeSpan.Zero, "Retry delay must be positive.")
            .Validate(static value => value.ConsumeTimeout > TimeSpan.Zero, "Consume timeout must be positive.")
            .Validate(static value => Enum.IsDefined(value.QueueAssignmentMode), "Queue assignment mode is invalid.")
            .Validate(static value => Enum.IsDefined(value.ConsumerMode), "Consumer mode is invalid.")
            .Validate(
                static value => BroadcastOffsetStore.IsValidPath(value.LocalOffsetStorePath),
                "Local broadcast offset store path is invalid.")
            .Validate(static value => Enum.IsDefined(value.InitialPosition), "Initial consume position is invalid.")
            .Validate(
                static value => value.InitialPosition != ConsumeFromPosition.Timestamp ||
                                value.ConsumeTimestamp.HasValue,
                "A consume timestamp is required when the initial consume position is Timestamp.");
        options.Validate(
            static value => value.ConsumeOrderly || value.PullMaxCachedMessages >= value.ConsumeMessageBatchSize,
            "Maximum cached PULL message count must be at least the consume message batch size.");

        var getConsumer = RemotingRocketMQRegistration.AddConsumerSingleton<IRemotingPushConsumer>(
            builder,
            provider => CreateRemotingPushConsumer(provider, roleRegistration, handlerLifetime));
        builder.Services.AddSingleton<IHostedService>(provider =>
            ActivatorUtilities.CreateInstance<RemotingPushConsumerHostedService>(
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

    private static IRemotingProducer CreateRemotingProducer(
        IServiceProvider provider,
        RemotingRocketMQRoleRegistration roleRegistration)
    {
        var roleKey = roleRegistration.RoleKey;
        return ActivatorUtilities.CreateInstance<RemotingProducer>(
            provider,
            RemotingRocketMQRegistration.GetNamedOptions<RemotingProducerOptions>(
                provider,
                roleRegistration.RoleOptionsName),
            RemotingRocketMQRegistration.GetClientOptions(provider, roleKey),
            GetTopicRouteService(provider, roleKey),
            GetRemotingClientRegistry(provider, roleKey).SharedClient);
    }

    private static IRemotingAdmin CreateRemotingAdmin(IServiceProvider provider, RemotingRocketMQRoleKey roleKey)
    {
        return ActivatorUtilities.CreateInstance<RemotingAdmin>(
            provider,
            RemotingRocketMQRegistration.GetClientOptions(provider, roleKey),
            GetTopicRouteService(provider, roleKey),
            GetRemotingClientRegistry(provider, roleKey).SharedClient);
    }

    private static IRemotingLitePullConsumer CreateRemotingLitePullConsumer(
        IServiceProvider provider,
        RemotingRocketMQRoleRegistration roleRegistration)
    {
        var roleKey = roleRegistration.RoleKey;
        var options = RemotingRocketMQRegistration.GetNamedOptions<RemotingLitePullConsumerOptions>(
            provider,
            roleRegistration.RoleOptionsName);
        var hasLocalGroupPeer = RemotingConsumerGroupCompatibilityValidator.ValidateGroupMemberSubscriptions(
            provider,
            roleRegistration,
            options.Value);
        var clientOptions = RemotingRocketMQRegistration.GetClientOptions(provider, roleKey);
        var settings = CreateRemotingConsumerSettings(
            options.Value,
            options.Value.PullBatchSize,
            options.Value.MaxMessageBytes,
            options.Value.LongPollingTimeout);
        // LitePull advertises active classic consumer-group membership, so a repeated local group member may
        // require a channel-distinct client. Producer and admin roles keep the registration's shared one.
        var groupMember = GetRemotingRebalanceServiceRegistry(provider, roleKey)
            .GetGroupMember(LegacyNamespace.Wrap(clientOptions.Value.Namespace, options.Value.GroupName));
        var routeResolver = CreateRemotingConsumerRouteResolver(provider, roleKey);
        return ActivatorUtilities.CreateInstance<RemotingLitePullConsumer>(
            provider,
            options,
            clientOptions,
            CreateRemotingConsumerEngine(provider, roleKey, settings, routeResolver, groupMember.Client),
            groupMember.Client,
            groupMember.RebalanceService,
            hasLocalGroupPeer);
    }

    private static IRemotingPushConsumer CreateRemotingPushConsumer(
        IServiceProvider provider,
        RemotingRocketMQRoleRegistration roleRegistration,
        ServiceLifetime handlerLifetime)
    {
        var roleKey = roleRegistration.RoleKey;
        var options = RemotingRocketMQRegistration.GetNamedOptions<RemotingPushConsumerOptions>(
            provider,
            roleRegistration.RoleOptionsName);
        var hasLocalGroupPeer = RemotingConsumerGroupCompatibilityValidator.ValidateGroupMemberSubscriptions(
            provider,
            roleRegistration,
            options.Value);
        var clientOptions = RemotingRocketMQRegistration.GetClientOptions(provider, roleKey);
        // Push uses the same active-group channel rule as LitePull; the registry shares whenever the Broker can still
        // distinguish group membership correctly and isolates only repeated members of the same group.
        var groupMember = GetRemotingRebalanceServiceRegistry(provider, roleKey)
            .GetGroupMember(LegacyNamespace.Wrap(clientOptions.Value.Namespace, options.Value.GroupName));
        var settings = CreateRemotingConsumerSettings(
            options.Value,
            options.Value.PullBatchSize,
            options.Value.MaxMessageBytes,
            options.Value.LongPollingTimeout);
        var routeResolver = CreateRemotingConsumerRouteResolver(provider, roleKey);
        var consumerEngine = CreateRemotingConsumerEngine(
            provider,
            roleKey,
            settings,
            routeResolver,
            groupMember.Client);
        return ActivatorUtilities.CreateInstance<RemotingPushConsumer>(
            provider,
            options,
            clientOptions,
            consumerEngine,
            routeResolver,
            groupMember.Client,
            groupMember.RebalanceService,
            RemotingPushMessageHandlerFactory.Create(provider, roleKey, handlerLifetime),
            hasLocalGroupPeer);
    }

    private static IRemotingConsumerEngine CreateRemotingConsumerEngine(
        IServiceProvider provider,
        RemotingRocketMQRoleKey roleKey,
        RemotingConsumerSettings settings,
        RemotingConsumerRouteResolver routeResolver,
        IRemotingClient? remotingClient = null)
    {
        return ActivatorUtilities.CreateInstance<RemotingConsumerEngine>(
            provider,
            settings,
            RemotingRocketMQRegistration.GetClientOptions(provider, roleKey),
            routeResolver,
            remotingClient ?? GetRemotingClientRegistry(provider, roleKey).SharedClient);
    }

    private static RemotingConsumerRouteResolver CreateRemotingConsumerRouteResolver(
        IServiceProvider provider,
        RemotingRocketMQRoleKey roleKey) =>
        new(
            RemotingRocketMQRegistration.GetClientOptions(provider, roleKey),
            GetTopicRouteService(provider, roleKey));

    private static RemotingConsumerSettings CreateRemotingConsumerSettings(
        ConsumerOptions options,
        int batchSize,
        int maxMessageBytes,
        TimeSpan longPollingTimeout) =>
        new(
            options.GroupName,
            batchSize,
            maxMessageBytes,
            longPollingTimeout);

    private static void RegisterRemotingTransportServices(
        IServiceCollection services,
        RemotingRocketMQRoleKey roleKey)
    {
        services.TryAddSingleton<IRemoteCommandSerializer, RemoteCommandSerializer>();
        // The options name is the AddRocketMQRemoting registration boundary. Repeating role methods under one
        // registration reuses this registry; a separate keyed registration gets an independent client lifetime.
        services.TryAddKeyedSingleton<RemotingClientRegistry>(roleKey.OptionsName, (provider, _) =>
            ActivatorUtilities.CreateInstance<RemotingClientRegistry>(
                provider,
                RemotingRocketMQRegistration.GetNamedOptions<RemotingClientOptions>(provider, roleKey.OptionsName)));
        services.TryAddKeyedSingleton<RemotingRebalanceServiceRegistry>(roleKey.OptionsName, (provider, _) =>
            ActivatorUtilities.CreateInstance<RemotingRebalanceServiceRegistry>(
                provider,
                provider.GetRequiredKeyedService<RemotingClientRegistry>(roleKey.OptionsName),
                RemotingRocketMQRegistration.GetNamedOptions<RemotingClientOptions>(provider, roleKey.OptionsName)));
        services.TryAddKeyedSingleton<INameServer>(roleKey.OptionsName, (provider, _) =>
            new NameServer(
                provider.GetRequiredKeyedService<RemotingClientRegistry>(roleKey.OptionsName).SharedClient,
                RemotingRocketMQRegistration.GetNamedOptions<RemotingClientOptions>(provider, roleKey.OptionsName)));
        services.TryAddKeyedSingleton<ITopicRouteService>(roleKey.OptionsName, (provider, _) =>
            new TopicRouteService(
                provider.GetRequiredKeyedService<INameServer>(roleKey.OptionsName),
                RemotingRocketMQRegistration.GetNamedOptions<RemotingClientOptions>(provider, roleKey.OptionsName),
                provider.GetRequiredService<TimeProvider>()));
    }

    private static RemotingClientRegistry GetRemotingClientRegistry(
        IServiceProvider provider,
        RemotingRocketMQRoleKey roleKey) =>
        provider.GetRequiredKeyedService<RemotingClientRegistry>(roleKey.OptionsName);

    private static RemotingRebalanceServiceRegistry GetRemotingRebalanceServiceRegistry(
        IServiceProvider provider,
        RemotingRocketMQRoleKey roleKey) =>
        provider.GetRequiredKeyedService<RemotingRebalanceServiceRegistry>(roleKey.OptionsName);

    private static ITopicRouteService GetTopicRouteService(
        IServiceProvider provider,
        RemotingRocketMQRoleKey roleKey) =>
        provider.GetRequiredKeyedService<ITopicRouteService>(roleKey.OptionsName);

    private static bool IsWithinLegacyRemotingFrameBudget(int messageSize, RemotingClientOptions clientOptions) =>
        messageSize <= clientOptions.MaxRemotingFrameSize - RemotingClientOptions.RemotingMessageFrameReserve;
}
