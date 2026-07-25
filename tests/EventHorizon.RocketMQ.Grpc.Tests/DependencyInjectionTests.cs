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
using EventHorizon.RocketMQ.Consumer;
using EventHorizon.RocketMQ.Grpc.Consumer.Push;
using EventHorizon.RocketMQ.Grpc.Consumer.Simple;
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
    public void AddRocketMQGrpc_RejectsBlankEndpoint()
    {
        var services = new ServiceCollection();
        services.AddRocketMQGrpc(options => options.Endpoint = " ");
        using var provider = services.BuildServiceProvider();

        var exception = Assert.Throws<OptionsValidationException>(
            () => provider.GetRequiredService<IOptions<GrpcClientOptions>>().Value);

        Assert.Contains("gRPC proxy endpoint", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task AddGrpcProducer_RegistersProtocolSpecificProducerAsSingleton()
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
    public void AddRocketMQGrpc_RejectsIncompleteCredentials()
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
    public async Task GrpcSimpleConsumer_CanSubscribeAfterResolutionBeforeStartup()
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
    public async Task AddGrpcPushConsumer_RegistersProtocolSpecificConsumerAndLifecycle()
    {
        var services = new ServiceCollection();
        services
            .AddRocketMQGrpc(options => options.Endpoint = "127.0.0.1:8081")
            .AddGrpcPushConsumer(options =>
            {
                options.GroupName = "orders";
                options.Subscribe("orders");
                options.MessageHandler = static (_, _) => ValueTask.FromResult(ConsumeResult.Success);
            });
        await using var provider = services.BuildServiceProvider();

        var consumer = provider.GetRequiredService<IGrpcPushConsumer>();
        var hostedService = Assert.Single(provider.GetServices<IHostedService>());

        Assert.IsType<GrpcPushConsumer>(consumer);
        Assert.IsType<GrpcPushConsumerHostedService>(hostedService);
        Assert.Same(consumer, provider.GetRequiredService<IGrpcPushConsumer>());
    }

    [Fact]
    public void AddRocketMQGrpc_RejectsNonPositiveRequestTimeout()
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
    public void AddRocketMQGrpc_RejectsNonPositiveRouteCacheDuration()
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
    public void AddRocketMQGrpc_RejectsNonPositiveHeartbeatInterval()
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
    public void AddRocketMQGrpc_DoesNotReplaceRegisteredTimeProvider()
    {
        var services = new ServiceCollection();
        var timeProvider = TimeProvider.System;
        services.AddSingleton(timeProvider);
        services.AddRocketMQGrpc(options => options.Endpoint = "127.0.0.1:8081");
        using var provider = services.BuildServiceProvider();

        Assert.Same(timeProvider, provider.GetRequiredService<TimeProvider>());
    }

    [Fact]
    public void AddGrpcProducer_RejectsDuplicateRoleForDefaultClientRegistration()
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
    public async Task GrpcRolesUseDistinctClientIdentitiesAndRouteServices()
    {
        var services = new ServiceCollection();
        services
            .AddRocketMQGrpc(options =>
            {
                options.Endpoint = "127.0.0.1:8081";
                options.InstanceName = "di-test";
            })
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
    public async Task KeyedClientRegistrationsRegisterMultipleGrpcProducersWithIndependentOptionsAndLifecycle()
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

    private static string GetClientId(GrpcMetadataFactory metadata) =>
        metadata.Create().Single(entry =>
            string.Equals(entry.Key, "x-mq-client-id", StringComparison.Ordinal)).Value;
}
