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
using EventHorizon.RocketMQ.Grpc.Consumer.Lite;
using EventHorizon.RocketMQ.Grpc.Consumer.Push;
using EventHorizon.RocketMQ.Grpc.Exceptions;
using EventHorizon.RocketMQ.Grpc.Protocol;
using EventHorizon.RocketMQ.Grpc.Protocol.Route;
using EventHorizon.RocketMQ.Grpc.Protocol.Telemetry;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Moq;
using Xunit;
using Proto = Apache.Rocketmq.V2;

namespace EventHorizon.RocketMQ.Grpc.Tests.Consumer.Lite;

public sealed class GrpcLitePushConsumerTests
{
    [Fact]
    public async Task StartAsync_ConfiguredTopicsAndBindTopic_SynchronizesSubscriptions()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var endpoint = new Uri("http://127.0.0.1:8081");
        var requests = new List<(Uri Endpoint, Proto.SyncLiteSubscriptionRequest Request)>();
        var client = CreateSyncClient(requests);
        var manager = CreateManager(
            client.Object,
            CreateRouteService(endpoint),
            options =>
            {
                options.LiteTopics.Add("alpha");
                options.LiteTopics.Add("beta");
            });
        var dispatcher = new Mock<IGrpcPushConsumer>(MockBehavior.Strict);
        dispatcher.Setup(value => value.StartAsync(cancellationToken)).Returns(ValueTask.CompletedTask);
        dispatcher.Setup(value => value.StopAsync(CancellationToken.None)).Returns(ValueTask.CompletedTask);
        dispatcher.Setup(value => value.DisposeAsync()).Returns(ValueTask.CompletedTask);
        await using var consumer = new GrpcLitePushConsumer(dispatcher.Object, manager);

        await consumer.StartAsync(cancellationToken);

        var request = Assert.Single(requests);
        Assert.Equal(endpoint, request.Endpoint);
        Assert.Equal(Proto.LiteSubscriptionAction.CompleteAdd, request.Request.Action);
        Assert.Equal("orders", request.Request.Topic.Name);
        Assert.Equal("tenant-a", request.Request.Topic.ResourceNamespace);
        Assert.Equal("lite-tests", request.Request.Group.Name);
        Assert.Equal("tenant-a", request.Request.Group.ResourceNamespace);
        Assert.Equal(["alpha", "beta"], request.Request.LiteTopicSet);
        Assert.Equal(["alpha", "beta"], consumer.LiteTopics.OrderBy(static topic => topic));

        await consumer.StopAsync(CancellationToken.None);
        dispatcher.Verify(value => value.StartAsync(cancellationToken), Times.Once);
        dispatcher.Verify(value => value.StopAsync(CancellationToken.None), Times.Once);
        dispatcher.VerifyNoOtherCalls();
    }

    [Fact]
    public async Task SubscribeAndUnsubscribe_PartialUpdatesAndOffsetOption_SendsAndPreserves()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var endpoint = new Uri("http://127.0.0.1:8081");
        var requests = new List<(Uri Endpoint, Proto.SyncLiteSubscriptionRequest Request)>();
        var client = CreateSyncClient(requests);
        await using var manager = CreateManager(client.Object, CreateRouteService(endpoint));

        await manager.StartAsync(cancellationToken);
        await manager.SubscribeAsync("alpha", GrpcLiteOffsetOption.Tail(12), cancellationToken);
        await manager.UnsubscribeAsync("alpha", cancellationToken);

        Assert.Collection(
            requests,
            request =>
            {
                Assert.Equal(Proto.LiteSubscriptionAction.CompleteAdd, request.Request.Action);
                Assert.Empty(request.Request.LiteTopicSet);
            },
            request =>
            {
                Assert.Equal(Proto.LiteSubscriptionAction.PartialAdd, request.Request.Action);
                Assert.Equal(["alpha"], request.Request.LiteTopicSet);
                Assert.Equal(Proto.OffsetOption.OffsetTypeOneofCase.TailN, request.Request.OffsetOption.OffsetTypeCase);
                Assert.Equal(12, request.Request.OffsetOption.TailN);
            },
            request =>
            {
                Assert.Equal(Proto.LiteSubscriptionAction.PartialRemove, request.Request.Action);
                Assert.Equal(["alpha"], request.Request.LiteTopicSet);
                Assert.Null(request.Request.OffsetOption);
            });
        Assert.Empty(manager.LiteTopics);
    }

    [Fact]
    public async Task FailedSubscription_LocalTopicSet_DoesNotMutate()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var endpoint = new Uri("http://127.0.0.1:8081");
        var calls = 0;
        var client = new Mock<IRocketMQGrpcClient>(MockBehavior.Strict);
        client
            .Setup(value => value.SyncLiteSubscriptionAsync(
                endpoint,
                It.IsAny<Proto.SyncLiteSubscriptionRequest>(),
                cancellationToken))
            .Returns(() => Task.FromResult(new Proto.SyncLiteSubscriptionResponse
            {
                Status = new Proto.Status
                {
                    Code = Interlocked.Increment(ref calls) == 1 ? Proto.Code.Ok : Proto.Code.InternalError,
                    Message = "subscription rejected"
                }
            }));
        await using var manager = CreateManager(client.Object, CreateRouteService(endpoint));

        await manager.StartAsync(cancellationToken);

        await Assert.ThrowsAsync<GrpcServiceException>(
            () => manager.SubscribeAsync("alpha", null, cancellationToken));

        Assert.Empty(manager.LiteTopics);
    }

    [Fact]
    public async Task NotifyUnsubscribeLite_DuplicateSynchronization_RemovesTopic()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var endpoint = new Uri("http://127.0.0.1:8081");
        var requests = new List<(Uri Endpoint, Proto.SyncLiteSubscriptionRequest Request)>();
        var client = CreateSyncClient(requests);
        await using var manager = CreateManager(
            client.Object,
            CreateRouteService(endpoint),
            options => options.LiteTopics.Add("alpha"));

        await manager.StartAsync(cancellationToken);
        await manager.HandleTelemetryCommandAsync(
            endpoint,
            new Proto.TelemetryCommand
            {
                NotifyUnsubscribeLiteCommand = new Proto.NotifyUnsubscribeLiteCommand { LiteTopic = "alpha" }
            },
            cancellationToken);

        Assert.Empty(manager.LiteTopics);
        Assert.Single(requests);
    }

    [Fact]
    public async Task SubscribeBeforeStart_ServiceCall_IsRejected()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var client = new Mock<IRocketMQGrpcClient>(MockBehavior.Strict);
        await using var manager = CreateManager(client.Object, new Mock<IGrpcRouteService>(MockBehavior.Strict).Object);

        await Assert.ThrowsAsync<InvalidOperationException>(
            () => manager.SubscribeAsync("alpha", null, cancellationToken));

        client.VerifyNoOtherCalls();
    }

    [Fact]
    public void OffsetOptions_ProtobufVariants_MapsOffsets()
    {
        var timestamp = new DateTimeOffset(2026, 7, 23, 8, 9, 10, TimeSpan.Zero);

        Assert.Equal(
            Proto.OffsetOption.Types.Policy.Last,
            GrpcLiteOffsetOption.Last.ToProtobuf().Policy);
        Assert.Equal(42, GrpcLiteOffsetOption.AtOffset(42).ToProtobuf().Offset);
        Assert.Equal(7, GrpcLiteOffsetOption.Tail(7).ToProtobuf().TailN);
        Assert.Equal(
            timestamp.ToUnixTimeMilliseconds(),
            GrpcLiteOffsetOption.AtTimestamp(timestamp).ToProtobuf().Timestamp);
        Assert.Equal(
            Proto.OffsetOption.Types.Policy.Min,
            GrpcLiteOffsetOption.Min.ToProtobuf().Policy);
        Assert.Equal(
            Proto.OffsetOption.Types.Policy.Max,
            GrpcLiteOffsetOption.Max.ToProtobuf().Policy);
        Assert.Throws<ArgumentOutOfRangeException>(() => GrpcLiteOffsetOption.AtOffset(-1));
        Assert.Throws<ArgumentOutOfRangeException>(() => GrpcLiteOffsetOption.Tail(0));
    }

    [Fact]
    public async Task LitePushReceiveEngine_PushLongPolling_UsesExpectedFields()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var endpoint = new Uri("http://127.0.0.1:8081");
        var queue = Queue(endpoint);
        var client = new Mock<IRocketMQGrpcClient>(MockBehavior.Strict);
        var session = CreateTelemetrySession();
        Proto.Settings? openedSettings = null;
        Proto.ReceiveMessageRequest? receiveRequest = null;
        Func<Proto.TelemetryCommand, CancellationToken, Task>? telemetryHandler = null;
        Proto.TelemetryCommand? deliveredTelemetryCommand = null;
        Uri? telemetryEndpoint = null;
        client.SetupGet(value => value.AccessPoint).Returns(new Uri("http://127.0.0.1:8080"));
        client
            .SetupGet(value => value.AccessPointProtobuf)
            .Returns(GrpcEndpoint.ToProtobuf([new Uri("http://127.0.0.1:8080")]));
        client
            .Setup(value => value.OpenTelemetryAsync(
                endpoint,
                It.IsAny<Proto.Settings>(),
                It.IsAny<Func<Proto.TelemetryCommand, CancellationToken, Task>>(),
                cancellationToken))
            .Callback<Uri, Proto.Settings, Func<Proto.TelemetryCommand, CancellationToken, Task>?, CancellationToken>(
                (_, settings, handler, _) =>
                {
                    openedSettings = settings.Clone();
                    telemetryHandler = handler;
                })
            .ReturnsAsync(session.Object);
        client
            .Setup(value => value.ReceiveMessageAsync(
                endpoint,
                It.IsAny<Proto.ReceiveMessageRequest>(),
                It.IsAny<TimeSpan>(),
                cancellationToken))
            .Callback<Uri, Proto.ReceiveMessageRequest, TimeSpan, CancellationToken>(
                (_, request, _, _) => receiveRequest = request.Clone())
            .ReturnsAsync([new Proto.ReceiveMessageResponse { Status = OkStatus() }]);
        client
            .Setup(value => value.NotifyTerminationAsync(
                endpoint,
                It.IsAny<Proto.NotifyClientTerminationRequest>(),
                It.IsAny<CancellationToken>()))
            .ReturnsAsync(new Proto.NotifyClientTerminationResponse { Status = OkStatus() });
        await using var engine = new GrpcReceiveConsumerEngine(
            client.Object,
            new Mock<IGrpcRouteService>(MockBehavior.Strict).Object,
            Options.Create(new GrpcClientOptions()),
            "lite-tests",
            new Dictionary<string, FilterExpression> { ["orders"] = FilterExpression.All },
            Proto.ClientType.LitePushConsumer,
            TimeSpan.FromSeconds(9),
            NullLogger<GrpcReceiveConsumerEngine>.Instance,
            NullLogger<GrpcSessionManager>.Instance,
            (receivedEndpoint, command, _) =>
            {
                telemetryEndpoint = receivedEndpoint;
                deliveredTelemetryCommand = command.Clone();
                return Task.CompletedTask;
            });

        await engine.StartAsync(cancellationToken);
        var messages = await engine.ReceiveAsync(
            queue,
            FilterExpression.All,
            8,
            TimeSpan.FromSeconds(30),
            TimeSpan.FromSeconds(9),
            true,
            cancellationToken);
        Assert.NotNull(telemetryHandler);
        await telemetryHandler(
            new Proto.TelemetryCommand
            {
                NotifyUnsubscribeLiteCommand = new Proto.NotifyUnsubscribeLiteCommand { LiteTopic = "alpha" }
            },
            cancellationToken);
        await engine.StopAsync(cancellationToken);

        Assert.Empty(messages);
        Assert.NotNull(openedSettings);
        Assert.Equal(Proto.ClientType.LitePushConsumer, openedSettings.ClientType);
        Assert.NotNull(receiveRequest);
        Assert.True(receiveRequest.HasAttemptId);
        Assert.Equal(TimeSpan.FromSeconds(9), receiveRequest.LongPollingTimeout.ToTimeSpan());
        Assert.True(receiveRequest.AutoRenew);
        Assert.Equal(endpoint, telemetryEndpoint);
        Assert.Equal("alpha", deliveredTelemetryCommand?.NotifyUnsubscribeLiteCommand.LiteTopic);
    }

    [Fact]
    public async Task AddGrpcLitePushConsumer_ProtocolSpecificConsumerAndLifecycle_Registers()
    {
        var services = new ServiceCollection();
        services
            .AddRocketMQGrpc(options => options.Endpoint = "127.0.0.1:8081")
            .AddGrpcLitePushConsumer<TestMessageHandler>(ServiceLifetime.Singleton, options =>
            {
                options.GroupName = "lite-tests";
                options.BindTopic = "orders";
                options.LiteTopics.Add("alpha");
            });
        await using var provider = services.BuildServiceProvider();

        var consumer = provider.GetRequiredService<IGrpcLitePushConsumer>();
        var hostedService = Assert.Single(provider.GetServices<IHostedService>());

        Assert.IsType<GrpcLitePushConsumer>(consumer);
        Assert.IsType<GrpcLitePushConsumerHostedService>(hostedService);
        Assert.Same(consumer, provider.GetRequiredService<IGrpcLitePushConsumer>());
    }

    [Fact]
    public void AddGrpcLitePushConsumer_StandardTopicSubscriptions_Rejects()
    {
        var services = new ServiceCollection();
        services
            .AddRocketMQGrpc(options => options.Endpoint = "127.0.0.1:8081")
            .AddGrpcLitePushConsumer<TestMessageHandler>(ServiceLifetime.Singleton, options =>
            {
                options.GroupName = "lite-tests";
                options.BindTopic = "orders";
                options.Subscribe("payments");
            });
        using var provider = services.BuildServiceProvider();

        var exception = Assert.Throws<OptionsValidationException>(
            () => provider.GetRequiredService<IGrpcLitePushConsumer>());

        Assert.Contains("BindTopic", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task GrpcLitePushConsumerHostedService_LifecycleCancellationTokens_ForwardsCancellationTokens()
    {
        using var start = new CancellationTokenSource();
        using var stop = new CancellationTokenSource();
        var consumer = new Mock<IGrpcLitePushConsumer>(MockBehavior.Strict);
        consumer.Setup(value => value.StartAsync(start.Token)).Returns(ValueTask.CompletedTask);
        consumer.Setup(value => value.StopAsync(stop.Token)).Returns(ValueTask.CompletedTask);
        var hostedService = new GrpcLitePushConsumerHostedService(consumer.Object);

        await hostedService.StartAsync(start.Token);
        await hostedService.StopAsync(stop.Token);

        consumer.Verify(value => value.StartAsync(start.Token), Times.Once);
        consumer.Verify(value => value.StopAsync(stop.Token), Times.Once);
        consumer.VerifyNoOtherCalls();
    }

    [Fact]
    public async Task SubscriptionManager_DuplicateTopicsAndTelemetryCommands_Ignores()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var endpoint = new Uri("http://127.0.0.1:8081");
        var requests = new List<(Uri Endpoint, Proto.SyncLiteSubscriptionRequest Request)>();
        var client = CreateSyncClient(requests);
        await using var manager = CreateManager(
            client.Object,
            CreateRouteService(endpoint),
            options => options.LiteTopics.Add("alpha"));

        await manager.StartAsync(cancellationToken);
        await manager.StartAsync(cancellationToken);
        await manager.SubscribeAsync("alpha", null, cancellationToken);
        await manager.UnsubscribeAsync("missing", cancellationToken);
        await manager.HandleTelemetryCommandAsync(
            endpoint,
            new Proto.TelemetryCommand
            {
                PrintThreadStackTraceCommand = new Proto.PrintThreadStackTraceCommand { Nonce = "stack-1" }
            },
            cancellationToken);
        await manager.HandleTelemetryCommandAsync(
            endpoint,
            new Proto.TelemetryCommand
            {
                NotifyUnsubscribeLiteCommand = new Proto.NotifyUnsubscribeLiteCommand()
            },
            cancellationToken);

        Assert.Single(requests);
        Assert.Equal(["alpha"], manager.LiteTopics);
    }

    [Fact]
    public async Task SubscriptionManager_PeriodicSynchronizationAfterANonFatalFailure_Continues()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var endpoint = new Uri("http://127.0.0.1:8081");
        var secondSynchronization = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var calls = 0;
        var client = new Mock<IRocketMQGrpcClient>(MockBehavior.Strict);
        client
            .Setup(value => value.SyncLiteSubscriptionAsync(
                endpoint,
                It.IsAny<Proto.SyncLiteSubscriptionRequest>(),
                It.IsAny<CancellationToken>()))
            .Returns(() =>
            {
                if (Interlocked.Increment(ref calls) == 2)
                {
                    secondSynchronization.TrySetResult();
                    return Task.FromResult(new Proto.SyncLiteSubscriptionResponse
                    {
                        Status = new Proto.Status { Code = Proto.Code.InternalError, Message = "temporary" }
                    });
                }

                return Task.FromResult(new Proto.SyncLiteSubscriptionResponse { Status = OkStatus() });
            });
        await using var manager = CreateManager(
            client.Object,
            CreateRouteService(endpoint),
            options => options.SubscriptionSyncInterval = TimeSpan.FromMilliseconds(10));

        await manager.StartAsync(cancellationToken);
        await secondSynchronization.Task.WaitAsync(TimeSpan.FromSeconds(3), cancellationToken);
        await manager.StopAsync(cancellationToken);

        Assert.True(Volatile.Read(ref calls) >= 2);
    }

    [Fact]
    public async Task SubscriptionManager_RepeatedDispose_IsIdempotent()
    {
        var client = new Mock<IRocketMQGrpcClient>(MockBehavior.Strict);
        var routes = new Mock<IGrpcRouteService>(MockBehavior.Strict);
        var manager = CreateManager(client.Object, routes.Object);

        await manager.DisposeAsync();
        await manager.DisposeAsync();

        client.VerifyNoOtherCalls();
        routes.VerifyNoOtherCalls();
    }

    [Fact]
    public async Task LitePushConsumer_StartedDependencies_IsIdempotentAndDisposes()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var endpoint = new Uri("http://127.0.0.1:8081");
        var requests = new List<(Uri Endpoint, Proto.SyncLiteSubscriptionRequest Request)>();
        var client = CreateSyncClient(requests);
        var manager = CreateManager(client.Object, CreateRouteService(endpoint));
        var dispatcher = new Mock<IGrpcPushConsumer>(MockBehavior.Strict);
        dispatcher.Setup(value => value.StartAsync(cancellationToken)).Returns(ValueTask.CompletedTask);
        dispatcher.Setup(value => value.StopAsync(CancellationToken.None)).Returns(ValueTask.CompletedTask);
        dispatcher.Setup(value => value.DisposeAsync()).Returns(ValueTask.CompletedTask);
        var consumer = new GrpcLitePushConsumer(dispatcher.Object, manager);

        await consumer.StopAsync(cancellationToken);
        await consumer.StartAsync(cancellationToken);
        await consumer.StartAsync(cancellationToken);
        await consumer.DisposeAsync();
        await consumer.DisposeAsync();

        await Assert.ThrowsAsync<ObjectDisposedException>(() =>
            consumer.SubscribeLiteAsync("alpha", cancellationToken: cancellationToken));
        await Assert.ThrowsAsync<ObjectDisposedException>(() =>
            consumer.UnsubscribeLiteAsync("alpha", cancellationToken));
        dispatcher.Verify(value => value.StartAsync(cancellationToken), Times.Once);
        dispatcher.Verify(value => value.StopAsync(CancellationToken.None), Times.Once);
        dispatcher.Verify(value => value.DisposeAsync(), Times.Once);
    }

    [Fact]
    public async Task LitePushConsumer_StopAfterSubscriptionManagerCancellation_RetriesStop()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var endpoint = new Uri("http://127.0.0.1:8081");
        var subscriptionSynchronizationStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseSubscriptionSynchronization = new TaskCompletionSource<Proto.SyncLiteSubscriptionResponse>(
            TaskCreationOptions.RunContinuationsAsynchronously);
        var synchronizationCalls = 0;
        var client = new Mock<IRocketMQGrpcClient>(MockBehavior.Strict);
        client
            .Setup(value => value.SyncLiteSubscriptionAsync(
                endpoint,
                It.IsAny<Proto.SyncLiteSubscriptionRequest>(),
                It.IsAny<CancellationToken>()))
            .Returns(() =>
            {
                if (Interlocked.Increment(ref synchronizationCalls) == 2)
                {
                    subscriptionSynchronizationStarted.TrySetResult();
                    return releaseSubscriptionSynchronization.Task;
                }

                return Task.FromResult(new Proto.SyncLiteSubscriptionResponse { Status = OkStatus() });
            });
        var manager = CreateManager(client.Object, CreateRouteService(endpoint));
        var dispatcher = new Mock<IGrpcPushConsumer>(MockBehavior.Strict);
        dispatcher.Setup(value => value.StartAsync(cancellationToken)).Returns(ValueTask.CompletedTask);
        dispatcher.Setup(value => value.StopAsync(CancellationToken.None)).Returns(ValueTask.CompletedTask);
        dispatcher.Setup(value => value.DisposeAsync()).Returns(ValueTask.CompletedTask);
        var consumer = new GrpcLitePushConsumer(dispatcher.Object, manager);

        try
        {
            await consumer.StartAsync(cancellationToken);
            var subscribe = consumer.SubscribeLiteAsync("alpha", cancellationToken: cancellationToken);
            await subscriptionSynchronizationStarted.Task.WaitAsync(TimeSpan.FromSeconds(3), cancellationToken);

            using var stopCancellation = new CancellationTokenSource();
            var canceledStop = consumer.StopAsync(stopCancellation.Token).AsTask();
            Assert.False(canceledStop.IsCompleted);
            stopCancellation.Cancel();
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => canceledStop);

            releaseSubscriptionSynchronization.TrySetResult(new Proto.SyncLiteSubscriptionResponse { Status = OkStatus() });
            await subscribe.WaitAsync(TimeSpan.FromSeconds(3), cancellationToken);
            await consumer.StopAsync(CancellationToken.None);

            dispatcher.Verify(value => value.StopAsync(CancellationToken.None), Times.Once);
        }
        finally
        {
            releaseSubscriptionSynchronization.TrySetResult(new Proto.SyncLiteSubscriptionResponse { Status = OkStatus() });
            await consumer.DisposeAsync();
        }
    }

    [Fact]
    public async Task LitePushConsumer_SubscriptionStartupFailure_StopsDispatcher()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var endpoint = new Uri("http://127.0.0.1:8081");
        var client = new Mock<IRocketMQGrpcClient>(MockBehavior.Strict);
        client
            .Setup(value => value.SyncLiteSubscriptionAsync(
                endpoint,
                It.IsAny<Proto.SyncLiteSubscriptionRequest>(),
                cancellationToken))
            .ReturnsAsync(new Proto.SyncLiteSubscriptionResponse
            {
                Status = new Proto.Status { Code = Proto.Code.InternalError, Message = "rejected" }
            });
        var manager = CreateManager(client.Object, CreateRouteService(endpoint));
        var dispatcher = new Mock<IGrpcPushConsumer>(MockBehavior.Strict);
        dispatcher.Setup(value => value.StartAsync(cancellationToken)).Returns(ValueTask.CompletedTask);
        dispatcher.Setup(value => value.StopAsync(CancellationToken.None)).Returns(ValueTask.CompletedTask);
        dispatcher.Setup(value => value.DisposeAsync()).Returns(ValueTask.CompletedTask);
        var consumer = new GrpcLitePushConsumer(dispatcher.Object, manager);

        await Assert.ThrowsAsync<GrpcServiceException>(() => consumer.StartAsync(cancellationToken).AsTask());
        await consumer.DisposeAsync();

        dispatcher.Verify(value => value.StopAsync(CancellationToken.None), Times.Once);
        dispatcher.Verify(value => value.DisposeAsync(), Times.Once);
    }

    [Fact]
    public async Task LitePushConsumer_RuntimeTopicUpdates_Delegates()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var endpoint = new Uri("http://127.0.0.1:8081");
        var requests = new List<(Uri Endpoint, Proto.SyncLiteSubscriptionRequest Request)>();
        var client = CreateSyncClient(requests);
        var manager = CreateManager(client.Object, CreateRouteService(endpoint));
        var dispatcher = new Mock<IGrpcPushConsumer>(MockBehavior.Strict);
        dispatcher.Setup(value => value.StartAsync(cancellationToken)).Returns(ValueTask.CompletedTask);
        dispatcher.Setup(value => value.StopAsync(cancellationToken)).Returns(ValueTask.CompletedTask);
        dispatcher.Setup(value => value.DisposeAsync()).Returns(ValueTask.CompletedTask);
        await using var consumer = new GrpcLitePushConsumer(dispatcher.Object, manager);

        await consumer.StartAsync(cancellationToken);
        await consumer.SubscribeLiteAsync("alpha", GrpcLiteOffsetOption.Min, cancellationToken);
        await consumer.UnsubscribeLiteAsync("alpha", cancellationToken);
        await consumer.StopAsync(cancellationToken);

        Assert.Collection(
            requests,
            request => Assert.Equal(Proto.LiteSubscriptionAction.CompleteAdd, request.Request.Action),
            request => Assert.Equal(Proto.LiteSubscriptionAction.PartialAdd, request.Request.Action),
            request => Assert.Equal(Proto.LiteSubscriptionAction.PartialRemove, request.Request.Action));
    }

    private static Mock<IRocketMQGrpcClient> CreateSyncClient(
        ICollection<(Uri Endpoint, Proto.SyncLiteSubscriptionRequest Request)> requests)
    {
        var client = new Mock<IRocketMQGrpcClient>(MockBehavior.Strict);
        client
            .Setup(value => value.SyncLiteSubscriptionAsync(
                It.IsAny<Uri>(),
                It.IsAny<Proto.SyncLiteSubscriptionRequest>(),
                It.IsAny<CancellationToken>()))
            .Callback<Uri, Proto.SyncLiteSubscriptionRequest, CancellationToken>((endpoint, request, _) =>
                requests.Add((endpoint, request.Clone())))
            .ReturnsAsync(new Proto.SyncLiteSubscriptionResponse { Status = OkStatus() });
        return client;
    }

    private static GrpcLiteSubscriptionManager CreateManager(
        IRocketMQGrpcClient client,
        IGrpcRouteService routes,
        Action<GrpcLitePushConsumerOptions>? configure = null)
    {
        var options = new GrpcLitePushConsumerOptions
        {
            GroupName = "lite-tests",
            BindTopic = "orders",
            SubscriptionSyncInterval = TimeSpan.FromDays(1)
        };
        configure?.Invoke(options);
        return new GrpcLiteSubscriptionManager(
            Options.Create(options),
            Options.Create(new GrpcClientOptions { Namespace = "tenant-a" }),
            client,
            routes,
            NullLogger<GrpcLiteSubscriptionManager>.Instance);
    }

    private static IGrpcRouteService CreateRouteService(Uri endpoint)
    {
        var routes = new Mock<IGrpcRouteService>(MockBehavior.Strict);
        routes
            .Setup(value => value.GetAsync("orders", false, It.IsAny<CancellationToken>()))
            .Returns(() => Task.FromResult<IReadOnlyList<Proto.MessageQueue>>([Queue(endpoint)]));
        return routes.Object;
    }

    private static Proto.MessageQueue Queue(Uri endpoint) => new()
    {
        Topic = new Proto.Resource { ResourceNamespace = "tenant-a", Name = "orders" },
        Id = 0,
        Permission = Proto.Permission.ReadWrite,
        Broker = new Proto.Broker
        {
            Name = "broker-a",
            Endpoints = GrpcEndpoint.ToProtobuf([endpoint])
        }
    };

    private static Mock<IGrpcTelemetrySession> CreateTelemetrySession()
    {
        var completion = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var session = new Mock<IGrpcTelemetrySession>(MockBehavior.Strict);
        session.SetupGet(value => value.ServerSettings).Returns((Proto.Settings?)null);
        session.SetupGet(value => value.Completion).Returns(completion.Task);
        session
            .Setup(value => value.DisposeAsync())
            .Callback(() => completion.TrySetResult())
            .Returns(ValueTask.CompletedTask);
        return session;
    }

    private static Proto.Status OkStatus() => new() { Code = Proto.Code.Ok };

    private sealed class TestMessageHandler : IGrpcPushMessageHandler
    {
        public ValueTask<ConsumeResult> HandleAsync(
            GrpcMessageView message,
            CancellationToken cancellationToken) =>
            ValueTask.FromResult(ConsumeResult.Success);
    }
}
