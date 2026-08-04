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

using System.Net;
using System.Text.Json;
using EventHorizon.RocketMQ.Remoting.Consumer;
using EventHorizon.RocketMQ.Remoting.Consumer.Coordination;
using EventHorizon.RocketMQ.Remoting.Consumer.Coordination.Rebalance;
using EventHorizon.RocketMQ.Remoting.Consumer.Push;
using EventHorizon.RocketMQ.Remoting.Consumer.Push.Assignment;
using EventHorizon.RocketMQ.Remoting.Consumer.Push.Processing;
using EventHorizon.RocketMQ.Remoting.Consumer.Push.Pull.Dispatch;
using EventHorizon.RocketMQ.Remoting.Consumer.Push.Pull.Offset;
using EventHorizon.RocketMQ.Remoting.Consumer.Push.Pull.Receive;
using EventHorizon.RocketMQ.Remoting.Consumer.Route;
using EventHorizon.RocketMQ.Remoting.Instrumentation;
using EventHorizon.RocketMQ.Remoting.Protocol;
using Microsoft.Extensions.Logging.Abstractions;
using Moq;
using Xunit;

namespace EventHorizon.RocketMQ.Remoting.Tests.Consumer.Push.Pull.Offset;

public sealed class PullOffsetResetHandlerTests
{
    [Fact]
    public async Task Request_MismatchedGroupOrTopic_ReturnsNotHandled()
    {
        using var fixture = CreateFixture();
        var cancellationToken = TestContext.Current.CancellationToken;

        var requests = new[]
        {
            new Dictionary<string, object>
            {
                ["group"] = "tenant-a%another-group",
                ["topic"] = "tenant-a%orders"
            },
            new Dictionary<string, object>
            {
                ["group"] = "tenant-a%legacy-group",
                ["topic"] = "tenant-a%other-topic"
            },
            new Dictionary<string, object>
            {
                ["group"] = "tenant-a%legacy-group",
                ["topic"] = "orders"
            },
            new Dictionary<string, object>
            {
                ["group"] = "tenant-a%legacy-group"
            }
        };

        foreach (var fields in requests)
        {
            var result = await fixture.InvokeAsync(fields, ResetBody("tenant-a%orders"), cancellationToken);

            Assert.False(result.IsHandled);
            Assert.False(result.SuppressResponse);
        }
    }

    [Fact]
    public async Task Request_QueueOutsideRequestedTopic_ThrowsInvalidDataException()
    {
        using var fixture = CreateFixture();

        var exception = await Assert.ThrowsAsync<InvalidDataException>(() => fixture.InvokeAsync(
            new Dictionary<string, object>
            {
                ["group"] = "tenant-a%legacy-group",
                ["topic"] = "tenant-a%orders"
            },
            ResetBody("tenant-a%payments"),
            TestContext.Current.CancellationToken).AsTask());

        Assert.Contains("outside the requested topic", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Request_BeforeRun_PersistsResetBoundaryForQueueInitialization()
    {
        using var fixture = CreateFixture();

        var result = await fixture.InvokeAsync(
            RequestFields(),
            ResetBody("tenant-a%orders", brokerName: "broker-a", queueId: 0, offset: 7),
            TestContext.Current.CancellationToken);

        Assert.True(result.IsHandled);
        Assert.True(result.SuppressResponse);

        var queue = new RemotingConsumerQueue("orders", "broker-a", 0);
        var initialized = await fixture.OffsetManager.InitializeAsync(
            queue,
            TestContext.Current.CancellationToken);

        Assert.Equal(7, initialized);
        fixture.ConsumerEngine.Verify(
            engine => engine.GetOffsetAsync(It.IsAny<RemotingConsumerQueue>(), It.IsAny<CancellationToken>()),
            Times.Never);
        fixture.ConsumerEngine.Verify(
            engine => engine.UpdateOffsetAsync(
                It.IsAny<RemotingConsumerQueue>(),
                It.IsAny<long>(),
                It.IsAny<CancellationToken>()),
            Times.Never);
    }

    [Fact]
    public async Task Request_ActiveRun_PersistsOnlyRemovedPullReceiversAndRetainsAbsentBoundary()
    {
        using var fixture = CreateFixture();
        var run = fixture.CreateRun();
        fixture.Run = run;

        var target = CreateTarget("orders", "broker-a", 0);
        var receiver = new PullAssignmentReceiver(target, run.Stopping.Token, createProcessQueue: true);
        var receiverCancellation = receiver.Cancellation;
        Assert.True(run.Receivers.TryAdd(PushReceiverKey.Create(target), receiver));

        var result = await fixture.InvokeAsync(
            RequestFields(),
            ResetBody(
                "tenant-a%orders",
                [
                    ("broker-a", 0, 7),
                    ("broker-b", 1, 11)
                ]),
            TestContext.Current.CancellationToken);

        Assert.True(result.IsHandled);
        Assert.True(result.SuppressResponse);
        Assert.Empty(run.Receivers.Snapshot());
        Assert.True(receiverCancellation.IsCancellationRequested);
        Assert.True(fixture.OffsetManager.HasResetBoundary(new RemotingConsumerQueue("orders", "broker-b", 1)));
        Assert.False(fixture.OffsetManager.HasResetBoundary(new RemotingConsumerQueue("orders", "broker-a", 0)));
        fixture.ConsumerEngine.Verify(
            engine => engine.UpdateOffsetAsync(
                It.Is<RemotingConsumerQueue>(queue =>
                    queue.Topic == "orders" && queue.BrokerName == "broker-a" && queue.QueueId == 0),
                7,
                It.IsAny<CancellationToken>()),
            Times.Once);
        fixture.ConsumerEngine.Verify(
            engine => engine.UpdateOffsetAsync(
                It.Is<RemotingConsumerQueue>(queue =>
                    queue.Topic == "orders" && queue.BrokerName == "broker-b" && queue.QueueId == 1),
                11,
                It.IsAny<CancellationToken>()),
                Times.Never);
    }

    [Fact]
    public async Task Request_ActiveSettlement_WaitsBeforePersistingResetOffset()
    {
        using var fixture = CreateFixture();
        var cancellationToken = TestContext.Current.CancellationToken;
        var run = fixture.CreateRun();
        fixture.Run = run;
        var target = CreateTarget("orders", "broker-a", 0);
        var receiver = new PullAssignmentReceiver(target, run.Stopping.Token, createProcessQueue: true);
        var processQueue = receiver.PullProcessQueue!;
        processQueue.Initialize(0, forcePersist: false);
        processQueue.PutMessages([CreateMessage()]);
        processQueue.AdvanceNextPullOffset(1);
        Assert.True(processQueue.TryCreateConsumeRequest(1, out var request));
        Assert.True(processQueue.TryStartConsumeRequest(request));
        Assert.True(processQueue.TryBeginSettlement(
            request.Entries[0],
            ConsumeResult.Retry,
            delayLevel: 3,
            out _));
        receiver.Start(static _ => Task.CompletedTask);
        Assert.True(run.Receivers.TryAdd(PushReceiverKey.Create(target), receiver));

        var reset = fixture.InvokeAsync(
            RequestFields(),
            ResetBody("tenant-a%orders", brokerName: "broker-a", queueId: 0, offset: 7),
            cancellationToken).AsTask();
        await Task.Delay(TimeSpan.FromMilliseconds(50), cancellationToken);

        Assert.False(reset.IsCompleted);
        fixture.ConsumerEngine.Verify(
            engine => engine.UpdateOffsetAsync(
                It.IsAny<RemotingConsumerQueue>(),
                It.IsAny<long>(),
                It.IsAny<CancellationToken>()),
            Times.Never);

        Assert.False(processQueue.CompleteConsumeRequest(request, [true]));
        var result = await reset.WaitAsync(TimeSpan.FromSeconds(1), cancellationToken);

        Assert.True(result.IsHandled);
        fixture.ConsumerEngine.Verify(
            engine => engine.UpdateOffsetAsync(
                It.Is<RemotingConsumerQueue>(queue =>
                    queue.Topic == "orders" && queue.BrokerName == "broker-a" && queue.QueueId == 0),
                7,
                It.IsAny<CancellationToken>()),
            Times.Once);
    }

    [Fact]
    public void Dispose_RegisteredRequestHandler_DisposesRegistration()
    {
        using var fixture = CreateFixture();

        fixture.DisposeHandler();

        fixture.Registration.Verify(registration => registration.Dispose(), Times.Once);
    }

    private static TestFixture CreateFixture()
    {
        var options = new RemotingPushConsumerOptions
        {
            GroupName = "legacy-group",
            ConsumerMode = ConsumerMode.Clustering,
            MaxConcurrency = 1,
            PullMaxCachedMessages = 8,
            PullMaxCachedMessageBytes = 1024
        };
        options.Subscribe("orders");

        var clientOptions = new RemotingClientOptions
        {
            ClientIP = "127.0.0.1",
            InstanceName = "offset-handler-test",
            Namespace = "tenant-a"
        };
        var registration = new Mock<IDisposable>(MockBehavior.Strict);
        registration.Setup(value => value.Dispose());
        var remoting = new Mock<IRemotingClient>(MockBehavior.Strict);
        RemotingRequestHandler? requestHandler = null;
        remoting
            .Setup(client => client.RegisterRequestHandler(
                RequestCode.ResetConsumerOffset,
                It.IsAny<RemotingRequestHandler>()))
            .Returns<int, RemotingRequestHandler>((_, handler) =>
            {
                requestHandler = handler;
                return registration.Object;
            });

        var consumerEngine = new Mock<IRemotingConsumerEngine>(MockBehavior.Strict);
        consumerEngine
            .Setup(engine => engine.UpdateOffsetAsync(
                It.IsAny<RemotingConsumerQueue>(),
                It.IsAny<long>(),
                It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask);
        var groupClient = new RemotingConsumerGroupClient(
            remoting.Object,
            clientOptions,
            options.GroupName,
            NullLogger<RemotingConsumerGroupClient>.Instance);
        var offsetManager = new PullOffsetManager(
            options,
            consumerEngine.Object,
            groupClient.ClientId,
            groupClient.ConsumerGroup,
            NullLogger<PullOffsetManager>.Instance);
        var lifecycle = new SemaphoreSlim(1, 1);
        var subscriptions = new PushSubscriptionState(options.Subscriptions);
        var runHolder = new RunHolder();
        var handler = new PullOffsetResetHandler(
            options,
            clientOptions,
            groupClient,
            offsetManager,
            subscriptions,
            lifecycle,
            () => runHolder.Run,
            remoting.Object,
            NullLogger<PullOffsetResetHandler>.Instance);

        Assert.NotNull(requestHandler);
        return new TestFixture(
            handler,
            requestHandler!,
            registration,
            consumerEngine,
            offsetManager,
            options,
            groupClient,
            lifecycle,
            runHolder);
    }

    private static Dictionary<string, object> RequestFields() => new()
    {
        ["group"] = "tenant-a%legacy-group",
        ["topic"] = "tenant-a%orders"
    };

    private static byte[] ResetBody(
        string topic,
        string brokerName = "broker-a",
        int queueId = 0,
        long offset = 7) =>
        ResetBody(topic, [(brokerName, queueId, offset)]);

    private static byte[] ResetBody(
        string topic,
        IReadOnlyList<(string BrokerName, int QueueId, long Offset)> entries)
    {
        return JsonSerializer.SerializeToUtf8Bytes(new
        {
            offsetTable = entries.Select(entry => new
            {
                topic,
                brokerName = entry.BrokerName,
                queueId = entry.QueueId,
                offset = entry.Offset
            })
        });
    }

    private static PushReceiveTarget CreateTarget(string topic, string brokerName, int queueId) =>
        new(
            new RemotingPushAssignment(
                topic,
                brokerName,
                queueId,
                RemotingPushReceiveMode.Pull,
                new Dictionary<string, string>(StringComparer.Ordinal)),
            new RemotingBrokerEndpoint(brokerName, "127.0.0.1:10911"),
            FilterExpression.All);

    private static RemotingMessageView CreateMessage() => new(
        "orders",
        new byte[] { 1, 2, 3 },
        "message-0",
        "offset-0",
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

    private sealed class TestFixture(
        PullOffsetResetHandler handler,
        RemotingRequestHandler requestHandler,
        Mock<IDisposable> registration,
        Mock<IRemotingConsumerEngine> consumerEngine,
        PullOffsetManager offsetManager,
        RemotingPushConsumerOptions options,
        RemotingConsumerGroupClient groupClient,
        SemaphoreSlim lifecycle,
        RunHolder runHolder) : IDisposable
    {
        private bool _handlerDisposed;

        public PullOffsetResetHandler Handler { get; } = handler;

        public RemotingRequestHandler RequestHandler { get; } = requestHandler;

        public Mock<IDisposable> Registration { get; } = registration;

        public Mock<IRemotingConsumerEngine> ConsumerEngine { get; } = consumerEngine;

        public PullOffsetManager OffsetManager { get; } = offsetManager;

        public RemotingPushConsumerOptions Options { get; } = options;

        public RemotingConsumerGroupClient GroupClient { get; } = groupClient;

        public RemotingPushConsumerRun? Run
        {
            get => runHolder.Run;
            set => runHolder.Run = value;
        }

        public async ValueTask<RemotingRequestResult> InvokeAsync(
            Dictionary<string, object> fields,
            byte[] body,
            CancellationToken cancellationToken) =>
            await RequestHandler(new RemotingRequestContext(
                new IPEndPoint(IPAddress.Loopback, 10911),
                new RemotingCommand
                {
                    Code = RequestCode.ResetConsumerOffset,
                    ExtFields = fields,
                    Body = body
                },
                cancellationToken));

        public RemotingPushConsumerRun CreateRun()
        {
            var rebalanceService = new Mock<IRemotingRebalanceService>(MockBehavior.Strict);
            var session = new RemotingConsumerGroupSession(
                GroupClient,
                rebalanceService.Object,
                subscriptionVersion: 0);
            var invoker = new PushMessageHandlerInvoker(
                Options,
                static (_, _, _) => ValueTask.FromResult(ConsumeResult.Success),
                TimeProvider.System,
                RemotingRocketMQTelemetry.Disabled,
                NullLogger<PushMessageHandlerInvoker>.Instance);
            var run = new RemotingPushConsumerRun(session, invoker)
            {
                PullDeliveryCoordinator = new PullDeliveryCoordinator(
                    Options.PullMaxCachedMessages,
                    Options.PullMaxCachedMessageBytes)
            };
            return run;
        }

        public void DisposeHandler()
        {
            if (!_handlerDisposed)
            {
                Handler.Dispose();
                _handlerDisposed = true;
            }
        }

        public void Dispose()
        {
            DisposeHandler();
            Run?.Dispose();
            lifecycle.Dispose();
        }
    }

    private sealed class RunHolder
    {
        public RemotingPushConsumerRun? Run { get; set; }
    }
}
