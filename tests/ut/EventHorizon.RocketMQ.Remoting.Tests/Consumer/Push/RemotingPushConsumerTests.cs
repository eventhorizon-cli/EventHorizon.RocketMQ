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

using System.Text;
using EventHorizon.RocketMQ.Remoting.Consumer;
using EventHorizon.RocketMQ.Remoting.Consumer.Push;
using EventHorizon.RocketMQ.Remoting.Consumer.Route;
using EventHorizon.RocketMQ.Remoting.Protocol;
using EventHorizon.RocketMQ.Remoting.Protocol.Route;
using EventHorizon.RocketMQ.Remoting.Tests.Consumer.Coordination.Rebalance;
using EventHorizon.RocketMQ.Remoting.Tests.Consumer.Push.Support;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Moq;
using Xunit;

namespace EventHorizon.RocketMQ.Remoting.Tests.Consumer.Push;

public sealed class RemotingPushConsumerTests : RemotingPushConsumerTestSupport
{
    [Fact]
    public async Task RebalanceRegistration_OnRebalance_RebalancesAndUnregisters()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var remoting = new FakeRemotingClient("127.0.0.1@notify-test");
        var options = WithMessageHandler(new RemotingPushConsumerOptions
        {
            GroupName = "legacy-group",
            MaxConcurrency = 1,
            PullMaxCachedMessages = 8,
            LongPollingTimeout = TimeSpan.FromSeconds(1),
        }, static (_, _, _) => ValueTask.FromResult(ConsumeResult.Success));
        options.Subscribe("orders");
        var rebalance = new TestRemotingRebalanceService();
        var consumer = CreateRemotingPushConsumer(
            Options.Create(options),
            Options.Create(new RemotingClientOptions
            {
                ClientIP = "127.0.0.1",
                InstanceName = "notify-test",
                Namespace = "tenant-a",
                PollNameServerInterval = TimeSpan.FromHours(1),
                HeartbeatBrokerInterval = TimeSpan.FromHours(1)
            }),
            CreateRouteServiceMock().Object,
            remoting,
            TimeProvider.System,
            NullLogger<RemotingPushConsumer>.Instance,
            rebalanceService: rebalance);

        try
        {
            Assert.Equal(1, remoting.ActiveRequestHandlerRegistrations);
            await consumer.StartAsync(cancellationToken);
            Assert.Equal(1, rebalance.ActiveRegistrations);
            var initialCoordinationCount = remoting.Requests.Count(
                static request => request.Code == RequestCode.GetConsumerListByGroup);

            await rebalance.RebalanceAsync(
                "tenant-a%legacy-group",
                cancellationToken);
            await remoting.WaitForRequestCountAsync(
                RequestCode.GetConsumerListByGroup,
                initialCoordinationCount + 1,
                cancellationToken);
        }
        finally
        {
            await consumer.DisposeAsync();
        }

        Assert.Equal(0, remoting.ActiveRequestHandlerRegistrations);
        Assert.Equal(0, rebalance.ActiveRegistrations);
    }

    [Fact]
    public void Constructor_ResetRequestHandlerRegistrationFailure_Propagates()
    {
        var remoting = new Mock<IRemotingClient>(MockBehavior.Strict);
        remoting
            .Setup(value => value.RegisterRequestHandler(
                RequestCode.ResetConsumerOffset,
                It.IsAny<RemotingRequestHandler>()))
            .Throws(new InvalidOperationException("The reset-offset registration failed."));
        var options = WithMessageHandler(new RemotingPushConsumerOptions
        {
            GroupName = "legacy-group",
        }, static (_, _, _) => ValueTask.FromResult(ConsumeResult.Success));
        options.Subscribe("orders");
        var clientOptions = Options.Create(new RemotingClientOptions());

        var exception = Assert.Throws<InvalidOperationException>(() => new RemotingPushConsumer(
            Options.Create(options),
            clientOptions,
            new Mock<IRemotingConsumerEngine>(MockBehavior.Strict).Object,
            new RemotingConsumerRouteResolver(
                clientOptions,
                new Mock<ITopicRouteService>(MockBehavior.Strict).Object),
            remoting.Object,
            new TestRemotingRebalanceService(),
            TimeProvider.System,
            NullLogger<RemotingPushConsumer>.Instance,
            GetMessageHandler(options)));

        Assert.Equal("The reset-offset registration failed.", exception.Message);
    }

    [Fact]
    public async Task UnsubscribeAsync_PendingResetBoundaryBeforeResubscribing_Clears()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var remoting = new FakeRemotingClient("127.0.0.1@clear-reset-boundary");
        var options = WithMessageHandler(new RemotingPushConsumerOptions
        {
            GroupName = "legacy-group",
            InitialPosition = ConsumeFromPosition.Beginning,
            MaxConcurrency = 1,
            PullMaxCachedMessages = 8,
            LongPollingTimeout = TimeSpan.FromSeconds(1),
        }, static (_, _, _) => ValueTask.FromResult(ConsumeResult.Success));
        options.Subscribe("orders");
        await using var consumer = CreateRemotingPushConsumer(
            options,
            CreateRouteServiceMock().Object,
            remoting,
            "clear-reset-boundary");
        var body = Encoding.UTF8.GetBytes(
            """
            {"offsetTable":[[{"topic":"orders","brokerName":"broker-a","queueId":0},9]]}
            """);

        var reset = await remoting.InvokeBrokerRequestAsync(
            RequestCode.ResetConsumerOffset,
            new Dictionary<string, object>
            {
                ["group"] = "legacy-group",
                ["topic"] = "orders"
            },
            body,
            cancellationToken);
        Assert.True(reset.IsHandled);
        Assert.True(reset.SuppressResponse);

        await consumer.UnsubscribeAsync("orders", cancellationToken);
        await consumer.SubscribeAsync("orders", cancellationToken: cancellationToken);
        await consumer.StartAsync(cancellationToken);
        await remoting.WaitForTopicPullsAsync("orders", 1, cancellationToken);

        Assert.Equal(
            0,
            remoting.PullOffsets.First(static pull => pull.Topic == "orders").Offset);
    }

    [Fact]
    public async Task StopAsync_BrokerUnregisterFailure_CompletesAndRejects()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var remoting = new FakeRemotingClient("127.0.0.1@unregister-rejected")
        {
            UnregisterHandler = static (_, _) => Task.FromResult(new RemotingCommand
            {
                Code = ResponseCodes.ResError,
                Remark = "The broker rejected the unregister request."
            })
        };
        var options = WithMessageHandler(new RemotingPushConsumerOptions
        {
            GroupName = "legacy-group",
            MaxConcurrency = 1,
            PullMaxCachedMessages = 8,
            LongPollingTimeout = TimeSpan.FromSeconds(1),
        }, static (_, _, _) => ValueTask.FromResult(ConsumeResult.Success));
        options.Subscribe("orders");
        await using var consumer = CreateRemotingPushConsumer(
            options,
            CreateRouteServiceMock().Object,
            remoting,
            "unregister-rejected");

        await consumer.StartAsync(cancellationToken);
        await consumer.StopAsync(cancellationToken);

        Assert.Equal(1, remoting.UnregisterCalls);
    }

    [Fact]
    public async Task StopAsync_RebalanceStopFails_UnregistersBeforePropagatingFailure()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var remoting = new FakeRemotingClient("127.0.0.1@rebalance-stop-failure");
        var options = WithMessageHandler(new RemotingPushConsumerOptions
        {
            GroupName = "legacy-group",
            InitialPosition = ConsumeFromPosition.Beginning,
            MaxConcurrency = 1,
            PullMaxCachedMessages = 8,
            LongPollingTimeout = TimeSpan.FromSeconds(1),
        }, static (_, _, _) => ValueTask.FromResult(ConsumeResult.Success));
        options.Subscribe("orders");
        await using var consumer = CreateRemotingPushConsumer(
            options,
            CreateRouteServiceMock().Object,
            remoting,
            "rebalance-stop-failure",
            rebalanceService: new FailingStopRebalanceService());

        await consumer.StartAsync(cancellationToken);

        var exception = await Assert.ThrowsAsync<IOException>(() =>
            consumer.StopAsync(cancellationToken).AsTask());

        Assert.Equal("rebalance stop failed", exception.Message);
        Assert.Equal(1, remoting.UnregisterCalls);
    }

    [Fact]
    public async Task StartAsync_QueuedBeforeDispose_RejectsAfterLifecycleGateIsAcquired()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var unregisterStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseUnregister = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var remoting = new FakeRemotingClient("127.0.0.1@queued-start-dispose")
        {
            UnregisterHandler = async (_, token) =>
            {
                unregisterStarted.TrySetResult();
                await releaseUnregister.Task.WaitAsync(token);
                return new RemotingCommand { Code = ResponseCodes.ResSuccess };
            }
        };
        var options = WithMessageHandler(new RemotingPushConsumerOptions
        {
            GroupName = "legacy-group",
            MaxConcurrency = 1,
            PullMaxCachedMessages = 8,
            LongPollingTimeout = TimeSpan.FromSeconds(1),
        }, static (_, _, _) => ValueTask.FromResult(ConsumeResult.Success));
        options.Subscribe("orders");
        var consumer = CreateRemotingPushConsumer(
            options,
            CreateRouteServiceMock().Object,
            remoting,
            "queued-start-dispose");
        Task? firstStop = null;
        Task? dispose = null;
        try
        {
            await consumer.StartAsync(cancellationToken);
            firstStop = consumer.StopAsync(cancellationToken).AsTask();
            await unregisterStarted.Task.WaitAsync(TimeSpan.FromSeconds(1), cancellationToken);
            var queuedStart = consumer.StartAsync(cancellationToken).AsTask();
            dispose = consumer.DisposeAsync().AsTask();

            releaseUnregister.TrySetResult();
            await firstStop.WaitAsync(TimeSpan.FromSeconds(1), cancellationToken);

            await Assert.ThrowsAsync<ObjectDisposedException>(() => queuedStart);
            await dispose.WaitAsync(TimeSpan.FromSeconds(1), cancellationToken);
            Assert.Equal(1, remoting.UnregisterCalls);
        }
        finally
        {
            releaseUnregister.TrySetResult();
            if (firstStop is not null)
            {
                await firstStop;
            }

            if (dispose is not null)
            {
                await dispose;
            }
            else
            {
                await consumer.DisposeAsync();
            }
        }
    }

    private sealed class FailingStopRebalanceService :
        EventHorizon.RocketMQ.Remoting.Consumer.Coordination.Rebalance.IRemotingRebalanceService
    {
        public EventHorizon.RocketMQ.Remoting.Consumer.Coordination.Rebalance.IRemotingRebalanceRegistration Register(
            string consumerGroup,
            Func<CancellationToken, Task<bool>> rebalance) =>
            new FailingRegistration();

        private sealed class FailingRegistration :
            EventHorizon.RocketMQ.Remoting.Consumer.Coordination.Rebalance.IRemotingRebalanceRegistration
        {
            public void Wakeup()
            {
            }

            public ValueTask DisposeAsync() =>
                ValueTask.FromException(new IOException("rebalance stop failed"));
        }
    }
}
