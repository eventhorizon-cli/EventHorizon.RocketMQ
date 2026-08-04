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

using System.Collections.Concurrent;
using System.Text.Json;
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

namespace EventHorizon.RocketMQ.Remoting.Tests.Consumer.Push.Assignment;

public sealed class RemotingPushConsumerAssignmentTests : RemotingPushConsumerTestSupport
{
    [Fact]
    public async Task ConcurrentPushConsumer_OneReceiverPerQueueAcrossThreeBrokers_CreatesReceiverPerQueue()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var routes = new Mock<ITopicRouteService>(MockBehavior.Strict);
        routes
            .Setup(value => value.GetAsync(
                It.IsAny<string>(),
                It.IsAny<bool>(),
                It.IsAny<CancellationToken>()))
            .Returns<string, bool, CancellationToken>((topic, _, _) =>
                Task.FromResult(topic == "orders" ? MultiBrokerRoute() : new TopicRouteData()));
        var remoting = new FakeRemotingClient("127.0.0.1@multi-broker-unit")
        {
            PullHandler = static (_, token) => WaitForCanceledPullAsync(token)
        };
        var options = WithMessageHandler(new RemotingPushConsumerOptions
        {
            GroupName = "legacy-group",
            InitialPosition = ConsumeFromPosition.Beginning,
            MaxConcurrency = 3,
            PullMaxCachedMessages = 9,
            LongPollingTimeout = TimeSpan.FromSeconds(1),
        }, static (_, _, _) => ValueTask.FromResult(ConsumeResult.Success));
        options.Subscribe("orders");
        await using var consumer = CreateRemotingPushConsumer(
            options,
            routes.Object,
            remoting,
            "multi-broker-unit");

        await consumer.StartAsync(cancellationToken);
        await remoting.WaitForTopicPullsAsync("orders", 9, cancellationToken);

        Assert.Equal(
            CreateMultiBrokerQueues()
                .Select(static queue => (queue.BrokerName, queue.QueueId))
                .Order(),
            remoting.Requests
                .Where(static request => request.Code == RequestCode.PullMessage &&
                                         Assert.IsType<string>(request.ExtFields["topic"]) == "orders")
                .Select(static request => (
                    Assert.IsType<string>(request.ExtFields["bname"]),
                    Convert.ToInt32(request.ExtFields["queueId"])))
                .Distinct()
                .Order());
    }

    [Fact]
    public async Task Broadcasting_NoMembershipOrOffsetQueries_AssignsAllQueues()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var offsetPath = System.IO.Path.Combine(
            System.IO.Path.GetTempPath(),
            $"rocketmq-broadcast-{Guid.NewGuid():N}.json");
        try
        {
            var remoting = new FakeRemotingClient("127.0.0.1@broadcast-test");
            var routeTopics = new ConcurrentQueue<string>();
            var routes = CreateRouteServiceMock(3, routeTopics);
            var options = CreateBroadcastOptions(offsetPath, static (_, _, _) =>
                ValueTask.FromResult(ConsumeResult.Success));
            await using var consumer = CreateRemotingPushConsumer(options, routes.Object, remoting, "broadcast-test");

            await consumer.StartAsync(cancellationToken);
            await remoting.WaitForPullsAsync(3, cancellationToken);
            await consumer.StopAsync(cancellationToken);

            Assert.Equal([0, 1, 2], remoting.PullOffsets.Select(static pull => pull.QueueId).Order());
            Assert.Equal(["orders"], routeTopics.Distinct(StringComparer.Ordinal));
            Assert.DoesNotContain(remoting.Requests, static request => request.Code is
                RequestCode.GetConsumerListByGroup or RequestCode.QueryConsumerOffset or
                RequestCode.UpdateConsumerOffset or RequestCode.ConsumerSendMsgBack);
            using var heartbeat = JsonDocument.Parse(Assert.Single(remoting.HeartbeatBodies));
            var consumerData = Assert.Single(
                heartbeat.RootElement.GetProperty("consumerDataSet").EnumerateArray());
            Assert.Equal("BROADCASTING", consumerData.GetProperty("messageModel").GetString());
            var subscriptions = consumerData.GetProperty("subscriptionDataSet").EnumerateArray().ToArray();
            Assert.Single(subscriptions);
            Assert.Equal("orders", subscriptions[0].GetProperty("topic").GetString());
        }
        finally
        {
            File.Delete(offsetPath);
        }
    }

    [Fact]
    public async Task BrokerMembership_Missing_RevokesOwnedQueues()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var remoting = new FakeRemotingClient("127.0.0.1@legacy-test");
        var options = WithMessageHandler(new RemotingPushConsumerOptions
        {
            GroupName = "legacy-group",
            MaxConcurrency = 1,
            PullMaxCachedMessages = 8,
            LongPollingTimeout = TimeSpan.FromSeconds(1),
        }, static (_, _, _) => ValueTask.FromResult(ConsumeResult.Success));
        options.Subscribe("orders");
        var clientOptions = new RemotingClientOptions
        {
            ClientIP = "127.0.0.1",
            InstanceName = "legacy-test",
            PollNameServerInterval = TimeSpan.FromMilliseconds(20),
            HeartbeatBrokerInterval = TimeSpan.FromMilliseconds(20)
        };
        var rebalance = new TestRemotingRebalanceService();
        await using var consumer = CreateRemotingPushConsumer(
            Options.Create(options),
            Options.Create(clientOptions),
            CreateRouteServiceMock().Object,
            remoting,
            TimeProvider.System,
            NullLogger<RemotingPushConsumer>.Instance,
            rebalanceService: rebalance);

        await consumer.StartAsync(cancellationToken);
        await remoting.WaitForPullsAsync(2, cancellationToken);
        remoting.SetConsumerIds("another-client");
        await rebalance.RebalanceAsync("legacy-group", cancellationToken);
        await remoting.WaitForCanceledPullsAsync(2, cancellationToken);
        var pullCountAfterRevocation = remoting.PullOffsets.Count;
        await Task.Delay(100, cancellationToken);
        await consumer.StopAsync(cancellationToken);

        Assert.Equal(2, pullCountAfterRevocation);
        Assert.Equal(pullCountAfterRevocation, remoting.PullOffsets.Count);
    }

    [Fact]
    public async Task SubscribeAsync_PeriodicRebalanceHasOldFilter_NeverPairsOldFilterWithNewVersion()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var oldFilter = new FilterExpression("old");
        var newFilter = new FilterExpression("new");
        var routeCallCount = 0;
        var blockedRouteStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseBlockedRoute = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var routes = new Mock<ITopicRouteService>(MockBehavior.Strict);
        routes
            .Setup(value => value.GetAsync(
                It.IsAny<string>(),
                It.IsAny<bool>(),
                It.IsAny<CancellationToken>()))
            .Returns<string, bool, CancellationToken>(async (_, _, token) =>
            {
                if (Interlocked.Increment(ref routeCallCount) == 2)
                {
                    blockedRouteStarted.TrySetResult();
                    await releaseBlockedRoute.Task.WaitAsync(token).ConfigureAwait(false);
                }

                return Route(0);
            });
        var engine = new Mock<IRemotingConsumerEngine>(MockBehavior.Strict);
        engine
            .Setup(value => value.StartAsync(It.IsAny<CancellationToken>()))
            .Returns(ValueTask.CompletedTask);
        engine
            .Setup(value => value.StopAsync(It.IsAny<CancellationToken>()))
            .Returns(ValueTask.CompletedTask);
        engine
            .Setup(value => value.DisposeAsync())
            .Returns(ValueTask.CompletedTask);
        var options = WithMessageHandler(new RemotingPushConsumerOptions
        {
            GroupName = "subscription-race-group",
            ConsumerMode = ConsumerMode.Broadcasting,
            MaxConcurrency = 1,
            PullMaxCachedMessages = 8,
            LongPollingTimeout = TimeSpan.FromSeconds(1)
        }, static (_, _, _) => ValueTask.FromResult(ConsumeResult.Success));
        options.Subscribe("orders", oldFilter);
        var clientOptions = Options.Create(new RemotingClientOptions
        {
            ClientIP = "127.0.0.1",
            InstanceName = "subscription-race",
            PollNameServerInterval = TimeSpan.FromHours(1),
            HeartbeatBrokerInterval = TimeSpan.FromHours(1)
        });
        var remoting = new FakeRemotingClient("127.0.0.1@subscription-race");
        var rebalance = new TestRemotingRebalanceService();
        await using var consumer = new RemotingPushConsumer(
            Options.Create(options),
            clientOptions,
            engine.Object,
            new RemotingConsumerRouteResolver(clientOptions, routes.Object),
            remoting,
            rebalance,
            TimeProvider.System,
            NullLogger<RemotingPushConsumer>.Instance,
            GetMessageHandler(options));

        await consumer.StartAsync(cancellationToken);
        var initialRecord = Assert.Single(
            remoting.HeartbeatBodies.SelectMany(ReadHeartbeatSubscriptions),
            value => value.Filter == oldFilter.Expression);
        while (remoting.HeartbeatBodies.TryDequeue(out _))
        {
        }

        try
        {
            var periodic = rebalance.RebalanceAsync(options.GroupName, cancellationToken);
            await blockedRouteStarted.Task.WaitAsync(TimeSpan.FromSeconds(1), cancellationToken);
            var replacement = consumer.SubscribeAsync("orders", newFilter, cancellationToken);
            Assert.False(replacement.IsCompleted);

            releaseBlockedRoute.TrySetResult();
            await periodic.WaitAsync(TimeSpan.FromSeconds(1), cancellationToken);
            await replacement.WaitAsync(TimeSpan.FromSeconds(1), cancellationToken);
        }
        finally
        {
            releaseBlockedRoute.TrySetResult();
        }

        var observed = remoting.HeartbeatBodies.SelectMany(ReadHeartbeatSubscriptions).ToArray();
        var periodicRecord = Assert.Single(observed, value => value.Filter == oldFilter.Expression);
        var replacementRecord = Assert.Single(observed, value =>
            value.Filter == newFilter.Expression && value.Version != initialRecord.Version);
        Assert.Equal(initialRecord.Version, periodicRecord.Version);
        Assert.DoesNotContain(observed, value =>
            value.Filter == oldFilter.Expression && value.Version == replacementRecord.Version);
        Assert.NotEqual(initialRecord.Version, replacementRecord.Version);
    }

    private static IReadOnlyList<(long Version, string Filter)> ReadHeartbeatSubscriptions(byte[] heartbeat)
    {
        using var document = JsonDocument.Parse(heartbeat);
        var consumerData = Assert.Single(document.RootElement.GetProperty("consumerDataSet").EnumerateArray());
        return consumerData.GetProperty("subscriptionDataSet")
            .EnumerateArray()
            .Select(static subscription => (
                subscription.GetProperty("subVersion").GetInt64(),
                subscription.GetProperty("subString").GetString() ?? string.Empty))
            .ToArray();
    }
}
