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
using EventHorizon.RocketMQ.Remoting.Consumer.Coordination.Rebalance;
using EventHorizon.RocketMQ.Remoting.Consumer.LitePull;
using EventHorizon.RocketMQ.Remoting.Consumer.Push;
using EventHorizon.RocketMQ.Remoting.Consumer.Route;
using EventHorizon.RocketMQ.Remoting.Protocol;
using EventHorizon.RocketMQ.Remoting.Protocol.Route;
using EventHorizon.RocketMQ.Remoting.Tests.Consumer.Coordination.Rebalance;
using EventHorizon.RocketMQ.Remoting.Tests.Consumer.Push.Support;
using Microsoft.Extensions.Options;
using Moq;
using Xunit;

namespace EventHorizon.RocketMQ.Remoting.Tests.Consumer.Push.Route;

public sealed class RemotingPushConsumerRouteTests : RemotingPushConsumerTestSupport
{
    [Fact]
    public async Task SubscribeBeforeStart_BroadcastingWithConfiguredSqlFilter_PullsWithConfiguredSqlFilter()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var offsetPath = System.IO.Path.Combine(
            System.IO.Path.GetTempPath(),
            $"rocketmq-dynamic-subscription-{Guid.NewGuid():N}.json");
        try
        {
            var options = WithMessageHandler(new RemotingPushConsumerOptions
            {
                GroupName = "broadcast-group",
                ConsumerMode = ConsumerMode.Broadcasting,
                LocalOffsetStorePath = offsetPath,
                InitialPosition = ConsumeFromPosition.Beginning,
                MaxConcurrency = 1,
                PullMaxCachedMessages = 8,
                LongPollingTimeout = TimeSpan.FromSeconds(1),
            }, static (_, _, _) => ValueTask.FromResult(ConsumeResult.Success));
            var remoting = new FakeRemotingClient("127.0.0.1@dynamic-test");
            await using var consumer = CreateRemotingPushConsumer(
                options,
                CreateRouteServiceMock().Object,
                remoting,
                "dynamic-test");

            await consumer.SubscribeAsync(
                "orders",
                new FilterExpression("region = 'west'", FilterExpressionType.Sql),
                cancellationToken);
            await consumer.StartAsync(cancellationToken);
            await remoting.WaitForPullsAsync(1, cancellationToken);
            await consumer.StopAsync(cancellationToken);

            var pull = Assert.Single(remoting.Requests, static request =>
                request.Code == RequestCode.PullMessage &&
                Assert.IsType<string>(request.ExtFields["topic"]) == "orders");
            Assert.Equal("region = 'west'", pull.ExtFields["subscription"]);
            Assert.Equal("SQL92", pull.ExtFields["expressionType"]);
            using var heartbeat = JsonDocument.Parse(Assert.Single(remoting.HeartbeatBodies));
            var subscriptions = Assert.Single(
                    heartbeat.RootElement.GetProperty("consumerDataSet").EnumerateArray())
                .GetProperty("subscriptionDataSet")
                .EnumerateArray()
                .ToArray();
            var subscription = Assert.Single(subscriptions);
            Assert.Equal("orders", subscription.GetProperty("topic").GetString());
            Assert.Equal("region = 'west'", subscription.GetProperty("subString").GetString());
            Assert.DoesNotContain(remoting.Requests, static request =>
                request.Code == RequestCode.GetConsumerListByGroup);
        }
        finally
        {
            File.Delete(offsetPath);
        }
    }

    [Fact]
    public async Task RunningSubscribeAndFilterUpdate_RestartWithLatestFilter_PullsWithLatestFilter()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var options = CreateClusteringOptions();
        options.Subscribe("orders", new FilterExpression("created"));
        var remoting = new FakeRemotingClient("127.0.0.1@dynamic-test");
        await using var consumer = CreateRemotingPushConsumer(
            options,
            CreateRouteServiceMock().Object,
            remoting,
            "dynamic-test");

        await consumer.StartAsync(cancellationToken);
        await remoting.WaitForPullsAsync(2, cancellationToken);
        await consumer.SubscribeAsync(
            "payments",
            new FilterExpression("region = 'west'", FilterExpressionType.Sql),
            cancellationToken);
        await remoting.WaitForTopicPullsAsync("payments", 1, cancellationToken);
        await consumer.SubscribeAsync(
            "orders",
            new FilterExpression("paid || shipped"),
            cancellationToken);
        await remoting.WaitForTopicPullsAsync("orders", 2, cancellationToken);
        await consumer.StopAsync(cancellationToken);

        var paymentPull = remoting.Requests.Last(request =>
            request.Code == RequestCode.PullMessage &&
            Assert.IsType<string>(request.ExtFields["topic"]) == "payments");
        Assert.Equal("region = 'west'", paymentPull.ExtFields["subscription"]);
        Assert.Equal("SQL92", paymentPull.ExtFields["expressionType"]);
        var updatedOrderPull = remoting.Requests.Last(request =>
            request.Code == RequestCode.PullMessage &&
            Assert.IsType<string>(request.ExtFields["topic"]) == "orders");
        Assert.Equal("paid || shipped", updatedOrderPull.ExtFields["subscription"]);
        Assert.Equal("TAG", updatedOrderPull.ExtFields["expressionType"]);
        Assert.Contains("orders", remoting.CanceledPullTopics);
        var latestHeartbeat = JsonDocument.Parse(remoting.HeartbeatBodies.Last());
        using (latestHeartbeat)
        {
            var subscriptions = Assert.Single(
                    latestHeartbeat.RootElement.GetProperty("consumerDataSet").EnumerateArray())
                .GetProperty("subscriptionDataSet")
                .EnumerateArray()
                .ToArray();
            Assert.Contains(subscriptions, static subscription =>
                subscription.GetProperty("topic").GetString() == "payments" &&
                subscription.GetProperty("expressionType").GetString() == "SQL92");
            Assert.Contains(subscriptions, static subscription =>
                subscription.GetProperty("topic").GetString() == "orders" &&
                subscription.GetProperty("subString").GetString() == "paid || shipped");
            Assert.Contains(subscriptions, static subscription =>
                subscription.GetProperty("topic").GetString() == "%RETRY%legacy-group");
        }
    }

    [Fact]
    public async Task RunningUnsubscribe_TopicFromHeartbeat_RevokesAndRemoves()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var options = CreateClusteringOptions();
        options.Subscribe("orders");
        var remoting = new FakeRemotingClient("127.0.0.1@dynamic-test");
        await using var consumer = CreateRemotingPushConsumer(
            options,
            CreateRouteServiceMock().Object,
            remoting,
            "dynamic-test");

        await consumer.StartAsync(cancellationToken);
        await remoting.WaitForPullsAsync(2, cancellationToken);
        await consumer.UnsubscribeAsync("orders", cancellationToken);
        var orderPullCount = remoting.PullOffsets.Count(static pull => pull.Topic == "orders");
        await Task.Delay(100, cancellationToken);
        await consumer.StopAsync(cancellationToken);

        Assert.Contains("orders", remoting.CanceledPullTopics);
        Assert.Equal(orderPullCount, remoting.PullOffsets.Count(static pull => pull.Topic == "orders"));
        using var heartbeat = JsonDocument.Parse(remoting.HeartbeatBodies.Last());
        var subscriptions = Assert.Single(
                heartbeat.RootElement.GetProperty("consumerDataSet").EnumerateArray())
            .GetProperty("subscriptionDataSet")
            .EnumerateArray()
            .ToArray();
        Assert.DoesNotContain(subscriptions, static subscription =>
            subscription.GetProperty("topic").GetString() == "orders");
        Assert.Contains(subscriptions, static subscription =>
            subscription.GetProperty("topic").GetString() == "%RETRY%legacy-group");
    }

    [Fact]
    public async Task SubscriptionChanges_DisposedConsumerAndHonorCancellation_RejectsDisposedConsumerAndCancellation()
    {
        var options = CreateClusteringOptions();
        var consumer = CreateRemotingPushConsumer(
            options,
            CreateRouteServiceMock().Object,
            new FakeRemotingClient("127.0.0.1@dynamic-test"),
            "dynamic-test");
        await consumer.DisposeAsync();

        await Assert.ThrowsAsync<ObjectDisposedException>(() =>
            consumer.SubscribeAsync("orders", cancellationToken: TestContext.Current.CancellationToken));
        await Assert.ThrowsAsync<ObjectDisposedException>(() =>
            consumer.UnsubscribeAsync("orders", TestContext.Current.CancellationToken));

        var active = CreateRemotingPushConsumer(
            options,
            CreateRouteServiceMock().Object,
            new FakeRemotingClient("127.0.0.1@dynamic-test"),
            "dynamic-test");
        await using (active)
        {
            using var canceled = new CancellationTokenSource();
            canceled.Cancel();
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() =>
                active.SubscribeAsync("orders", cancellationToken: canceled.Token));
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() =>
                active.UnsubscribeAsync("orders", canceled.Token));
        }
    }

    [Fact]
    public async Task StartAsync_RouteWithInvalidEntries_UsesReadableSlaveQueue()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var route = new TopicRouteData
        {
            QueueDatas =
            [
                new QueueData
                {
                    BrokerName = "write-only",
                    ReadQueueNums = 1,
                    WriteQueueNums = 1,
                    Perm = 2
                },
                new QueueData
                {
                    BrokerName = "missing-broker",
                    ReadQueueNums = 1,
                    WriteQueueNums = 1,
                    Perm = 6
                },
                new QueueData
                {
                    BrokerName = "no-address",
                    ReadQueueNums = 1,
                    WriteQueueNums = 1,
                    Perm = 6
                },
                new QueueData
                {
                    BrokerName = "slave-only",
                    ReadQueueNums = 1,
                    WriteQueueNums = 1,
                    Perm = 6
                }
            ],
            BrokerDatas =
            [
                new BrokerData { BrokerName = "no-address" },
                new BrokerData
                {
                    BrokerName = "slave-only",
                    BrokerAddrs = new ConcurrentDictionary<long, string>(
                        [new KeyValuePair<long, string>(1, "127.0.0.1:10912")])
                }
            ]
        };
        var routes = new Mock<ITopicRouteService>(MockBehavior.Strict);
        routes
            .Setup(value => value.GetAsync(
                It.IsAny<string>(),
                It.IsAny<bool>(),
                It.IsAny<CancellationToken>()))
            .Returns<string, bool, CancellationToken>((topic, _, _) =>
                Task.FromResult(topic == "orders" ? route : new TopicRouteData()));
        var remoting = new FakeRemotingClient("127.0.0.1@route-with-invalid-entries");
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
            routes.Object,
            remoting,
            "route-with-invalid-entries");

        await consumer.StartAsync(cancellationToken);
        await remoting.WaitForTopicPullsAsync("orders", 1, cancellationToken);

        var membershipRequest = Assert.Single(
            remoting.Requests,
            static request => request.Code == RequestCode.GetConsumerListByGroup);
        Assert.Equal("slave-only", membershipRequest.ExtFields["bname"]);
        Assert.Equal(
            1,
            remoting.PullOffsets.Count(static pull => pull.Topic == "orders"));
    }

    [Fact]
    public async Task Coordination_HealthyTopicAfterRouteMembershipAndHeartbeatFailures_Recovers()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var membershipRequests = 0;
        var routes = new Mock<ITopicRouteService>(MockBehavior.Strict);
        routes
            .Setup(value => value.GetAsync(
                It.IsAny<string>(),
                It.IsAny<bool>(),
                It.IsAny<CancellationToken>()))
            .Returns<string, bool, CancellationToken>((topic, _, _) => topic == "invoices"
                ? Task.FromResult(Route(1))
                : Task.FromException<TopicRouteData>(
                    new IOException($"The route for {topic} is temporarily unavailable.")));
        var remoting = new FakeRemotingClient("127.0.0.1@coordination-recovery")
        {
            HeartbeatHandler = static (_, _) => Task.FromResult(new RemotingCommand
            {
                Code = ResponseCodes.ResError,
                Remark = "The heartbeat was rejected."
            }),
            ConsumerIdsHandler = (_, _) => Interlocked.Increment(ref membershipRequests) == 1
                ? Task.FromResult(new RemotingCommand
                {
                    Code = ResponseCodes.ResError,
                    Remark = "The consumer-list request failed."
                })
                : Task.FromResult(new RemotingCommand
                {
                    Code = ResponseCodes.ResSuccess,
                    Body = JsonSerializer.SerializeToUtf8Bytes(new
                    {
                        consumerIdList = new[] { "127.0.0.1@coordination-recovery" }
                    })
                })
        };
        var options = WithMessageHandler(new RemotingPushConsumerOptions
        {
            GroupName = "legacy-group",
            InitialPosition = ConsumeFromPosition.Beginning,
            MaxConcurrency = 1,
            PullMaxCachedMessages = 8,
            LongPollingTimeout = TimeSpan.FromSeconds(1),
        }, static (_, _, _) => ValueTask.FromResult(ConsumeResult.Success));
        options.Subscribe("orders");
        options.Subscribe("invoices");
        var rebalance = new TestRemotingRebalanceService();
        await using var consumer = CreateRemotingPushConsumer(
            options,
            routes.Object,
            remoting,
            "coordination-recovery",
            rebalanceService: rebalance);

        await consumer.StartAsync(cancellationToken);
        await remoting.WaitForTopicPullsAsync("invoices", 1, cancellationToken);

        Assert.Equal(2, membershipRequests);
        Assert.Contains(remoting.Requests, static request => request.Code == RequestCode.HeartBeat);
    }

    [Fact]
    public async Task StartAsync_IncompleteInitialCoordination_WakesRebalance()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var remoting = new FakeRemotingClient("127.0.0.1@initial-coordination-retry");
        remoting.SetConsumerIds("another-client");
        var options = WithMessageHandler(new RemotingPushConsumerOptions
        {
            GroupName = "legacy-group",
            MaxConcurrency = 1,
            PullMaxCachedMessages = 8,
            LongPollingTimeout = TimeSpan.FromSeconds(1),
        }, static (_, _, _) => ValueTask.FromResult(ConsumeResult.Success));
        options.Subscribe("orders");
        var registration = new Mock<IRemotingRebalanceRegistration>(MockBehavior.Strict);
        registration.Setup(value => value.Wakeup());
        registration.Setup(value => value.DisposeAsync()).Returns(ValueTask.CompletedTask);
        var rebalance = new Mock<IRemotingRebalanceService>(MockBehavior.Strict);
        rebalance
            .Setup(value => value.Register(
                "legacy-group",
                It.IsAny<Func<CancellationToken, Task<bool>>>()))
            .Returns(registration.Object);
        await using var consumer = CreateRemotingPushConsumer(
            options,
            CreateRouteServiceMock().Object,
            remoting,
            "initial-coordination-retry",
            rebalanceService: rebalance.Object);

        await consumer.StartAsync(cancellationToken);

        Assert.Empty(consumer.Assignment);
        registration.Verify(value => value.Wakeup(), Times.Once);
    }

    [Fact]
    public async Task RunningSubscribe_IncompleteDirectCoordination_WakesRebalance()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var remoting = new FakeRemotingClient("127.0.0.1@subscription-coordination-retry");
        var options = WithMessageHandler(new RemotingPushConsumerOptions
        {
            GroupName = "legacy-group",
            MaxConcurrency = 1,
            PullMaxCachedMessages = 8,
            LongPollingTimeout = TimeSpan.FromSeconds(1),
        }, static (_, _, _) => ValueTask.FromResult(ConsumeResult.Success));
        options.Subscribe("orders");
        var registration = new Mock<IRemotingRebalanceRegistration>(MockBehavior.Strict);
        registration.Setup(value => value.Wakeup());
        registration.Setup(value => value.DisposeAsync()).Returns(ValueTask.CompletedTask);
        var rebalance = new Mock<IRemotingRebalanceService>(MockBehavior.Strict);
        rebalance
            .Setup(value => value.Register(
                "legacy-group",
                It.IsAny<Func<CancellationToken, Task<bool>>>()))
            .Returns(registration.Object);
        await using var consumer = CreateRemotingPushConsumer(
            options,
            CreateRouteServiceMock().Object,
            remoting,
            "subscription-coordination-retry",
            rebalanceService: rebalance.Object);
        await consumer.StartAsync(cancellationToken);
        remoting.SetConsumerIds("another-client");

        await consumer.SubscribeAsync("payments", cancellationToken: cancellationToken);

        registration.Verify(value => value.Wakeup(), Times.Once);
    }

    [Fact]
    public async Task ConsumerEngine_NamespaceIsConfigured_WrapsWireTopicsAndReturnsLogicalQueues()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var options = new RemotingLitePullConsumerOptions { GroupName = "legacy-group" };
        options.Subscribe("orders");
        var clientOptions = new RemotingClientOptions { Namespace = "tenant-a" };
        var routeTopics = new ConcurrentQueue<string>();
        var routes = CreateRouteServiceMock(topics: routeTopics);
        var remoting = new FakeRemotingClient("client-a");
        await using var consumer = CreateRemotingConsumerEngine(
            options,
            options.PullBatchSize,
            options.MaxMessageBytes,
            options.LongPollingTimeout,
            Options.Create(clientOptions),
            new RemotingConsumerRouteResolver(Options.Create(clientOptions), routes.Object),
            remoting,
            TimeProvider.System);

        await consumer.StartAsync(cancellationToken);
        var queue = Assert.Single(await consumer.GetMessageQueuesAsync("orders", cancellationToken));
        var committed = await consumer.GetOffsetAsync(queue, cancellationToken);
        var end = await consumer.QueryOffsetAsync(queue, ConsumeFromPosition.End, cancellationToken: cancellationToken);
        await consumer.UpdateOffsetAsync(queue, end, cancellationToken);
        await consumer.StopAsync(cancellationToken);

        Assert.Equal("orders", queue.Topic);
        Assert.Equal(-1, committed);
        Assert.Equal(17, end);
        Assert.Equal(4, routeTopics.Count);
        Assert.All(routeTopics, static topic => Assert.Equal("tenant-a%orders", topic));
        Assert.All(
            remoting.Requests.Where(static request => request.Code is
                RequestCode.QueryConsumerOffset or RequestCode.UpdateConsumerOffset),
            static request => Assert.Equal("tenant-a%legacy-group", request.ExtFields["consumerGroup"]));
        Assert.All(
            remoting.Requests.Where(static request => request.Code is
                RequestCode.QueryConsumerOffset or RequestCode.GetMaxOffset or RequestCode.UpdateConsumerOffset),
            static request => Assert.Equal("tenant-a%orders", request.ExtFields["topic"]));
    }
}
