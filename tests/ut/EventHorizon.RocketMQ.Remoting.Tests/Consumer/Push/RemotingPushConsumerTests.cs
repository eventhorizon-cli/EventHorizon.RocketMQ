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

using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Net;
using System.Runtime.CompilerServices;
using System.Text;
using System.Text.Json;
using EventHorizon.RocketMQ.Remoting.Consumer;
using EventHorizon.RocketMQ.Remoting.Consumer.Pull.Lite;
using EventHorizon.RocketMQ.Remoting.Consumer.Push;
using EventHorizon.RocketMQ.Remoting.Consumer.Push.Offset;
using EventHorizon.RocketMQ.Remoting.Instrumentation;
using EventHorizon.RocketMQ.Remoting.Protocol;
using EventHorizon.RocketMQ.Remoting.Protocol.Route;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Moq;
using Xunit;

namespace EventHorizon.RocketMQ.Remoting.Tests.Consumer.Push;

public sealed partial class RemotingPushConsumerTests
{
    private static readonly ConditionalWeakTable<
        RemotingPushConsumerOptions,
        Func<IReadOnlyList<RemotingMessageView>, RemotingPushConsumeContext, CancellationToken, ValueTask<ConsumeResult>>>
        MessageHandlers = new();

    [Theory]
    [InlineData(ConsumeFromPosition.Beginning, "CONSUME_FROM_FIRST_OFFSET")]
    [InlineData(ConsumeFromPosition.End, "CONSUME_FROM_LAST_OFFSET")]
    [InlineData(ConsumeFromPosition.Timestamp, "CONSUME_FROM_TIMESTAMP")]
    public void Heartbeat_InitialPosition_UsesConfiguredValue(
        ConsumeFromPosition position,
        string expectedConsumeFromWhere)
    {
        var body = LegacyConsumerProtocol.EncodeHeartbeat(
            "client-a",
            "group-a",
            new Dictionary<string, FilterExpression> { ["orders"] = FilterExpression.All },
            123,
            ConsumerMode.Clustering,
            position,
            false);

        using var heartbeat = JsonDocument.Parse(body);
        var consumerData = Assert.Single(
            heartbeat.RootElement.GetProperty("consumerDataSet").EnumerateArray());
        Assert.Equal(expectedConsumeFromWhere, consumerData.GetProperty("consumeFromWhere").GetString());
    }

    [Theory]
    [InlineData(ConsumerMode.Clustering, "CLUSTERING")]
    [InlineData(ConsumerMode.Broadcasting, "BROADCASTING")]
    public void Heartbeat_ConsumerMode_UsesConfiguredValue(ConsumerMode mode, string expectedMessageModel)
    {
        var body = LegacyConsumerProtocol.EncodeHeartbeat(
            "client-a",
            "group-a",
            new Dictionary<string, FilterExpression> { ["orders"] = FilterExpression.All },
            123,
            mode,
            ConsumeFromPosition.End,
            false);

        using var heartbeat = JsonDocument.Parse(body);
        var consumerData = Assert.Single(
            heartbeat.RootElement.GetProperty("consumerDataSet").EnumerateArray());
        Assert.Equal(expectedMessageModel, consumerData.GetProperty("messageModel").GetString());
    }

    [Fact]
    public void ResetOffsetDecoder_GsonComplexMapFixture_DecodesOffsets()
    {
        var body = Encoding.UTF8.GetBytes(
            """
            {"offsetTable":[[{"topic":"tenant-a%orders","brokerName":"broker-a","queueId":0},7]]}
            """);

        var offset = Assert.Single(LegacyResetOffsetDecoder.Decode(body));

        Assert.Equal(new LegacyResetOffset("tenant-a%orders", "broker-a", 0, 7), offset);
    }

    [Fact]
    public void ResetOffsetDecoder_FastJsonObjectKeyFixture_DecodesOffsets()
    {
        var body = Encoding.UTF8.GetBytes(
            """
            {"offsetTable":{{"brokerName":"broker-a","queueId":0,"topic":"tenant-a%orders"}:7}}
            """);

        var offset = Assert.Single(LegacyResetOffsetDecoder.Decode(body));

        Assert.Equal(new LegacyResetOffset("tenant-a%orders", "broker-a", 0, 7), offset);
    }

    [Fact]
    public void ResetOffsetDecoder_CFlatListFixture_DecodesOffsets()
    {
        var body = Encoding.UTF8.GetBytes(
            """
            {"offsetTable":[{"topic":"tenant-a%orders","brokerName":"broker-a","queueId":0,"offset":7}]}
            """);

        var offset = Assert.Single(LegacyResetOffsetDecoder.Decode(body));

        Assert.Equal(new LegacyResetOffset("tenant-a%orders", "broker-a", 0, 7), offset);
    }

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
    public async Task ResetOffsetNotification_ResetOffsetWithActiveReceiver_StopsOldReceiverAndPullsFromReset()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var remoting = new FakeRemotingClient("127.0.0.1@reset-test")
        {
            TrackCommittedOffsets = true
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
        var consumer = CreateRemotingPushConsumer(
            Options.Create(options),
            Options.Create(new RemotingClientOptions
            {
                ClientIP = "127.0.0.1",
                InstanceName = "reset-test",
                Namespace = "tenant-a",
                PollNameServerInterval = TimeSpan.FromHours(1),
                HeartbeatBrokerInterval = TimeSpan.FromHours(1)
            }),
            CreateRouteServiceMock().Object,
            remoting,
            TimeProvider.System,
            NullLogger<RemotingPushConsumer>.Instance);
        var body = Encoding.UTF8.GetBytes(
            """
            {"offsetTable":[[{"topic":"tenant-a%orders","brokerName":"broker-a","queueId":0},7]]}
            """);

        try
        {
            Assert.Equal(1, remoting.ActiveRequestHandlerRegistrations);
            await consumer.StartAsync(cancellationToken);
            await remoting.WaitForTopicPullsAsync("tenant-a%orders", 1, cancellationToken);
            var originalPullCount = remoting.PullOffsets.Count;

            var ignored = await remoting.InvokeBrokerRequestAsync(
                RequestCode.ResetConsumerOffset,
                new Dictionary<string, object>
                {
                    ["group"] = "tenant-a%another-group",
                    ["topic"] = "tenant-a%orders"
                },
                body,
                cancellationToken);
            Assert.False(ignored.IsHandled);
            await Task.Delay(100, cancellationToken);
            Assert.Equal(originalPullCount, remoting.PullOffsets.Count);

            var handled = await remoting.InvokeBrokerRequestAsync(
                RequestCode.ResetConsumerOffset,
                new Dictionary<string, object>
                {
                    ["group"] = "tenant-a%legacy-group",
                    ["topic"] = "tenant-a%orders"
                },
                body,
                cancellationToken);
            Assert.True(handled.IsHandled);
            Assert.True(handled.SuppressResponse);
            await remoting.WaitForTopicPullsAsync("tenant-a%orders", 2, cancellationToken);

            Assert.Contains(
                remoting.PullOffsets,
                static pull => pull.Topic == "tenant-a%orders" && pull.QueueId == 0 && pull.Offset == 7);
            Assert.Contains(
                remoting.UpdatedOffsets,
                static update => update.Topic == "tenant-a%orders" && update.Offset == 7);
        }
        finally
        {
            await consumer.DisposeAsync();
        }

        Assert.Equal(0, remoting.ActiveRequestHandlerRegistrations);
    }

    [Fact]
    public async Task ResetOffsetNotification_ReplacementPullPending_RetriesBoundary()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var resetPersisted = new TaskCompletionSource<long>(TaskCreationOptions.RunContinuationsAsynchronously);
        var remoting = new FakeRemotingClient("127.0.0.1@reset-failure")
        {
            TrackCommittedOffsets = true,
            UpdateOffsetHandler = (request, _) =>
            {
                var offset = Convert.ToInt64(request.ExtFields["commitOffset"]);
                if (offset == 7)
                {
                    resetPersisted.TrySetResult(offset);
                }

                return Task.FromResult(new RemotingCommand { Code = ResponseCodes.ResSuccess });
            }
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
        await using var consumer = CreateRemotingPushConsumer(
            Options.Create(options),
            Options.Create(new RemotingClientOptions
            {
                ClientIP = "127.0.0.1",
                InstanceName = "reset-failure",
                Namespace = "tenant-a",
                PollNameServerInterval = TimeSpan.FromHours(1),
                HeartbeatBrokerInterval = TimeSpan.FromHours(1)
            }),
            CreateRouteServiceMock().Object,
            remoting,
            TimeProvider.System,
            NullLogger<RemotingPushConsumer>.Instance);
        var body = Encoding.UTF8.GetBytes(
            """
            {"offsetTable":[[{"topic":"tenant-a%orders","brokerName":"broker-a","queueId":0},7]]}
            """);

        await consumer.StartAsync(cancellationToken);
        await remoting.WaitForTopicPullsAsync("tenant-a%orders", 1, cancellationToken);
        remoting.FailNextOffsetUpdate();

        await Assert.ThrowsAsync<IOException>(async () => await remoting.InvokeBrokerRequestAsync(
            RequestCode.ResetConsumerOffset,
            new Dictionary<string, object>
            {
                ["group"] = "tenant-a%legacy-group",
                ["topic"] = "tenant-a%orders"
            },
            body,
            cancellationToken));
        await remoting.WaitForTopicPullsAsync("tenant-a%orders", 2, cancellationToken);
        var persistedOffset = await resetPersisted.Task.WaitAsync(TimeSpan.FromSeconds(3), cancellationToken);

        Assert.Equal(
            [0, 7],
            remoting.PullOffsets
                .Where(static pull => pull.Topic == "tenant-a%orders")
                .Select(static pull => pull.Offset));
        Assert.Equal(7, persistedOffset);
    }

    [Fact]
    public async Task ResetOffsetNotification_FailedCommitAfterEmptyPull_RetriesCommitAndRestartsFromOffset()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var returnedResetEmptyPull = 0;
        var remoting = new FakeRemotingClient("127.0.0.1@reset-recovery")
        {
            TrackCommittedOffsets = true,
            PullHandler = async (request, token) =>
            {
                var offset = Convert.ToInt64(request.ExtFields["queueOffset"]);
                if (offset == 7 && Interlocked.Exchange(ref returnedResetEmptyPull, 1) == 0)
                {
                    return PullEmpty(offset);
                }

                await Task.Delay(Timeout.InfiniteTimeSpan, token);
                throw new InvalidOperationException("An infinite pull completed unexpectedly.");
            }
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
        await using var consumer = CreateRemotingPushConsumer(
            Options.Create(options),
            Options.Create(new RemotingClientOptions
            {
                ClientIP = "127.0.0.1",
                InstanceName = "reset-recovery",
                Namespace = "tenant-a",
                PollNameServerInterval = TimeSpan.FromHours(1),
                HeartbeatBrokerInterval = TimeSpan.FromHours(1)
            }),
            CreateRouteServiceMock().Object,
            remoting,
            TimeProvider.System,
            NullLogger<RemotingPushConsumer>.Instance);
        var body = Encoding.UTF8.GetBytes(
            """
            {"offsetTable":[[{"topic":"tenant-a%orders","brokerName":"broker-a","queueId":0},7]]}
            """);

        await consumer.StartAsync(cancellationToken);
        await remoting.WaitForTopicPullsAsync("tenant-a%orders", 1, cancellationToken);
        remoting.FailNextOffsetUpdate();

        await Assert.ThrowsAsync<IOException>(async () => await remoting.InvokeBrokerRequestAsync(
            RequestCode.ResetConsumerOffset,
            new Dictionary<string, object>
            {
                ["group"] = "tenant-a%legacy-group",
                ["topic"] = "tenant-a%orders"
            },
            body,
            cancellationToken));
        await remoting.WaitForTopicPullsAsync("tenant-a%orders", 3, cancellationToken);

        Assert.Contains(
            remoting.UpdatedOffsets,
            static update => update.Topic == "tenant-a%orders" && update.Offset == 7);

        await consumer.StopAsync(cancellationToken);
        var pullsBeforeRestart = remoting.PullOffsets.Count(
            static pull => pull.Topic == "tenant-a%orders");
        await consumer.StartAsync(cancellationToken);
        await remoting.WaitForTopicPullsAsync("tenant-a%orders", pullsBeforeRestart + 1, cancellationToken);

        Assert.Equal(
            7,
            remoting.PullOffsets.Last(static pull => pull.Topic == "tenant-a%orders").Offset);
    }

    [Fact]
    public async Task ResetOffsetNotifications_BeforeStart_IsPersisted()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var resetPersisted = new TaskCompletionSource<long>(TaskCreationOptions.RunContinuationsAsynchronously);
        var remoting = new FakeRemotingClient("127.0.0.1@reset-before-start")
        {
            TrackCommittedOffsets = true,
            PullHandler = async (_, token) =>
            {
                await Task.Delay(Timeout.InfiniteTimeSpan, token);
                throw new InvalidOperationException("An infinite pull completed unexpectedly.");
            },
            UpdateOffsetHandler = (request, _) =>
            {
                var offset = Convert.ToInt64(request.ExtFields["commitOffset"]);
                if (offset == 9)
                {
                    resetPersisted.TrySetResult(offset);
                }

                return Task.FromResult(new RemotingCommand { Code = ResponseCodes.ResSuccess });
            }
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
        await using var consumer = CreateRemotingPushConsumer(
            Options.Create(options),
            Options.Create(new RemotingClientOptions
            {
                ClientIP = "127.0.0.1",
                InstanceName = "reset-before-start",
                Namespace = "tenant-a",
                PollNameServerInterval = TimeSpan.FromHours(1),
                HeartbeatBrokerInterval = TimeSpan.FromHours(1)
            }),
            CreateRouteServiceMock().Object,
            remoting,
            TimeProvider.System,
            NullLogger<RemotingPushConsumer>.Instance);
        var firstBody = Encoding.UTF8.GetBytes(
            """
            {"offsetTable":[[{"topic":"tenant-a%orders","brokerName":"broker-a","queueId":0},7]]}
            """);
        var latestBody = Encoding.UTF8.GetBytes(
            """
            {"offsetTable":[[{"topic":"tenant-a%orders","brokerName":"broker-a","queueId":0},9]]}
            """);

        var first = await remoting.InvokeBrokerRequestAsync(
            RequestCode.ResetConsumerOffset,
            new Dictionary<string, object>
            {
                ["group"] = "tenant-a%legacy-group",
                ["topic"] = "tenant-a%orders"
            },
            firstBody,
            cancellationToken);
        var latest = await remoting.InvokeBrokerRequestAsync(
            RequestCode.ResetConsumerOffset,
            new Dictionary<string, object>
            {
                ["group"] = "tenant-a%legacy-group",
                ["topic"] = "tenant-a%orders"
            },
            latestBody,
            cancellationToken);
        Assert.True(first.IsHandled);
        Assert.True(first.SuppressResponse);
        Assert.True(latest.IsHandled);
        Assert.True(latest.SuppressResponse);

        await consumer.StartAsync(cancellationToken);
        await remoting.WaitForTopicPullsAsync("tenant-a%orders", 1, cancellationToken);
        var persistedOffset = await resetPersisted.Task.WaitAsync(TimeSpan.FromSeconds(3), cancellationToken);

        Assert.Equal(
            9,
            remoting.PullOffsets.First(static pull => pull.Topic == "tenant-a%orders").Offset);
        Assert.Equal(9, persistedOffset);
    }

    [Fact]
    public async Task BroadcastingResetOffset_FailedLocalCommitAfterEmptyPullAtSameOffset_RetriesFailedCommit()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var directory = System.IO.Path.Combine(
            System.IO.Path.GetTempPath(),
            $"rocketmq-broadcast-reset-{Guid.NewGuid():N}");
        var offsetPath = System.IO.Path.Combine(directory, "offsets.json");
        var allowResetPull = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var returnedResetEmptyPull = 0;
        var remoting = new FakeRemotingClient("127.0.0.1@broadcast-reset")
        {
            PullHandler = async (request, token) =>
            {
                var offset = Convert.ToInt64(request.ExtFields["queueOffset"]);
                if (offset == 7)
                {
                    await allowResetPull.Task.WaitAsync(token);
                    if (Interlocked.Exchange(ref returnedResetEmptyPull, 1) == 0)
                    {
                        return PullEmpty(offset);
                    }
                }

                await Task.Delay(Timeout.InfiniteTimeSpan, token);
                throw new InvalidOperationException("An infinite pull completed unexpectedly.");
            }
        };
        var options = CreateBroadcastOptions(
            offsetPath,
            static (_, _, _) => ValueTask.FromResult(ConsumeResult.Success));
        options.MaxConcurrency = 1;
        options.RetryDelay = TimeSpan.FromMilliseconds(20);
        var consumer = CreateRemotingPushConsumer(
            options,
            CreateRouteServiceMock().Object,
            remoting,
            "broadcast-reset");
        var body = Encoding.UTF8.GetBytes(
            """
            {"offsetTable":[[{"topic":"orders","brokerName":"broker-a","queueId":0},7]]}
            """);

        try
        {
            await consumer.StartAsync(cancellationToken);
            await remoting.WaitForTopicPullsAsync("orders", 1, cancellationToken);
            Assert.True(File.Exists(offsetPath));
            File.Delete(offsetPath);
            Directory.CreateDirectory(offsetPath);

            var exception = await Record.ExceptionAsync(async () => await remoting.InvokeBrokerRequestAsync(
                RequestCode.ResetConsumerOffset,
                new Dictionary<string, object>
                {
                    ["group"] = "broadcast-group",
                    ["topic"] = "orders"
                },
                body,
                cancellationToken));
            Assert.True(
                exception is IOException or UnauthorizedAccessException,
                $"Expected an offset persistence failure, but received {exception?.GetType().Name ?? "no exception"}.");

            Directory.Delete(offsetPath);
            allowResetPull.TrySetResult();
            await remoting.WaitForTopicPullsAsync("orders", 3, cancellationToken);

            Assert.True(File.Exists(offsetPath));
            var store = new BroadcastOffsetStore(offsetPath, "unused-client", "unused-group");
            var persisted = await store.ReadAsync(
                new RemotingConsumerQueue("orders", 0, "broker-a", "127.0.0.1:10911"),
                cancellationToken);
            Assert.Equal(7, persisted);
        }
        finally
        {
            allowResetPull.TrySetResult();
            if (Directory.Exists(offsetPath))
            {
                Directory.Delete(offsetPath, true);
            }

            await consumer.DisposeAsync();
            if (Directory.Exists(directory))
            {
                Directory.Delete(directory, true);
            }
        }
    }

    [Fact]
    public void Allocate_SortedApacheAverageStrategy_UsesQueues()
    {
        var queues = Enumerable.Range(0, 8)
            .Select(queueId => new RemotingConsumerQueue("orders", queueId, "broker-a", "127.0.0.1:10911"))
            .Reverse()
            .ToArray();
        string[] consumers = ["client-c", "client-a", "client-b"];

        var first = LegacyConsumerProtocol.Allocate(queues, consumers, "client-a");
        var second = LegacyConsumerProtocol.Allocate(queues, consumers, "client-b");
        var third = LegacyConsumerProtocol.Allocate(queues, consumers, "client-c");

        Assert.Equal([0, 1, 2], first.Select(static queue => queue.QueueId));
        Assert.Equal([3, 4, 5], second.Select(static queue => queue.QueueId));
        Assert.Equal([6, 7], third.Select(static queue => queue.QueueId));
    }

    [Theory]
    [InlineData(3)]
    [InlineData(10)]
    public void Allocate_ThreeBrokerNineQueueRouteWithoutOverlap_Distributes(int consumerCount)
    {
        var queues = CreateMultiBrokerQueues();
        var consumers = Enumerable.Range(0, consumerCount)
            .Select(static index => $"client-{index:D2}")
            .ToArray();

        var allocations = consumers
            .Select(consumer => LegacyConsumerProtocol.Allocate(queues, consumers, consumer))
            .ToArray();
        var assigned = allocations
            .SelectMany(static allocation => allocation)
            .Select(static queue => (queue.BrokerName, queue.QueueId))
            .ToArray();

        Assert.Equal(9, assigned.Length);
        Assert.Equal(9, assigned.Distinct().Count());
        Assert.Equal(
            queues.Select(static queue => (queue.BrokerName, queue.QueueId)).Order(),
            assigned.Order());
        Assert.Equal(Math.Min(consumerCount, queues.Count), allocations.Count(static allocation => allocation.Count > 0));
        Assert.Equal(Math.Max(0, consumerCount - queues.Count), allocations.Count(static allocation => allocation.Count == 0));
        Assert.True(allocations.Max(static allocation => allocation.Count) -
                    allocations.Min(static allocation => allocation.Count) <= 1);
    }

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
    public async Task StartStopStart_UncommittedOrdinaryAndRetryQueues_RestartsFromExpectedOffsets()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var remoting = new FakeRemotingClient("127.0.0.1@legacy-test");
        var routeTopics = new ConcurrentQueue<string>();
        var routes = CreateRouteServiceMock(topics: routeTopics);
        var options = WithMessageHandler(new RemotingPushConsumerOptions
        {
            GroupName = "legacy-group",
            MaxConcurrency = 1,
            PullMaxCachedMessages = 8,
            LongPollingTimeout = TimeSpan.FromSeconds(1),
        }, static (_, _, _) => ValueTask.FromResult(ConsumeResult.Success));
        options.Subscribe("orders", new FilterExpression("created || paid"));
        var clientOptions = new RemotingClientOptions
        {
            ClientIP = "127.0.0.1",
            InstanceName = "legacy-test",
            Namespace = "tenant-a",
            PollNameServerInterval = TimeSpan.FromHours(1),
            HeartbeatBrokerInterval = TimeSpan.FromHours(1)
        };
        await using var consumer = CreateRemotingPushConsumer(
            Options.Create(options),
            Options.Create(clientOptions),
            routes.Object,
            remoting,
            TimeProvider.System,
            NullLogger<RemotingPushConsumer>.Instance);

        await consumer.StartAsync(cancellationToken);
        await remoting.WaitForPullsAsync(2, cancellationToken);
        await consumer.StopAsync(cancellationToken);
        await consumer.StartAsync(cancellationToken);
        await remoting.WaitForPullsAsync(4, cancellationToken);
        await consumer.StopAsync(cancellationToken);

        Assert.Equal(2, remoting.HeartbeatBodies.Count);
        Assert.Equal(2, remoting.UnregisterCalls);
        Assert.All(
            remoting.PullOffsets.Where(static value => value.Topic == "%RETRY%tenant-a%legacy-group"),
            static value => Assert.Equal(0, value.Offset));
        Assert.Equal(2, remoting.PullOffsets.Count(static value => value.Topic == "%RETRY%tenant-a%legacy-group"));
        Assert.All(
            remoting.PullOffsets.Where(static value => value.Topic == "tenant-a%orders"),
            static value => Assert.Equal(17, value.Offset));
        Assert.Equal(2, remoting.PullOffsets.Count(static value => value.Topic == "tenant-a%orders"));
        Assert.All(
            remoting.UpdatedOffsets.Where(static value => value.Topic == "%RETRY%tenant-a%legacy-group"),
            static value => Assert.Equal(0, value.Offset));
        Assert.Equal(2, remoting.UpdatedOffsets.Count(static value => value.Topic == "%RETRY%tenant-a%legacy-group"));
        Assert.All(
            remoting.UpdatedOffsets.Where(static value => value.Topic == "tenant-a%orders"),
            static value => Assert.Equal(17, value.Offset));
        Assert.Equal(2, remoting.UpdatedOffsets.Count(static value => value.Topic == "tenant-a%orders"));
        Assert.Contains("tenant-a%orders", routeTopics);
        Assert.Contains("%RETRY%tenant-a%legacy-group", routeTopics);
        Assert.All(routeTopics, static topic => Assert.Contains(
            topic,
            new[] { "tenant-a%orders", "%RETRY%tenant-a%legacy-group" }));
        Assert.All(
            remoting.Requests.Where(static request => request.Code is
                RequestCode.GetConsumerListByGroup or RequestCode.QueryConsumerOffset or
                RequestCode.UpdateConsumerOffset or RequestCode.PullMessage or RequestCode.UnregisterClient),
            static request => Assert.Equal("tenant-a%legacy-group", request.ExtFields["consumerGroup"]));

        using var heartbeat = JsonDocument.Parse(remoting.HeartbeatBodies.First());
        var root = heartbeat.RootElement;
        Assert.Equal("127.0.0.1@legacy-test", root.GetProperty("clientID").GetString());
        var consumerData = Assert.Single(root.GetProperty("consumerDataSet").EnumerateArray());
        Assert.Equal("tenant-a%legacy-group", consumerData.GetProperty("groupName").GetString());
        Assert.Equal("CONSUME_PASSIVELY", consumerData.GetProperty("consumeType").GetString());
        Assert.Equal("CLUSTERING", consumerData.GetProperty("messageModel").GetString());
        Assert.Equal("CONSUME_FROM_LAST_OFFSET", consumerData.GetProperty("consumeFromWhere").GetString());
        var subscriptions = consumerData.GetProperty("subscriptionDataSet").EnumerateArray().ToArray();
        Assert.Contains(subscriptions, static value => value.GetProperty("topic").GetString() == "tenant-a%orders");
        Assert.Contains(subscriptions, static value =>
            value.GetProperty("topic").GetString() == "%RETRY%tenant-a%legacy-group");
    }

    [Theory]
    [InlineData(ConsumeFromPosition.Beginning, RequestCode.GetMinOffset, 0L)]
    [InlineData(ConsumeFromPosition.End, RequestCode.GetMaxOffset, 17L)]
    [InlineData(ConsumeFromPosition.Timestamp, RequestCode.SearchOffsetByTimestamp, 9L)]
    public async Task UncommittedOrdinaryQueue_ConfiguredInitialPosition_UsesPositionAndRetriesRetryQueue(
        ConsumeFromPosition position,
        int expectedRequestCode,
        long expectedOffset)
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var timestamp = new DateTimeOffset(2026, 7, 18, 9, 30, 0, TimeSpan.Zero);
        var remoting = new FakeRemotingClient("127.0.0.1@legacy-test");
        var options = WithMessageHandler(new RemotingPushConsumerOptions
        {
            GroupName = "legacy-group",
            InitialPosition = position,
            ConsumeTimestamp = position == ConsumeFromPosition.Timestamp ? timestamp : null,
            MaxConcurrency = 1,
            PullMaxCachedMessages = 8,
            LongPollingTimeout = TimeSpan.FromSeconds(1),
        }, static (_, _, _) => ValueTask.FromResult(ConsumeResult.Success));
        options.Subscribe("orders");
        await using var consumer = CreateRemotingPushConsumer(
            Options.Create(options),
            Options.Create(new RemotingClientOptions
            {
                ClientIP = "127.0.0.1",
                InstanceName = "legacy-test",
                PollNameServerInterval = TimeSpan.FromHours(1),
                HeartbeatBrokerInterval = TimeSpan.FromHours(1)
            }),
            CreateRouteServiceMock().Object,
            remoting,
            TimeProvider.System,
            NullLogger<RemotingPushConsumer>.Instance);

        await consumer.StartAsync(cancellationToken);
        await remoting.WaitForPullsAsync(2, cancellationToken);
        await consumer.StopAsync(cancellationToken);

        Assert.Contains(remoting.PullOffsets, pull => pull.Topic == "orders" && pull.Offset == expectedOffset);
        Assert.Contains(remoting.PullOffsets, static pull =>
            pull.Topic == "%RETRY%legacy-group" && pull.Offset == 0);
        var initialRequest = Assert.Single(remoting.Requests, request =>
            request.Code == expectedRequestCode &&
            Assert.IsType<string>(request.ExtFields["topic"]) == "orders");
        if (position == ConsumeFromPosition.Timestamp)
        {
            Assert.Equal(
                timestamp.ToUnixTimeMilliseconds(),
                Convert.ToInt64(initialRequest.ExtFields["timestamp"]));
        }

        Assert.DoesNotContain(remoting.Requests, request =>
            request.Code is RequestCode.GetMinOffset or RequestCode.GetMaxOffset or
                RequestCode.SearchOffsetByTimestamp &&
            Assert.IsType<string>(request.ExtFields["topic"]) == "%RETRY%legacy-group");
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
    public async Task Broadcasting_DropsAndLocalOffsetResumesAcrossConsumerInstances_RetriesDroppedMessagesAndResumesOffset()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var offsetPath = System.IO.Path.Combine(
            System.IO.Path.GetTempPath(),
            $"rocketmq-broadcast-resume-{Guid.NewGuid():N}.json");
        try
        {
            var body = CreateMessageRecord("orders", "broadcast-retry", "account-7", 0, 1_000);
            var delivered = 0;
            var firstHandled = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            var firstRemoting = new FakeRemotingClient("127.0.0.1@broadcast-test")
            {
                PullHandler = async (request, token) =>
                {
                    var offset = Convert.ToInt64(request.ExtFields["queueOffset"]);
                    if (offset == 0 && Interlocked.Exchange(ref delivered, 1) == 0)
                    {
                        return PullSuccess(body, 1);
                    }

                    await Task.Delay(Timeout.InfiniteTimeSpan, token);
                    throw new InvalidOperationException("An infinite pull completed unexpectedly.");
                }
            };
            var firstOptions = CreateBroadcastOptions(offsetPath, (_, _, _) =>
            {
                firstHandled.TrySetResult();
                return ValueTask.FromResult(ConsumeResult.Retry);
            });
            await using (var first = CreateRemotingPushConsumer(
                             firstOptions,
                             CreateRouteServiceMock().Object,
                             firstRemoting,
                             "broadcast-test"))
            {
                await first.StartAsync(cancellationToken);
                await firstHandled.Task.WaitAsync(TimeSpan.FromSeconds(3), cancellationToken);
                await firstRemoting.WaitForPullsAsync(2, cancellationToken);
                var queue = new RemotingConsumerQueue("orders", 0, "broker-a", "127.0.0.1:10911");
                var deadline = DateTimeOffset.UtcNow + TimeSpan.FromSeconds(3);
                long? persistedOffset;
                do
                {
                    var store = new BroadcastOffsetStore(offsetPath, "unused-client", "unused-group");
                    persistedOffset = await store.ReadAsync(queue, cancellationToken);
                    if (persistedOffset != 1)
                    {
                        await Task.Delay(10, cancellationToken);
                    }
                }
                while (persistedOffset != 1 && DateTimeOffset.UtcNow < deadline);

                Assert.Equal(1, persistedOffset);

                await first.StopAsync(cancellationToken);
            }

            Assert.DoesNotContain(firstRemoting.Requests, static request => request.Code is
                RequestCode.GetConsumerListByGroup or RequestCode.QueryConsumerOffset or
                RequestCode.UpdateConsumerOffset or RequestCode.ConsumerSendMsgBack);

            var secondRemoting = new FakeRemotingClient("127.0.0.1@broadcast-test");
            var secondOptions = CreateBroadcastOptions(offsetPath, static (_, _, _) =>
                ValueTask.FromResult(ConsumeResult.Success));
            await using (var second = CreateRemotingPushConsumer(
                             secondOptions,
                             CreateRouteServiceMock().Object,
                             secondRemoting,
                             "broadcast-test"))
            {
                await second.StartAsync(cancellationToken);
                await secondRemoting.WaitForPullsAsync(1, cancellationToken);
                await second.StopAsync(cancellationToken);
            }

            Assert.Contains(secondRemoting.PullOffsets, static pull =>
                pull.Topic == "orders" && pull.QueueId == 0 && pull.Offset == 1);
            Assert.DoesNotContain(secondRemoting.Requests, static request => request.Code is
                RequestCode.GetConsumerListByGroup or RequestCode.QueryConsumerOffset or
                RequestCode.UpdateConsumerOffset or RequestCode.ConsumerSendMsgBack or
                RequestCode.GetMinOffset or RequestCode.GetMaxOffset or RequestCode.SearchOffsetByTimestamp);
        }
        finally
        {
            File.Delete(offsetPath);
        }
    }

    [Fact]
    public async Task BroadcastingConsumer_ConsumeTimeout_DoesNotApply()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var offsetPath = System.IO.Path.Combine(
            System.IO.Path.GetTempPath(),
            $"rocketmq-broadcast-timeout-{Guid.NewGuid():N}.json");
        var handlerStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var handlerCancellationRequested = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var handlerFinished = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseHandler = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var delivered = 0;
        var remoting = new FakeRemotingClient("127.0.0.1@broadcast-timeout")
        {
            PullHandler = async (request, token) =>
            {
                if (Assert.IsType<string>(request.ExtFields["topic"]) == "orders" &&
                    Interlocked.Exchange(ref delivered, 1) == 0)
                {
                    return PullSuccess(CreateMessageRecord("orders", "broadcast", null, 0, 1_000), 1);
                }

                await Task.Delay(Timeout.InfiniteTimeSpan, token);
                throw new InvalidOperationException("An infinite pull completed unexpectedly.");
            }
        };
        var options = CreateBroadcastOptions(
            offsetPath,
            async (_, _, token) =>
            {
                handlerStarted.TrySetResult();
                using var registration = token.Register(() => handlerCancellationRequested.TrySetResult());
                await releaseHandler.Task.WaitAsync(token);
                handlerFinished.TrySetResult();
                return ConsumeResult.Success;
            });
        options.ConsumeTimeout = TimeSpan.FromMilliseconds(50);
        options.MaxConcurrency = 1;
        options.PullMaxCachedMessages = 1;
        try
        {
            await using var consumer = CreateRemotingPushConsumer(
                options,
                CreateRouteServiceMock().Object,
                remoting,
                "broadcast-timeout");

            await consumer.StartAsync(cancellationToken);
            await handlerStarted.Task.WaitAsync(TimeSpan.FromSeconds(3), cancellationToken);
            await Task.Delay(100, cancellationToken);
            Assert.False(handlerCancellationRequested.Task.IsCompleted);

            releaseHandler.TrySetResult();
            await handlerFinished.Task.WaitAsync(TimeSpan.FromSeconds(3), cancellationToken);
            await consumer.StopAsync(cancellationToken);

            Assert.DoesNotContain(remoting.Requests, static request => request.Code == RequestCode.ConsumerSendMsgBack);
        }
        finally
        {
            File.Delete(offsetPath);
        }
    }

    [Theory]
    [InlineData(ConsumeResult.Retry)]
    [InlineData(ConsumeResult.DeadLetter)]
    public async Task UnsuccessfulHandlerResult_FailedProcessTelemetry_RecordsFailure(ConsumeResult result)
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var offsetPath = System.IO.Path.Combine(
            System.IO.Path.GetTempPath(),
            $"rocketmq-broadcast-telemetry-{Guid.NewGuid():N}.json");
        var receiveOperation = new Mock<IRemotingRocketMQTelemetryOperation>(MockBehavior.Strict);
        receiveOperation.SetupGet(value => value.Activity).Returns((Activity?)null);
        receiveOperation.Setup(value => value.Complete());
        receiveOperation.Setup(value => value.Dispose());
        var processCompleted = new TaskCompletionSource<(bool Success, string? Outcome)>(
            TaskCreationOptions.RunContinuationsAsynchronously);
        var processOperation = new Mock<IRemotingRocketMQTelemetryOperation>(MockBehavior.Strict);
        processOperation
            .Setup(value => value.Complete(It.IsAny<bool>(), It.IsAny<string?>()))
            .Callback<bool, string?>((success, outcome) => processCompleted.TrySetResult((success, outcome)));
        processOperation.Setup(value => value.Dispose());
        var telemetry = new Mock<IRemotingRocketMQTelemetry>(MockBehavior.Strict);
        telemetry
            .Setup(value => value.StartReceive(
                It.IsAny<string>(),
                It.IsAny<string>(),
                It.IsAny<int>(),
                It.IsAny<ActivityContext?>(),
                It.IsAny<DateTimeOffset>(),
                It.IsAny<long>(),
                It.IsAny<int?>(),
                It.IsAny<bool>(),
                It.IsAny<IEnumerable<IReadOnlyDictionary<string, string>>?>()))
            .Returns(receiveOperation.Object);
        telemetry
            .Setup(value => value.StartProcessBatch(
                It.IsAny<string>(),
                It.IsAny<string>(),
                It.Is<IReadOnlyList<RemotingMessageView>>(messages => messages.Count == 1)))
            .Returns(processOperation.Object);
        var delivered = 0;
        var remoting = new FakeRemotingClient("127.0.0.1@telemetry-test")
        {
            PullHandler = async (request, token) =>
            {
                if (Assert.IsType<string>(request.ExtFields["topic"]) == "orders" &&
                    Interlocked.Exchange(ref delivered, 1) == 0)
                {
                    return PullSuccess(CreateMessageRecord("orders", "telemetry-message", null, 0, 1_000), 1);
                }

                await Task.Delay(Timeout.InfiniteTimeSpan, token);
                throw new InvalidOperationException("An infinite pull completed unexpectedly.");
            }
        };
        var options = CreateBroadcastOptions(
            offsetPath,
            (_, _, _) => ValueTask.FromResult(result));
        try
        {
            await using var consumer = CreateRemotingPushConsumer(
                options,
                CreateRouteServiceMock().Object,
                remoting,
                "telemetry-test",
                telemetry.Object);
            await consumer.StartAsync(cancellationToken);
            var processOutcome = await processCompleted.Task.WaitAsync(TimeSpan.FromSeconds(3), cancellationToken);
            Assert.False(processOutcome.Success);
            Assert.Equal(result.ToString(), processOutcome.Outcome);
            await consumer.StopAsync(cancellationToken);
        }
        finally
        {
            File.Delete(offsetPath);
        }

        telemetry.VerifyAll();
        receiveOperation.VerifyAll();
        processOperation.VerifyAll();
    }

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
    public async Task RestartWithUncommittedRetryBacklog_FromBeginning_Delivers()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var retryDelivered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var body = CreateMessageRecord("%RETRY%legacy-group", "retry-after-crash", "account-7", 0, 1_000);
        var remoting = new FakeRemotingClient("127.0.0.1@legacy-test")
        {
            PullHandler = async (request, token) =>
            {
                var topic = Assert.IsType<string>(request.ExtFields["topic"]);
                var offset = Convert.ToInt64(request.ExtFields["queueOffset"]);
                if (topic == "%RETRY%legacy-group" && offset == 0)
                {
                    return PullSuccess(body, 1);
                }

                await Task.Delay(Timeout.InfiniteTimeSpan, token);
                throw new InvalidOperationException("An infinite pull completed unexpectedly.");
            }
        };
        var options = WithMessageHandler(new RemotingPushConsumerOptions
        {
            GroupName = "legacy-group",
            MaxConcurrency = 1,
            PullMaxCachedMessages = 8,
            LongPollingTimeout = TimeSpan.FromSeconds(1),
        }, (messages, _, _) =>
            {
                var message = Assert.Single(messages);
                if (message.MessageId == "retry-after-crash")
                {
                    retryDelivered.TrySetResult();
                }

                return ValueTask.FromResult(ConsumeResult.Success);
            });
        options.Subscribe("orders");
        await using var consumer = CreateRemotingPushConsumer(
            Options.Create(options),
            Options.Create(new RemotingClientOptions
            {
                ClientIP = "127.0.0.1",
                InstanceName = "legacy-test",
                PollNameServerInterval = TimeSpan.FromHours(1),
                HeartbeatBrokerInterval = TimeSpan.FromHours(1)
            }),
            CreateRouteServiceMock().Object,
            remoting,
            TimeProvider.System,
            NullLogger<RemotingPushConsumer>.Instance);

        await consumer.StartAsync(cancellationToken);
        await retryDelivered.Task.WaitAsync(TimeSpan.FromSeconds(3), cancellationToken);
        await consumer.StopAsync(cancellationToken);

        Assert.Contains(remoting.PullOffsets, static pull =>
            pull.Topic == "%RETRY%legacy-group" && pull.Offset == 0);
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
    public async Task ConcurrentPushConsumer_MessagesInConfiguredBatches_DispatchesConfiguredBatches()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var completed = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var batches = new ConcurrentQueue<string[]>();
        var delivered = 0;
        var body = CreateMessageRecord("orders", "one", null, 0, 1_000)
            .Concat(CreateMessageRecord("orders", "two", null, 1, 1_001))
            .Concat(CreateMessageRecord("orders", "three", null, 2, 1_002))
            .Concat(CreateMessageRecord("orders", "four", null, 3, 1_003))
            .Concat(CreateMessageRecord("orders", "five", null, 4, 1_004))
            .ToArray();
        var remoting = new FakeRemotingClient("127.0.0.1@batch-dispatch")
        {
            PullHandler = async (request, token) =>
            {
                if (Assert.IsType<string>(request.ExtFields["topic"]) == "orders" &&
                    Interlocked.Exchange(ref delivered, 1) == 0)
                {
                    return PullSuccess(body, 5);
                }

                await Task.Delay(Timeout.InfiniteTimeSpan, token);
                throw new InvalidOperationException("An infinite pull completed unexpectedly.");
            }
        };
        var options = WithMessageHandler(new RemotingPushConsumerOptions
        {
            GroupName = "legacy-group",
            InitialPosition = ConsumeFromPosition.Beginning,
            PullBatchSize = 5,
            ConsumeMessageBatchSize = 2,
            MaxConcurrency = 1,
            PullMaxCachedMessages = 5,
            LongPollingTimeout = TimeSpan.FromSeconds(1),
        }, (messages, _, _) =>
            {
                batches.Enqueue(messages.Select(static message => message.MessageId).ToArray());
                if (batches.Count == 3)
                {
                    completed.TrySetResult();
                }

                return ValueTask.FromResult(ConsumeResult.Success);
            });
        options.Subscribe("orders");
        await using var consumer = CreateRemotingPushConsumer(
            options,
            CreateRouteServiceMock().Object,
            remoting,
            "batch-dispatch");

        await consumer.StartAsync(cancellationToken);
        await completed.Task.WaitAsync(TimeSpan.FromSeconds(3), cancellationToken);
        while (!remoting.UpdatedOffsets.Any(static update => update.Topic == "orders" && update.Offset == 5))
        {
            await Task.Delay(10, cancellationToken);
        }

        await consumer.StopAsync(cancellationToken);

        Assert.Collection(
            batches.ToArray(),
            batch => Assert.Equal(["one", "two"], batch),
            batch => Assert.Equal(["three", "four"], batch),
            batch => Assert.Equal(["five"], batch));
        var pull = remoting.Requests.First(request =>
            request.Code == RequestCode.PullMessage &&
            string.Equals(request.ExtFields["topic"] as string, "orders", StringComparison.Ordinal));
        Assert.Equal(5, Convert.ToInt32(pull.ExtFields["maxMsgNums"]));
    }

    [Fact]
    public async Task ConcurrentPushConsumer_BatchRetryResult_SendsBackEveryMessage()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var delivered = 0;
        var body = CreateMessageRecord("orders", "one", null, 0, 1_000)
            .Concat(CreateMessageRecord("orders", "two", null, 1, 1_001))
            .ToArray();
        var remoting = new FakeRemotingClient("127.0.0.1@batch-retry")
        {
            PullHandler = async (request, token) =>
            {
                if (Assert.IsType<string>(request.ExtFields["topic"]) == "orders" &&
                    Interlocked.Exchange(ref delivered, 1) == 0)
                {
                    return PullSuccess(body, 2);
                }

                await Task.Delay(Timeout.InfiniteTimeSpan, token);
                throw new InvalidOperationException("An infinite pull completed unexpectedly.");
            }
        };
        var options = WithMessageHandler(new RemotingPushConsumerOptions
        {
            GroupName = "legacy-group",
            InitialPosition = ConsumeFromPosition.Beginning,
            PullBatchSize = 2,
            ConsumeMessageBatchSize = 2,
            MaxConcurrency = 1,
            PullMaxCachedMessages = 2,
            LongPollingTimeout = TimeSpan.FromSeconds(1),
        }, static (_, context, _) =>
            {
                context.AckIndex = 0;
                return ValueTask.FromResult(ConsumeResult.Retry);
            });
        options.Subscribe("orders");
        await using var consumer = CreateRemotingPushConsumer(
            options,
            CreateRouteServiceMock().Object,
            remoting,
            "batch-retry");

        await consumer.StartAsync(cancellationToken);
        await remoting.WaitForRequestCountAsync(RequestCode.ConsumerSendMsgBack, 2, cancellationToken);
        await consumer.StopAsync(cancellationToken);

        Assert.Equal(
            2,
            remoting.Requests.Count(static request => request.Code == RequestCode.ConsumerSendMsgBack));
    }

    [Fact]
    public async Task ConcurrentPushConsumer_BatchPrefixAndRemainingMessages_AcknowledgesAndRetries()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var delivered = 0;
        var body = CreateMessageRecord("orders", "one", null, 0, 1_000)
            .Concat(CreateMessageRecord("orders", "two", null, 1, 1_001))
            .Concat(CreateMessageRecord("orders", "three", null, 2, 1_002))
            .ToArray();
        var remoting = new FakeRemotingClient("127.0.0.1@batch-partial-ack")
        {
            PullHandler = async (request, token) =>
            {
                if (Assert.IsType<string>(request.ExtFields["topic"]) == "orders" &&
                    Interlocked.Exchange(ref delivered, 1) == 0)
                {
                    return PullSuccess(body, 3);
                }

                await Task.Delay(Timeout.InfiniteTimeSpan, token);
                throw new InvalidOperationException("An infinite pull completed unexpectedly.");
            }
        };
        var options = WithMessageHandler(new RemotingPushConsumerOptions
        {
            GroupName = "legacy-group",
            InitialPosition = ConsumeFromPosition.Beginning,
            PullBatchSize = 3,
            ConsumeMessageBatchSize = 3,
            MaxConcurrency = 1,
            PullMaxCachedMessages = 3,
            LongPollingTimeout = TimeSpan.FromSeconds(1),
        }, (messages, context, _) =>
            {
                Assert.Equal(["one", "two", "three"], messages.Select(static message => message.MessageId));
                context.AckIndex = 0;
                context.DelayLevelWhenNextConsume = 7;
                return ValueTask.FromResult(ConsumeResult.Success);
            });
        options.Subscribe("orders");
        await using var consumer = CreateRemotingPushConsumer(
            options,
            CreateRouteServiceMock().Object,
            remoting,
            "batch-partial-ack");

        await consumer.StartAsync(cancellationToken);
        await remoting.WaitForRequestCountAsync(RequestCode.ConsumerSendMsgBack, 2, cancellationToken);
        while (!remoting.UpdatedOffsets.Any(static update => update.Topic == "orders" && update.Offset == 3))
        {
            await Task.Delay(10, cancellationToken);
        }

        await consumer.StopAsync(cancellationToken);

        var sendBackRequests = remoting.Requests
            .Where(static request => request.Code == RequestCode.ConsumerSendMsgBack)
            .ToArray();
        Assert.Equal(2, sendBackRequests.Length);
        Assert.Equal(
            [1_001L, 1_002L],
            sendBackRequests.Select(static request => Convert.ToInt64(request.ExtFields["offset"])).Order());
        Assert.All(
            sendBackRequests,
            static request => Assert.Equal(7, Convert.ToInt32(request.ExtFields["delayLevel"])));
    }

    [Fact]
    public async Task ConcurrentPushConsumer_PartialAcknowledgement_RecordsPartialSuccessTelemetry()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var processCompleted = new TaskCompletionSource<(bool Success, string? Outcome)>(
            TaskCreationOptions.RunContinuationsAsynchronously);
        var processOperation = new Mock<IRemotingRocketMQTelemetryOperation>(MockBehavior.Strict);
        processOperation
            .Setup(value => value.Complete(false, "partial_success"))
            .Callback(() => processCompleted.TrySetResult((false, "partial_success")));
        processOperation.Setup(value => value.Dispose());
        var telemetry = new ProcessTelemetry(processOperation.Object);
        var delivered = 0;
        var body = CreateMessageRecord("orders", "one", null, 0, 1_000)
            .Concat(CreateMessageRecord("orders", "two", null, 1, 1_001))
            .ToArray();
        var remoting = new FakeRemotingClient("127.0.0.1@batch-partial-telemetry")
        {
            PullHandler = async (request, token) =>
            {
                if (Assert.IsType<string>(request.ExtFields["topic"]) == "orders" &&
                    Interlocked.Exchange(ref delivered, 1) == 0)
                {
                    return PullSuccess(body, 2);
                }

                await Task.Delay(Timeout.InfiniteTimeSpan, token);
                throw new InvalidOperationException("An infinite pull completed unexpectedly.");
            }
        };
        var options = WithMessageHandler(new RemotingPushConsumerOptions
        {
            GroupName = "legacy-group",
            InitialPosition = ConsumeFromPosition.Beginning,
            PullBatchSize = 2,
            ConsumeMessageBatchSize = 2,
            MaxConcurrency = 1,
            PullMaxCachedMessages = 2,
            LongPollingTimeout = TimeSpan.FromSeconds(1),
        }, static (_, context, _) =>
            {
                context.AckIndex = 0;
                return ValueTask.FromResult(ConsumeResult.Success);
            });
        options.Subscribe("orders");
        await using var consumer = CreateRemotingPushConsumer(
            options,
            CreateRouteServiceMock().Object,
            remoting,
            "batch-partial-telemetry",
            telemetry);

        await consumer.StartAsync(cancellationToken);
        var outcome = await processCompleted.Task.WaitAsync(TimeSpan.FromSeconds(3), cancellationToken);
        await remoting.WaitForRequestCountAsync(RequestCode.ConsumerSendMsgBack, 1, cancellationToken);
        await consumer.StopAsync(cancellationToken);

        Assert.False(outcome.Success);
        Assert.Equal("partial_success", outcome.Outcome);
        Assert.Equal(1, telemetry.ProcessBatchCalls);
        processOperation.VerifyAll();
    }

    [Fact]
    public async Task ConcurrentPushConsumer_CacheMatchesBatchSize_AdmitsFullBatches()
    {
        const int queueCount = 4;
        const int messagesPerBatch = 2;
        var cancellationToken = TestContext.Current.CancellationToken;
        var readyPulls = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releasePulls = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var allBatchesHandled = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var deliveredQueues = new ConcurrentDictionary<int, byte>();
        var handledBatches = new ConcurrentQueue<string[]>();
        var waitingPulls = 0;
        var handledBatchCount = 0;
        var remoting = new FakeRemotingClient("127.0.0.1@batch-admission")
        {
            PullHandler = async (request, token) =>
            {
                if (!string.Equals(request.ExtFields["topic"] as string, "orders", StringComparison.Ordinal))
                {
                    await Task.Delay(Timeout.InfiniteTimeSpan, token);
                    throw new InvalidOperationException("An infinite pull completed unexpectedly.");
                }

                var queueId = Convert.ToInt32(request.ExtFields["queueId"]);
                if (!deliveredQueues.TryAdd(queueId, 0))
                {
                    await Task.Delay(Timeout.InfiniteTimeSpan, token);
                    throw new InvalidOperationException("An infinite pull completed unexpectedly.");
                }

                if (Interlocked.Increment(ref waitingPulls) == queueCount)
                {
                    readyPulls.TrySetResult();
                }

                await releasePulls.Task.WaitAsync(token);
                return PullSuccess(
                    CreateMessageRecord("orders", $"queue-{queueId}-first", null, 0, 1_000 + queueId)
                        .Concat(CreateMessageRecord("orders", $"queue-{queueId}-second", null, 1, 2_000 + queueId))
                        .ToArray(),
                    messagesPerBatch);
            }
        };
        var options = WithMessageHandler(new RemotingPushConsumerOptions
        {
            GroupName = "legacy-group",
            InitialPosition = ConsumeFromPosition.Beginning,
            PullBatchSize = messagesPerBatch,
            ConsumeMessageBatchSize = messagesPerBatch,
            MaxConcurrency = queueCount,
            PullMaxCachedMessages = messagesPerBatch,
            LongPollingTimeout = TimeSpan.FromSeconds(1),
        }, (messages, _, _) =>
            {
                Assert.Equal(messagesPerBatch, messages.Count);
                handledBatches.Enqueue(messages.Select(static message => message.MessageId).ToArray());
                if (Interlocked.Increment(ref handledBatchCount) == queueCount)
                {
                    allBatchesHandled.TrySetResult();
                }

                return ValueTask.FromResult(ConsumeResult.Success);
            });
        options.Subscribe("orders");
        await using var consumer = CreateRemotingPushConsumer(
            options,
            CreateRouteServiceMock(queueCount).Object,
            remoting,
            "batch-admission");

        await consumer.StartAsync(cancellationToken);
        await readyPulls.Task.WaitAsync(TimeSpan.FromSeconds(3), cancellationToken);
        releasePulls.TrySetResult();
        await allBatchesHandled.Task.WaitAsync(TimeSpan.FromSeconds(3), cancellationToken);
        await consumer.StopAsync(cancellationToken);

        Assert.Equal(queueCount, deliveredQueues.Count);
        Assert.Equal(
            Enumerable.Range(0, queueCount).Select(static queueId => $"queue-{queueId}-first").Order(),
            handledBatches.Select(static batch => batch[0]).Order());
        Assert.All(
            handledBatches,
            batch =>
            {
                var prefix = batch[0][..^"-first".Length];
                Assert.Equal(new[] { $"{prefix}-first", $"{prefix}-second" }, batch);
            });
    }

    [Fact]
    public async Task ConcurrentPushConsumer_TimedOutHandlerAndWithQueuedMessages_CancelsTimedOutHandlerAndContinues()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var firstHandlerCanceled = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var secondHandled = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var delivered = 0;
        var body = CreateMessageRecord("orders", "first", null, 0, 1_000)
            .Concat(CreateMessageRecord("orders", "second", null, 1, 1_001))
            .ToArray();
        var remoting = new FakeRemotingClient("127.0.0.1@consume-timeout")
        {
            PullHandler = async (request, token) =>
            {
                if (Assert.IsType<string>(request.ExtFields["topic"]) == "orders" &&
                    Interlocked.Exchange(ref delivered, 1) == 0)
                {
                    return PullSuccess(body, 2);
                }

                await Task.Delay(Timeout.InfiniteTimeSpan, token);
                throw new InvalidOperationException("An infinite pull completed unexpectedly.");
            }
        };
        var options = WithMessageHandler(new RemotingPushConsumerOptions
        {
            GroupName = "legacy-group",
            InitialPosition = ConsumeFromPosition.Beginning,
            PullBatchSize = 2,
            ConsumeMessageBatchSize = 1,
            ConsumeTimeout = TimeSpan.FromMilliseconds(50),
            MaxConcurrency = 1,
            PullMaxCachedMessages = 2,
            LongPollingTimeout = TimeSpan.FromSeconds(1),
        }, async (messages, _, token) =>
            {
                var message = Assert.Single(messages);
                if (message.MessageId == "first")
                {
                    using var failingRegistration = token.Register(
                        static () => throw new InvalidOperationException("The test cancellation callback failed."));
                    try
                    {
                        await Task.Delay(Timeout.InfiniteTimeSpan, token);
                    }
                    catch (OperationCanceledException) when (token.IsCancellationRequested)
                    {
                        firstHandlerCanceled.TrySetResult();
                        throw;
                    }
                }

                secondHandled.TrySetResult();
                return ConsumeResult.Success;
            });
        options.Subscribe("orders");
        await using var consumer = CreateRemotingPushConsumer(
            options,
            CreateRouteServiceMock().Object,
            remoting,
            "consume-timeout");

        await consumer.StartAsync(cancellationToken);
        await firstHandlerCanceled.Task.WaitAsync(TimeSpan.FromSeconds(3), cancellationToken);
        await secondHandled.Task.WaitAsync(TimeSpan.FromSeconds(3), cancellationToken);
        await remoting.WaitForRequestCountAsync(RequestCode.ConsumerSendMsgBack, 1, cancellationToken);
        while (!remoting.UpdatedOffsets.Any(static update => update.Topic == "orders" && update.Offset == 2))
        {
            await Task.Delay(10, cancellationToken);
        }

        await consumer.StopAsync(cancellationToken);

        Assert.Single(remoting.Requests, static request => request.Code == RequestCode.ConsumerSendMsgBack);
    }

    [Fact]
    public async Task ConcurrentPushConsumer_NonCooperativeTimedOutHandler_RetriesAndContinues()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var handlerStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var handlerFinished = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseHandler = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var processCompleted = new TaskCompletionSource<(bool Success, string? Outcome)>(
            TaskCreationOptions.RunContinuationsAsynchronously);
        var processOperation = new Mock<IRemotingRocketMQTelemetryOperation>(MockBehavior.Strict);
        processOperation
            .Setup(value => value.Complete(false, "timeout"))
            .Callback(() => processCompleted.TrySetResult((false, "timeout")));
        processOperation.Setup(value => value.Dispose());
        var telemetry = new ProcessTelemetry(processOperation.Object);
        var delivered = 0;
        var body = CreateMessageRecord("orders", "stuck", null, 0, 1_000);
        var remoting = new FakeRemotingClient("127.0.0.1@consume-timeout-uncooperative")
        {
            PullHandler = async (request, token) =>
            {
                if (Assert.IsType<string>(request.ExtFields["topic"]) == "orders" &&
                    Interlocked.Exchange(ref delivered, 1) == 0)
                {
                    return PullSuccess(body, 1);
                }

                await Task.Delay(Timeout.InfiniteTimeSpan, token);
                throw new InvalidOperationException("An infinite pull completed unexpectedly.");
            }
        };
        var options = WithMessageHandler(new RemotingPushConsumerOptions
        {
            GroupName = "legacy-group",
            InitialPosition = ConsumeFromPosition.Beginning,
            PullBatchSize = 1,
            ConsumeTimeout = TimeSpan.FromMilliseconds(50),
            MaxConcurrency = 1,
            PullMaxCachedMessages = 1,
            LongPollingTimeout = TimeSpan.FromSeconds(1),
        }, async (_, _, _) =>
            {
                handlerStarted.TrySetResult();
                await releaseHandler.Task;
                handlerFinished.TrySetResult();
                return ConsumeResult.Success;
            });
        options.Subscribe("orders");
        await using var consumer = CreateRemotingPushConsumer(
            options,
            CreateRouteServiceMock().Object,
            remoting,
            "consume-timeout-uncooperative",
            telemetry);

        await consumer.StartAsync(cancellationToken);
        await handlerStarted.Task.WaitAsync(TimeSpan.FromSeconds(3), cancellationToken);
        var processOutcome = await processCompleted.Task.WaitAsync(TimeSpan.FromSeconds(3), cancellationToken);
        Assert.False(processOutcome.Success);
        Assert.Equal("timeout", processOutcome.Outcome);
        await remoting.WaitForRequestCountAsync(RequestCode.ConsumerSendMsgBack, 1, cancellationToken);
        while (!remoting.UpdatedOffsets.Any(static update => update.Topic == "orders" && update.Offset == 1))
        {
            await Task.Delay(10, cancellationToken);
        }

        await consumer.StopAsync(cancellationToken).AsTask().WaitAsync(TimeSpan.FromSeconds(3), cancellationToken);
        releaseHandler.TrySetResult();
        await handlerFinished.Task.WaitAsync(TimeSpan.FromSeconds(3), cancellationToken);

        Assert.Single(remoting.Requests, static request => request.Code == RequestCode.ConsumerSendMsgBack);
        Assert.Equal(1, telemetry.ProcessBatchCalls);
        processOperation.VerifyAll();
    }

    [Fact]
    public async Task ConcurrentPushConsumer_EveryMessageInTimedOutBatch_RetriesEveryTimedOutMessage()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var handlerStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var handlerFinished = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseHandler = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var delivered = 0;
        var body = CreateMessageRecord("orders", "first", null, 0, 1_000)
            .Concat(CreateMessageRecord("orders", "second", null, 1, 1_001))
            .ToArray();
        var remoting = new FakeRemotingClient("127.0.0.1@consume-timeout-batch")
        {
            PullHandler = async (request, token) =>
            {
                if (Assert.IsType<string>(request.ExtFields["topic"]) == "orders" &&
                    Interlocked.Exchange(ref delivered, 1) == 0)
                {
                    return PullSuccess(body, 2);
                }

                await Task.Delay(Timeout.InfiniteTimeSpan, token);
                throw new InvalidOperationException("An infinite pull completed unexpectedly.");
            }
        };
        var options = WithMessageHandler(new RemotingPushConsumerOptions
        {
            GroupName = "legacy-group",
            InitialPosition = ConsumeFromPosition.Beginning,
            PullBatchSize = 2,
            ConsumeMessageBatchSize = 2,
            ConsumeTimeout = TimeSpan.FromMilliseconds(50),
            RetryDelay = TimeSpan.FromSeconds(5),
            MaxConcurrency = 1,
            PullMaxCachedMessages = 2,
            LongPollingTimeout = TimeSpan.FromSeconds(1),
        }, async (messages, context, _) =>
            {
                Assert.Equal(2, messages.Count);
                context.DelayLevelWhenNextConsume = -1;
                handlerStarted.TrySetResult();
                await releaseHandler.Task;
                handlerFinished.TrySetResult();
                return ConsumeResult.Success;
            });
        options.Subscribe("orders");
        await using var consumer = CreateRemotingPushConsumer(
            options,
            CreateRouteServiceMock().Object,
            remoting,
            "consume-timeout-batch");

        await consumer.StartAsync(cancellationToken);
        await handlerStarted.Task.WaitAsync(TimeSpan.FromSeconds(3), cancellationToken);
        await remoting.WaitForRequestCountAsync(RequestCode.ConsumerSendMsgBack, 2, cancellationToken);
        while (!remoting.UpdatedOffsets.Any(static update => update.Topic == "orders" && update.Offset == 2))
        {
            await Task.Delay(10, cancellationToken);
        }

        await consumer.StopAsync(cancellationToken).AsTask().WaitAsync(TimeSpan.FromSeconds(3), cancellationToken);
        releaseHandler.TrySetResult();
        await handlerFinished.Task.WaitAsync(TimeSpan.FromSeconds(3), cancellationToken);

        Assert.Equal(
            2,
            remoting.Requests.Count(static request => request.Code == RequestCode.ConsumerSendMsgBack));
        Assert.All(
            remoting.Requests.Where(static request => request.Code == RequestCode.ConsumerSendMsgBack),
            static request => Assert.Equal(2, Convert.ToInt32(request.ExtFields["delayLevel"])));
    }

    [Fact]
    public async Task ConcurrentPushConsumer_IncompleteMessage_PullsAheadWithoutCommitting()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var releaseFirst = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var secondCompleted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var handlerCalls = new ConcurrentDictionary<string, int>(StringComparer.Ordinal);
        var remoting = new FakeRemotingClient("127.0.0.1@pull-ahead-watermark")
        {
            PullHandler = async (request, token) =>
            {
                if (Assert.IsType<string>(request.ExtFields["topic"]) == "orders")
                {
                    return Convert.ToInt64(request.ExtFields["queueOffset"]) switch
                    {
                        0 => PullSuccess(CreateMessageRecord("orders", "first", null, 0, 1_000), 1),
                        1 => PullSuccess(CreateMessageRecord("orders", "second", null, 1, 1_001), 2),
                        _ => await WaitForCanceledPullAsync(token)
                    };
                }

                return await WaitForCanceledPullAsync(token);
            }
        };
        var options = WithMessageHandler(new RemotingPushConsumerOptions
        {
            GroupName = "legacy-group",
            InitialPosition = ConsumeFromPosition.Beginning,
            PullBatchSize = 1,
            ConsumeMessageBatchSize = 1,
            MaxConcurrency = 2,
            PullMaxCachedMessages = 2,
            LongPollingTimeout = TimeSpan.FromSeconds(1),
        }, async (messages, _, token) =>
            {
                var messageId = Assert.Single(messages).MessageId;
                handlerCalls.AddOrUpdate(messageId, 1, static (_, count) => count + 1);
                if (messageId == "first")
                {
                    await releaseFirst.Task.WaitAsync(token);
                }
                else
                {
                    secondCompleted.TrySetResult();
                }

                return ConsumeResult.Success;
            });
        options.Subscribe("orders");
        await using var consumer = CreateRemotingPushConsumer(
            options,
            CreateRouteServiceMock().Object,
            remoting,
            "pull-ahead-watermark");

        await consumer.StartAsync(cancellationToken);
        await secondCompleted.Task.WaitAsync(TimeSpan.FromSeconds(3), cancellationToken);
        Assert.Contains(
            remoting.PullOffsets,
            static pull => pull.Topic == "orders" && pull.Offset == 1);
        Assert.DoesNotContain(
            remoting.UpdatedOffsets,
            static update => update.Topic == "orders" && update.Offset == 2);

        releaseFirst.TrySetResult();
        while (!remoting.UpdatedOffsets.Any(static update => update.Topic == "orders" && update.Offset == 2))
        {
            await Task.Delay(10, cancellationToken);
        }

        Assert.Equal(1, handlerCalls["first"]);
        Assert.Equal(1, handlerCalls["second"]);
    }

    [Fact]
    public async Task ConcurrentPushConsumer_AllBatchesBeforeCommit_Waits()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var allBatchesStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseBatches = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var delivered = 0;
        var handlerCalls = 0;
        var body = CreateMessageRecord("orders", "one", null, 0, 1_000)
            .Concat(CreateMessageRecord("orders", "two", null, 1, 1_001))
            .Concat(CreateMessageRecord("orders", "three", null, 2, 1_002))
            .Concat(CreateMessageRecord("orders", "four", null, 3, 1_003))
            .Concat(CreateMessageRecord("orders", "five", null, 4, 1_004))
            .ToArray();
        var remoting = new FakeRemotingClient("127.0.0.1@batch-completion")
        {
            PullHandler = async (request, token) =>
            {
                if (Assert.IsType<string>(request.ExtFields["topic"]) == "orders" &&
                    Interlocked.Exchange(ref delivered, 1) == 0)
                {
                    return PullSuccess(body, 5);
                }

                await Task.Delay(Timeout.InfiniteTimeSpan, token);
                throw new InvalidOperationException("An infinite pull completed unexpectedly.");
            }
        };
        var options = WithMessageHandler(new RemotingPushConsumerOptions
        {
            GroupName = "legacy-group",
            InitialPosition = ConsumeFromPosition.Beginning,
            PullBatchSize = 5,
            ConsumeMessageBatchSize = 2,
            MaxConcurrency = 3,
            PullMaxCachedMessages = 2,
            LongPollingTimeout = TimeSpan.FromSeconds(1),
        }, async (_, _, token) =>
            {
                if (Interlocked.Increment(ref handlerCalls) == 3)
                {
                    allBatchesStarted.TrySetResult();
                }

                await releaseBatches.Task.WaitAsync(token);
                return ConsumeResult.Success;
            });
        options.Subscribe("orders");
        await using var consumer = CreateRemotingPushConsumer(
            options,
            CreateRouteServiceMock().Object,
            remoting,
            "batch-completion");

        await consumer.StartAsync(cancellationToken);
        await allBatchesStarted.Task.WaitAsync(TimeSpan.FromSeconds(3), cancellationToken);
        Assert.DoesNotContain(
            remoting.UpdatedOffsets,
            static update => update.Topic == "orders" && update.Offset == 5);

        releaseBatches.TrySetResult();
        while (!remoting.UpdatedOffsets.Any(static update => update.Topic == "orders" && update.Offset == 5))
        {
            await Task.Delay(10, cancellationToken);
        }

        await consumer.StopAsync(cancellationToken);

        Assert.Equal(3, handlerCalls);
    }

    [Fact]
    public async Task ConcurrentPushConsumer_OffsetCommitWithoutRedeliveringHandler_RetriesOffsetCommitWithoutRedelivery()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var releaseFirstPull = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var orderPulls = 0;
        var handlerCalls = 0;
        var body = CreateMessageRecord("orders", "commit-retry", null, 0, 1_000);
        var remoting = new FakeRemotingClient("127.0.0.1@concurrent-commit-retry")
        {
            PullHandler = async (request, token) =>
            {
                if (Assert.IsType<string>(request.ExtFields["topic"]) == "orders" &&
                    Interlocked.Increment(ref orderPulls) == 1)
                {
                    await releaseFirstPull.Task.WaitAsync(token);
                    return PullSuccess(body, 1);
                }

                await Task.Delay(Timeout.InfiniteTimeSpan, token);
                throw new InvalidOperationException("An infinite pull completed unexpectedly.");
            }
        };
        var options = WithMessageHandler(new RemotingPushConsumerOptions
        {
            GroupName = "legacy-group",
            InitialPosition = ConsumeFromPosition.Beginning,
            MaxConcurrency = 1,
            PullMaxCachedMessages = 1,
            RetryDelay = TimeSpan.FromMilliseconds(10),
            LongPollingTimeout = TimeSpan.FromSeconds(1),
        }, (_, _, _) =>
            {
                Interlocked.Increment(ref handlerCalls);
                return ValueTask.FromResult(ConsumeResult.Success);
            });
        options.Subscribe("orders");
        await using var consumer = CreateRemotingPushConsumer(
            options,
            CreateRouteServiceMock().Object,
            remoting,
            "concurrent-commit-retry");

        await consumer.StartAsync(cancellationToken);
        await remoting.WaitForTopicPullsAsync("orders", 1, cancellationToken);
        remoting.FailNextOffsetUpdate();
        releaseFirstPull.TrySetResult();
        await remoting.WaitForTopicPullsAsync("orders", 2, cancellationToken);
        while (!remoting.UpdatedOffsets.Any(static update =>
                   update.Topic == "orders" && update.Offset == 1))
        {
            await Task.Delay(10, cancellationToken);
        }

        Assert.Equal(
            [0L, 1L],
            remoting.PullOffsets
                .Where(static pull => pull.Topic == "orders")
                .Take(2)
                .Select(static pull => pull.Offset));
        Assert.Equal(1, handlerCalls);
    }

    [Fact]
    public async Task ConcurrentPushConsumer_OffsetPersistence_SerializesAndCoalesces()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var commitOneStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseCommitOne = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var secondHandled = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var activeUpdates = 0;
        var maximumActiveUpdates = 0;
        var remoting = new FakeRemotingClient("127.0.0.1@serialized-offsets")
        {
            PullHandler = async (request, token) =>
            {
                if (Assert.IsType<string>(request.ExtFields["topic"]) == "orders")
                {
                    return Convert.ToInt64(request.ExtFields["queueOffset"]) switch
                    {
                        0 => PullSuccess(CreateMessageRecord("orders", "first", null, 0, 1_000), 1),
                        1 => PullSuccess(CreateMessageRecord("orders", "second", null, 1, 1_001), 2),
                        _ => await WaitForCanceledPullAsync(token)
                    };
                }

                return await WaitForCanceledPullAsync(token);
            },
            UpdateOffsetHandler = async (request, token) =>
            {
                var current = Interlocked.Increment(ref activeUpdates);
                UpdateMaximum(ref maximumActiveUpdates, current);
                try
                {
                    if (Convert.ToInt64(request.ExtFields["commitOffset"]) == 1)
                    {
                        commitOneStarted.TrySetResult();
                        await releaseCommitOne.Task.WaitAsync(token);
                    }

                    return new RemotingCommand { Code = ResponseCodes.ResSuccess };
                }
                finally
                {
                    Interlocked.Decrement(ref activeUpdates);
                }
            }
        };
        var options = WithMessageHandler(new RemotingPushConsumerOptions
        {
            GroupName = "legacy-group",
            InitialPosition = ConsumeFromPosition.Beginning,
            PullBatchSize = 1,
            ConsumeMessageBatchSize = 1,
            MaxConcurrency = 2,
            PullMaxCachedMessages = 2,
            LongPollingTimeout = TimeSpan.FromSeconds(1),
        }, async (messages, _, token) =>
            {
                if (Assert.Single(messages).MessageId == "second")
                {
                    await commitOneStarted.Task.WaitAsync(token);
                    secondHandled.TrySetResult();
                }

                return ConsumeResult.Success;
            });
        options.Subscribe("orders");
        await using var consumer = CreateRemotingPushConsumer(
            options,
            CreateRouteServiceMock().Object,
            remoting,
            "serialized-offsets");

        await consumer.StartAsync(cancellationToken);
        await secondHandled.Task.WaitAsync(TimeSpan.FromSeconds(3), cancellationToken);
        Assert.DoesNotContain(
            remoting.Requests,
            static request => request.Code == RequestCode.UpdateConsumerOffset &&
                              Convert.ToInt64(request.ExtFields["commitOffset"]) == 2);

        releaseCommitOne.TrySetResult();
        while (!remoting.UpdatedOffsets.Any(static update => update.Topic == "orders" && update.Offset == 2))
        {
            await Task.Delay(10, cancellationToken);
        }

        Assert.Equal(1, maximumActiveUpdates);
    }

    [Fact]
    public async Task ConcurrentPushConsumer_OffsetIllegalResetBehindInFlightCommit_SerializesIllegalResetBehindCommit()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var commitOneStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseCommitOne = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var resetCommitCompleted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var attemptedOffsets = new ConcurrentQueue<long>();
        var activeUpdates = 0;
        var maximumActiveUpdates = 0;
        var remoting = new FakeRemotingClient("127.0.0.1@serialized-reset")
        {
            PullHandler = async (request, token) =>
            {
                if (Assert.IsType<string>(request.ExtFields["topic"]) == "orders")
                {
                    return Convert.ToInt64(request.ExtFields["queueOffset"]) switch
                    {
                        0 => PullSuccess(CreateMessageRecord("orders", "first", null, 0, 1_000), 1),
                        1 => await ReturnOffsetIllegalAfterCommitStartsAsync(),
                        _ => await WaitForCanceledPullAsync(token)
                    };
                }

                return await WaitForCanceledPullAsync(token);

                async Task<RemotingCommand> ReturnOffsetIllegalAfterCommitStartsAsync()
                {
                    await commitOneStarted.Task.WaitAsync(token);
                    return PullOffsetIllegal(5);
                }
            },
            UpdateOffsetHandler = async (request, token) =>
            {
                var offset = Convert.ToInt64(request.ExtFields["commitOffset"]);
                attemptedOffsets.Enqueue(offset);
                var current = Interlocked.Increment(ref activeUpdates);
                UpdateMaximum(ref maximumActiveUpdates, current);
                try
                {
                    if (offset == 1)
                    {
                        commitOneStarted.TrySetResult();
                        await releaseCommitOne.Task.WaitAsync(token);
                    }
                    else if (offset == 5)
                    {
                        resetCommitCompleted.TrySetResult();
                    }

                    return new RemotingCommand { Code = ResponseCodes.ResSuccess };
                }
                finally
                {
                    Interlocked.Decrement(ref activeUpdates);
                }
            }
        };
        var options = WithMessageHandler(new RemotingPushConsumerOptions
        {
            GroupName = "legacy-group",
            InitialPosition = ConsumeFromPosition.Beginning,
            MaxConcurrency = 1,
            PullMaxCachedMessages = 1,
            LongPollingTimeout = TimeSpan.FromSeconds(1),
        }, static (_, _, _) => ValueTask.FromResult(ConsumeResult.Success));
        options.Subscribe("orders");
        await using var consumer = CreateRemotingPushConsumer(
            options,
            CreateRouteServiceMock().Object,
            remoting,
            "serialized-reset");

        try
        {
            await consumer.StartAsync(cancellationToken);
            await commitOneStarted.Task.WaitAsync(TimeSpan.FromSeconds(3), cancellationToken);
            await Task.Delay(100, cancellationToken);
            Assert.DoesNotContain(5, attemptedOffsets);

            releaseCommitOne.TrySetResult();
            await resetCommitCompleted.Task.WaitAsync(TimeSpan.FromSeconds(3), cancellationToken);
            await remoting.WaitForTopicPullsAsync("orders", 3, cancellationToken);

            Assert.Equal(1, maximumActiveUpdates);
            Assert.Equal(5, attemptedOffsets.Last());
            Assert.Contains(remoting.PullOffsets, static pull => pull.Topic == "orders" && pull.Offset == 5);
        }
        finally
        {
            releaseCommitOne.TrySetResult();
        }
    }

    [Fact]
    public async Task ConcurrentPushConsumer_OffsetPersistenceAcrossBroker_IsolatesBrokerQueuePersistence()
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
        var deliveredQueues = new ConcurrentDictionary<(string BrokerName, int QueueId), byte>();
        var brokerACommitStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseBrokerACommit = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var brokerBCommitCompleted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var brokerBHandled = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var remoting = new FakeRemotingClient("127.0.0.1@isolated-offsets")
        {
            PullHandler = async (request, token) =>
            {
                if (Assert.IsType<string>(request.ExtFields["topic"]) != "orders")
                {
                    return await WaitForCanceledPullAsync(token);
                }

                var brokerName = Assert.IsType<string>(request.ExtFields["bname"]);
                var queueId = Convert.ToInt32(request.ExtFields["queueId"]);
                if (queueId == 0 && brokerName is "broker-a" or "broker-b" &&
                    deliveredQueues.TryAdd((brokerName, queueId), 0))
                {
                    return PullSuccess(
                        CreateMessageRecord(
                            "orders",
                            brokerName,
                            null,
                            queueOffset: 0,
                            commitLogOffset: brokerName == "broker-a" ? 1_000 : 2_000,
                            queueId),
                        1);
                }

                return await WaitForCanceledPullAsync(token);
            },
            UpdateOffsetHandler = async (request, token) =>
            {
                if (Assert.IsType<string>(request.ExtFields["topic"]) == "orders" &&
                    Convert.ToInt64(request.ExtFields["commitOffset"]) == 1)
                {
                    var brokerName = Assert.IsType<string>(request.ExtFields["bname"]);
                    if (brokerName == "broker-a")
                    {
                        brokerACommitStarted.TrySetResult();
                        await releaseBrokerACommit.Task.WaitAsync(token);
                    }
                    else if (brokerName == "broker-b")
                    {
                        brokerBCommitCompleted.TrySetResult();
                    }
                }

                return new RemotingCommand { Code = ResponseCodes.ResSuccess };
            }
        };
        var options = WithMessageHandler(new RemotingPushConsumerOptions
        {
            GroupName = "legacy-group",
            InitialPosition = ConsumeFromPosition.Beginning,
            PullBatchSize = 1,
            ConsumeMessageBatchSize = 1,
            MaxConcurrency = 2,
            PullMaxCachedMessages = 9,
            LongPollingTimeout = TimeSpan.FromSeconds(1),
        }, (messages, _, _) =>
            {
                if (Assert.Single(messages).MessageId == "broker-b")
                {
                    brokerBHandled.TrySetResult();
                }

                return ValueTask.FromResult(ConsumeResult.Success);
            });
        options.Subscribe("orders");
        await using var consumer = CreateRemotingPushConsumer(
            options,
            routes.Object,
            remoting,
            "isolated-offsets");

        try
        {
            await consumer.StartAsync(cancellationToken);
            await brokerACommitStarted.Task.WaitAsync(TimeSpan.FromSeconds(3), cancellationToken);
            await brokerBHandled.Task.WaitAsync(TimeSpan.FromSeconds(3), cancellationToken);
            await brokerBCommitCompleted.Task.WaitAsync(TimeSpan.FromSeconds(3), cancellationToken);
        }
        finally
        {
            releaseBrokerACommit.TrySetResult();
        }
    }

    [Fact]
    public async Task FifoRetry_SameGroupPredecessorAndBatchContext_KeepsSuccessorBlocked()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var firstSecondAttempt = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var firstCancellationRequested = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseFirst = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var secondHandled = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var firstCalls = 0;
        var delivered = 0;
        var body = CreateMessageRecord("orders", "first", "account-7", 17, 1_017)
            .Concat(CreateMessageRecord("orders", "second", "account-7", 18, 1_018))
            .ToArray();
        var remoting = new FakeRemotingClient("127.0.0.1@legacy-test")
        {
            PullHandler = async (request, token) =>
            {
                var topic = Assert.IsType<string>(request.ExtFields["topic"]);
                if (topic == "orders" && Interlocked.Exchange(ref delivered, 1) == 0)
                {
                    return PullSuccess(body, 19);
                }

                await Task.Delay(Timeout.InfiniteTimeSpan, token);
                throw new InvalidOperationException("An infinite pull completed unexpectedly.");
            }
        };
        var options = WithMessageHandler(new RemotingPushConsumerOptions
        {
            GroupName = "legacy-group",
            ConsumeMessageBatchSize = 2,
            ConsumeTimeout = TimeSpan.FromMilliseconds(50),
            MaxConcurrency = 2,
            PullMaxCachedMessages = 8,
            MaxDeliveryAttempts = 3,
            RetryDelay = TimeSpan.FromMilliseconds(10),
            LongPollingTimeout = TimeSpan.FromSeconds(1),
        }, async (messages, context, token) =>
            {
                var message = Assert.Single(messages);
                if (message.MessageId == "first")
                {
                    if (Interlocked.Increment(ref firstCalls) == 1)
                    {
                        return ConsumeResult.Retry;
                    }

                    firstSecondAttempt.TrySetResult();
                    using var registration = token.Register(() => firstCancellationRequested.TrySetResult());
                    await releaseFirst.Task.WaitAsync(token);
                    context.AckIndex = -1;
                    return ConsumeResult.Success;
                }

                secondHandled.TrySetResult();
                return ConsumeResult.Success;
            });
        options.Subscribe("orders");
        await using var consumer = CreateRemotingPushConsumer(
            Options.Create(options),
            Options.Create(new RemotingClientOptions
            {
                ClientIP = "127.0.0.1",
                InstanceName = "legacy-test",
                PollNameServerInterval = TimeSpan.FromHours(1),
                HeartbeatBrokerInterval = TimeSpan.FromHours(1)
            }),
            CreateRouteServiceMock().Object,
            remoting,
            TimeProvider.System,
            NullLogger<RemotingPushConsumer>.Instance);

        await consumer.StartAsync(cancellationToken);
        await firstSecondAttempt.Task.WaitAsync(TimeSpan.FromSeconds(3), cancellationToken);
        await Task.Delay(100, cancellationToken);
        Assert.False(firstCancellationRequested.Task.IsCompleted);
        Assert.False(secondHandled.Task.IsCompleted);

        releaseFirst.TrySetResult();
        await secondHandled.Task.WaitAsync(TimeSpan.FromSeconds(3), cancellationToken);
        await consumer.StopAsync(cancellationToken);

        Assert.Equal(2, firstCalls);
        Assert.DoesNotContain(remoting.Requests, static request => request.Code == RequestCode.ConsumerSendMsgBack);
    }

    [Fact]
    public async Task FifoCompletion_SameGroupSuccessor_WakesAcrossProcessQueue()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var firstStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var secondPullReturned = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseFirst = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var secondHandled = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var queueZeroDelivered = 0;
        var queueOneDelivered = 0;
        var remoting = new FakeRemotingClient("127.0.0.1@cross-queue-fifo")
        {
            PullHandler = async (request, token) =>
            {
                if (Assert.IsType<string>(request.ExtFields["topic"]) != "orders")
                {
                    return await WaitForCanceledPullAsync(token);
                }

                var queueId = Convert.ToInt32(request.ExtFields["queueId"]);
                if (queueId == 0 && Interlocked.Exchange(ref queueZeroDelivered, 1) == 0)
                {
                    return PullSuccess(
                        CreateMessageRecord("orders", "first", "account-7", 0, 1_000, queueId: 0),
                        1);
                }

                if (queueId == 1 && Interlocked.Exchange(ref queueOneDelivered, 1) == 0)
                {
                    await firstStarted.Task.WaitAsync(token);
                    secondPullReturned.TrySetResult();
                    return PullSuccess(
                        CreateMessageRecord("orders", "second", "account-7", 0, 1_001, queueId: 1),
                        1);
                }

                return await WaitForCanceledPullAsync(token);
            }
        };
        var options = WithMessageHandler(new RemotingPushConsumerOptions
        {
            GroupName = "legacy-group",
            InitialPosition = ConsumeFromPosition.Beginning,
            PullBatchSize = 1,
            ConsumeMessageBatchSize = 1,
            MaxConcurrency = 2,
            PullMaxCachedMessages = 2,
            LongPollingTimeout = TimeSpan.FromSeconds(1),
        }, async (messages, _, token) =>
            {
                if (Assert.Single(messages).MessageId == "first")
                {
                    firstStarted.TrySetResult();
                    await releaseFirst.Task.WaitAsync(token);
                }
                else
                {
                    secondHandled.TrySetResult();
                }

                return ConsumeResult.Success;
            });
        options.Subscribe("orders");
        await using var consumer = CreateRemotingPushConsumer(
            options,
            CreateRouteServiceMock(queueCount: 2).Object,
            remoting,
            "cross-queue-fifo");

        await consumer.StartAsync(cancellationToken);
        await secondPullReturned.Task.WaitAsync(TimeSpan.FromSeconds(3), cancellationToken);
        await Task.Delay(100, cancellationToken);
        Assert.False(secondHandled.Task.IsCompleted);

        releaseFirst.TrySetResult();
        await secondHandled.Task.WaitAsync(TimeSpan.FromSeconds(3), cancellationToken);
    }

    [Fact]
    public async Task Rebalance_DroppedInFlightHandler_KeepsCrossQueueSuccessorBlocked()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        const string clientId = "127.0.0.1@cross-queue-fifo-drop";
        var firstStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var firstCancellationRequested = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var secondPullReturned = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseFirst = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var secondHandled = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var queueZeroDelivered = 0;
        var queueOneDelivered = 0;
        var remoting = new FakeRemotingClient(clientId)
        {
            PullHandler = async (request, token) =>
            {
                if (Assert.IsType<string>(request.ExtFields["topic"]) != "orders")
                {
                    return await WaitForCanceledPullAsync(token);
                }

                var queueId = Convert.ToInt32(request.ExtFields["queueId"]);
                if (queueId == 0 && Interlocked.Exchange(ref queueZeroDelivered, 1) == 0)
                {
                    return PullSuccess(
                        CreateMessageRecord("orders", "first", "account-7", 0, 1_000, queueId: 0),
                        1);
                }

                if (queueId == 1 && Interlocked.Exchange(ref queueOneDelivered, 1) == 0)
                {
                    await firstStarted.Task.WaitAsync(token);
                    secondPullReturned.TrySetResult();
                    return PullSuccess(
                        CreateMessageRecord("orders", "second", "account-7", 0, 1_001, queueId: 1),
                        1);
                }

                return await WaitForCanceledPullAsync(token);
            }
        };
        var options = WithMessageHandler(new RemotingPushConsumerOptions
        {
            GroupName = "legacy-group",
            InitialPosition = ConsumeFromPosition.Beginning,
            PullBatchSize = 1,
            ConsumeMessageBatchSize = 1,
            MaxConcurrency = 2,
            PullMaxCachedMessages = 2,
            LongPollingTimeout = TimeSpan.FromSeconds(1),
        }, async (messages, _, token) =>
            {
                if (Assert.Single(messages).MessageId == "first")
                {
                    firstStarted.TrySetResult();
                    using var registration = token.Register(() => firstCancellationRequested.TrySetResult());
                    await releaseFirst.Task;
                }
                else
                {
                    secondHandled.TrySetResult();
                }

                return ConsumeResult.Success;
            });
        options.Subscribe("orders");
        var rebalance = new TestRemotingRebalanceService();
        await using var consumer = CreateRemotingPushConsumer(
            options,
            CreateRouteServiceMock(queueCount: 2).Object,
            remoting,
            "cross-queue-fifo-drop",
            rebalanceService: rebalance);

        await consumer.StartAsync(cancellationToken);
        try
        {
            await secondPullReturned.Task.WaitAsync(TimeSpan.FromSeconds(3), cancellationToken);
            Assert.False(secondHandled.Task.IsCompleted);

            remoting.SetConsumerIds("000-other-client", clientId);
            await rebalance.RebalanceAsync("legacy-group", cancellationToken);
            await firstCancellationRequested.Task.WaitAsync(TimeSpan.FromSeconds(3), cancellationToken);
            await Task.Delay(100, cancellationToken);
            Assert.False(secondHandled.Task.IsCompleted);

            releaseFirst.TrySetResult();
            await secondHandled.Task.WaitAsync(TimeSpan.FromSeconds(3), cancellationToken);

            Assert.DoesNotContain(remoting.Requests, static request =>
                request.Code == RequestCode.UpdateConsumerOffset &&
                Convert.ToInt32(request.ExtFields["queueId"]) == 0 &&
                Convert.ToInt64(request.ExtFields["commitOffset"]) == 1);
        }
        finally
        {
            releaseFirst.TrySetResult();
            await consumer.StopAsync(CancellationToken.None);
        }
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task StopAsync_NonCooperativeFifoOrOrderlyHandler_CancelsAndIgnoresLateResult(bool consumeOrderly)
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var handlerStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var handlerCancellationRequested = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseHandler = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var handlerFinished = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var processCompleted = new TaskCompletionSource<(Exception? Exception, bool? Success, string? Outcome)>(
            TaskCreationOptions.RunContinuationsAsynchronously);
        var processDisposed = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var processOperation = new Mock<IRemotingRocketMQTelemetryOperation>(MockBehavior.Strict);
        processOperation
            .Setup(value => value.Complete(It.IsAny<Exception>()))
            .Callback<Exception>(exception => { processCompleted.TrySetResult((exception, null, null)); });
        processOperation
            .Setup(value => value.Complete(It.IsAny<bool>(), It.IsAny<string?>()))
            .Callback<bool, string?>((success, outcome) => { processCompleted.TrySetResult((null, success, outcome)); });
        processOperation.Setup(value => value.Dispose()).Callback(() => { processDisposed.TrySetResult(); });
        var telemetry = new ProcessTelemetry(processOperation.Object);
        var delivered = 0;
        var body = CreateMessageRecord(
            "orders",
            "stuck",
            consumeOrderly ? null : "account-7",
            0,
            1_000);
        var remoting = new FakeRemotingClient($"127.0.0.1@stop-cancellation-{consumeOrderly}")
        {
            PullHandler = async (request, token) =>
            {
                if (Assert.IsType<string>(request.ExtFields["topic"]) == "orders" &&
                    Interlocked.Exchange(ref delivered, 1) == 0)
                {
                    return PullSuccess(body, 1);
                }

                await Task.Delay(Timeout.InfiniteTimeSpan, token);
                throw new InvalidOperationException("An infinite pull completed unexpectedly.");
            }
        };
        var options = WithMessageHandler(new RemotingPushConsumerOptions
        {
            GroupName = "legacy-group",
            ConsumeOrderly = consumeOrderly,
            InitialPosition = ConsumeFromPosition.Beginning,
            MaxConcurrency = 1,
            PullMaxCachedMessages = 1,
            LongPollingTimeout = TimeSpan.FromSeconds(1),
        }, async (_, _, token) =>
            {
                handlerStarted.TrySetResult();
                using var registration = token.Register(() => handlerCancellationRequested.TrySetResult());
                await releaseHandler.Task;
                handlerFinished.TrySetResult();
                return ConsumeResult.Success;
            });
        options.Subscribe("orders");
        var consumer = CreateRemotingPushConsumer(
            options,
            CreateRouteServiceMock().Object,
            remoting,
            $"stop-cancellation-{consumeOrderly}",
            telemetry);

        try
        {
            await consumer.StartAsync(cancellationToken);
            await handlerStarted.Task.WaitAsync(TimeSpan.FromSeconds(3), cancellationToken);

            using var stopCancellation = new CancellationTokenSource();
            var stop = consumer.StopAsync(stopCancellation.Token).AsTask();
            await handlerCancellationRequested.Task.WaitAsync(TimeSpan.FromSeconds(3), cancellationToken);
            stopCancellation.Cancel();
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() =>
                stop.WaitAsync(TimeSpan.FromSeconds(3), cancellationToken));

            releaseHandler.TrySetResult();
            await handlerFinished.Task.WaitAsync(TimeSpan.FromSeconds(3), cancellationToken);
            var completion = await processCompleted.Task.WaitAsync(TimeSpan.FromSeconds(3), cancellationToken);
            await processDisposed.Task.WaitAsync(TimeSpan.FromSeconds(3), cancellationToken);

            Assert.IsType<OperationCanceledException>(completion.Exception);
            Assert.DoesNotContain(remoting.Requests, static request => request.Code == RequestCode.ConsumerSendMsgBack);
            Assert.DoesNotContain(
                remoting.UpdatedOffsets,
                static update => update.Topic == "orders" && update.Offset == 1);
        }
        finally
        {
            releaseHandler.TrySetResult();
            await consumer.DisposeAsync();
        }
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

        var exception = Assert.Throws<InvalidOperationException>(() => new RemotingPushConsumer(
            Options.Create(options),
            Options.Create(new RemotingClientOptions()),
            new Mock<IRemotingConsumerEngine>(MockBehavior.Strict).Object,
            new Mock<ITopicRouteService>(MockBehavior.Strict).Object,
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
    public async Task ConcurrentPushConsumer_RetryDelayExceedsBrokerLimit_UsesMaximumDelayLevel()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var delivered = 0;
        var body = CreateMessageRecord("orders", "slow-retry", null, 0, 1_000);
        var remoting = new FakeRemotingClient("127.0.0.1@maximum-delay-level")
        {
            PullHandler = async (request, token) =>
            {
                if (Assert.IsType<string>(request.ExtFields["topic"]) == "orders" &&
                    Interlocked.Exchange(ref delivered, 1) == 0)
                {
                    return PullSuccess(body, 1);
                }

                await Task.Delay(Timeout.InfiniteTimeSpan, token);
                throw new InvalidOperationException("An infinite pull completed unexpectedly.");
            }
        };
        var options = WithMessageHandler(new RemotingPushConsumerOptions
        {
            GroupName = "legacy-group",
            InitialPosition = ConsumeFromPosition.Beginning,
            MaxConcurrency = 1,
            PullMaxCachedMessages = 1,
            LongPollingTimeout = TimeSpan.FromSeconds(1),
            RetryDelay = TimeSpan.FromHours(3),
        }, static (_, _, _) => ValueTask.FromResult(ConsumeResult.Retry));
        options.Subscribe("orders");
        await using var consumer = CreateRemotingPushConsumer(
            options,
            CreateRouteServiceMock().Object,
            remoting,
            "maximum-delay-level");

        await consumer.StartAsync(cancellationToken);
        await remoting.WaitForRequestCountAsync(RequestCode.ConsumerSendMsgBack, 1, cancellationToken);
        await consumer.StopAsync(cancellationToken);

        var sendBack = Assert.Single(
            remoting.Requests,
            static request => request.Code == RequestCode.ConsumerSendMsgBack);
        Assert.Equal(18, Convert.ToInt32(sendBack.ExtFields["delayLevel"]));
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
    public async Task PushConsumer_OffsetInitializationAfterTransientBrokerFailure_RetriesInitialization()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var offsetQueries = 0;
        var routes = new Mock<ITopicRouteService>(MockBehavior.Strict);
        routes
            .Setup(value => value.GetAsync(
                It.IsAny<string>(),
                It.IsAny<bool>(),
                It.IsAny<CancellationToken>()))
            .Returns<string, bool, CancellationToken>((topic, _, _) =>
                Task.FromResult(topic == "orders" ? Route(1) : new TopicRouteData()));
        var remoting = new FakeRemotingClient("127.0.0.1@offset-initialization-retry")
        {
            ConsumerOffsetHandler = (_, _) => Interlocked.Increment(ref offsetQueries) == 1
                ? Task.FromException<RemotingCommand>(
                    new IOException("The broker did not respond to the initial offset query."))
                : Task.FromResult(new RemotingCommand { Code = ResponseCodes.ResQueryNotFound })
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
        await using var consumer = CreateRemotingPushConsumer(
            options,
            routes.Object,
            remoting,
            "offset-initialization-retry");

        await consumer.StartAsync(cancellationToken);
        await remoting.WaitForTopicPullsAsync("orders", 1, cancellationToken);

        Assert.Equal(2, offsetQueries);
        Assert.Equal(
            0,
            remoting.PullOffsets.First(static pull => pull.Topic == "orders").Offset);
    }

    [Fact]
    public async Task ConcurrentPushConsumer_MissingRetryTopic_RecoversFromOffsetAndHandlerFailure()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var orderPulls = 0;
        var body = CreateMessageRecord("orders", "recoverable", null, 4, 1_004);
        var routes = new Mock<ITopicRouteService>(MockBehavior.Strict);
        routes
            .Setup(value => value.GetAsync(
                It.IsAny<string>(),
                It.IsAny<bool>(),
                It.IsAny<CancellationToken>()))
            .Returns<string, bool, CancellationToken>((topic, _, _) =>
                Task.FromResult(topic == "orders" ? Route(1) : new TopicRouteData()));
        var remoting = new FakeRemotingClient("127.0.0.1@offset-illegal-recovery")
        {
            PullHandler = async (request, token) =>
            {
                if (Assert.IsType<string>(request.ExtFields["topic"]) == "orders")
                {
                    return Interlocked.Increment(ref orderPulls) switch
                    {
                        1 => PullOffsetIllegal(4),
                        2 => PullSuccess(body, 5),
                        _ => await WaitForCanceledPullAsync(token)
                    };
                }

                return await WaitForCanceledPullAsync(token);
            },
            MaxOffsetHandler = (request, _) => Task.FromResult(
                Assert.IsType<string>(request.ExtFields["topic"]) == "%RETRY%legacy-group"
                    ? new RemotingCommand { Code = ResponseCodes.ResTopicNotExist }
                    : new RemotingCommand
                    {
                        Code = ResponseCodes.ResSuccess,
                        ExtFields = new Dictionary<string, object> { ["offset"] = "17" }
                    })
        };
        var options = WithMessageHandler(new RemotingPushConsumerOptions
        {
            GroupName = "legacy-group",
            InitialPosition = ConsumeFromPosition.Beginning,
            MaxConcurrency = 1,
            PullMaxCachedMessages = 1,
            LongPollingTimeout = TimeSpan.FromSeconds(1),
        }, static (_, _, _) => throw new InvalidOperationException("The application handler failed."));
        options.Subscribe("orders");
        await using var consumer = CreateRemotingPushConsumer(
            options,
            routes.Object,
            remoting,
            "offset-illegal-recovery");

        await consumer.StartAsync(cancellationToken);
        await remoting.WaitForRequestCountAsync(RequestCode.ConsumerSendMsgBack, 1, cancellationToken);
        while (!remoting.UpdatedOffsets.Any(static update =>
                   update.Topic == "orders" && update.Offset == 5) ||
               !remoting.UpdatedOffsets.Any(static update =>
                   update.Topic == "%RETRY%legacy-group" && update.Offset == 0))
        {
            await Task.Delay(10, cancellationToken);
        }

        Assert.Equal(
            [0L, 4L],
            remoting.PullOffsets
                .Where(static pull => pull.Topic == "orders")
                .Take(2)
                .Select(static pull => pull.Offset));
    }

    [Fact]
    public async Task ConcurrentPushConsumer_SendBackFailure_RetriesWithoutRedelivery()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var deliveries = 0;
        var handlerCalls = 0;
        var sendBackAttempts = 0;
        var body = CreateMessageRecord("orders", "send-back-retry", null, 0, 1_000);
        var remoting = new FakeRemotingClient("127.0.0.1@send-back-retry")
        {
            PullHandler = async (request, token) =>
            {
                if (Assert.IsType<string>(request.ExtFields["topic"]) == "orders" &&
                    Interlocked.Increment(ref deliveries) <= 2)
                {
                    return PullSuccess(body, 1);
                }

                return await WaitForCanceledPullAsync(token);
            },
            SendBackHandler = (_, _) => Interlocked.Increment(ref sendBackAttempts) == 1
                ? Task.FromException<RemotingCommand>(
                    new IOException("The broker did not accept the retry acknowledgement."))
                : Task.FromResult(new RemotingCommand { Code = ResponseCodes.ResSuccess })
        };
        var options = WithMessageHandler(new RemotingPushConsumerOptions
        {
            GroupName = "legacy-group",
            InitialPosition = ConsumeFromPosition.Beginning,
            MaxConcurrency = 1,
            PullMaxCachedMessages = 1,
            LongPollingTimeout = TimeSpan.FromSeconds(1),
            RetryDelay = TimeSpan.FromMilliseconds(10),
        }, (_, _, _) =>
            {
                Interlocked.Increment(ref handlerCalls);
                return ValueTask.FromResult(ConsumeResult.Retry);
            });
        options.Subscribe("orders");
        await using var consumer = CreateRemotingPushConsumer(
            options,
            CreateRouteServiceMock().Object,
            remoting,
            "send-back-retry");

        await consumer.StartAsync(cancellationToken);
        await remoting.WaitForRequestCountAsync(RequestCode.ConsumerSendMsgBack, 2, cancellationToken);
        while (!remoting.UpdatedOffsets.Any(static update =>
                   update.Topic == "orders" && update.Offset == 1))
        {
            await Task.Delay(10, cancellationToken);
        }

        Assert.Equal(2, sendBackAttempts);
        Assert.Equal(1, handlerCalls);
        Assert.Equal(
            [0L, 1L],
            remoting.PullOffsets
                .Where(static pull => pull.Topic == "orders")
                .Take(2)
                .Select(static pull => pull.Offset));
    }

    [Fact]
    public async Task SettlementFailure_HotQueue_LeavesHealthyQueueAdmitted()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var hotFollowerCached = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var settlementFailed = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var healthyHandled = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var hotPulls = 0;
        var healthyPulls = 0;
        var hotFollowerCalls = 0;
        var sendBackAttempts = 0;
        var remoting = new FakeRemotingClient("127.0.0.1@settlement-isolation")
        {
            PullHandler = async (request, token) =>
            {
                if (Assert.IsType<string>(request.ExtFields["topic"]) != "orders")
                {
                    return await WaitForCanceledPullAsync(token);
                }

                var queueId = Convert.ToInt32(request.ExtFields["queueId"]);
                if (queueId == 0)
                {
                    return Interlocked.Increment(ref hotPulls) switch
                    {
                        1 => PullSuccess(
                            CreateMessageRecord("orders", "hot-head", null, 0, 1_000, queueId: 0),
                            1),
                        2 => PullSuccess(
                            CreateMessageRecord("orders", "hot-follower", null, 1, 1_001, queueId: 0),
                            2),
                        3 => await SignalThenWaitForCanceledPullAsync(hotFollowerCached, token),
                        _ => await WaitForCanceledPullAsync(token)
                    };
                }

                await settlementFailed.Task.WaitAsync(token);
                if (Interlocked.Exchange(ref healthyPulls, 1) == 0)
                {
                    return PullSuccess(
                        CreateMessageRecord("orders", "healthy", null, 0, 2_000, queueId: 1),
                        1);
                }

                return await WaitForCanceledPullAsync(token);
            },
            SendBackHandler = (_, _) =>
            {
                Interlocked.Increment(ref sendBackAttempts);
                settlementFailed.TrySetResult();
                return Task.FromException<RemotingCommand>(
                    new IOException("The Broker did not accept the retry acknowledgement."));
            }
        };
        var options = WithMessageHandler(new RemotingPushConsumerOptions
        {
            GroupName = "legacy-group",
            InitialPosition = ConsumeFromPosition.Beginning,
            PullBatchSize = 1,
            ConsumeMessageBatchSize = 1,
            MaxConcurrency = 1,
            PullMaxCachedMessages = 1,
            RetryDelay = TimeSpan.FromHours(1),
            LongPollingTimeout = TimeSpan.FromSeconds(1),
        }, async (messages, _, token) =>
            {
                switch (Assert.Single(messages).MessageId)
                {
                    case "hot-head":
                        await hotFollowerCached.Task.WaitAsync(token);
                        return ConsumeResult.Retry;
                    case "hot-follower":
                        Interlocked.Increment(ref hotFollowerCalls);
                        return ConsumeResult.Success;
                    case "healthy":
                        healthyHandled.TrySetResult();
                        return ConsumeResult.Success;
                    default:
                        throw new InvalidOperationException("The test received an unexpected message.");
                }
            });
        options.Subscribe("orders");
        await using var consumer = CreateRemotingPushConsumer(
            options,
            CreateRouteServiceMock(queueCount: 2).Object,
            remoting,
            "settlement-isolation");

        await consumer.StartAsync(cancellationToken);
        await healthyHandled.Task.WaitAsync(TimeSpan.FromSeconds(3), cancellationToken);

        Assert.Equal(1, sendBackAttempts);
        Assert.Equal(0, hotFollowerCalls);
    }

    [Fact]
    public async Task BlockedFifoSuccessor_HealthyQueueAdmission_DoesNotConsume()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var predecessorStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var successorCached = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var settlementFailed = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var healthyHandled = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var queueZeroPulls = 0;
        var queueOnePulls = 0;
        var queueTwoPulls = 0;
        var successorCalls = 0;
        var sendBackAttempts = 0;
        var remoting = new FakeRemotingClient("127.0.0.1@cross-queue-fifo-admission")
        {
            PullHandler = async (request, token) =>
            {
                if (Assert.IsType<string>(request.ExtFields["topic"]) != "orders")
                {
                    return await WaitForCanceledPullAsync(token);
                }

                var queueId = Convert.ToInt32(request.ExtFields["queueId"]);
                if (queueId == 0)
                {
                    if (Interlocked.Increment(ref queueZeroPulls) == 1)
                    {
                        return PullSuccess(
                            CreateMessageRecord("orders", "fifo-head", "account-7", 0, 1_000, queueId: 0),
                            1);
                    }

                    return await WaitForCanceledPullAsync(token);
                }

                if (queueId == 1)
                {
                    if (Interlocked.Increment(ref queueOnePulls) == 1)
                    {
                        await predecessorStarted.Task.WaitAsync(token);
                        return PullSuccess(
                            CreateMessageRecord("orders", "fifo-successor", "account-7", 0, 1_001, queueId: 1),
                            1);
                    }

                    return await SignalThenWaitForCanceledPullAsync(successorCached, token);
                }

                await settlementFailed.Task.WaitAsync(token);
                if (Interlocked.Increment(ref queueTwoPulls) == 1)
                {
                    return PullSuccess(
                        CreateMessageRecord("orders", "healthy", null, 0, 2_000, queueId: 2),
                        1);
                }

                return await WaitForCanceledPullAsync(token);
            },
            SendBackHandler = (_, _) =>
            {
                Interlocked.Increment(ref sendBackAttempts);
                settlementFailed.TrySetResult();
                return Task.FromException<RemotingCommand>(
                    new IOException("The Broker did not accept the dead-letter settlement."));
            }
        };
        var options = WithMessageHandler(new RemotingPushConsumerOptions
        {
            GroupName = "legacy-group",
            InitialPosition = ConsumeFromPosition.Beginning,
            PullBatchSize = 1,
            ConsumeMessageBatchSize = 1,
            MaxConcurrency = 1,
            PullMaxCachedMessages = 1,
            RetryDelay = TimeSpan.FromHours(1),
            LongPollingTimeout = TimeSpan.FromSeconds(1),
        }, async (messages, _, token) =>
            {
                switch (Assert.Single(messages).MessageId)
                {
                    case "fifo-head":
                        predecessorStarted.TrySetResult();
                        await successorCached.Task.WaitAsync(token);
                        return ConsumeResult.DeadLetter;
                    case "fifo-successor":
                        Interlocked.Increment(ref successorCalls);
                        return ConsumeResult.Success;
                    case "healthy":
                        healthyHandled.TrySetResult();
                        return ConsumeResult.Success;
                    default:
                        throw new InvalidOperationException("The test received an unexpected message.");
                }
            });
        options.Subscribe("orders");
        await using var consumer = CreateRemotingPushConsumer(
            options,
            CreateRouteServiceMock(queueCount: 3).Object,
            remoting,
            "cross-queue-fifo-admission");

        await consumer.StartAsync(cancellationToken);
        await healthyHandled.Task.WaitAsync(TimeSpan.FromSeconds(3), cancellationToken);

        Assert.Equal(1, sendBackAttempts);
        Assert.Equal(0, successorCalls);
    }

    [Fact]
    public async Task ConcurrentPushConsumer_FirstRetryBoundaryPersistenceFails_RetriesOriginalBoundary()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var retryBoundaryPersisted = new TaskCompletionSource<long>(TaskCreationOptions.RunContinuationsAsynchronously);
        var routes = new Mock<ITopicRouteService>(MockBehavior.Strict);
        routes
            .Setup(value => value.GetAsync(
                It.IsAny<string>(),
                It.IsAny<bool>(),
                It.IsAny<CancellationToken>()))
            .Returns<string, bool, CancellationToken>((topic, _, _) =>
                Task.FromResult(topic == "orders" ? Route(1) : new TopicRouteData()));
        var delivered = 0;
        var retryEndOffset = 0;
        var retryOffsetUpdates = 0;
        var retryMaximumQueries = 0;
        var handlerCalls = 0;
        var body = CreateMessageRecord("orders", "first", null, 0, 1_000)
            .Concat(CreateMessageRecord("orders", "second", null, 1, 1_001))
            .ToArray();
        var remoting = new FakeRemotingClient("127.0.0.1@retry-boundary")
        {
            ConsumerOffsetHandler = static (_, _) =>
                Task.FromResult(new RemotingCommand { Code = ResponseCodes.ResQueryNotFound }),
            PullHandler = async (request, token) =>
            {
                if (Assert.IsType<string>(request.ExtFields["topic"]) == "orders" &&
                    Interlocked.Exchange(ref delivered, 1) == 0)
                {
                    return PullSuccess(body, 2);
                }

                return await WaitForCanceledPullAsync(token);
            },
            MaxOffsetHandler = (request, _) =>
            {
                Assert.Equal("%RETRY%legacy-group", Assert.IsType<string>(request.ExtFields["topic"]));
                Interlocked.Increment(ref retryMaximumQueries);
                return Task.FromResult(new RemotingCommand
                {
                    Code = ResponseCodes.ResSuccess,
                    ExtFields = new Dictionary<string, object>
                    {
                        ["offset"] = Volatile.Read(ref retryEndOffset).ToString()
                    }
                });
            },
            SendBackHandler = (_, _) =>
            {
                Interlocked.Increment(ref retryEndOffset);
                return Task.FromResult(new RemotingCommand { Code = ResponseCodes.ResSuccess });
            },
            UpdateOffsetHandler = (request, _) =>
            {
                if (Assert.IsType<string>(request.ExtFields["topic"]) == "%RETRY%legacy-group")
                {
                    var boundary = Convert.ToInt64(request.ExtFields["commitOffset"]);
                    if (Interlocked.Increment(ref retryOffsetUpdates) == 1)
                    {
                        return Task.FromException<RemotingCommand>(
                            new IOException("The first retry-boundary update failed."));
                    }

                    retryBoundaryPersisted.TrySetResult(boundary);
                }

                return Task.FromResult(new RemotingCommand { Code = ResponseCodes.ResSuccess });
            }
        };
        var options = WithMessageHandler(new RemotingPushConsumerOptions
        {
            GroupName = "legacy-group",
            InitialPosition = ConsumeFromPosition.Beginning,
            PullBatchSize = 2,
            ConsumeMessageBatchSize = 2,
            MaxConcurrency = 1,
            PullMaxCachedMessages = 2,
            LongPollingTimeout = TimeSpan.FromSeconds(1),
        }, (_, _, _) =>
            {
                Interlocked.Increment(ref handlerCalls);
                return ValueTask.FromResult(ConsumeResult.Retry);
            });
        options.Subscribe("orders");
        await using var consumer = CreateRemotingPushConsumer(
            options,
            routes.Object,
            remoting,
            "retry-boundary");

        await consumer.StartAsync(cancellationToken);
        var persistedBoundary = await retryBoundaryPersisted.Task.WaitAsync(
            TimeSpan.FromSeconds(3),
            cancellationToken);

        Assert.Equal(0, persistedBoundary);
        Assert.Equal(1, retryMaximumQueries);
        Assert.Equal(2, retryOffsetUpdates);
        Assert.Equal(2, retryEndOffset);
        Assert.Equal(1, handlerCalls);
    }

    [Fact]
    public async Task FifoSuccessor_FailedDeadLetterSettlement_WaitsForRetry()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var secondSettlementStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseSettlement = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var secondHandled = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var handlerCalls = new ConcurrentDictionary<string, int>(StringComparer.Ordinal);
        var sendBackAttempts = 0;
        var delivered = 0;
        var body = CreateMessageRecord("orders", "first", "account-7", 0, 1_000)
            .Concat(CreateMessageRecord("orders", "second", "account-7", 1, 1_001))
            .ToArray();
        var remoting = new FakeRemotingClient("127.0.0.1@fifo-settlement-retry")
        {
            PullHandler = async (request, token) =>
            {
                if (Assert.IsType<string>(request.ExtFields["topic"]) == "orders" &&
                    Interlocked.Exchange(ref delivered, 1) == 0)
                {
                    return PullSuccess(body, 2);
                }

                return await WaitForCanceledPullAsync(token);
            },
            SendBackHandler = async (_, token) =>
            {
                if (Interlocked.Increment(ref sendBackAttempts) == 1)
                {
                    throw new IOException("The Broker did not accept the dead-letter settlement.");
                }

                secondSettlementStarted.TrySetResult();
                await releaseSettlement.Task.WaitAsync(token);
                return new RemotingCommand { Code = ResponseCodes.ResSuccess };
            }
        };
        var options = WithMessageHandler(new RemotingPushConsumerOptions
        {
            GroupName = "legacy-group",
            InitialPosition = ConsumeFromPosition.Beginning,
            ConsumeMessageBatchSize = 2,
            MaxConcurrency = 2,
            PullMaxCachedMessages = 2,
            RetryDelay = TimeSpan.FromMilliseconds(10),
            LongPollingTimeout = TimeSpan.FromSeconds(1),
        }, (messages, _, _) =>
            {
                var messageId = Assert.Single(messages).MessageId;
                handlerCalls.AddOrUpdate(messageId, 1, static (_, count) => count + 1);
                if (messageId == "second")
                {
                    secondHandled.TrySetResult();
                    return ValueTask.FromResult(ConsumeResult.Success);
                }

                return ValueTask.FromResult(ConsumeResult.DeadLetter);
            });
        options.Subscribe("orders");
        await using var consumer = CreateRemotingPushConsumer(
            options,
            CreateRouteServiceMock().Object,
            remoting,
            "fifo-settlement-retry");

        await consumer.StartAsync(cancellationToken);
        await secondSettlementStarted.Task.WaitAsync(TimeSpan.FromSeconds(3), cancellationToken);
        Assert.False(secondHandled.Task.IsCompleted);
        Assert.Equal(1, handlerCalls["first"]);

        releaseSettlement.TrySetResult();
        await secondHandled.Task.WaitAsync(TimeSpan.FromSeconds(3), cancellationToken);
        while (!remoting.UpdatedOffsets.Any(static update => update.Topic == "orders" && update.Offset == 2))
        {
            await Task.Delay(10, cancellationToken);
        }

        Assert.Equal(2, sendBackAttempts);
        Assert.Equal(1, handlerCalls["first"]);
        Assert.Equal(1, handlerCalls["second"]);
    }

    [Fact]
    public async Task OrderlyConsumer_DeadLetterResult_CommitsPastMessage()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var delivered = 0;
        var body = CreateMessageRecord("orders", "dead-letter", null, 0, 1_000);
        var remoting = new FakeRemotingClient("127.0.0.1@orderly-dead-letter")
        {
            PullHandler = async (request, token) =>
            {
                if (Assert.IsType<string>(request.ExtFields["topic"]) == "orders" &&
                    Interlocked.Exchange(ref delivered, 1) == 0)
                {
                    return PullSuccess(body, 1);
                }

                return await WaitForCanceledPullAsync(token);
            }
        };
        var options = WithMessageHandler(new RemotingPushConsumerOptions
        {
            GroupName = "legacy-group",
            ConsumeOrderly = true,
            InitialPosition = ConsumeFromPosition.Beginning,
            MaxConcurrency = 1,
            PullMaxCachedMessages = 1,
            LongPollingTimeout = TimeSpan.FromSeconds(1),
        }, static (_, _, _) => ValueTask.FromResult(ConsumeResult.DeadLetter));
        options.Subscribe("orders");
        await using var consumer = CreateRemotingPushConsumer(
            options,
            CreateRouteServiceMock().Object,
            remoting,
            "orderly-dead-letter");

        await consumer.StartAsync(cancellationToken);
        await remoting.WaitForRequestCountAsync(RequestCode.ConsumerSendMsgBack, 1, cancellationToken);
        while (!remoting.UpdatedOffsets.Any(static update =>
                   update.Topic == "orders" && update.Offset == 1))
        {
            await Task.Delay(10, cancellationToken);
        }

        var sendBack = Assert.Single(
            remoting.Requests,
            static request => request.Code == RequestCode.ConsumerSendMsgBack);
        Assert.Equal(-1, Convert.ToInt32(sendBack.ExtFields["delayLevel"]));
    }

    [Fact]
    public async Task OrderlyConsumer_BrokerQueueLock_UsesBeforePulling()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var remoting = new FakeRemotingClient("127.0.0.1@orderly-lock-test");
        var options = WithMessageHandler(new RemotingPushConsumerOptions
        {
            GroupName = "legacy-group",
            ConsumeOrderly = true,
            InitialPosition = ConsumeFromPosition.Beginning,
            MaxConcurrency = 2,
            PullMaxCachedMessages = 8,
            LongPollingTimeout = TimeSpan.FromSeconds(1),
        }, static (_, _, _) => ValueTask.FromResult(ConsumeResult.Success));
        options.Subscribe("orders");
        await using var consumer = CreateRemotingPushConsumer(
            Options.Create(options),
            Options.Create(new RemotingClientOptions
            {
                ClientIP = "127.0.0.1",
                InstanceName = "orderly-lock-test",
                Namespace = "tenant-a",
                PollNameServerInterval = TimeSpan.FromHours(1),
                HeartbeatBrokerInterval = TimeSpan.FromHours(1)
            }),
            CreateRouteServiceMock().Object,
            remoting,
            TimeProvider.System,
            NullLogger<RemotingPushConsumer>.Instance);

        await consumer.StartAsync(cancellationToken);
        await remoting.WaitForTopicPullsAsync("tenant-a%orders", 1, cancellationToken);

        var requests = remoting.Requests.ToArray();
        var lockRequest = requests.First(request =>
            request.Code == RequestCode.LockBatchMq &&
            RequestContainsQueueTopic(request, "tenant-a%orders"));
        using var body = JsonDocument.Parse(lockRequest.Body!);
        Assert.Equal("tenant-a%legacy-group", body.RootElement.GetProperty("consumerGroup").GetString());
        Assert.Equal("127.0.0.1@orderly-lock-test", body.RootElement.GetProperty("clientId").GetString());
        Assert.False(body.RootElement.GetProperty("onlyThisBroker").GetBoolean());
        var queue = Assert.Single(body.RootElement.GetProperty("mqSet").EnumerateArray());
        Assert.Equal("tenant-a%orders", queue.GetProperty("topic").GetString());
        Assert.Equal("broker-a", queue.GetProperty("brokerName").GetString());
        Assert.Equal(0, queue.GetProperty("queueId").GetInt32());

        var lockIndex = Array.IndexOf(requests, lockRequest);
        var pullIndex = Array.FindIndex(requests, request =>
            request.Code == RequestCode.PullMessage &&
            string.Equals(request.ExtFields["topic"] as string, "tenant-a%orders", StringComparison.Ordinal));
        Assert.True(lockIndex >= 0 && pullIndex > lockIndex);

        await consumer.StopAsync(cancellationToken);
        Assert.Contains(
            remoting.Requests,
            request => request.Code == RequestCode.UnlockBatchMq &&
                       RequestContainsQueueTopic(request, "tenant-a%orders"));
    }

    [Fact]
    public async Task OrderlyConsumer_BrokerQueueLock_DoesNotPullEarly()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var lockOrders = 0;
        var remoting = new FakeRemotingClient("127.0.0.1@orderly-lock-wait")
        {
            LockHandler = (request, _) =>
            {
                var grant = Volatile.Read(ref lockOrders) != 0 ||
                            !RequestContainsQueueTopic(request, "orders");
                return Task.FromResult(CreateLockResponse(request, grant));
            }
        };
        var options = WithMessageHandler(new RemotingPushConsumerOptions
        {
            GroupName = "legacy-group",
            ConsumeOrderly = true,
            InitialPosition = ConsumeFromPosition.Beginning,
            MaxConcurrency = 1,
            PullMaxCachedMessages = 8,
            LongPollingTimeout = TimeSpan.FromSeconds(1),
        }, static (_, _, _) => ValueTask.FromResult(ConsumeResult.Success));
        options.Subscribe("orders");
        var rebalance = new TestRemotingRebalanceService();
        await using var consumer = CreateRemotingPushConsumer(
            Options.Create(options),
            Options.Create(new RemotingClientOptions
            {
                ClientIP = "127.0.0.1",
                InstanceName = "orderly-lock-wait",
                PollNameServerInterval = TimeSpan.FromMilliseconds(20),
                HeartbeatBrokerInterval = TimeSpan.FromMilliseconds(20)
            }),
            CreateRouteServiceMock().Object,
            remoting,
            TimeProvider.System,
            NullLogger<RemotingPushConsumer>.Instance,
            rebalanceService: rebalance);

        await consumer.StartAsync(cancellationToken);
        await Task.Delay(100, cancellationToken);
        Assert.DoesNotContain(
            remoting.PullOffsets,
            static pull => string.Equals(pull.Topic, "orders", StringComparison.Ordinal));

        Assert.False(await rebalance.RebalanceAsync("legacy-group", cancellationToken));

        Volatile.Write(ref lockOrders, 1);
        Assert.True(await rebalance.RebalanceAsync("legacy-group", cancellationToken));
        await remoting.WaitForTopicPullsAsync("orders", 1, cancellationToken);
        await consumer.StopAsync(cancellationToken);
    }

    [Fact]
    public async Task OrderlyConsumer_LeaseAfterTransientLockRenewalFailure_PreservesLease()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var orderLockCalls = 0;
        var remoting = new FakeRemotingClient("127.0.0.1@orderly-renewal")
        {
            LockHandler = (request, _) =>
            {
                if (!RequestContainsQueueTopic(request, "orders"))
                {
                    return Task.FromResult(CreateLockResponse(request, grant: true));
                }

                return Interlocked.Increment(ref orderLockCalls) == 1
                    ? Task.FromResult(CreateLockResponse(request, grant: true))
                    : Task.FromException<RemotingCommand>(
                        new IOException("The broker did not respond to the lock renewal."));
            },
            PullHandler = async (request, token) =>
            {
                var topic = Assert.IsType<string>(request.ExtFields["topic"]);
                if (topic == "orders")
                {
                    await Task.Delay(100, token);
                    return PullEmpty(0);
                }

                await Task.Delay(Timeout.InfiniteTimeSpan, token);
                throw new InvalidOperationException("An infinite pull completed unexpectedly.");
            }
        };
        var options = WithMessageHandler(new RemotingPushConsumerOptions
        {
            GroupName = "legacy-group",
            ConsumeOrderly = true,
            InitialPosition = ConsumeFromPosition.Beginning,
            MaxConcurrency = 1,
            PullMaxCachedMessages = 8,
            LongPollingTimeout = TimeSpan.FromSeconds(1),
        }, static (_, _, _) => ValueTask.FromResult(ConsumeResult.Success));
        options.Subscribe("orders");
        var rebalance = new TestRemotingRebalanceService();
        await using var consumer = CreateRemotingPushConsumer(
            Options.Create(options),
            Options.Create(new RemotingClientOptions
            {
                ClientIP = "127.0.0.1",
                InstanceName = "orderly-renewal",
                PollNameServerInterval = TimeSpan.FromMilliseconds(20),
                HeartbeatBrokerInterval = TimeSpan.FromMilliseconds(20)
            }),
            CreateRouteServiceMock().Object,
            remoting,
            TimeProvider.System,
            NullLogger<RemotingPushConsumer>.Instance,
            rebalanceService: rebalance);

        await consumer.StartAsync(cancellationToken);
        Assert.False(await rebalance.RebalanceAsync("legacy-group", cancellationToken));
        await remoting.WaitForTopicPullsAsync("orders", 2, cancellationToken);
        await consumer.StopAsync(cancellationToken);

        Assert.True(Volatile.Read(ref orderLockCalls) > 1);
    }

    [Fact]
    public async Task OrderlyConsumer_QueueMessagesWithoutMessageGroup_SerializesQueueMessages()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var firstStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var firstCancellationRequested = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseFirst = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var secondStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var delivered = 0;
        var body = CreateMessageRecord("orders", "first", null, 0, 1_000)
            .Concat(CreateMessageRecord("orders", "second", null, 1, 1_001))
            .ToArray();
        var remoting = new FakeRemotingClient("127.0.0.1@orderly-serial")
        {
            PullHandler = async (request, token) =>
            {
                var topic = Assert.IsType<string>(request.ExtFields["topic"]);
                if (topic == "orders" && Interlocked.Exchange(ref delivered, 1) == 0)
                {
                    return PullSuccess(body, 2);
                }

                await Task.Delay(Timeout.InfiniteTimeSpan, token);
                throw new InvalidOperationException("An infinite pull completed unexpectedly.");
            }
        };
        var options = WithMessageHandler(new RemotingPushConsumerOptions
        {
            GroupName = "legacy-group",
            ConsumeOrderly = true,
            InitialPosition = ConsumeFromPosition.Beginning,
            ConsumeTimeout = TimeSpan.FromMilliseconds(50),
            MaxConcurrency = 2,
            PullMaxCachedMessages = 8,
            LongPollingTimeout = TimeSpan.FromSeconds(1),
        }, async (messages, _, token) =>
            {
                var message = Assert.Single(messages);
                if (message.MessageId == "first")
                {
                    firstStarted.TrySetResult();
                    using var registration = token.Register(() => firstCancellationRequested.TrySetResult());
                    await releaseFirst.Task.WaitAsync(token);
                }
                else
                {
                    secondStarted.TrySetResult();
                }

                return ConsumeResult.Success;
            });
        options.Subscribe("orders");
        await using var consumer = CreateRemotingPushConsumer(
            options,
            CreateRouteServiceMock().Object,
            remoting,
            "orderly-serial");

        await consumer.StartAsync(cancellationToken);
        await firstStarted.Task.WaitAsync(TimeSpan.FromSeconds(3), cancellationToken);
        await Task.Delay(100, cancellationToken);
        Assert.False(firstCancellationRequested.Task.IsCompleted);
        Assert.False(secondStarted.Task.IsCompleted);

        releaseFirst.TrySetResult();
        await secondStarted.Task.WaitAsync(TimeSpan.FromSeconds(3), cancellationToken);
        await consumer.StopAsync(cancellationToken);

        Assert.Contains(
            remoting.UpdatedOffsets,
            static update => update.Topic == "orders" && update.Offset == 1);
        Assert.Contains(
            remoting.UpdatedOffsets,
            static update => update.Topic == "orders" && update.Offset == 2);
    }

    [Fact]
    public async Task OrderlyConsumer_CommitFailure_RepullsFromSameOffset()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var releaseFirstPull = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var orderPulls = 0;
        var body = CreateMessageRecord("orders", "orderly-commit-retry", null, 0, 1_000);
        var remoting = new FakeRemotingClient("127.0.0.1@orderly-commit-retry")
        {
            PullHandler = async (request, token) =>
            {
                if (Assert.IsType<string>(request.ExtFields["topic"]) == "orders" &&
                    Interlocked.Increment(ref orderPulls) == 1)
                {
                    await releaseFirstPull.Task.WaitAsync(token);
                    return PullSuccess(body, 1);
                }

                await Task.Delay(Timeout.InfiniteTimeSpan, token);
                throw new InvalidOperationException("An infinite pull completed unexpectedly.");
            }
        };
        var options = WithMessageHandler(new RemotingPushConsumerOptions
        {
            GroupName = "legacy-group",
            ConsumeOrderly = true,
            InitialPosition = ConsumeFromPosition.Beginning,
            MaxConcurrency = 1,
            PullMaxCachedMessages = 1,
            RetryDelay = TimeSpan.FromMilliseconds(10),
            LongPollingTimeout = TimeSpan.FromSeconds(1),
        }, static (_, _, _) => ValueTask.FromResult(ConsumeResult.Success));
        options.Subscribe("orders");
        await using var consumer = CreateRemotingPushConsumer(
            options,
            CreateRouteServiceMock().Object,
            remoting,
            "orderly-commit-retry");

        await consumer.StartAsync(cancellationToken);
        await remoting.WaitForTopicPullsAsync("orders", 1, cancellationToken);
        remoting.FailNextOffsetUpdate();
        releaseFirstPull.TrySetResult();
        await remoting.WaitForTopicPullsAsync("orders", 2, cancellationToken);

        Assert.Equal(
            [0L, 0L],
            remoting.PullOffsets
                .Where(static pull => pull.Topic == "orders")
                .Take(2)
                .Select(static pull => pull.Offset));
    }

    [Fact]
    public async Task OrderlyConsumer_BatchAcknowledgementContext_IgnoresBatchAcknowledgementContext()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var handled = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var delivered = 0;
        var body = CreateMessageRecord("orders", "order", null, 0, 1_000);
        var remoting = new FakeRemotingClient("127.0.0.1@orderly-context")
        {
            PullHandler = async (request, token) =>
            {
                if (Assert.IsType<string>(request.ExtFields["topic"]) == "orders" &&
                    Interlocked.Exchange(ref delivered, 1) == 0)
                {
                    return PullSuccess(body, 1);
                }

                await Task.Delay(Timeout.InfiniteTimeSpan, token);
                throw new InvalidOperationException("An infinite pull completed unexpectedly.");
            }
        };
        var options = WithMessageHandler(new RemotingPushConsumerOptions
        {
            GroupName = "legacy-group",
            ConsumeOrderly = true,
            InitialPosition = ConsumeFromPosition.Beginning,
            MaxConcurrency = 1,
            PullMaxCachedMessages = 1,
            MaxDeliveryAttempts = 1,
            RetryDelay = TimeSpan.FromMilliseconds(1),
            LongPollingTimeout = TimeSpan.FromSeconds(1),
        }, (messages, context, _) =>
            {
                Assert.Single(messages);
                context.AckIndex = -1;
                handled.TrySetResult();
                return ValueTask.FromResult(ConsumeResult.Success);
            });
        options.Subscribe("orders");
        await using var consumer = CreateRemotingPushConsumer(
            options,
            CreateRouteServiceMock().Object,
            remoting,
            "orderly-context");

        await consumer.StartAsync(cancellationToken);
        await handled.Task.WaitAsync(TimeSpan.FromSeconds(3), cancellationToken);
        while (!remoting.UpdatedOffsets.Any(static update => update.Topic == "orders" && update.Offset == 1))
        {
            await Task.Delay(10, cancellationToken);
        }

        await consumer.StopAsync(cancellationToken);

        Assert.DoesNotContain(remoting.Requests, static request => request.Code == RequestCode.ConsumerSendMsgBack);
    }

    [Fact]
    public async Task OrderlyConsumer_QueueLockAfterUnsubscribeDrainsHandler_ReleasesQueueLockAfterDrain()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var firstStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseFirst = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var delivered = 0;
        var body = CreateMessageRecord("orders", "first", null, 0, 1_000);
        var remoting = new FakeRemotingClient("127.0.0.1@orderly-unsubscribe")
        {
            PullHandler = async (request, token) =>
            {
                var topic = Assert.IsType<string>(request.ExtFields["topic"]);
                if (topic == "orders" && Interlocked.Exchange(ref delivered, 1) == 0)
                {
                    return PullSuccess(body, 1);
                }

                await Task.Delay(Timeout.InfiniteTimeSpan, token);
                throw new InvalidOperationException("An infinite pull completed unexpectedly.");
            }
        };
        var options = WithMessageHandler(new RemotingPushConsumerOptions
        {
            GroupName = "legacy-group",
            ConsumeOrderly = true,
            InitialPosition = ConsumeFromPosition.Beginning,
            MaxConcurrency = 1,
            PullMaxCachedMessages = 8,
            LongPollingTimeout = TimeSpan.FromSeconds(1),
        }, async (_, _, _) =>
            {
                firstStarted.TrySetResult();
                await releaseFirst.Task;
                return ConsumeResult.Success;
            });
        options.Subscribe("orders");
        await using var consumer = CreateRemotingPushConsumer(
            options,
            CreateRouteServiceMock().Object,
            remoting,
            "orderly-unsubscribe");

        await consumer.StartAsync(cancellationToken);
        await firstStarted.Task.WaitAsync(TimeSpan.FromSeconds(3), cancellationToken);
        var unsubscribe = consumer.UnsubscribeAsync("orders", cancellationToken);
        await Task.Delay(100, cancellationToken);
        Assert.False(unsubscribe.IsCompleted);
        Assert.DoesNotContain(
            remoting.OnewayRequests,
            static request => request.Code == RequestCode.UnlockBatchMq &&
                              RequestContainsQueueTopic(request, "orders"));

        releaseFirst.TrySetResult();
        await unsubscribe.WaitAsync(TimeSpan.FromSeconds(3), cancellationToken);
        await WaitForOnewayRequestAsync(
            remoting,
            static request => request.Code == RequestCode.UnlockBatchMq &&
                              RequestContainsQueueTopic(request, "orders"),
            cancellationToken);
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
            routes.Object,
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

    private static RemotingCommand CreateLockResponse(RemotingCommand request, bool grant)
    {
        using var body = JsonDocument.Parse(request.Body!);
        return new RemotingCommand
        {
            Code = ResponseCodes.ResSuccess,
            Body = JsonSerializer.SerializeToUtf8Bytes(new
            {
                lockOKMQSet = grant
                    ? (object)body.RootElement.GetProperty("mqSet").Clone()
                    : Array.Empty<object>()
            })
        };
    }

    private static bool RequestContainsQueueTopic(RemotingCommand request, string topic)
    {
        if (request.Body is not { Length: > 0 })
        {
            return false;
        }

        using var body = JsonDocument.Parse(request.Body);
        return body.RootElement.TryGetProperty("mqSet", out var queues) &&
               queues.EnumerateArray().Any(queue =>
                   string.Equals(queue.GetProperty("topic").GetString(), topic, StringComparison.Ordinal));
    }

    private static async Task WaitForOnewayRequestAsync(
        FakeRemotingClient remoting,
        Func<RemotingCommand, bool> predicate,
        CancellationToken cancellationToken)
    {
        while (!remoting.OnewayRequests.Any(predicate))
        {
            await Task.Delay(10, cancellationToken);
        }
    }

    private static RemotingCommand PullSuccess(byte[] body, long nextOffset) => new()
    {
        Code = ResponseCodes.ResSuccess,
        Body = body,
        ExtFields = new Dictionary<string, object>
        {
            ["nextBeginOffset"] = nextOffset.ToString(),
            ["minOffset"] = "0",
            ["maxOffset"] = nextOffset.ToString()
        }
    };

    private static RemotingCommand PullEmpty(long nextOffset) => new()
    {
        Code = ResponseCodes.ResPullNotFound,
        ExtFields = new Dictionary<string, object>
        {
            ["nextBeginOffset"] = nextOffset.ToString(),
            ["minOffset"] = "0",
            ["maxOffset"] = nextOffset.ToString()
        }
    };

    private static RemotingCommand PullOffsetIllegal(long nextOffset) => new()
    {
        Code = ResponseCodes.ResPullOffsetMoved,
        ExtFields = new Dictionary<string, object>
        {
            ["nextBeginOffset"] = nextOffset.ToString(),
            ["minOffset"] = "0",
            ["maxOffset"] = nextOffset.ToString()
        }
    };

    private static async Task<RemotingCommand> WaitForCanceledPullAsync(CancellationToken cancellationToken)
    {
        await Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken);
        throw new InvalidOperationException("An infinite pull completed unexpectedly.");
    }

    private static async Task<RemotingCommand> SignalThenWaitForCanceledPullAsync(
        TaskCompletionSource signal,
        CancellationToken cancellationToken)
    {
        signal.TrySetResult();
        return await WaitForCanceledPullAsync(cancellationToken);
    }

    private static void UpdateMaximum(ref int target, int candidate)
    {
        var observed = Volatile.Read(ref target);
        while (candidate > observed)
        {
            var replaced = Interlocked.CompareExchange(ref target, candidate, observed);
            if (replaced == observed)
            {
                return;
            }

            observed = replaced;
        }
    }

    private static byte[] CreateMessageRecord(
        string topic,
        string messageId,
        string? messageGroup,
        long queueOffset,
        long commitLogOffset,
        int queueId = 0)
    {
        var body = Encoding.UTF8.GetBytes(messageId);
        var propertyText = $"UNIQ_KEY\u0001{messageId}\u0002";
        if (messageGroup is not null)
        {
            propertyText += $"__SHARDINGKEY\u0001{messageGroup}\u0002";
        }

        var properties = Encoding.UTF8.GetBytes(propertyText);
        using var stream = new MemoryStream();
        WriteInt32(stream, 0);
        WriteInt32(stream, -626843481);
        WriteInt32(stream, 0);
        WriteInt32(stream, queueId);
        WriteInt32(stream, 0);
        WriteInt64(stream, queueOffset);
        WriteInt64(stream, commitLogOffset);
        WriteInt32(stream, 0);
        WriteInt64(stream, 1_700_000_000_000);
        stream.Write(new byte[] { 127, 0, 0, 1, 0x2A, 0x9F, 0, 0 });
        WriteInt64(stream, 1_700_000_000_100);
        stream.Write(new byte[] { 127, 0, 0, 1, 0x2A, 0x9F, 0, 0 });
        WriteInt32(stream, 0);
        WriteInt64(stream, 0);
        WriteInt32(stream, body.Length);
        stream.Write(body);
        stream.WriteByte(checked((byte)Encoding.UTF8.GetByteCount(topic)));
        stream.Write(Encoding.UTF8.GetBytes(topic));
        WriteUInt16(stream, checked((ushort)properties.Length));
        stream.Write(properties);

        var record = stream.ToArray();
        BinaryPrimitives.WriteInt32BigEndian(record, record.Length);
        return record;
    }

    private static void WriteInt32(Stream stream, int value)
    {
        Span<byte> bytes = stackalloc byte[sizeof(int)];
        BinaryPrimitives.WriteInt32BigEndian(bytes, value);
        stream.Write(bytes);
    }

    private static void WriteInt64(Stream stream, long value)
    {
        Span<byte> bytes = stackalloc byte[sizeof(long)];
        BinaryPrimitives.WriteInt64BigEndian(bytes, value);
        stream.Write(bytes);
    }

    private static void WriteUInt16(Stream stream, ushort value)
    {
        Span<byte> bytes = stackalloc byte[sizeof(ushort)];
        BinaryPrimitives.WriteUInt16BigEndian(bytes, value);
        stream.Write(bytes);
    }

    private static TopicRouteData Route(int queueCount) => new()
    {
        QueueDatas =
        [
            new QueueData
            {
                BrokerName = "broker-a",
                ReadQueueNums = queueCount,
                WriteQueueNums = queueCount,
                Perm = 6
            }
        ],
        BrokerDatas =
        [
            new BrokerData
            {
                BrokerName = "broker-a",
                BrokerAddrs = new ConcurrentDictionary<long, string>(
                    [new KeyValuePair<long, string>(0, "127.0.0.1:10911")])
            }
        ]
    };

    private static TopicRouteData MultiBrokerRoute() => new()
    {
        QueueDatas = Enumerable.Range(0, 3)
            .Select(static brokerIndex => new QueueData
            {
                BrokerName = $"broker-{(char)('a' + brokerIndex)}",
                ReadQueueNums = 3,
                WriteQueueNums = 3,
                Perm = 6
            })
            .ToArray(),
        BrokerDatas = Enumerable.Range(0, 3)
            .Select(static brokerIndex => new BrokerData
            {
                BrokerName = $"broker-{(char)('a' + brokerIndex)}",
                BrokerAddrs = new ConcurrentDictionary<long, string>(
                    [new KeyValuePair<long, string>(0, $"127.0.0.1:{10911 + (brokerIndex * 10)}")])
            })
            .ToArray()
    };

    private static IReadOnlyList<RemotingConsumerQueue> CreateMultiBrokerQueues() =>
        Enumerable.Range(0, 3)
            .SelectMany(brokerIndex => Enumerable.Range(0, 3)
                .Select(queueId => new RemotingConsumerQueue(
                    "orders",
                    queueId,
                    $"broker-{(char)('a' + brokerIndex)}",
                    $"127.0.0.1:{10911 + (brokerIndex * 10)}")))
            .ToArray();

    private static RemotingPushConsumerOptions CreateBroadcastOptions(
        string offsetPath,
        Func<IReadOnlyList<RemotingMessageView>, RemotingPushConsumeContext, CancellationToken, ValueTask<ConsumeResult>> handler)
    {
        var options = WithMessageHandler(new RemotingPushConsumerOptions
        {
            GroupName = "broadcast-group",
            ConsumerMode = ConsumerMode.Broadcasting,
            LocalOffsetStorePath = offsetPath,
            InitialPosition = ConsumeFromPosition.Beginning,
            MaxConcurrency = 2,
            PullMaxCachedMessages = 8,
            LongPollingTimeout = TimeSpan.FromSeconds(1),
        }, handler);
        options.Subscribe("orders");
        return options;
    }

    private static RemotingPushConsumerOptions CreateClusteringOptions() => WithMessageHandler(
        new RemotingPushConsumerOptions
        {
            GroupName = "legacy-group",
            MaxConcurrency = 2,
            PullMaxCachedMessages = 8,
            LongPollingTimeout = TimeSpan.FromSeconds(1)
        },
        static (_, _, _) => ValueTask.FromResult(ConsumeResult.Success));

    private static RemotingPushConsumerOptions WithMessageHandler(
        RemotingPushConsumerOptions options,
        Func<
            IReadOnlyList<RemotingMessageView>,
            RemotingPushConsumeContext,
            CancellationToken,
            ValueTask<ConsumeResult>> messageHandler)
    {
        MessageHandlers.Add(options, messageHandler);
        return options;
    }

    private static Func<
        IReadOnlyList<RemotingMessageView>,
        RemotingPushConsumeContext,
        CancellationToken,
        ValueTask<ConsumeResult>> GetMessageHandler(RemotingPushConsumerOptions options) =>
        MessageHandlers.TryGetValue(options, out var messageHandler)
            ? messageHandler
            : throw new InvalidOperationException("No test message handler was registered for these options.");

    private static RemotingPushConsumer CreateRemotingPushConsumer(
        RemotingPushConsumerOptions options,
        ITopicRouteService routes,
        IRemotingClient remoting,
        string instanceName,
        IRemotingRocketMQTelemetry? telemetry = null,
        IRemotingRebalanceService? rebalanceService = null) => CreateRemotingPushConsumer(
            Options.Create(options),
            Options.Create(new RemotingClientOptions
            {
                ClientIP = "127.0.0.1",
                InstanceName = instanceName,
                PollNameServerInterval = TimeSpan.FromHours(1),
                HeartbeatBrokerInterval = TimeSpan.FromHours(1)
            }),
            routes,
            remoting,
            TimeProvider.System,
            NullLogger<RemotingPushConsumer>.Instance,
            telemetry,
            rebalanceService);

    private static RemotingPushConsumer CreateRemotingPushConsumer(
        IOptions<RemotingPushConsumerOptions> options,
        IOptions<RemotingClientOptions> clientOptions,
        ITopicRouteService routes,
        IRemotingClient remoting,
        TimeProvider timeProvider,
        ILogger<RemotingPushConsumer> logger,
        IRemotingRocketMQTelemetry? telemetry = null,
        IRemotingRebalanceService? rebalanceService = null)
    {
        var consumerEngine = CreateRemotingConsumerEngine(
            options.Value,
            options.Value.PullBatchSize,
            options.Value.MaxMessageBytes,
            options.Value.LongPollingTimeout,
            clientOptions,
            routes,
            remoting,
            timeProvider,
            telemetry);
        return new RemotingPushConsumer(
            options,
            clientOptions,
            consumerEngine,
            routes,
            remoting,
            rebalanceService ?? new TestRemotingRebalanceService(),
            timeProvider,
            logger,
            GetMessageHandler(options.Value),
            telemetry);
    }

    private static RemotingConsumerEngine CreateRemotingConsumerEngine(
        ConsumerOptions options,
        int batchSize,
        int maxMessageBytes,
        TimeSpan longPollingTimeout,
        IOptions<RemotingClientOptions> clientOptions,
        ITopicRouteService routes,
        IRemotingClient remoting,
        TimeProvider timeProvider,
        IRemotingRocketMQTelemetry? telemetry = null) =>
        new(
            new RemotingConsumerSettings(
                options.GroupName,
                options.Subscriptions,
                batchSize,
                maxMessageBytes,
                longPollingTimeout),
            clientOptions,
            routes,
            remoting,
            timeProvider,
            telemetry);

    private static Mock<ITopicRouteService> CreateRouteServiceMock(
        int queueCount = 1,
        ConcurrentQueue<string>? topics = null)
    {
        var routes = new Mock<ITopicRouteService>(MockBehavior.Strict);
        routes
            .Setup(value => value.GetAsync(
                It.IsAny<string>(),
                It.IsAny<bool>(),
                It.IsAny<CancellationToken>()))
            .Returns<string, bool, CancellationToken>((topic, _, _) =>
            {
                topics?.Enqueue(topic);
                return Task.FromResult(Route(queueCount));
            });
        return routes;
    }

    private sealed class ProcessTelemetry(IRemotingRocketMQTelemetryOperation processOperation) : IRemotingRocketMQTelemetry
    {
        private int _processBatchCalls;

        public int ProcessBatchCalls => Volatile.Read(ref _processBatchCalls);

        public IRemotingRocketMQTelemetryOperation StartSend(string topic, int messageCount, long bodySize) =>
            RemotingRocketMQTelemetry.Operation.Disabled;

        public IRemotingRocketMQTelemetryOperation StartReceive(
            string topic,
            string consumerGroup,
            int messageCount,
            ActivityContext? parentContext,
            DateTimeOffset startTime,
            long startTimestamp,
            int? queueId = null,
            bool createActivity = true,
            IEnumerable<IReadOnlyDictionary<string, string>>? messageProperties = null) =>
            RemotingRocketMQTelemetry.Operation.Disabled;

        public IRemotingRocketMQTelemetryOperation StartProcess(
            string topic,
            string consumerGroup,
            string messageId,
            int queueId,
            long bodySize,
            IReadOnlyDictionary<string, string> properties,
            ActivityContext? receiveContext) =>
            RemotingRocketMQTelemetry.Operation.Disabled;

        public IRemotingRocketMQTelemetryOperation StartProcessBatch(
            string topic,
            string consumerGroup,
            IReadOnlyList<RemotingMessageView> messages)
        {
            Interlocked.Increment(ref _processBatchCalls);
            return processOperation;
        }

        public IRemotingRocketMQTelemetryOperation StartSettle(
            string operationName,
            string topic,
            string consumerGroup,
            string? messageId,
            int? queueId,
            IReadOnlyDictionary<string, string>? properties) =>
            RemotingRocketMQTelemetry.Operation.Disabled;

        public void InjectContext(Activity? activity, IDictionary<string, string> properties)
        {
        }
    }

    private sealed class FakeRemotingClient(string clientId) : IRemotingClient, IRemotingRequestHandlerRegistry
    {
        private readonly TaskCompletionSource _pullChanged =
            new(TaskCreationOptions.RunContinuationsAsynchronously);
        private readonly TaskCompletionSource _pullCanceled =
            new(TaskCreationOptions.RunContinuationsAsynchronously);
        private readonly ConcurrentDictionary<(string Topic, int QueueId), long> _committedOffsets = new();
        private readonly ConcurrentDictionary<int, RemotingRequestHandler> _requestHandlers = new();
        private string[] _consumerIds = [clientId];
        private int _failNextOffsetUpdate;

        public ConcurrentQueue<byte[]> HeartbeatBodies { get; } = new();
        public ConcurrentQueue<(string Topic, int QueueId, long Offset)> PullOffsets { get; } = new();
        public ConcurrentQueue<string> CanceledPullTopics { get; } = new();
        public ConcurrentQueue<(string Topic, long Offset)> UpdatedOffsets { get; } = new();
        public ConcurrentQueue<RemotingCommand> Requests { get; } = new();
        public ConcurrentQueue<RemotingCommand> OnewayRequests { get; } = new();
        public Func<RemotingCommand, CancellationToken, Task<RemotingCommand>>? HeartbeatHandler { get; init; }
        public Func<RemotingCommand, CancellationToken, Task<RemotingCommand>>? ConsumerIdsHandler { get; init; }
        public Func<RemotingCommand, CancellationToken, Task<RemotingCommand>>? ConsumerOffsetHandler { get; init; }
        public Func<RemotingCommand, CancellationToken, Task<RemotingCommand>>? LockHandler { get; set; }
        public Func<RemotingCommand, CancellationToken, Task<RemotingCommand>>? MaxOffsetHandler { get; init; }
        public Func<RemotingCommand, CancellationToken, Task<RemotingCommand>>? PullHandler { get; init; }
        public Func<RemotingCommand, CancellationToken, Task<RemotingCommand>>? QueryAssignmentHandler { get; init; }
        public Func<RemotingCommand, CancellationToken, Task<RemotingCommand>>? PopHandler { get; init; }
        public Func<RemotingCommand, CancellationToken, Task<RemotingCommand>>? AckHandler { get; init; }
        public Func<RemotingCommand, CancellationToken, Task<RemotingCommand>>? ChangeInvisibleHandler { get; init; }
        public Func<RemotingCommand, CancellationToken, Task<RemotingCommand>>? SendBackHandler { get; init; }
        public Func<RemotingCommand, CancellationToken, Task<RemotingCommand>>? UpdateOffsetHandler { get; init; }
        public Func<RemotingCommand, CancellationToken, Task<RemotingCommand>>? UnregisterHandler { get; init; }
        public bool TrackCommittedOffsets { get; init; }
        public int CanceledPulls;
        public int UnregisterCalls;

        public int ActiveRequestHandlerRegistrations =>
            _requestHandlers.Count;

        public void SetConsumerIds(params string[] consumerIds) =>
            Volatile.Write(ref _consumerIds, consumerIds);

        public void FailNextOffsetUpdate() => Interlocked.Exchange(ref _failNextOffsetUpdate, 1);

        public IDisposable RegisterRequestHandler(int requestCode, RemotingRequestHandler handler)
        {
            if (requestCode is not RequestCode.NotifyConsumerIdsChanged and not RequestCode.ResetConsumerOffset)
            {
                throw new ArgumentOutOfRangeException(nameof(requestCode));
            }

            if (!_requestHandlers.TryAdd(requestCode, handler))
            {
                throw new InvalidOperationException($"Request handler {requestCode} is already registered.");
            }

            return new CallbackRegistration(() =>
                ((ICollection<KeyValuePair<int, RemotingRequestHandler>>)_requestHandlers).Remove(
                    new KeyValuePair<int, RemotingRequestHandler>(requestCode, handler)));
        }

        public async ValueTask<RemotingCommand?> NotifyConsumerIdsChangedAsync(
            string consumerGroup,
            CancellationToken cancellationToken)
        {
            var result = await InvokeBrokerRequestAsync(
                RequestCode.NotifyConsumerIdsChanged,
                new Dictionary<string, object> { ["consumerGroup"] = consumerGroup },
                body: null,
                cancellationToken);
            return result.Response;
        }

        public ValueTask<RemotingRequestResult> InvokeBrokerRequestAsync(
            int requestCode,
            Dictionary<string, object> fields,
            byte[]? body,
            CancellationToken cancellationToken)
        {
            if (!_requestHandlers.TryGetValue(requestCode, out var handler))
            {
                throw new InvalidOperationException($"Request handler {requestCode} is not registered.");
            }

            return handler(new RemotingRequestContext(
                new IPEndPoint(IPAddress.Loopback, 10911),
                new RemotingCommand
                {
                    Code = requestCode,
                    ExtFields = fields,
                    Body = body
                },
                cancellationToken));
        }

        public async Task<RemotingCommand> InvokeAsync(
            EndPoint endPoint,
            RemotingCommand request,
            TimeSpan timeout,
            CancellationToken cancellationToken = default)
        {
            Requests.Enqueue(request);
            switch (request.Code)
            {
                case RequestCode.HeartBeat:
                    HeartbeatBodies.Enqueue(request.Body!);
                    return HeartbeatHandler is null
                        ? Success()
                        : await HeartbeatHandler(request, cancellationToken);
                case RequestCode.GetConsumerListByGroup:
                    return ConsumerIdsHandler is null
                        ? Success(JsonSerializer.SerializeToUtf8Bytes(new
                        {
                            consumerIdList = Volatile.Read(ref _consumerIds)
                        }))
                        : await ConsumerIdsHandler(request, cancellationToken);
                case RequestCode.LockBatchMq:
                    return LockHandler is null
                        ? LockSuccess(request)
                        : await LockHandler(request, cancellationToken);
                case RequestCode.QueryConsumerOffset:
                    if (ConsumerOffsetHandler is not null)
                    {
                        return await ConsumerOffsetHandler(request, cancellationToken);
                    }

                    if (TrackCommittedOffsets &&
                        _committedOffsets.TryGetValue((
                            Assert.IsType<string>(request.ExtFields["topic"]),
                            Convert.ToInt32(request.ExtFields["queueId"])), out var committedOffset))
                    {
                        return Success(offset: committedOffset);
                    }

                    return new RemotingCommand { Code = ResponseCodes.ResQueryNotFound };
                case RequestCode.GetMinOffset:
                    return Success(offset: 0);
                case RequestCode.GetMaxOffset:
                    return MaxOffsetHandler is null
                        ? Success(offset: 17)
                        : await MaxOffsetHandler(request, cancellationToken);
                case RequestCode.SearchOffsetByTimestamp:
                    return Success(offset: 9);
                case RequestCode.UpdateConsumerOffset:
                    var offsetKey = (
                        Assert.IsType<string>(request.ExtFields["topic"]),
                        Convert.ToInt32(request.ExtFields["queueId"]));
                    var updatedOffset = Convert.ToInt64(request.ExtFields["commitOffset"]);
                    if (Interlocked.Exchange(ref _failNextOffsetUpdate, 0) != 0)
                    {
                        throw new IOException("The configured offset update failed.");
                    }

                    if (UpdateOffsetHandler is not null)
                    {
                        var response = await UpdateOffsetHandler(request, cancellationToken);
                        UpdatedOffsets.Enqueue((offsetKey.Item1, updatedOffset));
                        return response;
                    }

                    UpdatedOffsets.Enqueue((offsetKey.Item1, updatedOffset));
                    if (TrackCommittedOffsets)
                    {
                        _committedOffsets[offsetKey] = updatedOffset;
                    }

                    return Success();
                case RequestCode.PullMessage:
                    PullOffsets.Enqueue((
                        Assert.IsType<string>(request.ExtFields["topic"]),
                        Convert.ToInt32(request.ExtFields["queueId"]),
                        Convert.ToInt64(request.ExtFields["queueOffset"])));
                    _pullChanged.TrySetResult();
                    try
                    {
                        if (PullHandler is not null)
                        {
                            return await PullHandler(request, cancellationToken);
                        }

                        await Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken);
                        throw new InvalidOperationException("An infinite pull completed unexpectedly.");
                    }
                    catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                    {
                        CanceledPullTopics.Enqueue(
                            Assert.IsType<string>(request.ExtFields["topic"]));
                        Interlocked.Increment(ref CanceledPulls);
                        _pullCanceled.TrySetResult();
                        throw;
                    }
                case RequestCode.QueryAssignment:
                    return QueryAssignmentHandler is null
                        ? throw new InvalidOperationException("No query-assignment response was configured.")
                        : await QueryAssignmentHandler(request, cancellationToken);
                case RequestCode.PopMessage:
                    return PopHandler is null
                        ? throw new InvalidOperationException("No POP response was configured.")
                        : await PopHandler(request, cancellationToken);
                case RequestCode.AckMessage:
                    return AckHandler is null
                        ? throw new InvalidOperationException("No POP acknowledgement response was configured.")
                        : await AckHandler(request, cancellationToken);
                case RequestCode.ChangeMessageInvisibleTime:
                    return ChangeInvisibleHandler is null
                        ? throw new InvalidOperationException("No POP invisibility response was configured.")
                        : await ChangeInvisibleHandler(request, cancellationToken);
                case RequestCode.ConsumerSendMsgBack:
                    return SendBackHandler is null
                        ? Success()
                        : await SendBackHandler(request, cancellationToken);
                case RequestCode.UnregisterClient:
                    Interlocked.Increment(ref UnregisterCalls);
                    return UnregisterHandler is null
                        ? Success()
                        : await UnregisterHandler(request, cancellationToken);
                default:
                    return Success();
            }
        }

        public async Task WaitForPullsAsync(int count, CancellationToken cancellationToken)
        {
            while (PullOffsets.Count < count)
            {
                await _pullChanged.Task.WaitAsync(TimeSpan.FromSeconds(5), cancellationToken);
                if (PullOffsets.Count < count)
                {
                    await Task.Delay(10, cancellationToken);
                }
            }
        }

        public async Task WaitForCanceledPullsAsync(int count, CancellationToken cancellationToken)
        {
            while (Volatile.Read(ref CanceledPulls) < count)
            {
                await _pullCanceled.Task.WaitAsync(TimeSpan.FromSeconds(5), cancellationToken);
                if (Volatile.Read(ref CanceledPulls) < count)
                {
                    await Task.Delay(10, cancellationToken);
                }
            }
        }

        public async Task WaitForTopicPullsAsync(
            string topic,
            int count,
            CancellationToken cancellationToken)
        {
            while (PullOffsets.Count(pull => pull.Topic == topic) < count)
            {
                await _pullChanged.Task.WaitAsync(TimeSpan.FromSeconds(5), cancellationToken);
                if (PullOffsets.Count(pull => pull.Topic == topic) < count)
                {
                    await Task.Delay(10, cancellationToken);
                }
            }
        }

        public async Task WaitForRequestCountAsync(
            int requestCode,
            int count,
            CancellationToken cancellationToken)
        {
            while (Requests.Count(request => request.Code == requestCode) < count)
            {
                await Task.Delay(10, cancellationToken);
            }
        }

        public Task InvokeOnewayAsync(
            EndPoint endPoint,
            RemotingCommand request,
            TimeSpan timeout,
            CancellationToken cancellationToken = default)
        {
            Requests.Enqueue(request);
            OnewayRequests.Enqueue(request);
            return Task.CompletedTask;
        }

        public ValueTask DisposeAsync() => ValueTask.CompletedTask;

        private static RemotingCommand Success(byte[]? body = null, long? offset = null) => new()
        {
            Code = ResponseCodes.ResSuccess,
            Body = body,
            ExtFields = offset.HasValue
                ? new Dictionary<string, object> { ["offset"] = offset.Value.ToString() }
                : new Dictionary<string, object>()
        };

        private static RemotingCommand LockSuccess(RemotingCommand request)
        {
            using var body = JsonDocument.Parse(request.Body!);
            return Success(JsonSerializer.SerializeToUtf8Bytes(new
            {
                lockOKMQSet = body.RootElement.GetProperty("mqSet").Clone()
            }));
        }

        private sealed class CallbackRegistration(Action callback) : IDisposable
        {
            private Action? _callback = callback;

            public void Dispose() => Interlocked.Exchange(ref _callback, null)?.Invoke();
        }
    }
}
