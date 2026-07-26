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
using System.Text;
using System.Text.Json;
using EventHorizon.RocketMQ.Remoting.Consumer;
using EventHorizon.RocketMQ.Remoting.Consumer.Pull;
using EventHorizon.RocketMQ.Remoting.Consumer.Push;
using EventHorizon.RocketMQ.Remoting.Instrumentation;
using EventHorizon.RocketMQ.Remoting.Protocol;
using EventHorizon.RocketMQ.Remoting.Protocol.Route;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Moq;
using Xunit;

namespace EventHorizon.RocketMQ.Remoting.Tests.Consumer.Push;

public sealed class RemotingPushConsumerTests
{
    [Theory]
    [InlineData(ConsumeFromPosition.Beginning, "CONSUME_FROM_FIRST_OFFSET")]
    [InlineData(ConsumeFromPosition.End, "CONSUME_FROM_LAST_OFFSET")]
    [InlineData(ConsumeFromPosition.Timestamp, "CONSUME_FROM_TIMESTAMP")]
    public void Heartbeat_UsesConfiguredInitialPosition(
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
    public void Heartbeat_UsesConfiguredConsumerMode(ConsumerMode mode, string expectedMessageModel)
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
    public void ResetOffsetDecoder_DecodesGsonComplexMapFixture()
    {
        var body = Encoding.UTF8.GetBytes(
            """
            {"offsetTable":[[{"topic":"tenant-a%orders","brokerName":"broker-a","queueId":0},7]]}
            """);

        var offset = Assert.Single(LegacyResetOffsetDecoder.Decode(body));

        Assert.Equal(new LegacyResetOffset("tenant-a%orders", "broker-a", 0, 7), offset);
    }

    [Fact]
    public void ResetOffsetDecoder_DecodesFastJsonObjectKeyFixture()
    {
        var body = Encoding.UTF8.GetBytes(
            """
            {"offsetTable":{{"brokerName":"broker-a","queueId":0,"topic":"tenant-a%orders"}:7}}
            """);

        var offset = Assert.Single(LegacyResetOffsetDecoder.Decode(body));

        Assert.Equal(new LegacyResetOffset("tenant-a%orders", "broker-a", 0, 7), offset);
    }

    [Fact]
    public void ResetOffsetDecoder_DecodesCFlatListFixture()
    {
        var body = Encoding.UTF8.GetBytes(
            """
            {"offsetTable":[{"topic":"tenant-a%orders","brokerName":"broker-a","queueId":0,"offset":7}]}
            """);

        var offset = Assert.Single(LegacyResetOffsetDecoder.Decode(body));

        Assert.Equal(new LegacyResetOffset("tenant-a%orders", "broker-a", 0, 7), offset);
    }

    [Fact]
    public async Task ConsumerIdsChangedNotification_RebalancesMatchingGroupAndUnregistersOnDispose()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var remoting = new FakeRemotingClient("127.0.0.1@notify-test");
        var options = new RemotingPushConsumerOptions
        {
            GroupName = "legacy-group",
            MaxConcurrency = 1,
            MaxCachedMessages = 8,
            LongPollingTimeout = TimeSpan.FromSeconds(1),
            MessageHandler = static (_, _) => ValueTask.FromResult(ConsumeResult.Success)
        };
        options.Subscribe("orders");
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
            NullLogger<RemotingPushConsumer>.Instance);

        try
        {
            Assert.Equal(2, remoting.ActiveRequestHandlerRegistrations);
            await consumer.StartAsync(cancellationToken);
            var initialCoordinationCount = remoting.Requests.Count(
                static request => request.Code == RequestCode.GetConsumerListByGroup);

            var ignoredResponse = await remoting.NotifyConsumerIdsChangedAsync(
                "tenant-a%another-group",
                cancellationToken);
            Assert.Null(ignoredResponse);
            await Task.Delay(100, cancellationToken);
            Assert.Equal(
                initialCoordinationCount,
                remoting.Requests.Count(static request => request.Code == RequestCode.GetConsumerListByGroup));

            var response = await remoting.NotifyConsumerIdsChangedAsync(
                "tenant-a%legacy-group",
                cancellationToken);
            Assert.Null(response);
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
    }

    [Fact]
    public async Task ResetOffsetNotification_StopsOldReceiverAndPullsFromResetOffset()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var remoting = new FakeRemotingClient("127.0.0.1@reset-test")
        {
            TrackCommittedOffsets = true
        };
        var options = new RemotingPushConsumerOptions
        {
            GroupName = "legacy-group",
            InitialPosition = ConsumeFromPosition.Beginning,
            MaxConcurrency = 1,
            MaxCachedMessages = 8,
            LongPollingTimeout = TimeSpan.FromSeconds(1),
            MessageHandler = static (_, _) => ValueTask.FromResult(ConsumeResult.Success)
        };
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
            Assert.Equal(2, remoting.ActiveRequestHandlerRegistrations);
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
    public async Task ResetOffsetNotification_RetainsBoundaryWhenBrokerUpdateFails()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var remoting = new FakeRemotingClient("127.0.0.1@reset-failure")
        {
            TrackCommittedOffsets = true
        };
        var options = new RemotingPushConsumerOptions
        {
            GroupName = "legacy-group",
            InitialPosition = ConsumeFromPosition.Beginning,
            MaxConcurrency = 1,
            MaxCachedMessages = 8,
            LongPollingTimeout = TimeSpan.FromSeconds(1),
            MessageHandler = static (_, _) => ValueTask.FromResult(ConsumeResult.Success)
        };
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

        Assert.Equal(
            [0, 7],
            remoting.PullOffsets
                .Where(static pull => pull.Topic == "tenant-a%orders")
                .Select(static pull => pull.Offset));
        Assert.DoesNotContain(
            remoting.UpdatedOffsets,
            static update => update.Topic == "tenant-a%orders" && update.Offset == 7);
    }

    [Fact]
    public async Task ResetOffsetNotification_RetriesFailedCommitAfterEmptyPullAndRestartUsesCommit()
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
        var options = new RemotingPushConsumerOptions
        {
            GroupName = "legacy-group",
            InitialPosition = ConsumeFromPosition.Beginning,
            MaxConcurrency = 1,
            MaxCachedMessages = 8,
            LongPollingTimeout = TimeSpan.FromSeconds(1),
            MessageHandler = static (_, _) => ValueTask.FromResult(ConsumeResult.Success)
        };
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
    public async Task ResetOffsetNotifications_BeforeStartApplyLatestOffsetWhenReceiverIsCreated()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var returnedResetEmptyPull = 0;
        var remoting = new FakeRemotingClient("127.0.0.1@reset-before-start")
        {
            TrackCommittedOffsets = true,
            PullHandler = async (request, token) =>
            {
                var offset = Convert.ToInt64(request.ExtFields["queueOffset"]);
                if (offset == 9 && Interlocked.Exchange(ref returnedResetEmptyPull, 1) == 0)
                {
                    return PullEmpty(offset);
                }

                await Task.Delay(Timeout.InfiniteTimeSpan, token);
                throw new InvalidOperationException("An infinite pull completed unexpectedly.");
            }
        };
        var options = new RemotingPushConsumerOptions
        {
            GroupName = "legacy-group",
            InitialPosition = ConsumeFromPosition.Beginning,
            MaxConcurrency = 1,
            MaxCachedMessages = 8,
            LongPollingTimeout = TimeSpan.FromSeconds(1),
            MessageHandler = static (_, _) => ValueTask.FromResult(ConsumeResult.Success)
        };
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

        Assert.Equal(
            9,
            remoting.PullOffsets.First(static pull => pull.Topic == "tenant-a%orders").Offset);

        await remoting.WaitForTopicPullsAsync("tenant-a%orders", 2, cancellationToken);
        Assert.Contains(
            remoting.UpdatedOffsets,
            static update => update.Topic == "tenant-a%orders" && update.Offset == 9);
    }

    [Fact]
    public async Task BroadcastingResetOffset_RetriesFailedLocalCommitAfterEmptyPullAtSameOffset()
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
            static (_, _) => ValueTask.FromResult(ConsumeResult.Success));
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
                new RemotingPullMessageQueue("orders", 0, "broker-a", "127.0.0.1:10911"),
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
    public void Allocate_UsesSortedApacheAverageStrategy()
    {
        var queues = Enumerable.Range(0, 8)
            .Select(queueId => new RemotingPullMessageQueue("orders", queueId, "broker-a", "127.0.0.1:10911"))
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

    [Fact]
    public async Task StartStopStart_RegistersAgainAndStartsUncommittedRetryQueuesAtBeginning()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var remoting = new FakeRemotingClient("127.0.0.1@legacy-test");
        var routeTopics = new ConcurrentQueue<string>();
        var routes = CreateRouteServiceMock(topics: routeTopics);
        var options = new RemotingPushConsumerOptions
        {
            GroupName = "legacy-group",
            MaxConcurrency = 1,
            MaxCachedMessages = 8,
            LongPollingTimeout = TimeSpan.FromSeconds(1),
            MessageHandler = static (_, _) => ValueTask.FromResult(ConsumeResult.Success)
        };
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
        Assert.Equal(2, routeTopics.Count(static topic => topic == "tenant-a%orders"));
        Assert.Equal(2, routeTopics.Count(static topic => topic == "%RETRY%tenant-a%legacy-group"));
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
    public async Task UncommittedOrdinaryQueue_UsesConfiguredInitialPositionWhileRetryStartsAtZero(
        ConsumeFromPosition position,
        int expectedRequestCode,
        long expectedOffset)
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var timestamp = new DateTimeOffset(2026, 7, 18, 9, 30, 0, TimeSpan.Zero);
        var remoting = new FakeRemotingClient("127.0.0.1@legacy-test");
        var options = new RemotingPushConsumerOptions
        {
            GroupName = "legacy-group",
            InitialPosition = position,
            ConsumeTimestamp = position == ConsumeFromPosition.Timestamp ? timestamp : null,
            MaxConcurrency = 1,
            MaxCachedMessages = 8,
            LongPollingTimeout = TimeSpan.FromSeconds(1),
            MessageHandler = static (_, _) => ValueTask.FromResult(ConsumeResult.Success)
        };
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
    public async Task Broadcasting_AssignsEveryQueueWithoutMembershipOffsetsOrRetrySubscription()
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
            var options = CreateBroadcastOptions(offsetPath, static (_, _) =>
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
    public async Task Broadcasting_RetryDropsAndLocalOffsetResumesAcrossConsumerInstances()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var offsetPath = System.IO.Path.Combine(
            System.IO.Path.GetTempPath(),
            $"rocketmq-broadcast-resume-{Guid.NewGuid():N}.json");
        try
        {
            var body = CreateMessageRecord("orders", "broadcast-retry", "account-7", 0, 1_000);
            var delivered = 0;
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
            var firstOptions = CreateBroadcastOptions(offsetPath, static (_, _) =>
                ValueTask.FromResult(ConsumeResult.Retry));
            await using (var first = CreateRemotingPushConsumer(
                             firstOptions,
                             CreateRouteServiceMock().Object,
                             firstRemoting,
                             "broadcast-test"))
            {
                await first.StartAsync(cancellationToken);
                await firstRemoting.WaitForPullsAsync(2, cancellationToken);
                await first.StopAsync(cancellationToken);
            }

            Assert.DoesNotContain(firstRemoting.Requests, static request => request.Code is
                RequestCode.GetConsumerListByGroup or RequestCode.QueryConsumerOffset or
                RequestCode.UpdateConsumerOffset or RequestCode.ConsumerSendMsgBack);

            var secondRemoting = new FakeRemotingClient("127.0.0.1@broadcast-test");
            var secondOptions = CreateBroadcastOptions(offsetPath, static (_, _) =>
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

    [Theory]
    [InlineData(ConsumeResult.Retry)]
    [InlineData(ConsumeResult.DeadLetter)]
    public async Task UnsuccessfulHandlerResult_RecordsFailedProcessTelemetry(ConsumeResult result)
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
            .Setup(value => value.StartProcess(
                It.IsAny<string>(),
                It.IsAny<string>(),
                It.IsAny<string>(),
                It.IsAny<int>(),
                It.IsAny<long>(),
                It.IsAny<IReadOnlyDictionary<string, string>>(),
                It.IsAny<ActivityContext?>()))
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
            (_, _) => ValueTask.FromResult(result));
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
    public async Task SubscribeBeforeStart_BroadcastingPullsWithConfiguredSqlFilter()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var offsetPath = System.IO.Path.Combine(
            System.IO.Path.GetTempPath(),
            $"rocketmq-dynamic-subscription-{Guid.NewGuid():N}.json");
        try
        {
            var options = new RemotingPushConsumerOptions
            {
                GroupName = "broadcast-group",
                ConsumerMode = ConsumerMode.Broadcasting,
                LocalOffsetStorePath = offsetPath,
                InitialPosition = ConsumeFromPosition.Beginning,
                MaxConcurrency = 1,
                MaxCachedMessages = 8,
                LongPollingTimeout = TimeSpan.FromSeconds(1),
                MessageHandler = static (_, _) => ValueTask.FromResult(ConsumeResult.Success)
            };
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
    public async Task RunningSubscribeAndFilterUpdate_RestartPullsWithLatestFilter()
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
    public async Task RunningUnsubscribe_RevokesTopicAndRemovesItFromHeartbeat()
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
    public async Task SubscriptionChanges_RejectDisposedConsumerAndHonorCancellation()
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
    public async Task RestartWithUncommittedRetryBacklog_DeliversFromBeginning()
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
        var options = new RemotingPushConsumerOptions
        {
            GroupName = "legacy-group",
            MaxConcurrency = 1,
            MaxCachedMessages = 8,
            LongPollingTimeout = TimeSpan.FromSeconds(1),
            MessageHandler = (message, _) =>
            {
                if (message.MessageId == "retry-after-crash")
                {
                    retryDelivered.TrySetResult();
                }

                return ValueTask.FromResult(ConsumeResult.Success);
            }
        };
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
    public async Task MissingBrokerMembershipRevokesOwnedQueues()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var remoting = new FakeRemotingClient("127.0.0.1@legacy-test");
        var options = new RemotingPushConsumerOptions
        {
            GroupName = "legacy-group",
            MaxConcurrency = 1,
            MaxCachedMessages = 8,
            LongPollingTimeout = TimeSpan.FromSeconds(1),
            MessageHandler = static (_, _) => ValueTask.FromResult(ConsumeResult.Success)
        };
        options.Subscribe("orders");
        var clientOptions = new RemotingClientOptions
        {
            ClientIP = "127.0.0.1",
            InstanceName = "legacy-test",
            PollNameServerInterval = TimeSpan.FromMilliseconds(20),
            HeartbeatBrokerInterval = TimeSpan.FromMilliseconds(20)
        };
        await using var consumer = CreateRemotingPushConsumer(
            Options.Create(options),
            Options.Create(clientOptions),
            CreateRouteServiceMock().Object,
            remoting,
            TimeProvider.System,
            NullLogger<RemotingPushConsumer>.Instance);

        await consumer.StartAsync(cancellationToken);
        await remoting.WaitForPullsAsync(2, cancellationToken);
        remoting.SetConsumerIds("another-client");
        await remoting.WaitForCanceledPullsAsync(2, cancellationToken);
        var pullCountAfterRevocation = remoting.PullOffsets.Count;
        await Task.Delay(100, cancellationToken);
        await consumer.StopAsync(cancellationToken);

        Assert.Equal(2, pullCountAfterRevocation);
        Assert.Equal(pullCountAfterRevocation, remoting.PullOffsets.Count);
    }

    [Fact]
    public async Task FifoRetryKeepsSameGroupSuccessorBlockedUntilPredecessorSucceeds()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var firstSecondAttempt = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
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
        var options = new RemotingPushConsumerOptions
        {
            GroupName = "legacy-group",
            MaxConcurrency = 2,
            MaxCachedMessages = 8,
            MaxDeliveryAttempts = 3,
            RetryDelay = TimeSpan.FromMilliseconds(10),
            LongPollingTimeout = TimeSpan.FromSeconds(1),
            MessageHandler = async (message, token) =>
            {
                if (message.MessageId == "first")
                {
                    if (Interlocked.Increment(ref firstCalls) == 1)
                    {
                        return ConsumeResult.Retry;
                    }

                    firstSecondAttempt.TrySetResult();
                    await releaseFirst.Task.WaitAsync(token);
                    return ConsumeResult.Success;
                }

                secondHandled.TrySetResult();
                return ConsumeResult.Success;
            }
        };
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
        Assert.False(secondHandled.Task.IsCompleted);

        releaseFirst.TrySetResult();
        await secondHandled.Task.WaitAsync(TimeSpan.FromSeconds(3), cancellationToken);
        await consumer.StopAsync(cancellationToken);

        Assert.Equal(2, firstCalls);
        Assert.DoesNotContain(remoting.Requests, static request => request.Code == RequestCode.ConsumerSendMsgBack);
    }

    [Fact]
    public async Task OrderlyConsumer_UsesBrokerQueueLockBeforePulling()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var remoting = new FakeRemotingClient("127.0.0.1@orderly-lock-test");
        var options = new RemotingPushConsumerOptions
        {
            GroupName = "legacy-group",
            ConsumeOrderly = true,
            InitialPosition = ConsumeFromPosition.Beginning,
            MaxConcurrency = 2,
            MaxCachedMessages = 8,
            LongPollingTimeout = TimeSpan.FromSeconds(1),
            MessageHandler = static (_, _) => ValueTask.FromResult(ConsumeResult.Success)
        };
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
    public async Task OrderlyConsumer_DoesNotPullUntilBrokerGrantsQueueLock()
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
        var options = new RemotingPushConsumerOptions
        {
            GroupName = "legacy-group",
            ConsumeOrderly = true,
            InitialPosition = ConsumeFromPosition.Beginning,
            MaxConcurrency = 1,
            MaxCachedMessages = 8,
            LongPollingTimeout = TimeSpan.FromSeconds(1),
            MessageHandler = static (_, _) => ValueTask.FromResult(ConsumeResult.Success)
        };
        options.Subscribe("orders");
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
            NullLogger<RemotingPushConsumer>.Instance);

        await consumer.StartAsync(cancellationToken);
        await Task.Delay(100, cancellationToken);
        Assert.DoesNotContain(
            remoting.PullOffsets,
            static pull => string.Equals(pull.Topic, "orders", StringComparison.Ordinal));

        Volatile.Write(ref lockOrders, 1);
        await remoting.WaitForTopicPullsAsync("orders", 1, cancellationToken);
        await consumer.StopAsync(cancellationToken);
    }

    [Fact]
    public async Task OrderlyConsumer_PreservesLeaseAfterTransientLockRenewalFailure()
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
        var options = new RemotingPushConsumerOptions
        {
            GroupName = "legacy-group",
            ConsumeOrderly = true,
            InitialPosition = ConsumeFromPosition.Beginning,
            MaxConcurrency = 1,
            MaxCachedMessages = 8,
            LongPollingTimeout = TimeSpan.FromSeconds(1),
            MessageHandler = static (_, _) => ValueTask.FromResult(ConsumeResult.Success)
        };
        options.Subscribe("orders");
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
            NullLogger<RemotingPushConsumer>.Instance);

        await consumer.StartAsync(cancellationToken);
        await remoting.WaitForTopicPullsAsync("orders", 2, cancellationToken);
        await consumer.StopAsync(cancellationToken);

        Assert.True(Volatile.Read(ref orderLockCalls) > 1);
    }

    [Fact]
    public async Task OrderlyConsumer_SerializesQueueMessagesWithoutMessageGroup()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var firstStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
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
        var options = new RemotingPushConsumerOptions
        {
            GroupName = "legacy-group",
            ConsumeOrderly = true,
            InitialPosition = ConsumeFromPosition.Beginning,
            MaxConcurrency = 2,
            MaxCachedMessages = 8,
            LongPollingTimeout = TimeSpan.FromSeconds(1),
            MessageHandler = async (message, token) =>
            {
                if (message.MessageId == "first")
                {
                    firstStarted.TrySetResult();
                    await releaseFirst.Task.WaitAsync(token);
                }
                else
                {
                    secondStarted.TrySetResult();
                }

                return ConsumeResult.Success;
            }
        };
        options.Subscribe("orders");
        await using var consumer = CreateRemotingPushConsumer(
            options,
            CreateRouteServiceMock().Object,
            remoting,
            "orderly-serial");

        await consumer.StartAsync(cancellationToken);
        await firstStarted.Task.WaitAsync(TimeSpan.FromSeconds(3), cancellationToken);
        await Task.Delay(100, cancellationToken);
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
    public async Task OrderlyConsumer_ReleasesQueueLockAfterUnsubscribeDrainsHandler()
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
        var options = new RemotingPushConsumerOptions
        {
            GroupName = "legacy-group",
            ConsumeOrderly = true,
            InitialPosition = ConsumeFromPosition.Beginning,
            MaxConcurrency = 1,
            MaxCachedMessages = 8,
            LongPollingTimeout = TimeSpan.FromSeconds(1),
            MessageHandler = async (message, _) =>
            {
                firstStarted.TrySetResult();
                await releaseFirst.Task;
                return ConsumeResult.Success;
            }
        };
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
    public async Task PullConsumer_WrapsNamespaceButReturnsLogicalQueues()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var options = new RemotingPullConsumerOptions { GroupName = "legacy-group" };
        options.Subscribe("orders");
        var clientOptions = new RemotingClientOptions { Namespace = "tenant-a" };
        var routeTopics = new ConcurrentQueue<string>();
        var routes = CreateRouteServiceMock(topics: routeTopics);
        var remoting = new FakeRemotingClient("client-a");
        await using var consumer = new RemotingPullConsumer(CreateRemotingConsumerEngine(
            options,
            options.BatchSize,
            options.MaxMessageBytes,
            options.LongPollingTimeout,
            Options.Create(clientOptions),
            routes.Object,
            remoting,
            TimeProvider.System));

        await consumer.StartAsync(cancellationToken);
        var queue = Assert.Single(await consumer.GetMessageQueuesAsync("orders", cancellationToken));
        var committed = await consumer.GetOffsetAsync(queue, cancellationToken);
        var end = await consumer.QueryOffsetAsync(queue, QueryOffsetPolicy.End, cancellationToken: cancellationToken);
        await consumer.UpdateOffsetAsync(queue, end, cancellationToken);
        await consumer.StopAsync(cancellationToken);

        Assert.Equal("orders", queue.Topic);
        Assert.Equal(-1, committed);
        Assert.Equal(17, end);
        Assert.Equal(["tenant-a%orders"], routeTopics);
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

    private static byte[] CreateMessageRecord(
        string topic,
        string messageId,
        string? messageGroup,
        long queueOffset,
        long commitLogOffset)
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
        WriteInt32(stream, 0);
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

    private static RemotingPushConsumerOptions CreateBroadcastOptions(
        string offsetPath,
        Func<RemotingMessageView, CancellationToken, ValueTask<ConsumeResult>> handler)
    {
        var options = new RemotingPushConsumerOptions
        {
            GroupName = "broadcast-group",
            ConsumerMode = ConsumerMode.Broadcasting,
            LocalOffsetStorePath = offsetPath,
            InitialPosition = ConsumeFromPosition.Beginning,
            MaxConcurrency = 2,
            MaxCachedMessages = 8,
            LongPollingTimeout = TimeSpan.FromSeconds(1),
            MessageHandler = handler
        };
        options.Subscribe("orders");
        return options;
    }

    private static RemotingPushConsumerOptions CreateClusteringOptions() => new()
    {
        GroupName = "legacy-group",
        MaxConcurrency = 2,
        MaxCachedMessages = 8,
        LongPollingTimeout = TimeSpan.FromSeconds(1),
        MessageHandler = static (_, _) => ValueTask.FromResult(ConsumeResult.Success)
    };

    private static RemotingPushConsumer CreateRemotingPushConsumer(
        RemotingPushConsumerOptions options,
        ITopicRouteService routes,
        IRemotingClient remoting,
        string instanceName,
        IRemotingRocketMQTelemetry? telemetry = null) => CreateRemotingPushConsumer(
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
            telemetry);

    private static RemotingPushConsumer CreateRemotingPushConsumer(
        IOptions<RemotingPushConsumerOptions> options,
        IOptions<RemotingClientOptions> clientOptions,
        ITopicRouteService routes,
        IRemotingClient remoting,
        TimeProvider timeProvider,
        ILogger<RemotingPushConsumer> logger,
        IRemotingRocketMQTelemetry? telemetry = null)
    {
        var consumerEngine = CreateRemotingConsumerEngine(
            options.Value,
            options.Value.BatchSize,
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
            timeProvider,
            logger,
            telemetry: telemetry);
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
        public Func<RemotingCommand, CancellationToken, Task<RemotingCommand>>? LockHandler { get; set; }
        public Func<RemotingCommand, CancellationToken, Task<RemotingCommand>>? PullHandler { get; init; }
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
                    return Success();
                case RequestCode.GetConsumerListByGroup:
                    return Success(JsonSerializer.SerializeToUtf8Bytes(new
                    {
                        consumerIdList = Volatile.Read(ref _consumerIds)
                    }));
                case RequestCode.LockBatchMq:
                    return LockHandler is null
                        ? LockSuccess(request)
                        : await LockHandler(request, cancellationToken);
                case RequestCode.QueryConsumerOffset:
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
                    return Success(offset: 17);
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
                case RequestCode.UnregisterClient:
                    Interlocked.Increment(ref UnregisterCalls);
                    return Success();
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
