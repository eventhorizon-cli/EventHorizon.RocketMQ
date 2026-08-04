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
using System.Text;
using System.Text.Json;
using EventHorizon.RocketMQ.Remoting.Consumer;
using EventHorizon.RocketMQ.Remoting.Consumer.Push;
using EventHorizon.RocketMQ.Remoting.Consumer.Push.Pull.Offset;
using EventHorizon.RocketMQ.Remoting.Protocol;
using EventHorizon.RocketMQ.Remoting.Protocol.Route;
using EventHorizon.RocketMQ.Remoting.Tests.Consumer.Push.Support;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Moq;
using Xunit;

namespace EventHorizon.RocketMQ.Remoting.Tests.Consumer.Push.Pull.Offset;

public sealed class RemotingPushConsumerOffsetTests : RemotingPushConsumerTestSupport
{
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
                new RemotingConsumerQueue("orders", "broker-a", 0),
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
                var queue = new RemotingConsumerQueue("orders", "broker-a", 0);
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
}
