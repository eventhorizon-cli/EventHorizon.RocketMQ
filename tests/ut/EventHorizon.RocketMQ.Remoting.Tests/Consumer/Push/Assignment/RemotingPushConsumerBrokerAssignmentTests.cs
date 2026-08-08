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
using System.Diagnostics;
using System.Text;
using System.Text.Json;
using EventHorizon.RocketMQ.Remoting.Consumer;
using EventHorizon.RocketMQ.Remoting.Consumer.Coordination.Rebalance;
using EventHorizon.RocketMQ.Remoting.Consumer.Push;
using EventHorizon.RocketMQ.Remoting.Consumer.Push.Assignment;
using EventHorizon.RocketMQ.Remoting.Protocol;
using EventHorizon.RocketMQ.Remoting.Protocol.Route;
using EventHorizon.RocketMQ.Remoting.Tests.Consumer.Coordination.Rebalance;
using EventHorizon.RocketMQ.Remoting.Tests.Consumer.Push.Support;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Moq;
using Xunit;

namespace EventHorizon.RocketMQ.Remoting.Tests.Consumer.Push.Assignment;

public sealed class RemotingPushConsumerBrokerAssignmentTests : RemotingPushConsumerTestSupport
{
    private const string BrokerAssignmentGroup = "broker-assignment-group";
    private const string BrokerAssignmentRetryTopic = "%RETRY%broker-assignment-group";
    private const string BrokerAssignmentInstance = "broker-assignment-test";
    private const string BrokerAssignmentClientId = "127.0.0.1@broker-assignment-test";
    private const long PopTimeMilliseconds = 1_700_000_000_000;

    [Fact]
    public async Task BrokerAssignment_ReconciliationStarts_HeartbeatsBeforeQueryWithoutSettingRequestMode()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var remoting = new FakeRemotingClient(BrokerAssignmentClientId)
        {
            QueryAssignmentHandler = static (request, _) => Task.FromResult(
                AssignmentQueryTopic(request) == BrokerAssignmentRetryTopic
                    ? AssignmentResponse(BrokerAssignmentRetryTopic, 0, "PULL")
                    : EmptyAssignmentResponse())
        };
        var options = CreateBrokerAssignmentOptions(
            static (_, _, _) => ValueTask.FromResult(ConsumeResult.Success));
        await using var consumer = CreateBrokerAssignmentConsumer(options, remoting);

        await consumer.StartAsync(cancellationToken);

        var requests = remoting.Requests.ToArray();
        var heartbeatIndex = Array.FindIndex(requests, static request => request.Code == RequestCode.HeartBeat);
        var queryIndex = Array.FindIndex(requests, static request => request.Code == RequestCode.QueryAssignment);
        Assert.True(heartbeatIndex >= 0, "Broker assignment must advertise membership before querying assignments.");
        Assert.True(queryIndex > heartbeatIndex, "QUERY_ASSIGNMENT must follow the initial heartbeat.");
        Assert.Equal(400, requests[queryIndex].Code);
        Assert.DoesNotContain(requests, static request => request.Code == RequestCode.SetMessageRequestMode);
        Assert.DoesNotContain(requests, static request => request.Code == RequestCode.GetConsumerListByGroup);
        Assert.Contains(requests, static request =>
            request.Code == RequestCode.QueryAssignment &&
            AssignmentQueryTopic(request) == BrokerAssignmentRetryTopic);
        await remoting.WaitForPullsAsync(1, cancellationToken);
        Assert.Contains(consumer.ReceiveAssignments, static assignment =>
            assignment.Topic == BrokerAssignmentRetryTopic &&
            assignment.Mode == RemotingPushReceiveMode.Pull);

        using var heartbeat = JsonDocument.Parse(remoting.HeartbeatBodies.First());
        var consumerData = Assert.Single(
            heartbeat.RootElement.GetProperty("consumerDataSet").EnumerateArray());
        Assert.Equal("CONSUME_PASSIVELY", consumerData.GetProperty("consumeType").GetString());
        var subscriptionTopics = consumerData.GetProperty("subscriptionDataSet")
            .EnumerateArray()
            .Select(static subscription => subscription.GetProperty("topic").GetString() ??
                throw new InvalidDataException("A heartbeat subscription did not contain a topic."))
            .Order(StringComparer.Ordinal)
            .ToArray();
        Assert.Equal([BrokerAssignmentRetryTopic, "orders"], subscriptionTopics);
    }

    [Fact]
    public async Task BrokerAssignmentRequest_BroadcastingConsumer_UsesClientPull()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var offsetPath = Path.Combine(
            Path.GetTempPath(),
            $"rocketmq-broker-request-broadcast-{Guid.NewGuid():N}.json");
        try
        {
            var remoting = new FakeRemotingClient("127.0.0.1@broker-request-broadcast");
            var options = CreateBroadcastOptions(
                offsetPath,
                static (_, _, _) => ValueTask.FromResult(ConsumeResult.Success));
            options.QueueAssignmentMode = RemotingPushQueueAssignmentMode.Broker;
            await using var consumer = CreateRemotingPushConsumer(
                options,
                CreateRouteServiceMock().Object,
                remoting,
                "broker-request-broadcast");

            await consumer.StartAsync(cancellationToken);

            Assert.DoesNotContain(remoting.Requests, static request => request.Code == RequestCode.QueryAssignment);
            await remoting.WaitForPullsAsync(1, cancellationToken);
            Assert.All(consumer.ReceiveAssignments, static assignment =>
                Assert.Equal(RemotingPushReceiveMode.Pull, assignment.Mode));
        }
        finally
        {
            File.Delete(offsetPath);
        }
    }

    [Fact]
    public async Task BrokerAssignmentRequest_OrderlyConsumer_UsesClientPull()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var remoting = new FakeRemotingClient("127.0.0.1@broker-request-orderly");
        var options = CreateClusteringOptions();
        options.QueueAssignmentMode = RemotingPushQueueAssignmentMode.Broker;
        options.ConsumeOrderly = true;
        options.Subscribe("orders");
        await using var consumer = CreateRemotingPushConsumer(
            options,
            CreateRouteServiceMock().Object,
            remoting,
            "broker-request-orderly");

        await consumer.StartAsync(cancellationToken);

        Assert.DoesNotContain(remoting.Requests, static request => request.Code == RequestCode.QueryAssignment);
        Assert.Contains(remoting.Requests, static request => request.Code == RequestCode.GetConsumerListByGroup);
        await remoting.WaitForPullsAsync(1, cancellationToken);
        Assert.All(consumer.ReceiveAssignments, static assignment =>
            Assert.Equal(RemotingPushReceiveMode.Pull, assignment.Mode));
    }

    [Fact]
    public async Task BrokerAssignment_NullThenEmptyResponse_PreservesThenRevokesCurrentReceiver()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var orderResponses = new ConcurrentQueue<RemotingCommand>(
        [
            AssignmentResponse("orders", 0, "PULL"),
            NullAssignmentResponse(),
            EmptyAssignmentResponse()
        ]);
        var remoting = new FakeRemotingClient(BrokerAssignmentClientId)
        {
            QueryAssignmentHandler = (request, _) => Task.FromResult(
                AssignmentQueryTopic(request) == "orders" && orderResponses.TryDequeue(out var response)
                    ? response
                    : EmptyAssignmentResponse())
        };
        var options = CreateBrokerAssignmentOptions(
            static (_, _, _) => ValueTask.FromResult(ConsumeResult.Success));
        var rebalance = new TestRemotingRebalanceService();
        await using var consumer = CreateBrokerAssignmentConsumer(options, remoting, rebalance);

        await consumer.StartAsync(cancellationToken);
        Assert.Equal(1, CountAssignmentQueries(remoting, "orders"));
        await remoting.WaitForTopicPullsAsync("orders", 1, cancellationToken);
        var canceledBeforeNull = Volatile.Read(ref remoting.CanceledPulls);

        await rebalance.RebalanceAsync(BrokerAssignmentGroup, cancellationToken);

        Assert.Equal(2, CountAssignmentQueries(remoting, "orders"));
        await Task.Delay(50, cancellationToken);
        Assert.Equal(canceledBeforeNull, Volatile.Read(ref remoting.CanceledPulls));
        Assert.Contains(consumer.Assignment, static queue => queue.Topic == "orders" && queue.QueueId == 0);

        await rebalance.RebalanceAsync(BrokerAssignmentGroup, cancellationToken);

        Assert.Equal(3, CountAssignmentQueries(remoting, "orders"));
        await remoting.WaitForCanceledPullsAsync(canceledBeforeNull + 1, cancellationToken);
        Assert.DoesNotContain(consumer.Assignment, static queue => queue.Topic == "orders");
        Assert.DoesNotContain(
            remoting.Requests,
            static request => request.Code == RequestCode.SetMessageRequestMode);
    }

    [Fact]
    public async Task BrokerAssignment_PullChangesToPop_DrainsPullBeforeStartingReplacement()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var orderResponses = new ConcurrentQueue<RemotingCommand>(
        [
            AssignmentResponse("orders", 0, "PULL"),
            AssignmentResponse("orders", 0, "POP")
        ]);
        var remoting = new FakeRemotingClient(BrokerAssignmentClientId)
        {
            QueryAssignmentHandler = (request, _) => Task.FromResult(
                AssignmentQueryTopic(request) == "orders" && orderResponses.TryDequeue(out var response)
                    ? response
                    : EmptyAssignmentResponse()),
            PopHandler = static (_, token) => WaitForCanceledBrokerRequestAsync(token)
        };
        var options = CreateBrokerAssignmentOptions(
            static (_, _, _) => ValueTask.FromResult(ConsumeResult.Success));
        var rebalance = new TestRemotingRebalanceService();
        await using var consumer = CreateBrokerAssignmentConsumer(options, remoting, rebalance);

        await consumer.StartAsync(cancellationToken);
        Assert.Equal(1, CountAssignmentQueries(remoting, "orders"));
        await remoting.WaitForTopicPullsAsync("orders", 1, cancellationToken);

        await rebalance.RebalanceAsync(BrokerAssignmentGroup, cancellationToken);

        Assert.Equal(2, CountAssignmentQueries(remoting, "orders"));
        var pop = await WaitForBrokerRequestAsync(remoting, RequestCode.PopMessage, cancellationToken);
        Assert.Equal(200050, pop.Code);
        Assert.Equal(0, Convert.ToInt32(pop.ExtFields["queueId"]));
        Assert.Contains("orders", remoting.CanceledPullTopics);
        Assert.DoesNotContain(
            remoting.Requests,
            static request => request.Code == RequestCode.SetMessageRequestMode);
    }

    [Fact]
    public async Task BrokerAssignedPush_MixedPullAndPopDeliveries_UsesConsumerWideHandlerConcurrencyLimit()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var firstHandlerStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var popResponseReturned = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var secondHandlerStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseFirstHandler = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var pullCount = 0;
        var popCount = 0;
        var handlerCount = 0;
        var remoting = new FakeRemotingClient(BrokerAssignmentClientId)
        {
            QueryAssignmentHandler = static (request, _) => Task.FromResult(
                AssignmentQueryTopic(request) == "orders"
                    ? MixedAssignmentResponse()
                    : EmptyAssignmentResponse()),
            PullHandler = async (_, token) =>
            {
                if (Interlocked.Increment(ref pullCount) != 1)
                {
                    return await WaitForCanceledBrokerRequestAsync(token);
                }

                return PullSuccess(
                    CreateMessageRecord("orders", "pull-message", null, 0, 100, queueId: 0),
                    nextOffset: 1);
            },
            PopHandler = async (_, token) =>
            {
                if (Interlocked.Increment(ref popCount) != 1)
                {
                    return await WaitForCanceledBrokerRequestAsync(token);
                }

                await firstHandlerStarted.Task.WaitAsync(token);
                popResponseReturned.TrySetResult();
                return CreatePopSuccessResponse(invisibleTimeMilliseconds: 5_000);
            },
            AckHandler = static (_, _) => Task.FromResult(BrokerSuccess())
        };
        var options = CreateBrokerAssignmentOptions(async (messages, _, token) =>
        {
            if (Interlocked.Increment(ref handlerCount) == 1)
            {
                Assert.Equal("pull-message", Assert.Single(messages).MessageId);
                firstHandlerStarted.TrySetResult();
                await releaseFirstHandler.Task.WaitAsync(token);
            }
            else
            {
                secondHandlerStarted.TrySetResult();
            }

            return ConsumeResult.Success;
        });
        await using var consumer = CreateBrokerAssignmentConsumer(options, remoting);

        await consumer.StartAsync(cancellationToken);
        try
        {
            await firstHandlerStarted.Task.WaitAsync(TimeSpan.FromSeconds(3), cancellationToken);
            await popResponseReturned.Task.WaitAsync(TimeSpan.FromSeconds(3), cancellationToken);

            await Assert.ThrowsAsync<TimeoutException>(() =>
                secondHandlerStarted.Task.WaitAsync(TimeSpan.FromMilliseconds(200), cancellationToken));

            releaseFirstHandler.TrySetResult();
            await secondHandlerStarted.Task.WaitAsync(TimeSpan.FromSeconds(3), cancellationToken);
        }
        finally
        {
            releaseFirstHandler.TrySetResult();
        }
    }

    [Fact]
    public async Task BrokerAssignment_QueryFails_DoesNotFallBackToClientAllocation()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var remoting = new FakeRemotingClient(BrokerAssignmentClientId)
        {
            QueryAssignmentHandler = (request, _) => Task.FromResult(
                AssignmentQueryTopic(request) == "orders"
                    ? new RemotingCommand
                    {
                        Code = ResponseCodes.ResError,
                        Remark = "QUERY_ASSIGNMENT is unavailable."
                    }
                    : EmptyAssignmentResponse())
        };
        var options = CreateBrokerAssignmentOptions(
            static (_, _, _) => ValueTask.FromResult(ConsumeResult.Success));
        await using var consumer = CreateBrokerAssignmentConsumer(options, remoting);

        await consumer.StartAsync(cancellationToken);

        Assert.Contains(remoting.Requests, static request => request.Code == RequestCode.QueryAssignment);
        Assert.DoesNotContain(remoting.Requests, static request => request.Code == RequestCode.GetConsumerListByGroup);
        Assert.DoesNotContain(remoting.Requests, static request => request.Code == RequestCode.PullMessage);
        Assert.DoesNotContain(remoting.Requests, static request => request.Code == RequestCode.PopMessage);
        Assert.DoesNotContain(
            remoting.Requests,
            static request => request.Code == RequestCode.SetMessageRequestMode);
    }

    [Fact]
    public async Task BrokerAssignment_RetryRouteMissingAfterSendBack_RemainsUnbalanced()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var routes = new Mock<ITopicRouteService>(MockBehavior.Strict);
        routes
            .Setup(value => value.GetAsync(
                It.IsAny<string>(),
                It.IsAny<bool>(),
                It.IsAny<CancellationToken>()))
            .Returns<string, bool, CancellationToken>((topic, _, _) =>
                string.Equals(topic, BrokerAssignmentRetryTopic, StringComparison.Ordinal)
                    ? Task.FromException<TopicRouteData>(
                        new IOException("The retry topic route is not available yet."))
                    : Task.FromResult(Route(1)));
        var orderPulls = 0;
        var remoting = new FakeRemotingClient(BrokerAssignmentClientId)
        {
            QueryAssignmentHandler = static (request, _) => Task.FromResult(
                AssignmentQueryTopic(request) == "orders"
                    ? AssignmentResponse("orders", 0, "PULL")
                    : EmptyAssignmentResponse()),
            PullHandler = (request, token) =>
                Assert.IsType<string>(request.ExtFields["topic"]) == "orders" &&
                Interlocked.Increment(ref orderPulls) == 1
                    ? Task.FromResult(PullSuccess(
                        CreateMessageRecord("orders", "retry-route", null, 0, 1_000),
                        nextOffset: 1))
                    : WaitForCanceledBrokerRequestAsync(token)
        };
        var options = CreateBrokerAssignmentOptions(
            static (_, _, _) => ValueTask.FromResult(ConsumeResult.Retry));
        var rebalance = new TestRemotingRebalanceService();
        await using var consumer = CreateBrokerAssignmentConsumer(
            options,
            remoting,
            rebalance,
            routes.Object);

        await consumer.StartAsync(cancellationToken);
        await remoting.WaitForRequestCountAsync(
            RequestCode.ConsumerSendMsgBack,
            1,
            cancellationToken);
        while (!remoting.UpdatedOffsets.Any(static update =>
                   update.Topic == BrokerAssignmentRetryTopic && update.Offset == 17))
        {
            await Task.Delay(10, cancellationToken);
        }

        Assert.False(await rebalance.RebalanceAsync(BrokerAssignmentGroup, cancellationToken));
    }

    [Fact]
    public async Task BrokerAssignedPop_HandlerSucceeds_AcknowledgesPhysicalReceiptQueue()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var handled = new TaskCompletionSource<IReadOnlyList<RemotingMessageView>>(
            TaskCreationOptions.RunContinuationsAsynchronously);
        var remoting = new FakeRemotingClient(BrokerAssignmentClientId)
        {
            QueryAssignmentHandler = static (request, _) => Task.FromResult(
                AssignmentQueryTopic(request) == "orders"
                    ? AssignmentResponse("orders", -1, "POP")
                    : EmptyAssignmentResponse()),
            PopHandler = ReceiveOnePopThenWait(CreatePopSuccessResponse(invisibleTimeMilliseconds: 5_000)),
            AckHandler = static (_, _) => Task.FromResult(BrokerSuccess())
        };
        var options = CreateBrokerAssignmentOptions((messages, _, _) =>
        {
            handled.TrySetResult(messages);
            return ValueTask.FromResult(ConsumeResult.Success);
        });
        await using var consumer = CreateBrokerAssignmentConsumer(options, remoting);

        await consumer.StartAsync(cancellationToken);
        Assert.Equal(1, CountAssignmentQueries(remoting, "orders"));
        Assert.Contains(consumer.ReceiveAssignments, static assignment =>
            assignment.Topic == "orders" &&
            assignment.QueueId == -1 &&
            assignment.Mode == RemotingPushReceiveMode.Pop);
        Assert.DoesNotContain(consumer.Assignment, static queue => queue.QueueId < 0);
        var messages = await handled.Task.WaitAsync(TimeSpan.FromSeconds(3), cancellationToken);
        var message = Assert.Single(messages);
        var pop = await WaitForBrokerRequestAsync(remoting, RequestCode.PopMessage, cancellationToken);
        var acknowledgement = await WaitForBrokerRequestAsync(remoting, RequestCode.AckMessage, cancellationToken);

        Assert.Equal(3, message.QueueId);
        Assert.Equal(200050, pop.Code);
        Assert.Equal(-1, Convert.ToInt32(pop.ExtFields["queueId"]));
        Assert.Equal(200051, acknowledgement.Code);
        Assert.Equal(3, Convert.ToInt32(acknowledgement.ExtFields["queueId"]));
        Assert.Equal(42L, Convert.ToInt64(acknowledgement.ExtFields["offset"]));
        Assert.Equal(
            "40 1700000000000 5000 2 0 broker-a 3 42",
            acknowledgement.ExtFields["extraInfo"]);
        Assert.DoesNotContain(
            remoting.Requests,
            static request => request.Code == RequestCode.SetMessageRequestMode);
    }

    [Fact]
    public async Task BrokerAssignedPop_HandlerUsesDefaultRetryDelay_ChangesInvisibleTimeFromDeliveryAttempt()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var remoting = new FakeRemotingClient(BrokerAssignmentClientId)
        {
            QueryAssignmentHandler = static (request, _) => Task.FromResult(
                AssignmentQueryTopic(request) == "orders"
                    ? AssignmentResponse("orders", -1, "POP")
                    : EmptyAssignmentResponse()),
            PopHandler = ReceiveOnePopThenWait(CreatePopSuccessResponse(invisibleTimeMilliseconds: 5_000)),
            ChangeInvisibleHandler = static (_, _) => Task.FromResult(
                ChangeInvisibleTimeResponse(PopTimeMilliseconds + 1_000, invisibleTimeMilliseconds: 5_000, reviveQueueId: 4))
        };
        var options = CreateBrokerAssignmentOptions(static (_, context, _) =>
        {
            Assert.Equal(0, context.DelayLevelWhenNextConsume);
            return ValueTask.FromResult(ConsumeResult.Retry);
        });
        await using var consumer = CreateBrokerAssignmentConsumer(options, remoting);

        await consumer.StartAsync(cancellationToken);
        Assert.Equal(1, CountAssignmentQueries(remoting, "orders"));
        var change = await WaitForBrokerRequestAsync(
            remoting,
            RequestCode.ChangeMessageInvisibleTime,
            cancellationToken);

        Assert.Equal(200053, change.Code);
        Assert.Equal(false, change.ExtFields["suspend"]);
        Assert.Equal(3, Convert.ToInt32(change.ExtFields["queueId"]));
        Assert.Equal(42L, Convert.ToInt64(change.ExtFields["offset"]));
        Assert.Equal(10_000L, Convert.ToInt64(change.ExtFields["invisibleTime"]));
        Assert.DoesNotContain(remoting.Requests, static request => request.Code == RequestCode.AckMessage);
        Assert.DoesNotContain(
            remoting.Requests,
            static request => request.Code == RequestCode.SetMessageRequestMode);
    }

    [Fact]
    public async Task BrokerAssignedPop_HandlerSelectsRetryDelayLevel_ChangesInvisibleTimeFromPopSchedule()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var remoting = new FakeRemotingClient(BrokerAssignmentClientId)
        {
            QueryAssignmentHandler = static (request, _) => Task.FromResult(
                AssignmentQueryTopic(request) == "orders"
                    ? AssignmentResponse("orders", -1, "POP")
                    : EmptyAssignmentResponse()),
            PopHandler = ReceiveOnePopThenWait(CreatePopSuccessResponse(invisibleTimeMilliseconds: 5_000)),
            ChangeInvisibleHandler = static (_, _) => Task.FromResult(
                ChangeInvisibleTimeResponse(PopTimeMilliseconds + 1_000, invisibleTimeMilliseconds: 60_000, reviveQueueId: 4))
        };
        var options = CreateBrokerAssignmentOptions(static (_, context, _) =>
        {
            context.DelayLevelWhenNextConsume = 2;
            return ValueTask.FromResult(ConsumeResult.Retry);
        });
        await using var consumer = CreateBrokerAssignmentConsumer(options, remoting);

        await consumer.StartAsync(cancellationToken);
        var change = await WaitForBrokerRequestAsync(
            remoting,
            RequestCode.ChangeMessageInvisibleTime,
            cancellationToken);

        Assert.Equal(60_000L, Convert.ToInt64(change.ExtFields["invisibleTime"]));
    }

    [Fact]
    public async Task BrokerAssignedPop_RetryAtDeliveryLimitBeforeAgeCutoff_ChangesInvisibleTimeFromMessageAge()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var body = CreateMessageRecord(
            "orders",
            "pop-message",
            null,
            42,
            100,
            queueId: 3,
            bornTimestampMilliseconds: PopTimeMilliseconds - 3_600_000,
            reconsumeTimes: 15);
        var remoting = new FakeRemotingClient(BrokerAssignmentClientId)
        {
            QueryAssignmentHandler = static (request, _) => Task.FromResult(
                AssignmentQueryTopic(request) == "orders"
                    ? AssignmentResponse("orders", -1, "POP")
                    : EmptyAssignmentResponse()),
            PopHandler = ReceiveOnePopThenWait(
                CreatePopSuccessResponse(body, invisibleTimeMilliseconds: 5_000, messageOffsets: "42")),
            ChangeInvisibleHandler = static (_, _) => Task.FromResult(
                ChangeInvisibleTimeResponse(PopTimeMilliseconds + 1_000, invisibleTimeMilliseconds: 7_200_000, reviveQueueId: 4)),
            AckHandler = static (_, _) => Task.FromResult(BrokerSuccess()),
            SendBackHandler = static (_, _) => Task.FromResult(BrokerSuccess())
        };
        var options = CreateBrokerAssignmentOptions(static (_, context, _) =>
        {
            context.DelayLevelWhenNextConsume = 2;
            return ValueTask.FromResult(ConsumeResult.Retry);
        });
        options.MaxDeliveryAttempts = 16;
        await using var consumer = CreateBrokerAssignmentConsumer(options, remoting);

        await consumer.StartAsync(cancellationToken);
        var change = await WaitForBrokerRequestAsync(
            remoting,
            RequestCode.ChangeMessageInvisibleTime,
            cancellationToken);

        Assert.Equal(7_200_000L, Convert.ToInt64(change.ExtFields["invisibleTime"]));
        Assert.DoesNotContain(remoting.Requests, static request =>
            request.Code is RequestCode.ConsumerSendMsgBack or RequestCode.AckMessage);
    }

    [Fact]
    public async Task BrokerAssignedPop_RetryAtDeliveryLimitAfterAgeCutoff_AcknowledgesWithoutDeadLetterForwarding()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var body = CreateMessageRecord(
            "orders",
            "pop-message",
            null,
            42,
            100,
            queueId: 3,
            bornTimestampMilliseconds: PopTimeMilliseconds - 14_400_001,
            reconsumeTimes: 15);
        var remoting = new FakeRemotingClient(BrokerAssignmentClientId)
        {
            QueryAssignmentHandler = static (request, _) => Task.FromResult(
                AssignmentQueryTopic(request) == "orders"
                    ? AssignmentResponse("orders", -1, "POP")
                    : EmptyAssignmentResponse()),
            PopHandler = ReceiveOnePopThenWait(
                CreatePopSuccessResponse(body, invisibleTimeMilliseconds: 5_000, messageOffsets: "42")),
            ChangeInvisibleHandler = static (_, _) => Task.FromResult(
                ChangeInvisibleTimeResponse(PopTimeMilliseconds + 1_000, invisibleTimeMilliseconds: 7_200_000, reviveQueueId: 4)),
            AckHandler = static (_, _) => Task.FromResult(BrokerSuccess()),
            SendBackHandler = static (_, _) => Task.FromResult(BrokerSuccess())
        };
        var options = CreateBrokerAssignmentOptions(
            static (_, _, _) => ValueTask.FromResult(ConsumeResult.Retry));
        options.MaxDeliveryAttempts = 16;
        await using var consumer = CreateBrokerAssignmentConsumer(options, remoting);

        await consumer.StartAsync(cancellationToken);
        await WaitForBrokerRequestAsync(remoting, RequestCode.AckMessage, cancellationToken);

        Assert.DoesNotContain(remoting.Requests, static request =>
            request.Code is RequestCode.ConsumerSendMsgBack or RequestCode.ChangeMessageInvisibleTime);
    }

    [Fact]
    public async Task BrokerAssignedPop_HandlerSelectsNegativeRetryDelay_ForwardsToDeadLetterWithoutChangingInvisibleTime()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var remoting = new FakeRemotingClient(BrokerAssignmentClientId)
        {
            QueryAssignmentHandler = static (request, _) => Task.FromResult(
                AssignmentQueryTopic(request) == "orders"
                    ? AssignmentResponse("orders", -1, "POP")
                    : EmptyAssignmentResponse()),
            PopHandler = ReceiveOnePopThenWait(CreatePopSuccessResponse(invisibleTimeMilliseconds: 5_000)),
            SendBackHandler = static (_, _) => Task.FromResult(BrokerSuccess()),
            AckHandler = static (_, _) => Task.FromResult(BrokerSuccess())
        };
        var options = CreateBrokerAssignmentOptions(static (_, context, _) =>
        {
            context.DelayLevelWhenNextConsume = -1;
            return ValueTask.FromResult(ConsumeResult.Retry);
        });
        await using var consumer = CreateBrokerAssignmentConsumer(options, remoting);

        await consumer.StartAsync(cancellationToken);
        await WaitForBrokerRequestAsync(remoting, RequestCode.AckMessage, cancellationToken);

        Assert.Equal(
            [RequestCode.ConsumerSendMsgBack, RequestCode.AckMessage],
            remoting.Requests
                .Where(static request =>
                    request.Code is RequestCode.ConsumerSendMsgBack or RequestCode.AckMessage)
                .Select(static request => request.Code)
                .ToArray());
        Assert.DoesNotContain(
            remoting.Requests,
            static request => request.Code == RequestCode.ChangeMessageInvisibleTime);
    }

    [Fact]
    public async Task BrokerAssignedPop_HandlerExceedsInvisibleDuration_DoesNotRenewOrSettleLateResult()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var handlerStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseHandler = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var handlerReturned = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var remoting = new FakeRemotingClient(BrokerAssignmentClientId)
        {
            QueryAssignmentHandler = static (request, _) => Task.FromResult(
                AssignmentQueryTopic(request) == "orders"
                    ? AssignmentResponse("orders", -1, "POP")
                    : EmptyAssignmentResponse()),
            PopHandler = ReceiveOnePopThenWait(CreatePopSuccessResponse(invisibleTimeMilliseconds: 500)),
            AckHandler = static (_, _) => Task.FromResult(BrokerSuccess())
        };
        var options = CreateBrokerAssignmentOptions(async (_, _, _) =>
        {
            handlerStarted.TrySetResult();
            await releaseHandler.Task;
            handlerReturned.TrySetResult();
            return ConsumeResult.Success;
        });
        await using var consumer = CreateBrokerAssignmentConsumer(options, remoting);

        await consumer.StartAsync(cancellationToken);
        try
        {
            Assert.Equal(1, CountAssignmentQueries(remoting, "orders"));
            await handlerStarted.Task.WaitAsync(TimeSpan.FromSeconds(3), cancellationToken);
            await Task.Delay(TimeSpan.FromMilliseconds(600), cancellationToken);
            releaseHandler.TrySetResult();
            await handlerReturned.Task.WaitAsync(TimeSpan.FromSeconds(1), cancellationToken);
            await Task.Delay(TimeSpan.FromMilliseconds(100), cancellationToken);

            Assert.DoesNotContain(
                remoting.Requests,
                static request => request.Code == RequestCode.ChangeMessageInvisibleTime);
            Assert.DoesNotContain(remoting.Requests, static request => request.Code == RequestCode.AckMessage);
        }
        finally
        {
            releaseHandler.TrySetResult();
        }
    }

    [Fact]
    public async Task BrokerAssignedPop_MessageWaitsPastReceiptExpiryBehindConcurrencyGate_DoesNotInvokeOrSettle()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var firstHandlerStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseFirstHandler = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var secondHandlerInvoked = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var handledMessageIds = new ConcurrentQueue<string>();
        var body = CreateMessageRecord("orders", "pop-first", null, 42, 100, queueId: 3)
            .Concat(CreateMessageRecord("orders", "pop-second", null, 43, 101, queueId: 3))
            .ToArray();
        var remoting = new FakeRemotingClient(BrokerAssignmentClientId)
        {
            QueryAssignmentHandler = static (request, _) => Task.FromResult(
                AssignmentQueryTopic(request) == "orders"
                    ? AssignmentResponse("orders", -1, "POP")
                    : EmptyAssignmentResponse()),
            PopHandler = ReceiveOnePopThenWait(
                CreatePopSuccessResponse(body, invisibleTimeMilliseconds: 3_600_000, messageOffsets: "42,43")),
            AckHandler = static (_, _) => Task.FromResult(BrokerSuccess()),
            ChangeInvisibleHandler = static (_, _) => Task.FromResult(
                ChangeInvisibleTimeResponse(PopTimeMilliseconds + 1_000, invisibleTimeMilliseconds: 5_000, reviveQueueId: 4)),
            SendBackHandler = static (_, _) => Task.FromResult(BrokerSuccess())
        };
        var options = CreateBrokerAssignmentOptions(async (messages, _, _) =>
        {
            var messageId = Assert.Single(messages).MessageId;
            handledMessageIds.Enqueue(messageId);
            if (messageId == "pop-first")
            {
                firstHandlerStarted.TrySetResult();
                await releaseFirstHandler.Task;
            }
            else
            {
                secondHandlerInvoked.TrySetResult();
            }

            return ConsumeResult.Success;
        });
        options.PopBatchSize = 2;
        options.PopMaxInflightMessagesPerAssignment = 2;
        options.ConsumeMessageBatchSize = 1;
        options.MaxConcurrency = 1;
        var timeProvider = new BrokerAssignmentTimeProvider(
            DateTimeOffset.FromUnixTimeMilliseconds(PopTimeMilliseconds));
        await using var consumer = CreateBrokerAssignmentConsumer(
            options,
            remoting,
            timeProvider: timeProvider);

        await consumer.StartAsync(cancellationToken);
        try
        {
            await firstHandlerStarted.Task.WaitAsync(TimeSpan.FromSeconds(3), cancellationToken);
            timeProvider.Advance(TimeSpan.FromHours(2));
            releaseFirstHandler.TrySetResult();

            await Assert.ThrowsAsync<TimeoutException>(() =>
                secondHandlerInvoked.Task.WaitAsync(TimeSpan.FromMilliseconds(500), cancellationToken));
            Assert.Equal(["pop-first"], handledMessageIds.ToArray());
            Assert.DoesNotContain(
                remoting.Requests,
                static request => request.Code is RequestCode.AckMessage
                    or RequestCode.ChangeMessageInvisibleTime
                    or RequestCode.ConsumerSendMsgBack);
        }
        finally
        {
            releaseFirstHandler.TrySetResult();
        }
    }

    [Fact]
    public async Task BrokerAssignedPop_RetryChangeInvisibleResponseIsUncertain_DoesNotRetryOldReceipt()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var firstChangeAttempt = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var secondChangeAttempt = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var changeAttempts = 0;
        var remoting = new FakeRemotingClient(BrokerAssignmentClientId)
        {
            QueryAssignmentHandler = static (request, _) => Task.FromResult(
                AssignmentQueryTopic(request) == "orders"
                    ? AssignmentResponse("orders", -1, "POP")
                    : EmptyAssignmentResponse()),
            PopHandler = ReceiveOnePopThenWait(CreatePopSuccessResponse(invisibleTimeMilliseconds: 5_000)),
            ChangeInvisibleHandler = (_, _) =>
            {
                if (Interlocked.Increment(ref changeAttempts) == 1)
                {
                    firstChangeAttempt.TrySetResult();
                    return Task.FromException<RemotingCommand>(
                        new IOException("The ChangeInvisibleTime response was lost."));
                }

                secondChangeAttempt.TrySetResult();
                return Task.FromResult(
                    ChangeInvisibleTimeResponse(PopTimeMilliseconds + 1_000, invisibleTimeMilliseconds: 5_000, reviveQueueId: 4));
            }
        };
        var options = CreateBrokerAssignmentOptions(
            static (_, _, _) => ValueTask.FromResult(ConsumeResult.Retry));
        await using var consumer = CreateBrokerAssignmentConsumer(options, remoting);

        await consumer.StartAsync(cancellationToken);

        await firstChangeAttempt.Task.WaitAsync(TimeSpan.FromSeconds(3), cancellationToken);
        await Assert.ThrowsAsync<TimeoutException>(() =>
            secondChangeAttempt.Task.WaitAsync(TimeSpan.FromMilliseconds(500), cancellationToken));
        Assert.Equal(1, Volatile.Read(ref changeAttempts));
    }

    [Fact]
    public async Task BrokerAssignedPop_AcknowledgementFailsBeforeLeaseExpiry_RetriesLatestReceipt()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var acknowledgementAttempts = 0;
        var acknowledgementRetried = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var remoting = new FakeRemotingClient(BrokerAssignmentClientId)
        {
            QueryAssignmentHandler = static (request, _) => Task.FromResult(
                AssignmentQueryTopic(request) == "orders"
                    ? AssignmentResponse("orders", -1, "POP")
                    : EmptyAssignmentResponse()),
            PopHandler = ReceiveOnePopThenWait(CreatePopSuccessResponse(invisibleTimeMilliseconds: 2_000)),
            AckHandler = (_, _) =>
            {
                if (Interlocked.Increment(ref acknowledgementAttempts) == 1)
                {
                    return Task.FromException<RemotingCommand>(
                        new InvalidOperationException("acknowledgement failed"));
                }

                acknowledgementRetried.TrySetResult();
                return Task.FromResult(BrokerSuccess());
            }
        };
        var options = CreateBrokerAssignmentOptions(
            static (_, _, _) => ValueTask.FromResult(ConsumeResult.Success));
        await using var consumer = CreateBrokerAssignmentConsumer(options, remoting);

        await consumer.StartAsync(cancellationToken);

        await acknowledgementRetried.Task.WaitAsync(TimeSpan.FromSeconds(1), cancellationToken);
        Assert.Equal(2, acknowledgementAttempts);
        Assert.Equal(
            2,
            remoting.Requests.Count(static request => request.Code == RequestCode.AckMessage));
    }

    [Fact]
    public async Task BrokerAssignedPop_FirstSettlementKeepsFailing_ProcessesNextDeliveryWithoutLeaseDelay()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var secondAcknowledgement = new TaskCompletionSource<RemotingCommand>(
            TaskCreationOptions.RunContinuationsAsynchronously);
        var body = CreateMessageRecord("orders", "first", null, 42, 100, queueId: 3)
            .Concat(CreateMessageRecord("orders", "second", null, 43, 101, queueId: 3))
            .ToArray();
        var remoting = new FakeRemotingClient(BrokerAssignmentClientId)
        {
            QueryAssignmentHandler = static (request, _) => Task.FromResult(
                AssignmentQueryTopic(request) == "orders"
                    ? AssignmentResponse("orders", -1, "POP")
                    : EmptyAssignmentResponse()),
            PopHandler = ReceiveOnePopThenWait(
                CreatePopSuccessResponse(body, invisibleTimeMilliseconds: 2_000, messageOffsets: "42,43")),
            AckHandler = (request, _) =>
            {
                if (Convert.ToInt64(request.ExtFields["offset"]) == 42)
                {
                    return Task.FromException<RemotingCommand>(
                        new InvalidOperationException("first acknowledgement keeps failing"));
                }

                secondAcknowledgement.TrySetResult(request);
                return Task.FromResult(BrokerSuccess());
            }
        };
        var options = CreateBrokerAssignmentOptions(
            static (_, _, _) => ValueTask.FromResult(ConsumeResult.Success));
        options.PopBatchSize = 2;
        options.ConsumeMessageBatchSize = 1;
        await using var consumer = CreateBrokerAssignmentConsumer(options, remoting);

        await consumer.StartAsync(cancellationToken);

        var acknowledgement = await secondAcknowledgement.Task.WaitAsync(
            TimeSpan.FromSeconds(1),
            cancellationToken);
        Assert.Equal(43L, Convert.ToInt64(acknowledgement.ExtFields["offset"]));
    }

    [Fact]
    public async Task BrokerAssignedPop_DeadLetterAckFails_RetriesAckWithoutForwardingAgain()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var acknowledgementAttempts = 0;
        var acknowledgementRetried = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var remoting = new FakeRemotingClient(BrokerAssignmentClientId)
        {
            QueryAssignmentHandler = static (request, _) => Task.FromResult(
                AssignmentQueryTopic(request) == "orders"
                    ? AssignmentResponse("orders", -1, "POP")
                    : EmptyAssignmentResponse()),
            PopHandler = ReceiveOnePopThenWait(CreatePopSuccessResponse(invisibleTimeMilliseconds: 2_000)),
            SendBackHandler = static (_, _) => Task.FromResult(BrokerSuccess()),
            AckHandler = (_, _) =>
            {
                if (Interlocked.Increment(ref acknowledgementAttempts) == 1)
                {
                    return Task.FromException<RemotingCommand>(
                        new InvalidOperationException("dead-letter acknowledgement failed"));
                }

                acknowledgementRetried.TrySetResult();
                return Task.FromResult(BrokerSuccess());
            }
        };
        var options = CreateBrokerAssignmentOptions(
            static (_, _, _) => ValueTask.FromResult(ConsumeResult.DeadLetter));
        await using var consumer = CreateBrokerAssignmentConsumer(options, remoting);

        await consumer.StartAsync(cancellationToken);
        await acknowledgementRetried.Task.WaitAsync(TimeSpan.FromSeconds(1), cancellationToken);

        Assert.Equal(
            [RequestCode.ConsumerSendMsgBack, RequestCode.AckMessage, RequestCode.AckMessage],
            remoting.Requests
                .Where(static request =>
                    request.Code is RequestCode.ConsumerSendMsgBack or RequestCode.AckMessage)
                .Select(static request => request.Code)
                .ToArray());
    }

    private static RemotingPushConsumerOptions CreateBrokerAssignmentOptions(
        Func<
            IReadOnlyList<RemotingMessageView>,
            RemotingPushConsumeContext,
            CancellationToken,
            ValueTask<ConsumeResult>> handler)
    {
        var options = WithMessageHandler(new RemotingPushConsumerOptions
        {
            GroupName = BrokerAssignmentGroup,
            QueueAssignmentMode = RemotingPushQueueAssignmentMode.Broker,
            InitialPosition = ConsumeFromPosition.Beginning,
            MaxConcurrency = 1,
            PullBatchSize = 1,
            PullMaxCachedMessages = 8,
            PullMaxCachedMessageBytes = 1024,
            PopBatchSize = 1,
            PopInvisibleDuration = TimeSpan.FromSeconds(5),
            PopMaxInflightMessagesPerAssignment = 1,
            LongPollingTimeout = TimeSpan.FromSeconds(1),
            RetryDelay = TimeSpan.FromMilliseconds(10)
        }, handler);
        options.Subscribe("orders");
        return options;
    }

    private static RemotingClientOptions CreateBrokerAssignmentClientOptions() => new()
    {
        ClientIP = "127.0.0.1",
        InstanceName = BrokerAssignmentInstance,
        PollNameServerInterval = TimeSpan.FromHours(1),
        HeartbeatBrokerInterval = TimeSpan.FromHours(1),
        RequestTimeout = TimeSpan.FromSeconds(1)
    };

    private static RemotingPushConsumer CreateBrokerAssignmentConsumer(
        RemotingPushConsumerOptions options,
        FakeRemotingClient remoting,
        IRemotingRebalanceService? rebalance = null,
        ITopicRouteService? routes = null,
        TimeProvider? timeProvider = null) =>
        CreateRemotingPushConsumer(
            Options.Create(options),
            Options.Create(CreateBrokerAssignmentClientOptions()),
            routes ?? CreateRouteServiceMock(queueCount: 4).Object,
            remoting,
            timeProvider ?? new BrokerAssignmentTimeProvider(
                DateTimeOffset.FromUnixTimeMilliseconds(PopTimeMilliseconds)),
            NullLogger<RemotingPushConsumer>.Instance,
            rebalanceService: rebalance);

    private static int CountAssignmentQueries(FakeRemotingClient remoting, string topic) =>
        remoting.Requests.Count(request =>
            request.Code == RequestCode.QueryAssignment && AssignmentQueryTopic(request) == topic);

    private static string AssignmentQueryTopic(RemotingCommand request)
    {
        if (request.Body is not { Length: > 0 })
        {
            throw new InvalidDataException("QUERY_ASSIGNMENT did not contain a request body.");
        }

        using var body = JsonDocument.Parse(request.Body);
        return body.RootElement.GetProperty("topic").GetString()
            ?? throw new InvalidDataException("QUERY_ASSIGNMENT did not contain a topic.");
    }

    private static RemotingCommand AssignmentResponse(string topic, int queueId, string mode) =>
        BrokerSuccess(JsonSerializer.SerializeToUtf8Bytes(new
        {
            messageQueueAssignments = new[]
            {
                new
                {
                    messageQueue = new { topic, brokerName = "broker-a", queueId },
                    mode,
                    attachments = new Dictionary<string, string>()
                }
            }
        }));

    private static RemotingCommand MixedAssignmentResponse() =>
        BrokerSuccess(JsonSerializer.SerializeToUtf8Bytes(new
        {
            messageQueueAssignments = new[]
            {
                new
                {
                    messageQueue = new { topic = "orders", brokerName = "broker-a", queueId = 0 },
                    mode = "PULL",
                    attachments = new Dictionary<string, string>()
                },
                new
                {
                    messageQueue = new { topic = "orders", brokerName = "broker-a", queueId = -1 },
                    mode = "POP",
                    attachments = new Dictionary<string, string>()
                }
            }
        }));

    private static RemotingCommand NullAssignmentResponse() =>
        BrokerSuccess(Encoding.UTF8.GetBytes("{\"messageQueueAssignments\":null}"));

    private static RemotingCommand EmptyAssignmentResponse() =>
        BrokerSuccess(Encoding.UTF8.GetBytes("{\"messageQueueAssignments\":[]}"));

    private static RemotingCommand CreatePopSuccessResponse(long invisibleTimeMilliseconds) =>
        CreatePopSuccessResponse(
            CreateMessageRecord("orders", "pop-message", null, 42, 100, queueId: 3),
            invisibleTimeMilliseconds,
            "42");

    private static RemotingCommand CreatePopSuccessResponse(
        byte[] body,
        long invisibleTimeMilliseconds,
        string messageOffsets) =>
        new()
        {
            Code = ResponseCodes.ResSuccess,
            Body = body,
            ExtFields = new Dictionary<string, object>
            {
                ["popTime"] = PopTimeMilliseconds.ToString(),
                ["invisibleTime"] = invisibleTimeMilliseconds.ToString(),
                ["reviveQid"] = "2",
                ["restNum"] = "0",
                ["startOffsetInfo"] = "0 3 40",
                ["msgOffsetInfo"] = $"0 3 {messageOffsets}"
            }
        };

    private static RemotingCommand ChangeInvisibleTimeResponse(
        long popTimeMilliseconds,
        long invisibleTimeMilliseconds,
        int reviveQueueId) => new()
        {
            Code = ResponseCodes.ResSuccess,
            ExtFields = new Dictionary<string, object>
            {
                ["popTime"] = popTimeMilliseconds.ToString(),
                ["invisibleTime"] = invisibleTimeMilliseconds.ToString(),
                ["reviveQid"] = reviveQueueId.ToString()
            }
        };

    private static Func<RemotingCommand, CancellationToken, Task<RemotingCommand>> ReceiveOnePopThenWait(
        RemotingCommand response)
    {
        var calls = 0;
        return (_, token) => Interlocked.Increment(ref calls) == 1
            ? Task.FromResult(response)
            : WaitForCanceledBrokerRequestAsync(token);
    }

    private static async Task<RemotingCommand> WaitForCanceledBrokerRequestAsync(
        CancellationToken cancellationToken)
    {
        await Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken);
        throw new InvalidOperationException("An infinite Broker request completed unexpectedly.");
    }

    private static async Task<RemotingCommand> WaitForBrokerRequestAsync(
        FakeRemotingClient remoting,
        int requestCode,
        CancellationToken cancellationToken)
    {
        async Task<RemotingCommand> WaitCoreAsync()
        {
            while (true)
            {
                var request = remoting.Requests.FirstOrDefault(value => value.Code == requestCode);
                if (request is not null)
                {
                    return request;
                }

                await Task.Delay(10, cancellationToken);
            }
        }

        return await WaitCoreAsync().WaitAsync(TimeSpan.FromSeconds(3), cancellationToken);
    }

    private static RemotingCommand BrokerSuccess(byte[]? body = null) => new()
    {
        Code = ResponseCodes.ResSuccess,
        Body = body
    };

    private sealed class BrokerAssignmentTimeProvider : TimeProvider
    {
        private readonly DateTimeOffset _utcNow;
        private readonly long _timestamp;
        private long _advancedTicks;

        public BrokerAssignmentTimeProvider(DateTimeOffset utcNow)
        {
            _utcNow = utcNow;
            _timestamp = Stopwatch.GetTimestamp();
        }

        public void Advance(TimeSpan duration) =>
            Interlocked.Add(ref _advancedTicks, duration.Ticks);

        public override DateTimeOffset GetUtcNow() =>
            _utcNow + Stopwatch.GetElapsedTime(_timestamp) +
            TimeSpan.FromTicks(Volatile.Read(ref _advancedTicks));
    }
}
