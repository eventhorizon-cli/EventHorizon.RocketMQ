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
using System.Net;
using System.Text;
using EventHorizon.RocketMQ.Remoting.Exceptions;
using EventHorizon.RocketMQ.Remoting.Producer;
using EventHorizon.RocketMQ.Remoting.Protocol;
using EventHorizon.RocketMQ.Remoting.Protocol.Route;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Moq;
using Xunit;

namespace EventHorizon.RocketMQ.Remoting.Tests.Producer;

public sealed class RemotingProducerTests
{
    [Fact]
    public async Task SendAsync_MapsResponseAndEncodesRequest()
    {
        RemotingCommand? captured = null;
        var remoting = CreateRemotingClientMock((_, request, _, _) =>
        {
            captured = request;
            return Task.FromResult(SuccessResponse());
        });
        var routes = CreateRouteServiceMock(Route("broker-a", "localhost:10911"));
        var producer = CreateProducer(remoting.Object, routes.Mock.Object);
        var message = new Message("orders", "payload"u8.ToArray());
        message.Properties["TAGS"] = "created";

        await producer.StartAsync(TestContext.Current.CancellationToken);
        var result = await producer.SendAsync(message, TestContext.Current.CancellationToken);

        Assert.Equal(RemotingSendStatus.SendOk, result.Status);
        Assert.NotEmpty(result.MessageId);
        Assert.Equal("offset-id", result.OffsetMessageId);
        Assert.Equal(7, result.MessageQueue.QueueId);
        Assert.Equal("orders", result.MessageQueue.Topic);
        Assert.Equal("broker-a", result.MessageQueue.BrokerName);
        Assert.Equal(42, result.QueueOffset);
        Assert.Null(result.TransactionId);
        Assert.Null(result.RecallHandle);
        Assert.Equal("DefaultRegion", result.RegionId);
        Assert.True(result.TraceEnabled);
        Assert.NotNull(captured);
        Assert.Equal(RequestCode.SendMessage, captured.Code);
        Assert.Equal(message.Body, captured.Body);
        Assert.Equal("orders", captured.ExtFields["topic"]);
        Assert.Equal("broker-a", captured.ExtFields["bname"]);
        Assert.Contains("UNIQ_KEY\u0001", Assert.IsType<string>(captured.ExtFields["properties"]));
        Assert.Contains("TAGS\u0001created\u0002", Assert.IsType<string>(captured.ExtFields["properties"]));
    }

    [Fact]
    public async Task SendAsync_RetriesWithRefreshedRoute()
    {
        var endpoints = new List<EndPoint>();
        var call = 0;
        var remoting = CreateRemotingClientMock((endpoint, _, _, _) =>
        {
            endpoints.Add(endpoint);
            if (Interlocked.Increment(ref call) == 1)
            {
                throw new IOException("first broker unavailable");
            }

            return Task.FromResult(SuccessResponse());
        });
        var routes = CreateRouteServiceMock(
            Route("broker-a", "localhost:10911"),
            Route("broker-b", "localhost:20911"));
        var producer = CreateProducer(
            remoting.Object,
            routes.Mock.Object,
            options => options.RetryTimesWhenSendFailed = 1);

        await producer.StartAsync(TestContext.Current.CancellationToken);
        var result = await producer.SendAsync(
            new Message("orders", "payload"u8.ToArray()),
            TestContext.Current.CancellationToken);

        Assert.Equal(RemotingSendStatus.SendOk, result.Status);
        Assert.Equal(2, endpoints.Count);
        Assert.Equal("broker-b", result.MessageQueue.BrokerName);
        Assert.Equal([false, true], routes.ForceRefreshCalls);
    }

    [Theory]
    [InlineData(ResponseCodes.ResNoPermission)]
    [InlineData(ResponseCodes.ResTopicNotExist)]
    public async Task SendAsync_DoesNotRetryPermanentBrokerRejection(int responseCode)
    {
        var remoting = CreateRemotingClientMock((_, _, _, _) => Task.FromResult(new RemotingCommand
        {
            Code = responseCode,
            Remark = "send rejected"
        }));
        var routes = CreateRouteServiceMock(
            Route("broker-a", "localhost:10911"),
            Route("broker-b", "localhost:20911"));
        var producer = CreateProducer(
            remoting.Object,
            routes.Mock.Object,
            options => options.RetryTimesWhenSendFailed = 2);
        await producer.StartAsync(TestContext.Current.CancellationToken);

        var exception = await Assert.ThrowsAsync<RemotingCommandException>(() => producer.SendAsync(
            new Message("orders", "payload"u8.ToArray()),
            TestContext.Current.CancellationToken));

        Assert.Equal(responseCode, exception.ResponseCode);
        Assert.Equal("send rejected", exception.Message);
        Assert.Equal([false], routes.ForceRefreshCalls);
        remoting.Verify(value => value.InvokeAsync(
            It.IsAny<EndPoint>(),
            It.IsAny<RemotingCommand>(),
            It.IsAny<TimeSpan>(),
            It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task SendAsync_RetriesSystemBusyResponse()
    {
        var attempt = 0;
        var remoting = CreateRemotingClientMock((_, _, _, _) =>
            Task.FromResult(Interlocked.Increment(ref attempt) == 1
                ? new RemotingCommand { Code = ResponseCodes.ResSystemBusy, Remark = "broker busy" }
                : SuccessResponse()));
        var routes = CreateRouteServiceMock(
            Route("broker-a", "localhost:10911"),
            Route("broker-b", "localhost:20911"));
        var producer = CreateProducer(
            remoting.Object,
            routes.Mock.Object,
            options => options.RetryTimesWhenSendFailed = 1);
        await producer.StartAsync(TestContext.Current.CancellationToken);

        var result = await producer.SendAsync(
            new Message("orders", "payload"u8.ToArray()),
            TestContext.Current.CancellationToken);

        Assert.Equal(RemotingSendStatus.SendOk, result.Status);
        Assert.Equal(2, attempt);
        Assert.Equal([false, true], routes.ForceRefreshCalls);
    }

    [Fact]
    public async Task SendAsync_RejectsSuccessfulResponseWithMalformedQueueFields()
    {
        var remoting = CreateRemotingClientMock((_, _, _, _) => Task.FromResult(new RemotingCommand
        {
            Code = ResponseCodes.ResSuccess,
            ExtFields = new Dictionary<string, object>
            {
                ["queueId"] = "not-a-number",
                ["queueOffset"] = "42"
            }
        }));
        var routes = CreateRouteServiceMock(Route("broker-a", "localhost:10911"));
        var producer = CreateProducer(
            remoting.Object,
            routes.Mock.Object,
            options => options.RetryTimesWhenSendFailed = 0);
        await producer.StartAsync(TestContext.Current.CancellationToken);

        await Assert.ThrowsAsync<InvalidDataException>(() => producer.SendAsync(
            new Message("orders", "payload"u8.ToArray()),
            TestContext.Current.CancellationToken));
    }

    [Theory]
    [InlineData(ResponseCodes.ResSuccess, RemotingSendStatus.SendOk)]
    [InlineData(ResponseCodes.ResFlushDiskTimeout, RemotingSendStatus.FlushDiskTimeout)]
    [InlineData(ResponseCodes.ResFlushSlaveTimeout, RemotingSendStatus.FlushSlaveTimeout)]
    [InlineData(ResponseCodes.ResSlaveNotAvailable, RemotingSendStatus.SlaveNotAvailable)]
    public async Task SendAsync_MapsBrokerStatus(int responseCode, RemotingSendStatus expectedStatus)
    {
        var remoting = CreateRemotingClientMock((_, _, _, _) => Task.FromResult(SuccessResponse(responseCode)));
        var routes = CreateRouteServiceMock(Route("broker-a", "localhost:10911"));
        var producer = CreateProducer(remoting.Object, routes.Mock.Object);
        await producer.StartAsync(TestContext.Current.CancellationToken);

        var result = await producer.SendAsync(
            new Message("orders", "payload"u8.ToArray()),
            TestContext.Current.CancellationToken);

        Assert.Equal(expectedStatus, result.Status);
        Assert.Equal([false], routes.ForceRefreshCalls);
    }

    [Theory]
    [InlineData(null, true)]
    [InlineData("true", true)]
    [InlineData("false", false)]
    public async Task SendAsync_MapsTraceSwitch(string? traceValue, bool expected)
    {
        var response = SuccessResponse();
        if (traceValue is null)
        {
            response.ExtFields.Remove("TRACE_ON");
        }
        else
        {
            response.ExtFields["TRACE_ON"] = traceValue;
        }

        response.ExtFields.Remove("MSG_REGION");
        var remoting = CreateRemotingClientMock((_, _, _, _) => Task.FromResult(response));
        var routes = CreateRouteServiceMock(Route("broker-a", "localhost:10911"));
        var producer = CreateProducer(remoting.Object, routes.Mock.Object);
        await producer.StartAsync(TestContext.Current.CancellationToken);

        var result = await producer.SendAsync(
            new Message("orders", "payload"u8.ToArray()),
            TestContext.Current.CancellationToken);

        Assert.Equal(expected, result.TraceEnabled);
        Assert.Equal("DefaultRegion", result.RegionId);
    }

    [Fact]
    public async Task SendAsync_RejectsSuccessfulResponseWithoutOffsetMessageId()
    {
        var response = SuccessResponse();
        response.ExtFields.Remove("msgId");
        var remoting = CreateRemotingClientMock((_, _, _, _) => Task.FromResult(response));
        var routes = CreateRouteServiceMock(Route("broker-a", "localhost:10911"));
        var producer = CreateProducer(remoting.Object, routes.Mock.Object);
        await producer.StartAsync(TestContext.Current.CancellationToken);

        await Assert.ThrowsAsync<InvalidDataException>(() => producer.SendAsync(
            new Message("orders", "payload"u8.ToArray()),
            TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task SendAsync_CompressesLargeBodyAndSetsFlag()
    {
        RemotingCommand? captured = null;
        var remoting = CreateRemotingClientMock((_, request, _, _) =>
        {
            captured = request;
            return Task.FromResult(SuccessResponse());
        });
        var routes = CreateRouteServiceMock(Route("broker-a", "localhost:10911"));
        var producer = CreateProducer(
            remoting.Object,
            routes.Mock.Object,
            options => options.CompressMsgBodyOverHowmuch = 1);
        var body = Enumerable.Repeat((byte)'a', 1024).ToArray();

        await producer.StartAsync(TestContext.Current.CancellationToken);
        await producer.SendAsync(new Message("orders", body), TestContext.Current.CancellationToken);

        Assert.NotNull(captured);
        Assert.True(captured.Body!.Length < body.Length);
        Assert.Equal(MessageSysFlag.CompressedFlag, captured.ExtFields["sysFlag"]);
    }

    [Fact]
    public async Task SendAsync_MessageGroupKeepsQueueAffinityAcrossRetriesAndSends()
    {
        var selectedQueues = new List<(string BrokerName, int QueueId)>();
        var call = 0;
        var remoting = CreateRemotingClientMock((_, request, _, _) =>
        {
            selectedQueues.Add((
                Assert.IsType<string>(request.ExtFields["bname"]),
                Convert.ToInt32(request.ExtFields["queueId"])));
            if (Interlocked.Increment(ref call) == 1)
            {
                throw new IOException("retry the grouped message");
            }

            return Task.FromResult(SuccessResponse());
        });
        var route = Route(
            ("broker-a", "localhost:10911"),
            ("broker-b", "localhost:20911"));
        var routes = CreateRouteServiceMock(route);
        var producer = CreateProducer(
            remoting.Object,
            routes.Mock.Object,
            options => options.RetryTimesWhenSendFailed = 1);

        await producer.StartAsync(TestContext.Current.CancellationToken);
        await producer.SendAsync(
            new Message("orders", "first"u8.ToArray()) { MessageGroup = "account-42" },
            TestContext.Current.CancellationToken);
        await producer.SendAsync(
            new Message("orders", "second"u8.ToArray()) { MessageGroup = "account-42" },
            TestContext.Current.CancellationToken);

        Assert.Equal(3, selectedQueues.Count);
        Assert.All(selectedQueues, queue => Assert.Equal(selectedQueues[0], queue));
    }

    [Fact]
    public async Task SendAsync_EncodesSpecializedFieldsAsClassicSystemProperties()
    {
        var captured = new List<RemotingCommand>();
        var remoting = CreateRemotingClientMock((_, request, _, _) =>
        {
            captured.Add(request);
            return Task.FromResult(SuccessResponse());
        });
        var routes = CreateRouteServiceMock(Route("broker-a", "localhost:10911"));
        var producer = CreateProducer(remoting.Object, routes.Mock.Object);
        var deliveryTimestamp = DateTimeOffset.FromUnixTimeMilliseconds(1_893_456_000_123);

        await producer.StartAsync(TestContext.Current.CancellationToken);
        await producer.SendAsync(
            new Message("orders", [1]) { DeliveryTimestamp = deliveryTimestamp },
            TestContext.Current.CancellationToken);
        await producer.SendAsync(
            new Message("orders", [2]) { Priority = 3 },
            TestContext.Current.CancellationToken);
        await producer.SendAsync(
            new Message("orders", [3]) { LiteTopic = "lite-orders" },
            TestContext.Current.CancellationToken);

        Assert.Equal("1893456000123", Properties(captured[0])["TIMER_DELIVER_MS"]);
        Assert.Equal("3", Properties(captured[1])["_SYS_MSG_PRIORITY_"]);
        Assert.Equal("lite-orders", Properties(captured[2])["__LITE_TOPIC"]);
    }

    [Fact]
    public async Task SendAsync_RejectsConflictingSpecializedMessageTypesBeforeRemoting()
    {
        var remoting = CreateRemotingClientMock((_, _, _, _) => Task.FromResult(SuccessResponse()));
        var routes = CreateRouteServiceMock(Route("broker-a", "localhost:10911"));
        var producer = CreateProducer(remoting.Object, routes.Mock.Object);
        await producer.StartAsync(TestContext.Current.CancellationToken);

        var exception = await Assert.ThrowsAsync<ArgumentException>(() => producer.SendAsync(
            new Message("orders", [1]) { MessageGroup = "account-42", Priority = 1 },
            TestContext.Current.CancellationToken));

        Assert.Contains("mutually exclusive", exception.Message);
        remoting.Verify(value => value.InvokeAsync(
            It.IsAny<EndPoint>(),
            It.IsAny<RemotingCommand>(),
            It.IsAny<TimeSpan>(),
            It.IsAny<CancellationToken>()), Times.Never);
    }

    [Fact]
    public async Task SendAsync_WrapsClassicNamespaceWithoutLeakingItToResult()
    {
        RemotingCommand? captured = null;
        var remoting = CreateRemotingClientMock((_, request, _, _) =>
        {
            captured = request;
            return Task.FromResult(SuccessResponse());
        });
        var routes = CreateRouteServiceMock(Route("broker-a", "localhost:10911"));
        var producer = CreateProducer(
            remoting.Object,
            routes.Mock.Object,
            configureClient: options => options.Namespace = "tenant-a");
        var message = new Message("orders", [1]);

        await producer.StartAsync(TestContext.Current.CancellationToken);
        var result = await producer.SendAsync(message, TestContext.Current.CancellationToken);

        Assert.Equal(["tenant-a%orders"], routes.Topics);
        Assert.NotNull(captured);
        Assert.Equal("tenant-a%orders", captured.ExtFields["topic"]);
        Assert.Equal("tenant-a%tests", captured.ExtFields["producerGroup"]);
        Assert.Equal("orders", message.Topic);
        Assert.Equal("orders", result.MessageQueue.Topic);
    }

    [Fact]
    public async Task GetPublishMessageQueuesAsync_ReturnsLogicalWritableQueues()
    {
        var remoting = CreateRemotingClientMock((_, _, _, _) => Task.FromResult(SuccessResponse()));
        var routes = CreateRouteServiceMock(Route(
            ("broker-b", "localhost:20911"),
            ("broker-a", "localhost:10911")));
        var producer = CreateProducer(remoting.Object, routes.Mock.Object);

        await producer.StartAsync(TestContext.Current.CancellationToken);
        var queues = await producer.GetPublishMessageQueuesAsync("orders", TestContext.Current.CancellationToken);

        Assert.Equal(16, queues.Count);
        Assert.All(queues, queue => Assert.Equal("orders", queue.Topic));
        Assert.Equal("broker-a", queues[0].BrokerName);
        Assert.Equal(0, queues[0].QueueId);
        Assert.Contains(queues, queue => queue.BrokerName == "broker-b" && queue.QueueId == 7);
    }

    [Fact]
    public async Task SendAsync_UsesExplicitWritableQueue()
    {
        RemotingCommand? captured = null;
        var remoting = CreateRemotingClientMock((_, request, _, _) =>
        {
            captured = request;
            return Task.FromResult(SuccessResponse());
        });
        var routes = CreateRouteServiceMock(Route(
            ("broker-a", "localhost:10911"),
            ("broker-b", "localhost:20911")));
        var producer = CreateProducer(remoting.Object, routes.Mock.Object);

        await producer.StartAsync(TestContext.Current.CancellationToken);
        await producer.SendAsync(
            new Message("orders", "payload"u8.ToArray()),
            new RemotingMessageQueue("orders", "broker-b", 3),
            TestContext.Current.CancellationToken);

        Assert.NotNull(captured);
        Assert.Equal("broker-b", captured.ExtFields["bname"]);
        Assert.Equal(3, captured.ExtFields["queueId"]);
    }

    [Fact]
    public async Task SendAsync_UsesSelectorAgainstCurrentWritableQueues()
    {
        RemotingCommand? captured = null;
        IReadOnlyList<RemotingMessageQueue>? availableQueues = null;
        var selectorArgument = new object();
        var message = new Message("orders", "payload"u8.ToArray());
        var remoting = CreateRemotingClientMock((_, request, _, _) =>
        {
            captured = request;
            return Task.FromResult(SuccessResponse());
        });
        var routes = CreateRouteServiceMock(Route(
            ("broker-a", "localhost:10911"),
            ("broker-b", "localhost:20911")));
        var producer = CreateProducer(remoting.Object, routes.Mock.Object);

        await producer.StartAsync(TestContext.Current.CancellationToken);
        await producer.SendAsync(
            message,
            (queues, selectedMessage, argument) =>
            {
                availableQueues = queues;
                Assert.Same(message, selectedMessage);
                Assert.Same(selectorArgument, argument);
                return queues.Single(queue => queue.BrokerName == "broker-b" && queue.QueueId == 5);
            },
            selectorArgument,
            TestContext.Current.CancellationToken);

        Assert.NotNull(availableQueues);
        Assert.Equal(16, availableQueues.Count);
        Assert.NotNull(captured);
        Assert.Equal("broker-b", captured.ExtFields["bname"]);
        Assert.Equal(5, captured.ExtFields["queueId"]);
    }

    [Fact]
    public async Task SendAsync_RejectsQueueThatIsNotWritableForTheTopic()
    {
        var remoting = CreateRemotingClientMock((_, _, _, _) => Task.FromResult(SuccessResponse()));
        var routes = CreateRouteServiceMock(Route("broker-a", "localhost:10911"));
        var producer = CreateProducer(remoting.Object, routes.Mock.Object);

        await producer.StartAsync(TestContext.Current.CancellationToken);
        var exception = await Assert.ThrowsAsync<ArgumentException>(() => producer.SendAsync(
            new Message("orders", "payload"u8.ToArray()),
            new RemotingMessageQueue("orders", "broker-missing", 0),
            TestContext.Current.CancellationToken));

        Assert.Contains("not writable", exception.Message);
        remoting.Verify(value => value.InvokeAsync(
            It.IsAny<EndPoint>(),
            It.IsAny<RemotingCommand>(),
            It.IsAny<TimeSpan>(),
            It.IsAny<CancellationToken>()), Times.Never);
    }

    [Fact]
    public async Task SendOnewayAsync_UsesExplicitAndSelectedQueues()
    {
        var requests = new List<RemotingCommand>();
        var remoting = new Mock<IRemotingClient>(MockBehavior.Strict);
        remoting
            .Setup(value => value.RegisterRequestHandler(
                It.IsAny<int>(),
                It.IsAny<RemotingRequestHandler>()))
            .Returns(new NoopRegistration());
        remoting
            .Setup(value => value.InvokeOnewayAsync(
                It.IsAny<EndPoint>(),
                It.IsAny<RemotingCommand>(),
                It.IsAny<TimeSpan>(),
                It.IsAny<CancellationToken>()))
            .Returns((EndPoint _, RemotingCommand request, TimeSpan _, CancellationToken _) =>
            {
                requests.Add(request);
                return Task.CompletedTask;
            });
        var routes = CreateRouteServiceMock(Route(
            ("broker-a", "localhost:10911"),
            ("broker-b", "localhost:20911")));
        var producer = CreateProducer(remoting.Object, routes.Mock.Object);

        await producer.StartAsync(TestContext.Current.CancellationToken);
        await producer.SendOnewayAsync(
            new Message("orders", "first"u8.ToArray()),
            new RemotingMessageQueue("orders", "broker-a", 2),
            TestContext.Current.CancellationToken);
        await producer.SendOnewayAsync(
            new Message("orders", "second"u8.ToArray()),
            (queues, _, _) => queues.Single(queue => queue.BrokerName == "broker-b" && queue.QueueId == 4),
            cancellationToken: TestContext.Current.CancellationToken);

        Assert.Equal(2, requests.Count);
        Assert.Equal("broker-a", requests[0].ExtFields["bname"]);
        Assert.Equal(2, requests[0].ExtFields["queueId"]);
        Assert.Equal("broker-b", requests[1].ExtFields["bname"]);
        Assert.Equal(4, requests[1].ExtFields["queueId"]);
    }

    [Fact]
    public async Task SendAsync_BatchEncodesMiniRecordsAndReturnsEachMessageId()
    {
        RemotingCommand? captured = null;
        var remoting = CreateRemotingClientMock((_, request, _, _) =>
        {
            captured = request;
            return Task.FromResult(SuccessResponse());
        });
        var routes = CreateRouteServiceMock(Route("broker-a", "localhost:10911"));
        var producer = CreateProducer(
            remoting.Object,
            routes.Mock.Object,
            options => options.CompressMsgBodyOverHowmuch = 1);
        var first = new Message("orders", "first"u8.ToArray()) { Tag = "created" };
        first.Properties["UNIQ_KEY"] = "first-id";
        var second = new Message("orders", "second"u8.ToArray());
        second.Properties["UNIQ_KEY"] = "second-id";

        await producer.StartAsync(TestContext.Current.CancellationToken);
        var result = await producer.SendAsync([first, second], TestContext.Current.CancellationToken);

        Assert.Equal("first-id,second-id", result.MessageId);
        Assert.NotNull(captured);
        Assert.True(Assert.IsType<bool>(captured.ExtFields["batch"]));
        Assert.Equal(0, captured.ExtFields["sysFlag"]);
        Assert.NotNull(captured.Body);
        Assert.True(captured.Body.Length > first.Body.Length + second.Body.Length);
        var headerProperties = Properties(captured);
        Assert.Single(headerProperties);
        Assert.True(headerProperties.ContainsKey("UNIQ_KEY"));
    }

    [Fact]
    public async Task SendAsync_BatchRejectsMixedTopicsAndDelayedMessagesBeforeRemoting()
    {
        var remoting = CreateRemotingClientMock((_, _, _, _) => Task.FromResult(SuccessResponse()));
        var routes = CreateRouteServiceMock(Route("broker-a", "localhost:10911"));
        var producer = CreateProducer(remoting.Object, routes.Mock.Object);
        await producer.StartAsync(TestContext.Current.CancellationToken);

        var mixedTopics = await Assert.ThrowsAsync<ArgumentException>(() => producer.SendAsync(
            [new Message("orders", [1]), new Message("payments", [2])],
            TestContext.Current.CancellationToken));
        var delayed = await Assert.ThrowsAsync<ArgumentException>(() => producer.SendAsync(
            [new Message("orders", [1]) { DeliveryTimestamp = DateTimeOffset.UtcNow.AddMinutes(1) }],
            TestContext.Current.CancellationToken));

        Assert.Contains("same topic", mixedTopics.Message);
        Assert.Contains("Delayed", delayed.Message);
        remoting.Verify(value => value.InvokeAsync(
            It.IsAny<EndPoint>(),
            It.IsAny<RemotingCommand>(),
            It.IsAny<TimeSpan>(),
            It.IsAny<CancellationToken>()), Times.Never);
    }

    [Fact]
    public async Task RecallAsync_UsesHandleBrokerAndWireTopic()
    {
        EndPoint? capturedEndpoint = null;
        RemotingCommand? captured = null;
        var recallHandle = CreateRecallHandle("tenant-a%orders", "broker-b", "1893456000000", "message-id");
        var remoting = CreateRemotingClientMock((endpoint, request, _, _) =>
        {
            capturedEndpoint = endpoint;
            captured = request;
            return Task.FromResult(new RemotingCommand
            {
                Code = ResponseCodes.ResSuccess,
                ExtFields = new Dictionary<string, object> { ["msgId"] = "recalled-message-id" }
            });
        });
        var routes = CreateRouteServiceMock(Route(
            ("broker-a", "localhost:10911"),
            ("broker-b", "localhost:20911")));
        var producer = CreateProducer(
            remoting.Object,
            routes.Mock.Object,
            configureClient: options => options.Namespace = "tenant-a");

        await producer.StartAsync(TestContext.Current.CancellationToken);
        var messageId = await producer.RecallAsync("orders", recallHandle, TestContext.Current.CancellationToken);

        Assert.Equal("recalled-message-id", messageId);
        Assert.Equal(["tenant-a%orders"], routes.Topics);
        var endpoint = Assert.IsType<DnsEndPoint>(capturedEndpoint);
        Assert.Equal("localhost", endpoint.Host);
        Assert.Equal(20911, endpoint.Port);
        Assert.NotNull(captured);
        Assert.Equal(RequestCode.RecallMessage, captured.Code);
        Assert.Equal("tenant-a%tests", captured.ExtFields["producerGroup"]);
        Assert.Equal("tenant-a%orders", captured.ExtFields["topic"]);
        Assert.Equal(recallHandle, captured.ExtFields["recallHandle"]);
        Assert.Equal("broker-b", captured.ExtFields["bname"]);
    }

    [Fact]
    public async Task RecallAsync_RejectsInvalidOrMismatchedHandleBeforeRemoting()
    {
        var remoting = CreateRemotingClientMock((_, _, _, _) => Task.FromResult(SuccessResponse()));
        var routes = CreateRouteServiceMock(Route("broker-a", "localhost:10911"));
        var producer = CreateProducer(remoting.Object, routes.Mock.Object);
        await producer.StartAsync(TestContext.Current.CancellationToken);

        await Assert.ThrowsAsync<ArgumentException>(() => producer.RecallAsync(
            "orders",
            "not-a-recall-handle",
            TestContext.Current.CancellationToken));
        await Assert.ThrowsAsync<ArgumentException>(() => producer.RecallAsync(
            "orders",
            CreateRecallHandle("payments", "broker-a", "1893456000000", "message-id"),
            TestContext.Current.CancellationToken));

        remoting.Verify(value => value.InvokeAsync(
            It.IsAny<EndPoint>(),
            It.IsAny<RemotingCommand>(),
            It.IsAny<TimeSpan>(),
            It.IsAny<CancellationToken>()), Times.Never);
    }

    private static RemotingProducer CreateProducer(
        IRemotingClient remotingClient,
        ITopicRouteService routeService,
        Action<RemotingProducerOptions>? configure = null,
        Action<RemotingClientOptions>? configureClient = null)
    {
        var producerOptions = new RemotingProducerOptions { GroupName = "tests" };
        configure?.Invoke(producerOptions);
        var clientOptions = new RemotingClientOptions();
        configureClient?.Invoke(clientOptions);
        var serviceProvider = new Mock<IServiceProvider>();
        serviceProvider
            .Setup(provider => provider.GetService(typeof(TimeProvider)))
            .Returns(TimeProvider.System);
        serviceProvider
            .Setup(provider => provider.GetService(typeof(ILoggerFactory)))
            .Returns(NullLoggerFactory.Instance);
        return ActivatorUtilities.CreateInstance<RemotingProducer>(
            serviceProvider.Object,
            Options.Create(producerOptions),
            Options.Create(clientOptions),
            routeService,
            remotingClient);
    }

    private static TopicRouteData Route(string brokerName, string address) => new()
    {
        QueueDatas =
        [
            new QueueData { BrokerName = brokerName, ReadQueueNums = 8, WriteQueueNums = 8, Perm = 6 }
        ],
        BrokerDatas =
        [
            new BrokerData
            {
                BrokerName = brokerName,
                BrokerAddrs = new ConcurrentDictionary<long, string>(new[]
                {
                    new KeyValuePair<long, string>(0, address)
                })
            }
        ]
    };

    private static TopicRouteData Route(params (string BrokerName, string Address)[] brokers) => new()
    {
        QueueDatas = brokers
            .Select(static broker => new QueueData
            {
                BrokerName = broker.BrokerName,
                ReadQueueNums = 8,
                WriteQueueNums = 8,
                Perm = 6
            })
            .ToArray(),
        BrokerDatas = brokers
            .Select(static broker => new BrokerData
            {
                BrokerName = broker.BrokerName,
                BrokerAddrs = new ConcurrentDictionary<long, string>(new[]
                {
                    new KeyValuePair<long, string>(0, broker.Address)
                })
            })
            .ToArray()
    };

    private static IReadOnlyDictionary<string, string> Properties(RemotingCommand command) =>
        MessagePropertyCodec.Deserialize(Assert.IsType<string>(command.ExtFields["properties"]));

    private static string CreateRecallHandle(string topic, string brokerName, string timestamp, string messageId) =>
        Convert.ToBase64String(Encoding.UTF8.GetBytes($"v1 {topic} {brokerName} {timestamp} {messageId}"))
            .TrimEnd('=')
            .Replace('+', '-')
            .Replace('/', '_');

    private static RemotingCommand SuccessResponse(int responseCode = ResponseCodes.ResSuccess) => new()
    {
        Code = responseCode,
        ExtFields = new Dictionary<string, object>
        {
            ["msgId"] = "offset-id",
            ["queueId"] = "7",
            ["queueOffset"] = "42",
            ["MSG_REGION"] = "DefaultRegion",
            ["TRACE_ON"] = "true"
        }
    };

    private static (
        Mock<ITopicRouteService> Mock,
        List<bool> ForceRefreshCalls,
        List<string> Topics) CreateRouteServiceMock(params TopicRouteData[] routes)
    {
        var routeService = new Mock<ITopicRouteService>(MockBehavior.Strict);
        var forceRefreshCalls = new List<bool>();
        var topics = new List<string>();
        var index = 0;
        routeService
            .Setup(value => value.GetAsync(
                It.IsAny<string>(),
                It.IsAny<bool>(),
                It.IsAny<CancellationToken>()))
            .Returns<string, bool, CancellationToken>((topic, forceRefresh, _) =>
            {
                topics.Add(topic);
                forceRefreshCalls.Add(forceRefresh);
                var route = routes[Math.Min(index, routes.Length - 1)];
                index++;
                return Task.FromResult(route);
            });
        return (routeService, forceRefreshCalls, topics);
    }

    private static Mock<IRemotingClient> CreateRemotingClientMock(
        Func<EndPoint, RemotingCommand, TimeSpan, CancellationToken, Task<RemotingCommand>> handler)
    {
        var remoting = new Mock<IRemotingClient>(MockBehavior.Strict);
        remoting
            .Setup(value => value.RegisterRequestHandler(
                It.IsAny<int>(),
                It.IsAny<RemotingRequestHandler>()))
            .Returns(new NoopRegistration());
        remoting
            .Setup(value => value.InvokeAsync(
                It.IsAny<EndPoint>(),
                It.IsAny<RemotingCommand>(),
                It.IsAny<TimeSpan>(),
                It.IsAny<CancellationToken>()))
            .Returns(handler);
        return remoting;
    }

    private sealed class NoopRegistration : IDisposable
    {
        public void Dispose()
        {
        }
    }
}
