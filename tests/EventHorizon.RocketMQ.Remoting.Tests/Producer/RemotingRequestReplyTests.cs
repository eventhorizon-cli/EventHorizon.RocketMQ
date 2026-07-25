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
using System.Text.Json;
using EventHorizon.RocketMQ.Producer;
using EventHorizon.RocketMQ.Remoting.Producer;
using EventHorizon.RocketMQ.Remoting.Protocol;
using EventHorizon.RocketMQ.Remoting.Protocol.Route;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Moq;
using Xunit;

namespace EventHorizon.RocketMQ.Remoting.Tests.Producer;

public sealed class RemotingRequestReplyTests
{
    [Fact]
    public async Task RequestAsync_HeartbeatsBeforeSendAndCompletesMatchingCallback()
    {
        var remoting = new RequestReplyRemotingClient();
        var producer = CreateProducer(remoting);
        await producer.StartAsync(TestContext.Current.CancellationToken);

        try
        {
            var message = new Message("orders", "request"u8.ToArray());
            var replyTask = producer.RequestAsync(
                message,
                TimeSpan.FromSeconds(5),
                TestContext.Current.CancellationToken);
            var sentRequest = await remoting.WaitForSendMessageAsync(TestContext.Current.CancellationToken);
            var invocations = remoting.Invocations.ToArray();

            Assert.Collection(
                invocations,
                invocation => Assert.Equal(RequestCode.HeartBeat, invocation.Request.Code),
                invocation => Assert.Equal(RequestCode.SendMessage, invocation.Request.Code));
            AssertProducerHeartbeat(invocations[0].Request);
            Assert.Equal("orders", sentRequest.ExtFields["topic"]);
            var requestProperties = MessagePropertyCodec.Deserialize(
                Assert.IsType<string>(sentRequest.ExtFields["properties"]));
            var correlationId = Assert.IsType<string>(requestProperties["CORRELATION_ID"]);
            Assert.Equal("127.0.0.1@request-reply-tests", requestProperties["REPLY_TO_CLIENT"]);
            Assert.Equal("5000", requestProperties["TTL"]);

            var callbackResult = await remoting.DispatchInboundAsync(
                CreateReplyCallback(correlationId, "reply"u8.ToArray()));
            var reply = await replyTask;

            Assert.True(callbackResult.IsHandled);
            Assert.False(callbackResult.SuppressResponse);
            Assert.Equal(ResponseCodes.ResSuccess, Assert.IsType<RemotingCommand>(callbackResult.Response).Code);
            Assert.Equal("orders", reply.Topic);
            Assert.Equal("reply"u8.ToArray(), reply.Body);
            Assert.Equal(correlationId, reply.CorrelationId);
            Assert.Equal("reply-id", reply.MessageId);
            Assert.Equal("reply-tag", reply.Tag);
            Assert.Equal(["first", "second"], reply.Keys);
            Assert.True(reply.Properties.ContainsKey("ARRIVE_TIME"));
        }
        finally
        {
            await producer.StopAsync(TestContext.Current.CancellationToken);
        }
    }

    [Fact]
    public async Task ReplyCallback_AcknowledgesUnknownCorrelation()
    {
        var remoting = new RequestReplyRemotingClient();
        var producer = CreateProducer(remoting);
        await producer.StartAsync(TestContext.Current.CancellationToken);

        try
        {
            var result = await remoting.DispatchInboundAsync(
                CreateReplyCallback("unknown-correlation", "reply"u8.ToArray()));

            Assert.True(result.IsHandled);
            Assert.False(result.SuppressResponse);
            Assert.Equal(ResponseCodes.ResSuccess, Assert.IsType<RemotingCommand>(result.Response).Code);
            Assert.Empty(remoting.Invocations);
        }
        finally
        {
            await producer.StopAsync(TestContext.Current.CancellationToken);
        }
    }

    [Fact]
    public async Task ReplyCallback_RejectsMalformedCallback()
    {
        var remoting = new RequestReplyRemotingClient();
        var producer = CreateProducer(remoting);
        await producer.StartAsync(TestContext.Current.CancellationToken);

        try
        {
            var result = await remoting.DispatchInboundAsync(new RemotingCommand
            {
                Code = RequestCode.PushReplyMessageToClient,
                ExtFields = new Dictionary<string, object>(),
                Body = "reply"u8.ToArray()
            });

            Assert.True(result.IsHandled);
            Assert.False(result.SuppressResponse);
            var response = Assert.IsType<RemotingCommand>(result.Response);
            Assert.Equal(ResponseCodes.ResError, response.Code);
            Assert.Equal("The reply callback could not be processed.", response.Remark);
        }
        finally
        {
            await producer.StopAsync(TestContext.Current.CancellationToken);
        }
    }

    [Fact]
    public async Task RequestAsync_TimesOutWhenNoCallbackArrives()
    {
        var remoting = new RequestReplyRemotingClient();
        var producer = CreateProducer(remoting);
        await producer.StartAsync(TestContext.Current.CancellationToken);

        try
        {
            var exception = await Assert.ThrowsAsync<TimeoutException>(() => producer.RequestAsync(
                new Message("orders", "request"u8.ToArray()),
                TimeSpan.FromMilliseconds(100),
                TestContext.Current.CancellationToken));

            Assert.Contains("orders", exception.Message, StringComparison.Ordinal);
            await remoting.WaitForSendMessageAsync(TestContext.Current.CancellationToken);
        }
        finally
        {
            await producer.StopAsync(TestContext.Current.CancellationToken);
        }
    }

    [Fact]
    public async Task StopAsync_FailsPendingRequestAndUnregistersProducer()
    {
        var remoting = new RequestReplyRemotingClient();
        var producer = CreateProducer(remoting);
        await producer.StartAsync(TestContext.Current.CancellationToken);

        try
        {
            var replyTask = producer.RequestAsync(
                new Message("orders", "request"u8.ToArray()),
                TimeSpan.FromMinutes(1),
                TestContext.Current.CancellationToken);
            await remoting.WaitForSendMessageAsync(TestContext.Current.CancellationToken);

            await producer.StopAsync(TestContext.Current.CancellationToken);

            var exception = await Assert.ThrowsAsync<InvalidOperationException>(() => replyTask);
            Assert.Equal("The producer stopped before the reply arrived.", exception.Message);
            Assert.Equal(0, remoting.RequestHandlerCount);
            var unregister = Assert.Single(
                remoting.Invocations,
                invocation => invocation.Request.Code == RequestCode.UnregisterClient);
            Assert.Equal("127.0.0.1@request-reply-tests", unregister.Request.ExtFields["clientID"]);
            Assert.Equal("request-reply-tests", unregister.Request.ExtFields["producerGroup"]);
            Assert.Equal("broker-a", unregister.Request.ExtFields["bname"]);
        }
        finally
        {
            await producer.StopAsync(TestContext.Current.CancellationToken);
        }
    }

    [Fact]
    public void RemotingReply_FromRequestProperties_CreatesWireReplyContext()
    {
        var reply = RemotingReply.FromRequestProperties(
            new Dictionary<string, string>
            {
                ["CLUSTER"] = "cluster-a",
                ["CORRELATION_ID"] = "correlation-id",
                ["REPLY_TO_CLIENT"] = "request-client",
                ["TTL"] = "1500"
            },
            "reply"u8.ToArray());

        Assert.Equal("cluster-a_REPLY_TOPIC", reply.Topic);
        Assert.Equal("cluster-a", reply.Cluster);
        Assert.Equal("correlation-id", reply.CorrelationId);
        Assert.Equal("request-client", reply.ReplyToClient);
        Assert.Equal(TimeSpan.FromMilliseconds(1500), reply.TimeToLive);
        Assert.Equal("reply", reply.Properties["MSG_TYPE"]);
        Assert.Equal("correlation-id", reply.Properties["CORRELATION_ID"]);
        Assert.Equal("request-client", reply.Properties["REPLY_TO_CLIENT"]);
        Assert.Equal("1500", reply.Properties["TTL"]);
    }

    [Theory]
    [InlineData("CLUSTER")]
    [InlineData("CORRELATION_ID")]
    [InlineData("REPLY_TO_CLIENT")]
    public void RemotingReply_FromRequestProperties_RejectsMissingRequiredProperty(string missingProperty)
    {
        var requestProperties = new Dictionary<string, string>
        {
            ["CLUSTER"] = "cluster-a",
            ["CORRELATION_ID"] = "correlation-id",
            ["REPLY_TO_CLIENT"] = "request-client",
            ["TTL"] = "1500"
        };
        requestProperties.Remove(missingProperty);

        Assert.Throws<ArgumentException>(
            () => RemotingReply.FromRequestProperties(requestProperties, "reply"u8.ToArray()));
    }

    [Theory]
    [InlineData("0")]
    [InlineData("invalid")]
    public void RemotingReply_FromRequestProperties_RejectsInvalidTimeToLive(string timeToLive)
    {
        var requestProperties = new Dictionary<string, string>
        {
            ["CLUSTER"] = "cluster-a",
            ["CORRELATION_ID"] = "correlation-id",
            ["REPLY_TO_CLIENT"] = "request-client",
            ["TTL"] = timeToLive
        };

        Assert.Throws<ArgumentException>(
            () => RemotingReply.FromRequestProperties(requestProperties, "reply"u8.ToArray()));
    }

    [Fact]
    public async Task SendReplyAsync_UsesReplyRequestCodeTopicAndReservedProperties()
    {
        var remoting = new RequestReplyRemotingClient();
        var producer = CreateProducer(remoting);
        await producer.StartAsync(TestContext.Current.CancellationToken);

        try
        {
            var reply = RemotingReply.FromRequestProperties(
                new Dictionary<string, string>
                {
                    ["CLUSTER"] = "cluster-a",
                    ["CORRELATION_ID"] = "correlation-id",
                    ["REPLY_TO_CLIENT"] = "request-client",
                    ["TTL"] = "1500"
                },
                "reply"u8.ToArray());
            reply.Properties["application"] = "orders";

            var result = await producer.SendReplyAsync(reply, TestContext.Current.CancellationToken);

            Assert.Equal(RemotingSendStatus.SendOk, result.Status);
            var request = Assert.Single(
                remoting.Invocations,
                invocation => invocation.Request.Code == RequestCode.SendReplyMessage).Request;
            Assert.Equal("cluster-a_REPLY_TOPIC", request.ExtFields["topic"]);
            Assert.Equal("broker-a", request.ExtFields["bname"]);
            Assert.Equal("reply"u8.ToArray(), request.Body);
            var properties = MessagePropertyCodec.Deserialize(Assert.IsType<string>(request.ExtFields["properties"]));
            Assert.Equal("reply", properties["MSG_TYPE"]);
            Assert.Equal("correlation-id", properties["CORRELATION_ID"]);
            Assert.Equal("request-client", properties["REPLY_TO_CLIENT"]);
            Assert.Equal("1500", properties["TTL"]);
            Assert.Equal("orders", properties["application"]);
        }
        finally
        {
            await producer.StopAsync(TestContext.Current.CancellationToken);
        }
    }

    private static RemotingProducer CreateProducer(RequestReplyRemotingClient remoting)
    {
        var routeService = new Mock<ITopicRouteService>(MockBehavior.Strict);
        routeService
            .Setup(value => value.GetAsync(
                It.IsAny<string>(),
                It.IsAny<bool>(),
                It.IsAny<CancellationToken>()))
            .Returns(Task.FromResult(CreateRoute()));
        return new RemotingProducer(
            Options.Create(new RemotingProducerOptions
            {
                GroupName = "request-reply-tests",
                RetryTimesWhenSendFailed = 0
            }),
            Options.Create(new RemotingClientOptions
            {
                ClientIP = "127.0.0.1",
                InstanceName = "request-reply-tests",
                HeartbeatBrokerInterval = TimeSpan.FromHours(1)
            }),
            routeService.Object,
            remoting,
            TimeProvider.System,
            NullLoggerFactory.Instance);
    }

    private static TopicRouteData CreateRoute() => new()
    {
        QueueDatas =
        [
            new QueueData { BrokerName = "broker-a", ReadQueueNums = 1, WriteQueueNums = 1, Perm = 6 }
        ],
        BrokerDatas =
        [
            new BrokerData
            {
                BrokerName = "broker-a",
                BrokerAddrs = new ConcurrentDictionary<long, string>(
                    new[] { new KeyValuePair<long, string>(0, "localhost:10911") })
            }
        ]
    };

    private static void AssertProducerHeartbeat(RemotingCommand heartbeat)
    {
        Assert.NotNull(heartbeat.Body);
        using var document = JsonDocument.Parse(heartbeat.Body);
        var root = document.RootElement;
        Assert.Equal("127.0.0.1@request-reply-tests", root.GetProperty("clientID").GetString());
        var producer = Assert.Single(root.GetProperty("producerDataSet").EnumerateArray());
        Assert.Equal("request-reply-tests", producer.GetProperty("groupName").GetString());
        Assert.Empty(root.GetProperty("consumerDataSet").EnumerateArray());
    }

    private static RemotingCommand CreateReplyCallback(string correlationId, byte[] body) => new()
    {
        Code = RequestCode.PushReplyMessageToClient,
        ExtFields = new Dictionary<string, object>
        {
            ["topic"] = "orders",
            ["sysFlag"] = "0",
            ["bornTimestamp"] = "1700000000000",
            ["storeTimestamp"] = "1700000000100",
            ["properties"] = MessagePropertyCodec.Serialize(new Dictionary<string, string>
            {
                ["CORRELATION_ID"] = correlationId,
                ["UNIQ_KEY"] = "reply-id",
                ["TAGS"] = "reply-tag",
                ["KEYS"] = "first second"
            })
        },
        Body = body
    };

    private sealed class RequestReplyRemotingClient : IRemotingClient
    {
        private readonly ConcurrentDictionary<int, RemotingRequestHandler> _requestHandlers = new();
        private readonly TaskCompletionSource<RemotingCommand> _sendMessage = new(
            TaskCreationOptions.RunContinuationsAsynchronously);

        public ConcurrentQueue<Invocation> Invocations { get; } = new();

        public int RequestHandlerCount => _requestHandlers.Count;

        public Task<RemotingCommand> InvokeAsync(
            EndPoint endPoint,
            RemotingCommand request,
            TimeSpan timeout,
            CancellationToken cancellationToken = default)
        {
            Invocations.Enqueue(new Invocation(endPoint, request));
            if (request.Code is RequestCode.SendMessage or RequestCode.SendReplyMessage)
            {
                _sendMessage.TrySetResult(request);
                return Task.FromResult(SendSuccessResponse());
            }

            return Task.FromResult(new RemotingCommand { Code = ResponseCodes.ResSuccess });
        }

        public Task InvokeOnewayAsync(
            EndPoint endPoint,
            RemotingCommand request,
            TimeSpan timeout,
            CancellationToken cancellationToken = default) => Task.CompletedTask;

        public IDisposable RegisterRequestHandler(int requestCode, RemotingRequestHandler handler)
        {
            if (!_requestHandlers.TryAdd(requestCode, handler))
            {
                throw new InvalidOperationException($"Request handler {requestCode} is already registered.");
            }

            return new Registration(() =>
                ((ICollection<KeyValuePair<int, RemotingRequestHandler>>)_requestHandlers).Remove(
                    new KeyValuePair<int, RemotingRequestHandler>(requestCode, handler)));
        }

        public ValueTask DisposeAsync() => ValueTask.CompletedTask;

        public Task<RemotingCommand> WaitForSendMessageAsync(CancellationToken cancellationToken) =>
            _sendMessage.Task.WaitAsync(cancellationToken);

        public ValueTask<RemotingRequestResult> DispatchInboundAsync(RemotingCommand request)
        {
            if (!_requestHandlers.TryGetValue(request.Code, out var handler))
            {
                return ValueTask.FromResult(RemotingRequestResult.NotHandled);
            }

            return handler(new RemotingRequestContext(
                new IPEndPoint(IPAddress.Loopback, 10911),
                request,
                CancellationToken.None));
        }

        private static RemotingCommand SendSuccessResponse() => new()
        {
            Code = ResponseCodes.ResSuccess,
            ExtFields = new Dictionary<string, object>
            {
                ["msgId"] = "offset-id",
                ["queueId"] = "0",
                ["queueOffset"] = "42",
                ["MSG_REGION"] = "DefaultRegion",
                ["TRACE_ON"] = "true"
            }
        };

        public sealed record Invocation(EndPoint EndPoint, RemotingCommand Request);

        private sealed class Registration(Action dispose) : IDisposable
        {
            private Action? _dispose = dispose;

            public void Dispose() => Interlocked.Exchange(ref _dispose, null)?.Invoke();
        }
    }
}
