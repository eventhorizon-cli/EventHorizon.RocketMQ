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
using System.Net;
using System.Text;
using System.Text.Json;
using EventHorizon.RocketMQ.Remoting.Exceptions;
using EventHorizon.RocketMQ.Remoting.Instrumentation;
using EventHorizon.RocketMQ.Remoting.Producer;
using EventHorizon.RocketMQ.Remoting.Producer.Transactions;
using EventHorizon.RocketMQ.Remoting.Protocol;
using EventHorizon.RocketMQ.Remoting.Protocol.Route;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Moq;
using Xunit;

namespace EventHorizon.RocketMQ.Remoting.Tests.Producer;

public sealed class RemotingProducerHeartbeatTests
{
    private static readonly TimeSpan TestTimeout = TimeSpan.FromSeconds(5);

    [Fact]
    public async Task SendAsync_KnownMasterRoute_SendsPeriodicProducerHeartbeat()
    {
        var clock = new ManualTimeProvider(DateTimeOffset.UtcNow);
        var heartbeat = new TaskCompletionSource<RemotingCommand>(TaskCreationOptions.RunContinuationsAsynchronously);
        var requests = new ConcurrentQueue<RemotingCommand>();
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
            .Returns((EndPoint _, RemotingCommand request, TimeSpan _, CancellationToken _) =>
            {
                requests.Enqueue(request);
                if (request.Code == RequestCode.HeartBeat)
                {
                    heartbeat.TrySetResult(request);
                }

                return Task.FromResult(SuccessResponse());
            });
        var routes = new Mock<ITopicRouteService>(MockBehavior.Strict);
        routes
            .Setup(value => value.GetAsync(
                It.IsAny<string>(),
                It.IsAny<bool>(),
                It.IsAny<CancellationToken>()))
            .Returns(Task.FromResult(Route("broker-a", "localhost:10911")));
        var producer = new RemotingProducer(
            Options.Create(new RemotingProducerOptions { GroupName = "heartbeat-tests", RetryTimesWhenSendFailed = 0 }),
            Options.Create(new RemotingClientOptions
            {
                ClientIP = "127.0.0.1",
                InstanceName = "heartbeat-tests",
                HeartbeatBrokerInterval = TimeSpan.FromSeconds(1)
            }),
            routes.Object,
            remoting.Object,
            clock,
            NullLogger<RemotingProducer>.Instance);

        await producer.StartAsync(TestContext.Current.CancellationToken);
        try
        {
            await producer.SendAsync(
                new Message("orders", "payload"u8.ToArray()),
                TestContext.Current.CancellationToken);

            Assert.DoesNotContain(requests, request => request.Code == RequestCode.HeartBeat);

            clock.Advance(TimeSpan.FromSeconds(1));
            var request = await heartbeat.Task.WaitAsync(
                TestTimeout,
                TestContext.Current.CancellationToken);

            Assert.Equal(RequestCode.HeartBeat, request.Code);
            Assert.Equal("127.0.0.1@heartbeat-tests", ReadHeartbeat(request).ClientId);
            Assert.Equal("heartbeat-tests", ReadHeartbeat(request).ProducerGroup);
            Assert.Empty(ReadHeartbeat(request).ConsumerGroups);
        }
        finally
        {
            await producer.StopAsync(TestContext.Current.CancellationToken);
        }
    }

    [Fact]
    public async Task GetPublishMessageQueuesAsync_RouteRegistersMasters_HeartbeatsOnlyMasters()
    {
        var harness = CreateHarness(RouteWithAddresses(
            ("broker-a", "localhost:10911", "localhost:11911"),
            ("broker-b", "localhost:20911", null)));
        await harness.Producer.StartAsync(TestContext.Current.CancellationToken);

        try
        {
            await harness.Producer.GetPublishMessageQueuesAsync(
                "orders",
                TestContext.Current.CancellationToken);
            await harness.Clock.WaitForTimerCountAsync(1, TestContext.Current.CancellationToken);
            harness.Clock.Advance(TimeSpan.FromSeconds(1));

            var heartbeats = await harness.WaitForHeartbeatsAsync(2, TestContext.Current.CancellationToken);
            var endpoints = heartbeats.Select(static invocation => EndpointName(invocation.EndPoint)).ToHashSet();
            Assert.Equal(["localhost:10911", "localhost:20911"], endpoints.OrderBy(static value => value));
            Assert.DoesNotContain("localhost:11911", endpoints);
        }
        finally
        {
            await harness.Producer.StopAsync(TestContext.Current.CancellationToken);
        }
    }

    [Fact]
    public async Task SendAsync_BatchRoute_SendsPeriodicProducerHeartbeat()
    {
        await AssertRouteOperationRegistersHeartbeatAsync(static harness => harness.Producer.SendAsync(
            [
                new Message("orders", "first"u8.ToArray()),
                new Message("orders", "second"u8.ToArray())
            ],
            TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task SendOnewayAsync_KnownRoute_SendsPeriodicProducerHeartbeat()
    {
        await AssertRouteOperationRegistersHeartbeatAsync(static harness => harness.Producer.SendOnewayAsync(
            new Message("orders", "payload"u8.ToArray()),
            TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task SendTransactionAsync_KnownRoute_SendsPeriodicProducerHeartbeat()
    {
        await AssertRouteOperationRegistersHeartbeatAsync(
            static harness => harness.Producer.SendTransactionAsync(
                new Message("orders", "payload"u8.ToArray()),
                cancellationToken: TestContext.Current.CancellationToken),
            static options =>
            {
                options.LocalTransactionExecutor = static (_, _, _) =>
                    ValueTask.FromResult(RemotingTransactionResolution.Commit);
                options.TransactionChecker = static (_, _) =>
                    ValueTask.FromResult(RemotingTransactionResolution.Unknown);
            });
    }

    [Fact]
    public async Task RecallAsync_KnownRoute_SendsPeriodicProducerHeartbeat()
    {
        var recallHandle = Convert.ToBase64String(
                Encoding.UTF8.GetBytes("v1 orders broker-a 1893456000000 message-id"))
            .TrimEnd('=')
            .Replace('+', '-')
            .Replace('/', '_');
        await AssertRouteOperationRegistersHeartbeatAsync(static harness => harness.Producer.RecallAsync(
            "orders",
            harness.RecallHandle,
            TestContext.Current.CancellationToken),
            configureHarness: harness => harness.RecallHandle = recallHandle);
    }

    [Fact]
    public async Task SendReplyAsync_KnownRoute_SendsPeriodicProducerHeartbeat()
    {
        await AssertRouteOperationRegistersHeartbeatAsync(static harness =>
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
            return harness.Producer.SendReplyAsync(reply, TestContext.Current.CancellationToken);
        });
    }

    [Fact]
    public async Task ProducerHeartbeat_BrokerFailure_DoesNotBlockOtherBrokersAndRecoversNextCycle()
    {
        var harness = CreateHarness(Route(
            ("broker-a", "localhost:10911"),
            ("broker-b", "localhost:20911")));
        var counts = new ConcurrentDictionary<string, int>(StringComparer.Ordinal);
        var firstCycle = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var secondCycle = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        harness.InvocationHandler = invocation =>
        {
            if (invocation.Request.Code == RequestCode.HeartBeat)
            {
                var endpoint = EndpointName(invocation.EndPoint);
                var count = counts.AddOrUpdate(endpoint, 1, static (_, previous) => previous + 1);
                var total = counts.Values.Sum();
                if (total >= 2)
                {
                    firstCycle.TrySetResult();
                }

                if (total >= 4)
                {
                    secondCycle.TrySetResult();
                }

                if (endpoint == "localhost:10911" && count == 1)
                {
                    return Task.FromException<RemotingCommand>(new IOException("broker-a unavailable"));
                }
            }

            return Task.FromResult(SuccessResponse());
        };
        await harness.Producer.StartAsync(TestContext.Current.CancellationToken);

        try
        {
            await harness.Producer.SendAsync(
                new Message("orders", "payload"u8.ToArray()),
                TestContext.Current.CancellationToken);
            await harness.Clock.WaitForTimerCountAsync(1, TestContext.Current.CancellationToken);

            harness.Clock.Advance(TimeSpan.FromSeconds(1));
            await firstCycle.Task.WaitAsync(
                TestTimeout,
                TestContext.Current.CancellationToken);
            await harness.Clock.WaitForTimerCountAsync(2, TestContext.Current.CancellationToken);

            harness.Clock.Advance(TimeSpan.FromSeconds(1));
            await secondCycle.Task.WaitAsync(
                TestTimeout,
                TestContext.Current.CancellationToken);

            Assert.Equal(2, counts["localhost:10911"]);
            Assert.Equal(2, counts["localhost:20911"]);
        }
        finally
        {
            await harness.Producer.StopAsync(TestContext.Current.CancellationToken);
        }
    }

    [Fact]
    public async Task RequestAsync_AnotherBrokerHeartbeatBlocked_CompletesOnHealthyBroker()
    {
        var harness = CreateHarness(Route(
            ("broker-a", "localhost:10911"),
            ("broker-b", "localhost:20911")));
        var heartbeatStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseHeartbeat = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        RemotingRequestHandler? replyHandler = null;
        harness.Remoting
            .Setup(value => value.RegisterRequestHandler(
                RequestCode.PushReplyMessageToClient,
                It.IsAny<RemotingRequestHandler>()))
            .Callback<int, RemotingRequestHandler>((_, handler) => replyHandler = handler)
            .Returns(new NoopRegistration());
        harness.InvocationHandler = async invocation =>
        {
            if (invocation.Request.Code == RequestCode.HeartBeat &&
                EndpointName(invocation.EndPoint) == "localhost:10911")
            {
                heartbeatStarted.TrySetResult();
                await releaseHeartbeat.Task.WaitAsync(invocation.CancellationToken);
            }

            if (invocation.Request.Code == RequestCode.SendMessage)
            {
                Assert.Equal("localhost:20911", EndpointName(invocation.EndPoint));
                var properties = MessagePropertyCodec.Deserialize(
                    Assert.IsType<string>(invocation.Request.ExtFields["properties"]));
                var callback = new RemotingCommand(RequestCode.PushReplyMessageToClient)
                {
                    ExtFields = new Dictionary<string, object>
                    {
                        ["topic"] = "orders",
                        ["sysFlag"] = "0",
                        ["bornTimestamp"] = "1700000000000",
                        ["storeTimestamp"] = "1700000000100",
                        ["properties"] = MessagePropertyCodec.Serialize(new Dictionary<string, string>
                        {
                            ["CORRELATION_ID"] = properties["CORRELATION_ID"],
                            ["UNIQ_KEY"] = "reply-id"
                        })
                    },
                    Body = "reply"u8.ToArray()
                };
                Assert.NotNull(replyHandler);
                var result = await replyHandler(new RemotingRequestContext(
                    invocation.EndPoint, callback, invocation.CancellationToken));
                Assert.True(result.IsHandled);
            }

            return SuccessResponse();
        };
        await harness.Producer.StartAsync(TestContext.Current.CancellationToken);

        try
        {
            await harness.Producer.GetPublishMessageQueuesAsync("orders", TestContext.Current.CancellationToken);
            await harness.Clock.WaitForTimerCountAsync(1, TestContext.Current.CancellationToken);
            harness.Clock.Advance(TimeSpan.FromSeconds(1));
            await heartbeatStarted.Task.WaitAsync(TestTimeout, TestContext.Current.CancellationToken);

            var reply = await harness.Producer.RequestAsync(
                new Message("orders", "request"u8.ToArray()),
                new RemotingMessageQueue("orders", "broker-b", 0),
                TestTimeout,
                TestContext.Current.CancellationToken);

            Assert.Equal("reply"u8.ToArray(), reply.Body);
            Assert.False(releaseHeartbeat.Task.IsCompleted);
            Assert.Contains(harness.HeartbeatInvocations,
                invocation => EndpointName(invocation.EndPoint) == "localhost:20911");
        }
        finally
        {
            releaseHeartbeat.TrySetResult();
            await harness.Producer.StopAsync(TestContext.Current.CancellationToken);
        }
    }

    [Fact]
    public async Task SendAsync_UpdatedMasterRoute_HeartbeatsLatestMasterAddress()
    {
        var harness = CreateHarness(
            Route("broker-a", "localhost:10911"),
            Route("broker-a", "localhost:21911"));
        await harness.Producer.StartAsync(TestContext.Current.CancellationToken);

        try
        {
            await harness.Producer.SendAsync(
                new Message("orders", "first"u8.ToArray()),
                TestContext.Current.CancellationToken);
            await harness.Producer.SendAsync(
                new Message("orders", "second"u8.ToArray()),
                TestContext.Current.CancellationToken);
            await harness.Clock.WaitForTimerCountAsync(1, TestContext.Current.CancellationToken);
            harness.Clock.Advance(TimeSpan.FromSeconds(1));

            var heartbeat = Assert.Single(
                await harness.WaitForHeartbeatsAsync(1, TestContext.Current.CancellationToken));
            Assert.Equal("localhost:21911", EndpointName(heartbeat.EndPoint));
        }
        finally
        {
            await harness.Producer.StopAsync(TestContext.Current.CancellationToken);
        }
    }

    [Fact]
    public async Task StopAsync_InFlightHeartbeat_CancelsAndRestartDoesNotReuseOldTargets()
    {
        var harness = CreateHarness(Route("broker-a", "localhost:10911"));
        var heartbeatStarted = new TaskCompletionSource<TestHarness.Invocation>(
            TaskCreationOptions.RunContinuationsAsynchronously);
        harness.InvocationHandler = async invocation =>
        {
            if (invocation.Request.Code == RequestCode.HeartBeat)
            {
                heartbeatStarted.TrySetResult(invocation);
                await Task.Delay(Timeout.InfiniteTimeSpan, invocation.CancellationToken);
            }

            return SuccessResponse();
        };
        await harness.Producer.StartAsync(TestContext.Current.CancellationToken);

        try
        {
            await harness.Producer.SendAsync(
                new Message("orders", "payload"u8.ToArray()),
                TestContext.Current.CancellationToken);
            await harness.Clock.WaitForTimerCountAsync(1, TestContext.Current.CancellationToken);
            harness.Clock.Advance(TimeSpan.FromSeconds(1));
            var heartbeat = await heartbeatStarted.Task.WaitAsync(
                TestTimeout,
                TestContext.Current.CancellationToken);

            await harness.Producer.StopAsync(TestContext.Current.CancellationToken);

            Assert.True(heartbeat.CancellationToken.IsCancellationRequested);
            Assert.Contains(
                harness.Invocations,
                invocation => invocation.Request.Code == RequestCode.UnregisterClient);

            var heartbeatCountAfterStop = harness.HeartbeatInvocations.Count;
            await harness.Producer.StartAsync(TestContext.Current.CancellationToken);
            await harness.Clock.WaitForTimerCountAsync(2, TestContext.Current.CancellationToken);
            harness.Clock.Advance(TimeSpan.FromSeconds(1));
            await harness.Clock.WaitForTimerCountAsync(3, TestContext.Current.CancellationToken);

            Assert.Equal(heartbeatCountAfterStop, harness.HeartbeatInvocations.Count);
        }
        finally
        {
            await harness.Producer.StopAsync(TestContext.Current.CancellationToken);
        }
    }

    [Fact]
    public async Task SendAsync_RouteCompletesAfterRestart_DoesNotRegisterTargetInNewSession()
    {
        var harness = CreateHarness(Route("broker-a", "localhost:10911"));
        var routeEntered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseRoute = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var routeCalls = 0;
        harness.RouteHandler = async (_, _, _) =>
        {
            if (Interlocked.Increment(ref routeCalls) == 1)
            {
                routeEntered.TrySetResult();
                await releaseRoute.Task;
            }

            return Route("broker-a", "localhost:10911");
        };
        await harness.Producer.StartAsync(TestContext.Current.CancellationToken);

        try
        {
            var pendingSend = harness.Producer.SendAsync(
                new Message("orders", "payload"u8.ToArray()),
                TestContext.Current.CancellationToken);
            await routeEntered.Task.WaitAsync(
                TestTimeout,
                TestContext.Current.CancellationToken);

            await harness.Producer.StopAsync(TestContext.Current.CancellationToken);
            await harness.Producer.StartAsync(TestContext.Current.CancellationToken);
            await harness.Clock.WaitForTimerCountAsync(2, TestContext.Current.CancellationToken);

            releaseRoute.TrySetResult();
            await pendingSend;
            harness.Clock.Advance(TimeSpan.FromSeconds(1));
            await harness.Clock.WaitForTimerCountAsync(3, TestContext.Current.CancellationToken);

            Assert.Empty(harness.HeartbeatInvocations);
        }
        finally
        {
            await harness.Producer.StopAsync(TestContext.Current.CancellationToken);
        }
    }

    [Theory]
    [InlineData("success")]
    [InlineData("failure")]
    [InlineData("cancelled")]
    public async Task ProducerHeartbeat_MessageTelemetry_DoesNotStartHeartbeatOperation(string outcome)
    {
        var operation = new Mock<IRemotingRocketMQTelemetryOperation>(MockBehavior.Strict);
        operation
            .SetupGet(value => value.Activity)
            .Returns((Activity?)null);
        operation.Setup(value => value.Complete());
        operation.Setup(value => value.Complete(It.IsAny<Exception>()));
        operation.Setup(value => value.Complete(It.IsAny<bool>(), It.IsAny<string?>()));
        operation.Setup(value => value.Dispose());
        var telemetry = new Mock<IRemotingRocketMQTelemetry>(MockBehavior.Strict);
        telemetry
            .Setup(value => value.StartSend("orders", 1, 7))
            .Returns(operation.Object);
        telemetry
            .Setup(value => value.InjectContext(
                It.IsAny<Activity?>(),
                It.IsAny<IDictionary<string, string>>()));
        var harness = new TestHarness(
            [Route("broker-a", "localhost:10911")],
            telemetry: telemetry.Object);
        using var callerCancellation = new CancellationTokenSource();
        harness.InvocationHandler = invocation =>
        {
            if (invocation.Request.Code == RequestCode.SendMessage)
            {
                if (outcome == "failure")
                {
                    return Task.FromException<RemotingCommand>(new IOException("send failed"));
                }

                if (outcome == "cancelled")
                {
                    callerCancellation.Cancel();
                    return Task.FromCanceled<RemotingCommand>(callerCancellation.Token);
                }
            }

            return Task.FromResult(SuccessResponse());
        };
        await harness.Producer.StartAsync(TestContext.Current.CancellationToken);

        try
        {
            if (outcome == "success")
            {
                await harness.Producer.SendAsync(
                    new Message("orders", "payload"u8.ToArray()),
                    TestContext.Current.CancellationToken);
            }
            else if (outcome == "failure")
            {
                await Assert.ThrowsAsync<RocketMQClientException>(() => harness.Producer.SendAsync(
                    new Message("orders", "payload"u8.ToArray()),
                    TestContext.Current.CancellationToken));
            }
            else
            {
                await Assert.ThrowsAnyAsync<OperationCanceledException>(() => harness.Producer.SendAsync(
                    new Message("orders", "payload"u8.ToArray()),
                    callerCancellation.Token));
            }

            await harness.Clock.WaitForTimerCountAsync(1, TestContext.Current.CancellationToken);
            harness.Clock.Advance(TimeSpan.FromSeconds(1));
            await harness.WaitForHeartbeatsAsync(1, TestContext.Current.CancellationToken);

            telemetry.Verify(value => value.StartSend("orders", 1, 7), Times.Once);
            if (outcome == "success")
            {
                operation.Verify(value => value.Complete(), Times.Once);
                operation.Verify(value => value.Complete(It.IsAny<Exception>()), Times.Never);
            }
            else
            {
                operation.Verify(value => value.Complete(It.IsAny<Exception>()), Times.Once);
                operation.Verify(value => value.Complete(), Times.Never);
            }

            operation.Verify(value => value.Dispose(), Times.Once);
        }
        finally
        {
            await harness.Producer.StopAsync(TestContext.Current.CancellationToken);
        }
    }

    private static TestHarness CreateHarness(params TopicRouteData[] routes) => new(routes);

    private static async Task AssertRouteOperationRegistersHeartbeatAsync(
        Func<TestHarness, Task> operation,
        Action<RemotingProducerOptions>? configureOptions = null,
        Action<TestHarness>? configureHarness = null)
    {
        var harness = new TestHarness(
            [Route("broker-a", "localhost:10911")],
            configureOptions);
        configureHarness?.Invoke(harness);

        await harness.Producer.StartAsync(TestContext.Current.CancellationToken);
        try
        {
            await operation(harness);
            await harness.Clock.WaitForTimerCountAsync(1, TestContext.Current.CancellationToken);
            harness.Clock.Advance(TimeSpan.FromSeconds(1));

            var heartbeat = Assert.Single(
                await harness.WaitForHeartbeatsAsync(1, TestContext.Current.CancellationToken));
            Assert.Equal("localhost:10911", EndpointName(heartbeat.EndPoint));
        }
        finally
        {
            await harness.Producer.StopAsync(TestContext.Current.CancellationToken);
        }
    }

    private static string EndpointName(EndPoint endpoint) => endpoint switch
    {
        DnsEndPoint dnsEndPoint => $"{dnsEndPoint.Host}:{dnsEndPoint.Port}",
        IPEndPoint ipEndPoint => $"{ipEndPoint.Address}:{ipEndPoint.Port}",
        _ => endpoint.ToString() ?? string.Empty
    };

    private static TopicRouteData Route(params (string BrokerName, string Address)[] brokers) => new()
    {
        QueueDatas = brokers
            .Select(static broker => new QueueData
            {
                BrokerName = broker.BrokerName,
                ReadQueueNums = 1,
                WriteQueueNums = 1,
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

    private static TopicRouteData RouteWithAddresses(
        params (string BrokerName, string MasterAddress, string? SlaveAddress)[] brokers) => new()
        {
            QueueDatas = brokers
            .Select(static broker => new QueueData
            {
                BrokerName = broker.BrokerName,
                ReadQueueNums = 1,
                WriteQueueNums = 1,
                Perm = 6
            })
            .ToArray(),
            BrokerDatas = brokers
            .Select(static broker => new BrokerData
            {
                BrokerName = broker.BrokerName,
                BrokerAddrs = CreateBrokerAddresses(broker.MasterAddress, broker.SlaveAddress)
            })
            .ToArray()
        };

    private static ConcurrentDictionary<long, string> CreateBrokerAddresses(
        string masterAddress,
        string? slaveAddress)
    {
        var addresses = new ConcurrentDictionary<long, string>(new[]
        {
            new KeyValuePair<long, string>(0, masterAddress)
        });
        if (slaveAddress is not null)
        {
            addresses[1] = slaveAddress;
        }

        return addresses;
    }

    private static HeartbeatPayload ReadHeartbeat(RemotingCommand request)
    {
        Assert.NotNull(request.Body);
        using var document = JsonDocument.Parse(request.Body);
        var root = document.RootElement;
        var producer = Assert.Single(root.GetProperty("producerDataSet").EnumerateArray());
        return new HeartbeatPayload(
            root.GetProperty("clientID").GetString()!,
            producer.GetProperty("groupName").GetString()!,
            root.GetProperty("consumerDataSet").EnumerateArray().ToArray());
    }

    private static TopicRouteData Route(string brokerName, string address) => new()
    {
        QueueDatas =
        [
            new QueueData { BrokerName = brokerName, ReadQueueNums = 1, WriteQueueNums = 1, Perm = 6 }
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

    private static RemotingCommand SuccessResponse() => new()
    {
        Code = ResponseCodes.ResSuccess,
        ExtFields = new Dictionary<string, object>
        {
            ["msgId"] = "00000000000000000000000000000000",
            ["queueId"] = "0",
            ["queueOffset"] = "0",
            ["MSG_REGION"] = "DefaultRegion",
            ["TRACE_ON"] = "true"
        }
    };

    private sealed record HeartbeatPayload(
        string ClientId,
        string ProducerGroup,
        JsonElement[] ConsumerGroups);

    private sealed class NoopRegistration : IDisposable
    {
        public void Dispose()
        {
        }
    }

    private sealed class TestHarness
    {
        private readonly TopicRouteData[] _routes;
        private int _routeIndex;

        public TestHarness(
            TopicRouteData[] routes,
            Action<RemotingProducerOptions>? configureOptions = null,
            IRemotingRocketMQTelemetry? telemetry = null)
        {
            _routes = routes;
            Clock = new ManualTimeProvider(DateTimeOffset.UtcNow);
            Remoting = new Mock<IRemotingClient>(MockBehavior.Strict);
            Routes = new Mock<ITopicRouteService>(MockBehavior.Strict);
            Invocations = new ConcurrentQueue<Invocation>();
            InvocationHandler = static _ => Task.FromResult(SuccessResponse());
            OnewayHandler = static _ => Task.CompletedTask;
            RouteHandler = (topic, forceRefresh, cancellationToken) =>
            {
                var index = Math.Min(Interlocked.Increment(ref _routeIndex) - 1, _routes.Length - 1);
                return Task.FromResult(_routes[index]);
            };

            Remoting
                .Setup(value => value.RegisterRequestHandler(
                    It.IsAny<int>(),
                    It.IsAny<RemotingRequestHandler>()))
                .Returns(new NoopRegistration());
            Remoting
                .Setup(value => value.InvokeAsync(
                    It.IsAny<EndPoint>(),
                    It.IsAny<RemotingCommand>(),
                    It.IsAny<TimeSpan>(),
                    It.IsAny<CancellationToken>()))
                .Returns((EndPoint endPoint, RemotingCommand request, TimeSpan _, CancellationToken cancellationToken) =>
                {
                    var invocation = new Invocation(endPoint, request, cancellationToken);
                    Invocations.Enqueue(invocation);
                    return InvocationHandler(invocation);
                });
            Remoting
                .Setup(value => value.InvokeOnewayAsync(
                    It.IsAny<EndPoint>(),
                    It.IsAny<RemotingCommand>(),
                    It.IsAny<TimeSpan>(),
                    It.IsAny<CancellationToken>()))
                .Returns((EndPoint endPoint, RemotingCommand request, TimeSpan _, CancellationToken cancellationToken) =>
                {
                    var invocation = new Invocation(endPoint, request, cancellationToken);
                    Invocations.Enqueue(invocation);
                    return OnewayHandler(invocation);
                });
            Routes
                .Setup(value => value.GetAsync(
                    It.IsAny<string>(),
                    It.IsAny<bool>(),
                    It.IsAny<CancellationToken>()))
                .Returns<string, bool, CancellationToken>(
                    (topic, forceRefresh, cancellationToken) => RouteHandler(topic, forceRefresh, cancellationToken));

            var options = new RemotingProducerOptions
            {
                GroupName = "heartbeat-tests",
                RetryTimesWhenSendFailed = 0
            };
            configureOptions?.Invoke(options);
            Producer = new RemotingProducer(
                Options.Create(options),
                Options.Create(new RemotingClientOptions
                {
                    ClientIP = "127.0.0.1",
                    InstanceName = "heartbeat-tests",
                    HeartbeatBrokerInterval = TimeSpan.FromSeconds(1),
                    RequestTimeout = TimeSpan.FromSeconds(1)
                }),
                Routes.Object,
                Remoting.Object,
                Clock,
                NullLogger<RemotingProducer>.Instance,
                telemetry);
        }

        public ManualTimeProvider Clock { get; }

        public Mock<IRemotingClient> Remoting { get; }

        public Mock<ITopicRouteService> Routes { get; }

        public RemotingProducer Producer { get; }

        public ConcurrentQueue<Invocation> Invocations { get; }

        public Func<Invocation, Task<RemotingCommand>> InvocationHandler { get; set; }

        public Func<Invocation, Task> OnewayHandler { get; set; }

        public Func<string, bool, CancellationToken, Task<TopicRouteData>> RouteHandler { get; set; }

        public string RecallHandle { get; set; } = Convert.ToBase64String(
                Encoding.UTF8.GetBytes("v1 orders broker-a 1893456000000 message-id"))
            .TrimEnd('=')
            .Replace('+', '-')
            .Replace('/', '_');

        public IReadOnlyList<Invocation> HeartbeatInvocations =>
            Invocations.Where(static invocation => invocation.Request.Code == RequestCode.HeartBeat).ToArray();

        public async Task<IReadOnlyList<Invocation>> WaitForHeartbeatsAsync(
            int count,
            CancellationToken cancellationToken)
        {
            using var timeout = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            timeout.CancelAfter(TestTimeout);
            while (true)
            {
                var heartbeats = HeartbeatInvocations;
                if (heartbeats.Count >= count)
                {
                    return heartbeats;
                }

                await Task.Delay(TimeSpan.FromMilliseconds(1), timeout.Token).ConfigureAwait(false);
            }
        }

        public sealed record Invocation(
            EndPoint EndPoint,
            RemotingCommand Request,
            CancellationToken CancellationToken);
    }

    private sealed class ManualTimeProvider(DateTimeOffset utcNow) : TimeProvider
    {
        private readonly object _gate = new();
        private readonly List<ManualTimer> _timers = [];
        private readonly List<(int Count, TaskCompletionSource Completion)> _timerWaiters = [];
        private DateTimeOffset _utcNow = utcNow;
        private int _createdTimerCount;

        public override DateTimeOffset GetUtcNow() => _utcNow;

        public override ITimer CreateTimer(
            TimerCallback callback,
            object? state,
            TimeSpan dueTime,
            TimeSpan period)
        {
            var timer = new ManualTimer(this, callback, state, dueTime, period);
            List<TaskCompletionSource>? completedWaiters = null;
            lock (_gate)
            {
                _timers.Add(timer);
                _createdTimerCount++;
                foreach (var waiter in _timerWaiters.Where(waiter => waiter.Count <= _createdTimerCount).ToArray())
                {
                    completedWaiters ??= [];
                    completedWaiters.Add(waiter.Completion);
                    _timerWaiters.Remove(waiter);
                }
            }

            if (completedWaiters is not null)
            {
                foreach (var waiter in completedWaiters)
                {
                    waiter.TrySetResult();
                }
            }

            return timer;
        }

        public Task WaitForTimerCountAsync(int count, CancellationToken cancellationToken)
        {
            Task task;
            lock (_gate)
            {
                if (_createdTimerCount >= count)
                {
                    return Task.CompletedTask;
                }

                var completion = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
                _timerWaiters.Add((count, completion));
                task = completion.Task;
            }

            return task.WaitAsync(TestTimeout, cancellationToken);
        }

        public void Advance(TimeSpan duration)
        {
            List<(TimerCallback Callback, object? State)> callbacks = [];
            lock (_gate)
            {
                _utcNow += duration;
                foreach (var timer in _timers.ToArray())
                {
                    timer.CollectDueCallbacks(_utcNow, callbacks);
                }
            }

            foreach (var (callback, state) in callbacks)
            {
                callback(state);
            }
        }

        private void Remove(ManualTimer timer)
        {
            lock (_gate)
            {
                _timers.Remove(timer);
            }
        }

        private sealed class ManualTimer(
            ManualTimeProvider owner,
            TimerCallback callback,
            object? state,
            TimeSpan dueTime,
            TimeSpan period) : ITimer
        {
            private readonly object _gate = new();
            private DateTimeOffset? _dueAt = GetDueAt(owner.GetUtcNow(), dueTime);
            private TimeSpan _period = period;
            private bool _disposed;

            public bool Change(TimeSpan newDueTime, TimeSpan newPeriod)
            {
                lock (_gate)
                {
                    if (_disposed)
                    {
                        return false;
                    }

                    _dueAt = GetDueAt(owner.GetUtcNow(), newDueTime);
                    _period = newPeriod;
                    return true;
                }
            }

            public void Dispose()
            {
                lock (_gate)
                {
                    if (_disposed)
                    {
                        return;
                    }

                    _disposed = true;
                    _dueAt = null;
                }

                owner.Remove(this);
            }

            public ValueTask DisposeAsync()
            {
                Dispose();
                return ValueTask.CompletedTask;
            }

            public void CollectDueCallbacks(
                DateTimeOffset now,
                List<(TimerCallback Callback, object? State)> callbacks)
            {
                lock (_gate)
                {
                    if (_disposed || _dueAt is null || _dueAt > now)
                    {
                        return;
                    }

                    callbacks.Add((callback, state));
                    if (_period == Timeout.InfiniteTimeSpan || _period <= TimeSpan.Zero)
                    {
                        _dueAt = null;
                    }
                    else
                    {
                        _dueAt = _dueAt.Value + _period;
                    }
                }
            }

            private static DateTimeOffset? GetDueAt(DateTimeOffset now, TimeSpan dueTime) =>
                dueTime == Timeout.InfiniteTimeSpan ? null : now + dueTime;
        }
    }
}
