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

using System.Net;
using System.Text.Json;
using EventHorizon.RocketMQ.Remoting.Consumer;
using EventHorizon.RocketMQ.Remoting.Consumer.Push.Orderly;
using EventHorizon.RocketMQ.Remoting.Protocol;
using Microsoft.Extensions.Logging.Abstractions;
using Moq;
using Xunit;

namespace EventHorizon.RocketMQ.Remoting.Tests.Consumer.Push.Orderly;

public sealed class RemotingPushQueueLockManagerTests
{
    [Fact]
    public async Task LockAsync_BrokerGrant_MapsGrantedQueuesAndNamespacedIdentity()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        RemotingCommand? captured = null;
        var remoting = new Mock<IRemotingClient>(MockBehavior.Strict);
        remoting
            .Setup(value => value.InvokeAsync(
                It.IsAny<EndPoint>(),
                It.IsAny<RemotingCommand>(),
                It.IsAny<TimeSpan>(),
                It.IsAny<CancellationToken>()))
            .Returns<EndPoint, RemotingCommand, TimeSpan, CancellationToken>((_, request, _, _) =>
            {
                captured = request;
                return Task.FromResult(new RemotingCommand
                {
                    Code = ResponseCodes.ResSuccess,
                    Body = JsonSerializer.SerializeToUtf8Bytes(new
                    {
                        lockOKMQSet = new[]
                        {
                            new { topic = "tenant-a%orders", brokerName = "broker-a", queueId = 1 },
                            new { topic = "tenant-a%unknown", brokerName = "broker-a", queueId = 9 }
                        }
                    })
                });
            });
        var manager = CreateManager(remoting.Object);
        var first = new RemotingConsumerQueue("orders", 0, "broker-a", "127.0.0.1:10911");
        var granted = new RemotingConsumerQueue("orders", 1, "broker-a", "127.0.0.1:10911");

        var result = await manager.LockAsync([first, granted], cancellationToken);

        Assert.Same(granted, Assert.Single(result.LockedQueues));
        Assert.Equal(2, result.RefreshedQueues.Count);
        Assert.Contains(first, result.RefreshedQueues);
        Assert.Contains(granted, result.RefreshedQueues);
        Assert.NotNull(captured);
        using var body = JsonDocument.Parse(captured.Body!);
        Assert.Equal("tenant-a%orders-consumer", body.RootElement.GetProperty("consumerGroup").GetString());
        Assert.Equal("10.0.0.4@consumer-instance@unit-a", body.RootElement.GetProperty("clientId").GetString());
        Assert.False(body.RootElement.GetProperty("onlyThisBroker").GetBoolean());
        var queues = body.RootElement.GetProperty("mqSet").EnumerateArray().ToArray();
        Assert.Equal(2, queues.Length);
        Assert.All(queues, queue =>
        {
            Assert.Equal("tenant-a%orders", queue.GetProperty("topic").GetString());
            Assert.Equal("broker-a", queue.GetProperty("brokerName").GetString());
        });
        Assert.Equal([0, 1], queues.Select(static queue => queue.GetProperty("queueId").GetInt32()).Order());
    }

    [Fact]
    public async Task LockAsync_OneBrokerFailure_ContinuesWithoutRefreshingFailedQueues()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var invokedPorts = new List<int>();
        var remoting = new Mock<IRemotingClient>(MockBehavior.Strict);
        remoting
            .Setup(value => value.InvokeAsync(
                It.IsAny<EndPoint>(),
                It.IsAny<RemotingCommand>(),
                It.IsAny<TimeSpan>(),
                It.IsAny<CancellationToken>()))
            .Returns<EndPoint, RemotingCommand, TimeSpan, CancellationToken>((endpoint, request, _, _) =>
            {
                var port = Assert.IsType<IPEndPoint>(endpoint).Port;
                invokedPorts.Add(port);
                if (port == 10911)
                {
                    return Task.FromException<RemotingCommand>(new IOException("expected broker failure"));
                }

                using var body = JsonDocument.Parse(request.Body!);
                return Task.FromResult(new RemotingCommand
                {
                    Code = ResponseCodes.ResSuccess,
                    Body = JsonSerializer.SerializeToUtf8Bytes(new
                    {
                        lockOKMQSet = (object)body.RootElement.GetProperty("mqSet").Clone()
                    })
                });
            });
        var manager = CreateManager(remoting.Object);
        var failed = new RemotingConsumerQueue("orders", 0, "broker-a", "127.0.0.1:10911");
        var healthy = new RemotingConsumerQueue("payments", 0, "broker-b", "127.0.0.1:10921");

        var result = await manager.LockAsync([failed, healthy], cancellationToken);

        Assert.Equal([10911, 10921], invokedPorts);
        Assert.DoesNotContain(failed, result.RefreshedQueues);
        Assert.DoesNotContain(failed, result.LockedQueues);
        Assert.Same(healthy, Assert.Single(result.RefreshedQueues));
        Assert.Same(healthy, Assert.Single(result.LockedQueues));
    }

    [Fact]
    public async Task UnlockAsync_OnewayQueueGroups_UsesNamespacedIdentity()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var invocations = new List<(EndPoint Endpoint, RemotingCommand Request, TimeSpan Timeout)>();
        var remoting = new Mock<IRemotingClient>(MockBehavior.Strict);
        remoting
            .Setup(value => value.InvokeOnewayAsync(
                It.IsAny<EndPoint>(),
                It.IsAny<RemotingCommand>(),
                It.IsAny<TimeSpan>(),
                It.IsAny<CancellationToken>()))
            .Returns<EndPoint, RemotingCommand, TimeSpan, CancellationToken>((endpoint, request, timeout, _) =>
            {
                invocations.Add((endpoint, request, timeout));
                return Task.CompletedTask;
            });
        var manager = CreateManager(remoting.Object);
        var first = new RemotingConsumerQueue("orders", 0, "broker-a", "127.0.0.1:10911");
        var second = new RemotingConsumerQueue("payments", 1, "broker-a", "127.0.0.1:10911");

        await manager.UnlockAsync([first, second], oneway: true, cancellationToken);

        var invocation = Assert.Single(invocations);
        Assert.Equal(10911, Assert.IsType<IPEndPoint>(invocation.Endpoint).Port);
        Assert.Equal(TimeSpan.FromSeconds(7), invocation.Timeout);
        Assert.Equal(RequestCode.UnlockBatchMq, invocation.Request.Code);
        using var body = JsonDocument.Parse(invocation.Request.Body!);
        Assert.Equal("tenant-a%orders-consumer", body.RootElement.GetProperty("consumerGroup").GetString());
        Assert.Equal("10.0.0.4@consumer-instance@unit-a", body.RootElement.GetProperty("clientId").GetString());
        var queues = body.RootElement.GetProperty("mqSet").EnumerateArray().ToArray();
        Assert.Equal(2, queues.Length);
        Assert.Equal(
            ["tenant-a%orders", "tenant-a%payments"],
            queues.Select(static queue => queue.GetProperty("topic").GetString()).Order(StringComparer.Ordinal));
    }

    private static RemotingPushQueueLockManager CreateManager(IRemotingClient remoting)
    {
        var options = new RemotingClientOptions
        {
            ClientIP = "10.0.0.4",
            InstanceName = "consumer-instance",
            UnitName = "unit-a",
            Namespace = "tenant-a",
            RequestTimeout = TimeSpan.FromSeconds(7)
        };
        var session = new RemotingConsumerGroupSession(
            remoting,
            options,
            "orders-consumer",
            NullLogger<RemotingConsumerGroupSession>.Instance);
        return new RemotingPushQueueLockManager(
            remoting,
            options,
            session,
            NullLogger<RemotingPushQueueLockManager>.Instance);
    }
}
