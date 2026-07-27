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

using EventHorizon.RocketMQ.Grpc.Exceptions;
using EventHorizon.RocketMQ.Grpc.Protocol;
using EventHorizon.RocketMQ.Grpc.Protocol.Route;
using Microsoft.Extensions.Options;
using Moq;
using Xunit;
using Proto = Apache.Rocketmq.V2;

namespace EventHorizon.RocketMQ.Grpc.Tests.Protocol.Route;

public sealed class GrpcRouteServiceTests
{
    [Fact]
    public async Task GetAsync_CachesClonedQueuesAndUsesNamespace()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var accessPoint = GrpcEndpoint.ToProtobuf([new Uri("http://127.0.0.1:8081")]);
        var serverQueue = Queue(3);
        Proto.QueryRouteRequest? capturedRequest = null;
        var client = new Mock<IRocketMQGrpcClient>(MockBehavior.Strict);
        client.SetupGet(value => value.AccessPointProtobuf).Returns(accessPoint);
        client.Setup(value => value.QueryRouteAsync(It.IsAny<Proto.QueryRouteRequest>(), cancellationToken))
            .Callback<Proto.QueryRouteRequest, CancellationToken>((request, _) => capturedRequest = request)
            .ReturnsAsync(Response(serverQueue));
        var service = new GrpcRouteService(client.Object, Options.Create(new GrpcClientOptions
        {
            Namespace = "tenant-a",
            RouteCacheDuration = TimeSpan.FromMinutes(5)
        }), TimeProvider.System);

        var first = await service.GetAsync("orders", false, cancellationToken);
        var second = await service.GetAsync("orders", false, cancellationToken);

        Assert.Same(first, second);
        Assert.NotSame(serverQueue, Assert.Single(first));
        Assert.Equal(3, first[0].Id);
        Assert.NotNull(capturedRequest);
        Assert.Equal("orders", capturedRequest.Topic.Name);
        Assert.Equal("tenant-a", capturedRequest.Topic.ResourceNamespace);
        Assert.Same(accessPoint, capturedRequest.Endpoints);
        client.VerifyAll();
        client.VerifyNoOtherCalls();
    }

    [Fact]
    public async Task GetAsync_RefreshesForcedAndExpiredEntries()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var timeProvider = new ManualTimeProvider(new DateTimeOffset(2026, 7, 26, 0, 0, 0, TimeSpan.Zero));
        var accessPoint = GrpcEndpoint.ToProtobuf([new Uri("http://127.0.0.1:8081")]);
        var calls = 0;
        var client = new Mock<IRocketMQGrpcClient>(MockBehavior.Strict);
        client.SetupGet(value => value.AccessPointProtobuf).Returns(accessPoint);
        client.Setup(value => value.QueryRouteAsync(It.IsAny<Proto.QueryRouteRequest>(), cancellationToken))
            .Returns<Proto.QueryRouteRequest, CancellationToken>((_, _) =>
                Task.FromResult(Response(Queue(++calls))));
        var service = new GrpcRouteService(client.Object, Options.Create(new GrpcClientOptions
        {
            RouteCacheDuration = TimeSpan.FromMinutes(1)
        }), timeProvider);

        var initial = await service.GetAsync("orders", false, cancellationToken);
        var forced = await service.GetAsync("orders", true, cancellationToken);
        timeProvider.Advance(TimeSpan.FromMinutes(1));
        var expired = await service.GetAsync("orders", false, cancellationToken);

        Assert.Equal(1, Assert.Single(initial).Id);
        Assert.Equal(2, Assert.Single(forced).Id);
        Assert.Equal(3, Assert.Single(expired).Id);
        Assert.Equal(3, calls);
        client.VerifyAll();
        client.VerifyNoOtherCalls();
    }

    [Fact]
    public async Task GetAsync_RejectsFailedAndEmptyResponses()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var accessPoint = GrpcEndpoint.ToProtobuf([new Uri("http://127.0.0.1:8081")]);
        var client = new Mock<IRocketMQGrpcClient>(MockBehavior.Strict);
        client.SetupGet(value => value.AccessPointProtobuf).Returns(accessPoint);
        client.SetupSequence(value => value.QueryRouteAsync(It.IsAny<Proto.QueryRouteRequest>(), cancellationToken))
            .ReturnsAsync(new Proto.QueryRouteResponse
            {
                Status = new Proto.Status { Code = Proto.Code.InternalError, Message = "route failed" }
            })
            .ReturnsAsync(new Proto.QueryRouteResponse
            {
                Status = new Proto.Status { Code = Proto.Code.Ok }
            });
        var service = new GrpcRouteService(
            client.Object,
            Options.Create(new GrpcClientOptions()),
            TimeProvider.System);

        var serviceException = await Assert.ThrowsAsync<GrpcServiceException>(
            () => service.GetAsync("orders", false, cancellationToken));
        var emptyException = await Assert.ThrowsAsync<InvalidOperationException>(
            () => service.GetAsync("orders", false, cancellationToken));

        Assert.Equal((int)Proto.Code.InternalError, serviceException.ResponseCode);
        Assert.Contains("orders", emptyException.Message, StringComparison.Ordinal);
        client.VerifyAll();
        client.VerifyNoOtherCalls();
    }

    [Fact]
    public async Task GetAsync_RejectsBlankTopicWithoutCallingClient()
    {
        var client = new Mock<IRocketMQGrpcClient>(MockBehavior.Strict);
        var service = new GrpcRouteService(
            client.Object,
            Options.Create(new GrpcClientOptions()),
            TimeProvider.System);

        await Assert.ThrowsAsync<ArgumentException>(
            () => service.GetAsync(" ", false, TestContext.Current.CancellationToken));

        client.VerifyNoOtherCalls();
    }

    private static Proto.QueryRouteResponse Response(Proto.MessageQueue queue)
    {
        var response = new Proto.QueryRouteResponse
        {
            Status = new Proto.Status { Code = Proto.Code.Ok }
        };
        response.MessageQueues.Add(queue);
        return response;
    }

    private static Proto.MessageQueue Queue(int id) => new()
    {
        Topic = new Proto.Resource { Name = "orders" },
        Id = id,
        Broker = new Proto.Broker { Name = "broker-a" }
    };

    private sealed class ManualTimeProvider(DateTimeOffset utcNow) : TimeProvider
    {
        private DateTimeOffset _utcNow = utcNow;

        public override DateTimeOffset GetUtcNow() => _utcNow;

        public void Advance(TimeSpan duration) => _utcNow += duration;
    }
}
