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

using EventHorizon.RocketMQ.Remoting.Protocol.Route;
using Microsoft.Extensions.Options;
using Moq;
using Xunit;

namespace EventHorizon.RocketMQ.Remoting.Tests.Protocol.Route;

public sealed class TopicRouteServiceTests
{
    [Fact]
    public async Task GetAsync_CachesUntilPollingIntervalExpires()
    {
        var nameServer = CreateNameServerMock();
        var utcNow = DateTimeOffset.Parse("2026-01-01T00:00:00Z");
        var clock = new Mock<TimeProvider>(MockBehavior.Strict);
        clock.Setup(value => value.GetUtcNow()).Returns(() => utcNow);
        var service = new TopicRouteService(
            nameServer.Object,
            Options.Create(new RemotingClientOptions { PollNameServerInterval = TimeSpan.FromSeconds(30) }),
            clock.Object);

        var first = await service.GetAsync("orders", cancellationToken: TestContext.Current.CancellationToken);
        var cached = await service.GetAsync("orders", cancellationToken: TestContext.Current.CancellationToken);
        utcNow += TimeSpan.FromSeconds(31);
        var refreshed = await service.GetAsync("orders", cancellationToken: TestContext.Current.CancellationToken);

        Assert.Same(first, cached);
        Assert.NotSame(first, refreshed);
        VerifyNameServerCalls(nameServer, 2);
    }

    [Fact]
    public async Task GetAsync_ForcedRefreshFallsBackToUnexpiredCachedRoute()
    {
        Exception? failure = null;
        var nameServer = CreateNameServerMock(() => failure);
        var utcNow = DateTimeOffset.Parse("2026-01-01T00:00:00Z");
        var clock = new Mock<TimeProvider>(MockBehavior.Strict);
        clock.Setup(value => value.GetUtcNow()).Returns(() => utcNow);
        var service = new TopicRouteService(
            nameServer.Object,
            Options.Create(new RemotingClientOptions { PollNameServerInterval = TimeSpan.FromSeconds(30) }),
            clock.Object);

        var cached = await service.GetAsync("orders", cancellationToken: TestContext.Current.CancellationToken);
        failure = new IOException("NameServer is unavailable.");
        var fallback = await service.GetAsync(
            "orders",
            forceRefresh: true,
            cancellationToken: TestContext.Current.CancellationToken);

        Assert.Same(cached, fallback);
        VerifyNameServerCalls(nameServer, 2);
    }

    [Fact]
    public async Task GetAsync_ForcedRefreshDoesNotFallBackToExpiredCachedRoute()
    {
        Exception? failure = null;
        var nameServer = CreateNameServerMock(() => failure);
        var utcNow = DateTimeOffset.Parse("2026-01-01T00:00:00Z");
        var clock = new Mock<TimeProvider>(MockBehavior.Strict);
        clock.Setup(value => value.GetUtcNow()).Returns(() => utcNow);
        var service = new TopicRouteService(
            nameServer.Object,
            Options.Create(new RemotingClientOptions { PollNameServerInterval = TimeSpan.FromSeconds(30) }),
            clock.Object);

        await service.GetAsync("orders", cancellationToken: TestContext.Current.CancellationToken);
        utcNow += TimeSpan.FromSeconds(31);
        failure = new IOException("NameServer is unavailable.");

        await Assert.ThrowsAsync<IOException>(() => service.GetAsync(
            "orders",
            forceRefresh: true,
            cancellationToken: TestContext.Current.CancellationToken));
        VerifyNameServerCalls(nameServer, 2);
    }

    private static Mock<INameServer> CreateNameServerMock(Func<Exception?>? getFailure = null)
    {
        var nameServer = new Mock<INameServer>(MockBehavior.Strict);
        nameServer
            .Setup(value => value.GetTopicRouteInfoAsync(
                It.IsAny<string>(),
                It.IsAny<TimeSpan>(),
                It.IsAny<HashSet<int>?>(),
                It.IsAny<CancellationToken>()))
            .Returns(() =>
            {
                var failure = getFailure?.Invoke();
                return failure is null
                    ? Task.FromResult(new TopicRouteData())
                    : Task.FromException<TopicRouteData>(failure);
            });
        return nameServer;
    }

    private static void VerifyNameServerCalls(Mock<INameServer> nameServer, int count) =>
        nameServer.Verify(value => value.GetTopicRouteInfoAsync(
            "orders",
            It.IsAny<TimeSpan>(),
            It.IsAny<HashSet<int>?>(),
            It.IsAny<CancellationToken>()), Times.Exactly(count));

}
