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
using EventHorizon.RocketMQ.Remoting.Consumer;
using EventHorizon.RocketMQ.Remoting.Protocol;
using Microsoft.Extensions.Logging.Abstractions;
using Moq;
using Xunit;

namespace EventHorizon.RocketMQ.Remoting.Tests.Consumer;

public sealed class RemotingRebalanceServiceTests
{
    private static readonly TimeSpan TestTimeout = TimeSpan.FromSeconds(5);

    [Fact]
    public async Task BrokerNotification_WakesMatchingParticipantWithoutRunningRebalanceInline()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var firstStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var secondCalls = 0;
        var fixture = CreateService();
        await using var service = fixture.Service;
        await using var first = service.Register("orders", async cancellationToken =>
        {
            firstStarted.TrySetResult();
            await release.Task.WaitAsync(cancellationToken);
            return true;
        });
        await using var second = service.Register("payments", async cancellationToken =>
        {
            Interlocked.Increment(ref secondCalls);
            await release.Task.WaitAsync(cancellationToken);
            return true;
        });

        try
        {
            var result = await fixture.Handler(CreateNotification("orders"))
                .AsTask()
                .WaitAsync(TimeSpan.FromMilliseconds(250), cancellationToken);

            Assert.True(result.IsHandled);
            Assert.True(result.SuppressResponse);
            Assert.False(release.Task.IsCompleted);
            await firstStarted.Task.WaitAsync(TestTimeout, cancellationToken);
            Assert.Equal(0, Volatile.Read(ref secondCalls));
        }
        finally
        {
            release.TrySetResult();
        }
    }

    [Fact]
    public async Task BrokerNotification_ForUnknownGroup_IsSafelyIgnored()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var calls = 0;
        var fixture = CreateService();
        await using var service = fixture.Service;
        await using var registration = service.Register("orders", _ =>
        {
            Interlocked.Increment(ref calls);
            return Task.FromResult(true);
        });

        var result = await fixture.Handler(CreateNotification("payments"));
        await Task.Delay(TimeSpan.FromMilliseconds(50), cancellationToken);

        Assert.True(result.IsHandled);
        Assert.True(result.SuppressResponse);
        Assert.Equal(0, Volatile.Read(ref calls));
    }

    [Fact]
    public async Task BrokerNotification_WithoutGroup_WakesEveryParticipant()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var ordersCalled = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var paymentsCalled = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var fixture = CreateService();
        await using var service = fixture.Service;
        await using var orders = service.Register("orders", _ =>
        {
            ordersCalled.TrySetResult();
            return Task.FromResult(true);
        });
        await using var payments = service.Register("payments", _ =>
        {
            paymentsCalled.TrySetResult();
            return Task.FromResult(true);
        });

        var result = await fixture.Handler(CreateNotification()).AsTask().WaitAsync(TestTimeout, cancellationToken);

        Assert.True(result.IsHandled);
        Assert.True(result.SuppressResponse);
        await Task.WhenAll(ordersCalled.Task, paymentsCalled.Task).WaitAsync(TestTimeout, cancellationToken);
    }

    [Fact]
    public async Task WakeupStorm_IsCoalescedAndNeverRunsParticipantConcurrently()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var firstStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var secondCompleted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseFirst = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var calls = 0;
        var inFlight = 0;
        var maximumInFlight = 0;
        var fixture = CreateService(retryInterval: TimeSpan.FromMilliseconds(20));
        await using var service = fixture.Service;
        await using var registration = service.Register("orders", async token =>
        {
            var currentInFlight = Interlocked.Increment(ref inFlight);
            UpdateMaximum(ref maximumInFlight, currentInFlight);
            try
            {
                var currentCall = Interlocked.Increment(ref calls);
                if (currentCall == 1)
                {
                    firstStarted.TrySetResult();
                    await releaseFirst.Task.WaitAsync(token);
                }
                else
                {
                    secondCompleted.TrySetResult();
                }

                return true;
            }
            finally
            {
                Interlocked.Decrement(ref inFlight);
            }
        });

        try
        {
            registration.Wakeup();
            await firstStarted.Task.WaitAsync(TestTimeout, cancellationToken);
            for (var index = 0; index < 100; index++)
            {
                registration.Wakeup();
            }

            releaseFirst.TrySetResult();
            await secondCompleted.Task.WaitAsync(TestTimeout, cancellationToken);
            await Task.Delay(TimeSpan.FromMilliseconds(100), cancellationToken);

            Assert.Equal(2, Volatile.Read(ref calls));
            Assert.Equal(1, Volatile.Read(ref maximumInFlight));
        }
        finally
        {
            releaseFirst.TrySetResult();
        }
    }

    [Fact]
    public async Task ParticipantFailure_IsolatedAndRetriedAtTheFastInterval()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var failingCalls = 0;
        var healthyCalled = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var retried = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var fixture = CreateService(retryInterval: TimeSpan.FromMilliseconds(20));
        await using var service = fixture.Service;
        await using var failing = service.Register("orders", _ =>
        {
            if (Interlocked.Increment(ref failingCalls) >= 2)
            {
                retried.TrySetResult();
            }

            throw new InvalidOperationException("expected test failure");
        });
        await using var healthy = service.Register("payments", _ =>
        {
            healthyCalled.TrySetResult();
            return Task.FromResult(true);
        });

        failing.Wakeup();
        healthy.Wakeup();

        await healthyCalled.Task.WaitAsync(TestTimeout, cancellationToken);
        await retried.Task.WaitAsync(TestTimeout, cancellationToken);
        Assert.True(Volatile.Read(ref failingCalls) >= 2);
    }

    [Fact]
    public async Task ParticipantIncompleteResult_IsRetriedAtTheFastInterval()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var calls = 0;
        var retried = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var fixture = CreateService(retryInterval: TimeSpan.FromMilliseconds(20));
        await using var service = fixture.Service;
        await using var registration = service.Register("orders", _ =>
        {
            var currentCall = Interlocked.Increment(ref calls);
            if (currentCall >= 2)
            {
                retried.TrySetResult();
            }

            return Task.FromResult(currentCall >= 2);
        });

        registration.Wakeup();

        await retried.Task.WaitAsync(TestTimeout, cancellationToken);
        Assert.Equal(2, Volatile.Read(ref calls));
    }

    [Fact]
    public async Task BlockedParticipant_DoesNotDelayAnotherGroup()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var blockedStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var healthyCalled = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var fixture = CreateService();
        await using var service = fixture.Service;
        await using var blocked = service.Register("orders", async _ =>
        {
            blockedStarted.TrySetResult();
            await release.Task;
            return true;
        });
        await using var healthy = service.Register("payments", _ =>
        {
            healthyCalled.TrySetResult();
            return Task.FromResult(true);
        });

        try
        {
            blocked.Wakeup();
            await blockedStarted.Task.WaitAsync(TestTimeout, cancellationToken);
            healthy.Wakeup();
            await healthyCalled.Task.WaitAsync(TestTimeout, cancellationToken);
        }
        finally
        {
            release.TrySetResult();
        }
    }

    [Fact]
    public async Task Unregister_WaitsForInFlightRebalanceAndPreventsLaterCalls()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var calls = 0;
        var fixture = CreateService();
        await using var service = fixture.Service;
        var registration = service.Register("orders", async _ =>
        {
            Interlocked.Increment(ref calls);
            started.TrySetResult();
            await release.Task;
            return true;
        });
        try
        {
            registration.Wakeup();
            await started.Task.WaitAsync(TestTimeout, cancellationToken);

            var unregister = registration.DisposeAsync().AsTask();

            Assert.False(unregister.IsCompleted);
            release.TrySetResult();
            await unregister.WaitAsync(TestTimeout, cancellationToken);
            registration.Wakeup();
            await Task.Delay(TimeSpan.FromMilliseconds(50), cancellationToken);
            Assert.Equal(1, Volatile.Read(ref calls));
        }
        finally
        {
            release.TrySetResult();
            await registration.DisposeAsync();
        }
    }

    [Fact]
    public async Task Register_RejectsDuplicateGroupOnTheSamePhysicalClient()
    {
        var fixture = CreateService();
        await using var service = fixture.Service;
        await using var registration = service.Register(
            "orders",
            static _ => Task.FromResult(true));

        var exception = Assert.Throws<InvalidOperationException>(() => service.Register(
            "orders",
            static _ => Task.FromResult(true)));

        Assert.Contains("orders", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task PeriodicInterval_RebalancesRegisteredParticipants()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var called = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var fixture = CreateService(rebalanceInterval: TimeSpan.FromMilliseconds(20));
        await using var service = fixture.Service;
        await using var registration = service.Register("orders", _ =>
        {
            called.TrySetResult();
            return Task.FromResult(true);
        });

        await called.Task.WaitAsync(TestTimeout, cancellationToken);
    }

    private static ServiceFixture CreateService(
        TimeSpan? rebalanceInterval = null,
        TimeSpan? retryInterval = null)
    {
        RemotingRequestHandler? handler = null;
        var handlerRegistration = new Mock<IDisposable>(MockBehavior.Strict);
        handlerRegistration.Setup(value => value.Dispose());
        var handlers = new Mock<IRemotingRequestHandlerRegistry>(MockBehavior.Strict);
        handlers
            .Setup(value => value.RegisterRequestHandler(
                RequestCode.NotifyConsumerIdsChanged,
                It.IsAny<RemotingRequestHandler>()))
            .Callback<int, RemotingRequestHandler>((_, value) => handler = value)
            .Returns(handlerRegistration.Object);
        var service = new RemotingRebalanceService(
            handlers.Object,
            NullLogger<RemotingRebalanceService>.Instance,
            rebalanceInterval ?? TimeSpan.FromHours(1),
            retryInterval ?? TimeSpan.FromSeconds(1));

        return new ServiceFixture(service, Assert.IsType<RemotingRequestHandler>(handler));
    }

    private static void UpdateMaximum(ref int maximum, int value)
    {
        var current = Volatile.Read(ref maximum);
        while (current < value)
        {
            var observed = Interlocked.CompareExchange(ref maximum, value, current);
            if (observed == current)
            {
                return;
            }

            current = observed;
        }
    }

    private static RemotingRequestContext CreateNotification(string? group = null) => new(
        new IPEndPoint(IPAddress.Loopback, 10911),
        new RemotingCommand
        {
            Code = RequestCode.NotifyConsumerIdsChanged,
            ExtFields = group is null
                ? new Dictionary<string, object>()
                : new Dictionary<string, object> { ["consumerGroup"] = group }
        },
        CancellationToken.None);

    private sealed record ServiceFixture(
        RemotingRebalanceService Service,
        RemotingRequestHandler Handler);
}
