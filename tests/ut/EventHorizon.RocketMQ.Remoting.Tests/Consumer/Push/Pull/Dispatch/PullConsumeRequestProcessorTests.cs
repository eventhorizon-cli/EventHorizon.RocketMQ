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

using EventHorizon.RocketMQ.Remoting.Consumer;
using EventHorizon.RocketMQ.Remoting.Consumer.Push;
using EventHorizon.RocketMQ.Remoting.Consumer.Push.Processing;
using EventHorizon.RocketMQ.Remoting.Consumer.Push.Pull.Dispatch;
using EventHorizon.RocketMQ.Remoting.Consumer.Push.Pull.Offset;
using EventHorizon.RocketMQ.Remoting.Consumer.Push.Pull.Settlement;
using EventHorizon.RocketMQ.Remoting.Instrumentation;
using Microsoft.Extensions.Logging.Abstractions;
using Moq;
using Xunit;

namespace EventHorizon.RocketMQ.Remoting.Tests.Consumer.Push.Pull.Dispatch;

public sealed class PullConsumeRequestProcessorTests
{
    [Fact]
    public async Task ProcessAsync_RetryOffsetPersistencePending_DoesNotWakeRebalance()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var sourceQueue = new RemotingConsumerQueue("orders", "broker-a", 0);
        var retryQueue = new RemotingConsumerQueue("%RETRY%legacy-group", "broker-a", 0);
        var message = CreateMessage();
        var persistStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var allowPersist = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var retryTopicRequired = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var rebalanceWoken = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var engine = new Mock<IRemotingConsumerEngine>(MockBehavior.Strict);
        engine
            .Setup(value => value.GetOffsetAsync(
                It.Is<RemotingConsumerQueue>(queue => queue == retryQueue),
                It.IsAny<CancellationToken>()))
            .ReturnsAsync(-1);
        engine
            .Setup(value => value.QueryOffsetAsync(
                It.Is<RemotingConsumerQueue>(queue => queue == retryQueue),
                ConsumeFromPosition.End,
                It.IsAny<DateTimeOffset?>(),
                It.IsAny<CancellationToken>()))
            .ReturnsAsync(17);
        engine
            .Setup(value => value.UpdateOffsetAsync(
                It.Is<RemotingConsumerQueue>(queue => queue == retryQueue),
                17,
                It.IsAny<CancellationToken>()))
            .Returns(async () =>
            {
                persistStarted.TrySetResult();
                await allowPersist.Task.ConfigureAwait(false);
            });
        engine
            .Setup(value => value.SendBackAsync(
                sourceQueue,
                message,
                It.IsAny<int>(),
                It.IsAny<int>(),
                It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask);
        var options = new RemotingPushConsumerOptions
        {
            GroupName = "legacy-group",
            MaxConcurrency = 1,
            RetryDelay = TimeSpan.FromSeconds(1)
        };
        using var handlerInvoker = new PushMessageHandlerInvoker(
            options,
            static (_, _, _) => ValueTask.FromResult(ConsumeResult.Retry),
            TimeProvider.System,
            RemotingRocketMQTelemetry.Disabled,
            NullLogger.Instance);
        using var scheduler = new PullDeliveryCoordinator(maxCachedMessages: 8, maxCachedMessageBytes: 1024);
        var offsetManager = new PullOffsetManager(
            options,
            engine.Object,
            "client-a",
            "legacy-group",
            NullLogger.Instance);
        var sendBackSettlement = new PullSendBackSettlement(
            options,
            engine.Object,
            offsetManager,
            NullLogger.Instance,
            static _ => false,
            () => retryTopicRequired.TrySetResult(),
            () => rebalanceWoken.TrySetResult());
        var processor = new PullConsumeRequestProcessor(
            options,
            sendBackSettlement,
            handlerInvoker,
            scheduler,
            NullLogger.Instance);
        var processQueue = new PullProcessQueue(sourceQueue, cancellationToken);
        processQueue.Initialize(0, forcePersist: false);
        await scheduler.ReserveDeliverySlotsAsync(1, message.Body.Length, cancellationToken);
        processQueue.PutMessages([message]);
        processQueue.AdvanceNextPullOffset(1);
        Assert.True(processQueue.TryCreateConsumeRequest(1, out var request));
        scheduler.ReleaseDeliverySlots(request.CacheReservation);
        Assert.True(processQueue.TryStartConsumeRequest(request));

        var processing = processor.ProcessAsync(request, cancellationToken).AsTask();
        try
        {
            await persistStarted.Task.WaitAsync(TimeSpan.FromSeconds(3), cancellationToken);

            Assert.True(retryTopicRequired.Task.IsCompleted);
            Assert.False(rebalanceWoken.Task.IsCompleted);
        }
        finally
        {
            allowPersist.TrySetResult();
            await processing.WaitAsync(TimeSpan.FromSeconds(3), cancellationToken);
        }

        await rebalanceWoken.Task.WaitAsync(TimeSpan.FromSeconds(3), cancellationToken);
        engine.VerifyAll();
    }

    [Fact]
    public async Task ProcessAsync_DroppedSettlementRetry_DoesNotSendBackMessage()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var sourceQueue = new RemotingConsumerQueue("orders", "broker-a", 0);
        var message = CreateMessage();
        var sendBackCalls = 0;
        var engine = new Mock<IRemotingConsumerEngine>(MockBehavior.Strict);
        engine
            .Setup(value => value.SendBackAsync(
                sourceQueue,
                message,
                -1,
                1,
                It.IsAny<CancellationToken>()))
            .Callback(() => Interlocked.Increment(ref sendBackCalls))
            .Returns(Task.CompletedTask);
        var options = new RemotingPushConsumerOptions
        {
            GroupName = "legacy-group",
            MaxConcurrency = 1,
            MaxDeliveryAttempts = 1,
            RetryDelay = TimeSpan.FromSeconds(1)
        };
        using var handlerInvoker = new PushMessageHandlerInvoker(
            options,
            static (_, _, _) => ValueTask.FromResult(ConsumeResult.Retry),
            TimeProvider.System,
            RemotingRocketMQTelemetry.Disabled,
            NullLogger.Instance);
        using var scheduler = new PullDeliveryCoordinator(maxCachedMessages: 8, maxCachedMessageBytes: 1024);
        var offsetManager = new PullOffsetManager(
            options,
            engine.Object,
            "client-a",
            "legacy-group",
            NullLogger.Instance);
        var sendBackSettlement = new PullSendBackSettlement(
            options,
            engine.Object,
            offsetManager,
            NullLogger.Instance,
            static _ => false,
            static () => { },
            static () => { });
        var processor = new PullConsumeRequestProcessor(
            options,
            sendBackSettlement,
            handlerInvoker,
            scheduler,
            NullLogger.Instance);
        var processQueue = new PullProcessQueue(sourceQueue, cancellationToken);
        processQueue.Initialize(0, forcePersist: false);
        processQueue.PutMessages([message]);
        processQueue.AdvanceNextPullOffset(1);
        Assert.True(processQueue.TryCreateConsumeRequest(1, out var first));
        Assert.True(processQueue.TryStartConsumeRequest(first));
        Assert.True(processQueue.TryBeginSettlement(
            first.Entries[0],
            ConsumeResult.Retry,
            delayLevel: 3,
            out _));
        processQueue.DelaySettlementRetry(first.Entries[0]);
        Assert.False(processQueue.CompleteConsumeRequest(first, [false]));
        Assert.True(processQueue.ActivateSettlementRetries(first.Entries));
        Assert.True(processQueue.TryCreateConsumeRequest(1, out var retry));
        Assert.True(processQueue.TryStartConsumeRequest(retry));
        processQueue.MarkDropped();

        await processor.ProcessAsync(retry, cancellationToken);

        Assert.Equal(0, Volatile.Read(ref sendBackCalls));
    }

    private static RemotingMessageView CreateMessage() => new(
        "orders",
        new byte[] { 1, 2, 3 },
        "message-0",
        "offset-0",
        null,
        [],
        new Dictionary<string, string>(),
        1,
        null,
        0,
        "broker-a",
        0,
        0,
        DateTimeOffset.UnixEpoch,
        DateTimeOffset.UnixEpoch);
}
