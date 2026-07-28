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
using EventHorizon.RocketMQ.Remoting.Consumer.Pull;
using EventHorizon.RocketMQ.Remoting.Consumer.Push;
using Xunit;

namespace EventHorizon.RocketMQ.Remoting.Tests.Consumer.Push;

public sealed class ProcessQueueTests
{
    [Fact]
    public void CompletionAdvancesOnlyAcrossContiguousRequestsAndReleasesCompletedPayloads()
    {
        var processQueue = CreateProcessQueue();
        processQueue.Initialize(0, forcePersist: false);
        processQueue.PutMessages(Enumerable.Range(0, 4).Select(static offset => CreateMessage(offset)).ToArray());
        processQueue.AdvanceNextPullOffset(4);

        Assert.True(processQueue.TryCreateConsumeRequest(2, out var first));
        Assert.True(processQueue.TryCreateConsumeRequest(2, out var second));
        Assert.True(processQueue.TryStartConsumeRequest(first));
        Assert.True(processQueue.TryStartConsumeRequest(second));

        Assert.False(processQueue.CompleteConsumeRequest(second, [true, true]));
        Assert.Equal(2, processQueue.CachedMessageCount);
        Assert.False(processQueue.TryGetOffsetCommit(out _));

        Assert.False(processQueue.CompleteConsumeRequest(first, [true, true]));
        Assert.Equal(0, processQueue.CachedMessageCount);
        Assert.True(processQueue.TryGetOffsetCommit(out var commit));
        Assert.Equal(4, commit.Offset);
        Assert.False(commit.IsForced);

        processQueue.MarkOffsetPersisted(commit);
        Assert.False(processQueue.TryGetOffsetCommit(out _));
    }

    [Fact]
    public void OffsetResetIgnoresAnOlderInFlightCommitGeneration()
    {
        var processQueue = CreateProcessQueue();
        processQueue.Initialize(10, forcePersist: false);
        processQueue.PutMessages([CreateMessage(10)]);
        processQueue.AdvanceNextPullOffset(11);
        Assert.True(processQueue.TryCreateConsumeRequest(1, out var request));
        Assert.True(processQueue.TryStartConsumeRequest(request));
        processQueue.CompleteConsumeRequest(request, [true]);
        Assert.True(processQueue.TryGetOffsetCommit(out var oldCommit));

        processQueue.RequestOffsetReset(3);
        processQueue.MarkOffsetPersisted(oldCommit);

        Assert.True(processQueue.TryGetOffsetCommit(out var resetCommit));
        Assert.Equal(3, resetCommit.Offset);
        Assert.True(resetCommit.IsForced);
        Assert.NotEqual(oldCommit.Generation, resetCommit.Generation);

        processQueue.MarkOffsetPersisted(resetCommit);
        Assert.False(processQueue.TryGetOffsetCommit(out _));
    }

    [Fact]
    public void DroppedAssignmentIgnoresLateCompletionAndReturnsUndispatchedSlots()
    {
        var processQueue = CreateProcessQueue();
        processQueue.Initialize(0, forcePersist: false);
        processQueue.PutMessages([CreateMessage(0), CreateMessage(1)]);
        processQueue.AdvanceNextPullOffset(2);
        Assert.True(processQueue.TryCreateConsumeRequest(1, out var request));
        Assert.True(processQueue.TryStartConsumeRequest(request));

        var releasedSlots = processQueue.MarkDropped();

        Assert.Equal(1, releasedSlots);
        Assert.Equal(0, processQueue.CachedMessageCount);
        Assert.False(processQueue.CompleteConsumeRequest(request, [true]));
        Assert.False(processQueue.TryGetOffsetCommit(out _));
        Assert.False(processQueue.TryQueueForDispatch());
    }

    [Fact]
    public void FailedSettlementRequiresExplicitDelayActivationAndSkipsTheHandler()
    {
        var processQueue = CreateProcessQueue();
        processQueue.Initialize(0, forcePersist: false);
        processQueue.PutMessages([CreateMessage(0)]);
        processQueue.AdvanceNextPullOffset(1);
        Assert.True(processQueue.TryCreateConsumeRequest(1, out var first));
        Assert.True(processQueue.TryStartConsumeRequest(first));
        first.Entries[0].PendingSettlementResult = ConsumeResult.Retry;
        first.Entries[0].PendingSettlementDelayLevel = 3;
        first.Entries[0].DelaySettlementRetry = true;

        Assert.False(processQueue.CompleteConsumeRequest(first, [false]));
        Assert.False(processQueue.TryQueueForDispatch());
        Assert.True(processQueue.ActivateSettlementRetries(first.Entries));
        Assert.True(processQueue.TryCreateConsumeRequest(1, out var retry));
        Assert.True(retry.IsSettlementRetry);
        Assert.True(processQueue.TryStartConsumeRequest(retry));

        processQueue.CompleteConsumeRequest(retry, [true]);
        Assert.True(processQueue.TryGetOffsetCommit(out var commit));
        Assert.Equal(1, commit.Offset);
    }

    [Fact]
    public void DroppingAnInFlightFifoEntryDoesNotReleaseItsSuccessor()
    {
        var firstQueue = CreateProcessQueue(queueId: 0);
        firstQueue.Initialize(0, forcePersist: false);
        var firstEntry = Assert.Single(firstQueue.PutMessages([CreateMessage(0, "account-7")]));
        firstEntry.FifoOrder = new ProcessQueue.FifoOrder("orders\0account-7", Task.CompletedTask);
        firstQueue.AdvanceNextPullOffset(1);
        Assert.True(firstQueue.TryCreateConsumeRequest(1, out var firstRequest));
        Assert.True(firstQueue.TryStartConsumeRequest(firstRequest));

        var secondQueue = CreateProcessQueue(queueId: 1);
        secondQueue.Initialize(0, forcePersist: false);
        var secondEntry = Assert.Single(secondQueue.PutMessages([CreateMessage(0, "account-7")]));
        secondEntry.FifoOrder = new ProcessQueue.FifoOrder(
            "orders\0account-7",
            firstEntry.FifoOrder.Completion.Task);
        secondQueue.AdvanceNextPullOffset(1);

        firstQueue.MarkDropped(static entry => entry.FifoOrder!.Completion.TrySetResult());

        Assert.False(firstEntry.FifoOrder.Completion.Task.IsCompleted);
        Assert.False(secondQueue.TryQueueForDispatch());

        firstEntry.FifoOrder.Completion.TrySetResult();
        Assert.True(secondQueue.TryQueueForDispatch());
    }

    private static ProcessQueue CreateProcessQueue(int queueId = 0) => new(
        new RemotingPullMessageQueue("orders", queueId, "broker-a", "127.0.0.1:10911"),
        CancellationToken.None);

    private static RemotingMessageView CreateMessage(int offset, string? messageGroup = null) => new(
        "orders",
        [],
        $"message-{offset}",
        $"offset-{offset}",
        messageGroup,
        [],
        new Dictionary<string, string>(),
        1,
        null,
        0,
        "broker-a",
        offset,
        offset,
        DateTimeOffset.UnixEpoch,
        DateTimeOffset.UnixEpoch);
}
