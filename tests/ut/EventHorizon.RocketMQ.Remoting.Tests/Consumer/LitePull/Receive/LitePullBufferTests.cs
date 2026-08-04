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
using EventHorizon.RocketMQ.Remoting.Consumer.LitePull.Assignment;
using EventHorizon.RocketMQ.Remoting.Consumer.LitePull.Receive;
using Xunit;

namespace EventHorizon.RocketMQ.Remoting.Tests.Consumer.LitePull.Receive;

public sealed class LitePullBufferTests
{
    [Fact]
    public void TryStartReceive_EmptyBuffer_ReturnsRequestedReceiveLimits()
    {
        var buffer = new LitePullBuffer(maxMessages: 4, maxBytes: 10);

        var attempt = buffer.TryStartReceive(requestedMessages: 2, requestedBytes: 7);

        Assert.True(attempt.CanReceive);
        Assert.Equal(2, attempt.MaxMessages);
        Assert.Equal(7, attempt.MaxBytes);
        Assert.Same(Task.CompletedTask, attempt.StateChanged);
    }

    [Fact]
    public void TryStartReceive_BufferHasRoom_DoesNotPreReserveCapacity()
    {
        var buffer = new LitePullBuffer(maxMessages: 3, maxBytes: 8);
        using var state = CreateState();
        Assert.True(buffer.TryAdmit([CreateEntry(state, bodyBytes: 2, deliveredOffset: 1)]).Admitted);

        var firstAttempt = buffer.TryStartReceive(requestedMessages: 3, requestedBytes: 8);
        var secondAttempt = buffer.TryStartReceive(requestedMessages: 3, requestedBytes: 8);

        Assert.True(firstAttempt.CanReceive);
        Assert.True(secondAttempt.CanReceive);
        Assert.Equal(3, firstAttempt.MaxMessages);
        Assert.Equal(8, firstAttempt.MaxBytes);
        Assert.Equal(3, secondAttempt.MaxMessages);
        Assert.Equal(8, secondAttempt.MaxBytes);
    }

    [Fact]
    public void TryStartReceive_TwoQueueReceivers_BothReceiveOpportunity()
    {
        var buffer = new LitePullBuffer(maxMessages: 4, maxBytes: 10);

        var firstQueueAttempt = buffer.TryStartReceive(requestedMessages: 2, requestedBytes: 5);
        var secondQueueAttempt = buffer.TryStartReceive(requestedMessages: 2, requestedBytes: 5);

        Assert.True(firstQueueAttempt.CanReceive);
        Assert.True(secondQueueAttempt.CanReceive);
    }

    [Fact]
    public void TryStartReceive_FullCache_ReturnsWaitUntilDrain()
    {
        var buffer = new LitePullBuffer(maxMessages: 2, maxBytes: 4);
        using var state = CreateState();
        Assert.True(
            buffer.TryAdmit(
                    [
                        CreateEntry(state, bodyBytes: 2, deliveredOffset: 1),
                        CreateEntry(state, bodyBytes: 2, deliveredOffset: 2)
                    ])
                .Admitted);

        var attempt = buffer.TryStartReceive(requestedMessages: 1, requestedBytes: 1);

        Assert.False(attempt.CanReceive);
        Assert.False(attempt.StateChanged.IsCompleted);
        Assert.Equal(2, buffer.Drain().Count);
        Assert.True(attempt.StateChanged.IsCompleted);
    }

    [Fact]
    public void TryAdmit_MessagesWithinCountAndBytes_AdmitsAndDrainsAtomically()
    {
        var buffer = new LitePullBuffer(maxMessages: 3, maxBytes: 10);
        using var state = CreateState();
        var first = CreateEntry(state, bodyBytes: 3, deliveredOffset: 1);
        var second = CreateEntry(state, bodyBytes: 4, deliveredOffset: 2);

        var attempt = buffer.TryAdmit([first, second]);

        Assert.True(attempt.Admitted);
        Assert.Same(Task.CompletedTask, attempt.StateChanged);
        var drained = buffer.Drain();
        Assert.Equal(2, drained.Count);
        Assert.Same(first, drained[0]);
        Assert.Same(second, drained[1]);
    }

    [Fact]
    public void TryAdmit_CurrentMessageCapacityIsInsufficient_ReturnsWaitWithoutPartialAdmission()
    {
        var buffer = new LitePullBuffer(maxMessages: 2, maxBytes: 10);
        using var state = CreateState();
        var existing = CreateEntry(state, bodyBytes: 1, deliveredOffset: 1);
        Assert.True(buffer.TryAdmit([existing]).Admitted);
        var batch = new[]
        {
            CreateEntry(state, bodyBytes: 1, deliveredOffset: 2),
            CreateEntry(state, bodyBytes: 1, deliveredOffset: 3)
        };

        var attempt = buffer.TryAdmit(batch);

        Assert.False(attempt.Admitted);
        Assert.False(attempt.StateChanged.IsCompleted);
        var drained = buffer.Drain();
        Assert.Same(existing, Assert.Single(drained));
        Assert.True(attempt.StateChanged.IsCompleted);
        Assert.True(buffer.TryAdmit(batch).Admitted);
    }

    [Fact]
    public void TryAdmit_CurrentByteCapacityIsInsufficient_ReturnsWaitWithoutPartialAdmission()
    {
        var buffer = new LitePullBuffer(maxMessages: 3, maxBytes: 5);
        using var state = CreateState();
        var existing = CreateEntry(state, bodyBytes: 4, deliveredOffset: 1);
        var waitingMessage = CreateEntry(state, bodyBytes: 2, deliveredOffset: 2);
        Assert.True(buffer.TryAdmit([existing]).Admitted);

        var attempt = buffer.TryAdmit([waitingMessage]);

        Assert.False(attempt.Admitted);
        Assert.False(attempt.StateChanged.IsCompleted);
        Assert.Same(existing, Assert.Single(buffer.Drain()));
        Assert.True(attempt.StateChanged.IsCompleted);
        Assert.True(buffer.TryAdmit([waitingMessage]).Admitted);
    }

    [Fact]
    public void Remove_MatchingQueueState_CompletesReceiveWaitAndRetainsOtherState()
    {
        var buffer = new LitePullBuffer(maxMessages: 2, maxBytes: 4);
        using var removedState = CreateState(queueId: 0);
        using var retainedState = CreateState(queueId: 1);
        var removed = CreateEntry(removedState, bodyBytes: 2, deliveredOffset: 1);
        var retained = CreateEntry(retainedState, bodyBytes: 2, deliveredOffset: 1);
        Assert.True(buffer.TryAdmit([removed, retained]).Admitted);
        var waitingAttempt = buffer.TryStartReceive(requestedMessages: 1, requestedBytes: 1);

        buffer.Remove(removedState);

        Assert.True(waitingAttempt.StateChanged.IsCompleted);
        Assert.True(buffer.TryStartReceive(requestedMessages: 1, requestedBytes: 1).CanReceive);
        Assert.Same(retained, Assert.Single(buffer.Drain()));
    }

    [Fact]
    public void Clear_BufferedMessages_CompletesReceiveWaitAndRestoresCapacity()
    {
        var buffer = new LitePullBuffer(maxMessages: 2, maxBytes: 4);
        using var state = CreateState();
        Assert.True(
            buffer.TryAdmit(
                    [
                        CreateEntry(state, bodyBytes: 2, deliveredOffset: 1),
                        CreateEntry(state, bodyBytes: 2, deliveredOffset: 2)
                    ])
                .Admitted);
        var waitingAttempt = buffer.TryStartReceive(requestedMessages: 1, requestedBytes: 1);

        buffer.Clear();

        Assert.True(waitingAttempt.StateChanged.IsCompleted);
        Assert.Empty(buffer.Drain());
        Assert.True(buffer.TryAdmit([CreateEntry(state, bodyBytes: 4, deliveredOffset: 3)]).Admitted);
    }

    [Fact]
    public void TryAdmit_OversizedSingleMessage_EmptyCacheAdmits()
    {
        var buffer = new LitePullBuffer(maxMessages: 1, maxBytes: 4);
        using var state = CreateState();
        var message = CreateEntry(state, bodyBytes: 5, deliveredOffset: 1);

        var attempt = buffer.TryAdmit([message]);

        Assert.True(attempt.Admitted);
        Assert.Same(message, Assert.Single(buffer.Drain()));
    }

    [Fact]
    public void TryAdmit_OversizedSingleMessage_NonEmptyCacheWaitsUntilDrain()
    {
        var buffer = new LitePullBuffer(maxMessages: 2, maxBytes: 4);
        using var state = CreateState();
        var regular = CreateEntry(state, bodyBytes: 1, deliveredOffset: 1);
        var oversized = CreateEntry(state, bodyBytes: 5, deliveredOffset: 2);
        Assert.True(buffer.TryAdmit([regular]).Admitted);

        var waitingAttempt = buffer.TryAdmit([oversized]);

        Assert.False(waitingAttempt.Admitted);
        Assert.False(waitingAttempt.StateChanged.IsCompleted);
        Assert.Same(regular, Assert.Single(buffer.Drain()));
        Assert.True(waitingAttempt.StateChanged.IsCompleted);
        Assert.True(buffer.TryAdmit([oversized]).Admitted);
        Assert.Same(oversized, Assert.Single(buffer.Drain()));
    }

    [Fact]
    public void TryAdmit_MessageCountExceedsCacheLimit_ThrowsAndLeavesCacheUsable()
    {
        var buffer = new LitePullBuffer(maxMessages: 2, maxBytes: 8);
        using var state = CreateState();
        var batch = new[]
        {
            CreateEntry(state, bodyBytes: 1, deliveredOffset: 1),
            CreateEntry(state, bodyBytes: 1, deliveredOffset: 2),
            CreateEntry(state, bodyBytes: 1, deliveredOffset: 3)
        };

        var exception = Assert.Throws<InvalidDataException>(() => buffer.TryAdmit(batch));

        Assert.Contains("messages", exception.Message, StringComparison.Ordinal);
        Assert.Empty(buffer.Drain());
        Assert.True(buffer.TryAdmit([CreateEntry(state, bodyBytes: 1, deliveredOffset: 4)]).Admitted);
    }

    [Fact]
    public void TryAdmit_MultipleMessageBytesExceedCacheLimit_ThrowsAndLeavesCacheUsable()
    {
        var buffer = new LitePullBuffer(maxMessages: 3, maxBytes: 4);
        using var state = CreateState();
        var batch = new[]
        {
            CreateEntry(state, bodyBytes: 3, deliveredOffset: 1),
            CreateEntry(state, bodyBytes: 2, deliveredOffset: 2)
        };

        var exception = Assert.Throws<InvalidDataException>(() => buffer.TryAdmit(batch));

        Assert.Contains("message bytes", exception.Message, StringComparison.Ordinal);
        Assert.Empty(buffer.Drain());
        Assert.True(buffer.TryAdmit([CreateEntry(state, bodyBytes: 4, deliveredOffset: 3)]).Admitted);
    }

    private static LitePullQueueState CreateState(int queueId = 0) =>
        new(Target(new RemotingConsumerQueue("orders", "broker-a", queueId)), 0);

    private static LitePullReceiveTarget Target(
        RemotingConsumerQueue queue,
        FilterExpression? filter = null) =>
        new(queue, filter ?? FilterExpression.All);

    private static LitePullBuffer.MessageEntry CreateEntry(
        LitePullQueueState state,
        int bodyBytes,
        long deliveredOffset)
    {
        var body = new byte[bodyBytes];
        var message = new RemotingMessageView(
            "orders",
            body,
            $"message-{state.Queue.QueueId}-{deliveredOffset}",
            $"offset-{state.Queue.QueueId}-{deliveredOffset}",
            null,
            [],
            new Dictionary<string, string>(),
            1,
            null,
            state.Queue.QueueId,
            state.Queue.BrokerName,
            deliveredOffset - 1,
            deliveredOffset - 1,
            DateTimeOffset.UnixEpoch,
            DateTimeOffset.UnixEpoch);
        return new LitePullBuffer.MessageEntry(state, message, deliveredOffset, body.Length);
    }
}
