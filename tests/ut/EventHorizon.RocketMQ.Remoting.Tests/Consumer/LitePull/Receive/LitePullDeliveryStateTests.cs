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
using EventHorizon.RocketMQ.Remoting.Consumer.Pull;
using Xunit;

namespace EventHorizon.RocketMQ.Remoting.Tests.Consumer.LitePull.Receive;

public sealed class LitePullDeliveryStateTests
{
    [Fact]
    public void ApplyDesiredTargets_DuplicateAndRemovedQueues_ReturnsAdditionsAndCanceledRemoval()
    {
        var buffer = new LitePullBuffer(maxMessages: 8, maxBytes: 64);
        var positions = new LitePullDeliveryState(buffer);
        var firstQueue = Queue(0);
        var equivalentFirstQueue = Queue(0);
        var secondQueue = Queue(1);

        var initialChanges = positions.ApplyDesiredTargets(
            [Target(firstQueue), Target(equivalentFirstQueue), Target(secondQueue)]);

        Assert.Equal(2, initialChanges.Additions.Count);
        Assert.Contains(initialChanges.Additions, target => ReferenceEquals(target.Queue, firstQueue));
        Assert.Contains(initialChanges.Additions, target => ReferenceEquals(target.Queue, secondQueue));

        var firstState = positions.AddInitializedQueue(Target(firstQueue), offset: 4);
        var secondState = positions.AddInitializedQueue(Target(secondQueue), offset: 8);
        Assert.NotNull(firstState);
        Assert.NotNull(secondState);
        Assert.Null(positions.AddInitializedQueue(Target(equivalentFirstQueue), offset: 20));

        var removalChanges = positions.ApplyDesiredTargets([Target(secondQueue)]);

        Assert.Empty(removalChanges.Additions);
        Assert.Same(firstState, Assert.Single(removalChanges.Removed));
        Assert.True(firstState.ReceiveCancellation.IsCancellationRequested);
        Assert.Same(secondQueue, Assert.Single(positions.GetAssignment()));

        var readdChanges = positions.ApplyDesiredTargets([Target(firstQueue), Target(secondQueue)]);

        Assert.Same(firstQueue, Assert.Single(readdChanges.Additions).Queue);
        var replacementGeneration = positions.AddInitializedQueue(Target(firstQueue), offset: 12);
        Assert.NotNull(replacementGeneration);
        Assert.NotSame(firstState, replacementGeneration);
        Assert.Equal(2, positions.GetAssignment().Count);
    }

    [Fact]
    public void ApplyDesiredTargets_FilterReplacement_CreatesReplacementAtDeliveredPosition()
    {
        var queue = Queue();
        var oldFilter = new FilterExpression("old");
        var newFilter = new FilterExpression("new");
        var positions = new LitePullDeliveryState(new LitePullBuffer(maxMessages: 8, maxBytes: 64));

        positions.ApplyDesiredTargets([Target(queue, oldFilter)]);
        var state = positions.AddInitializedQueue(Target(queue, oldFilter), offset: 0)!;

        var changes = positions.ApplyDesiredTargets([Target(queue, newFilter)]);
        var replacement = Assert.Single(changes.Replacements);

        var selection = positions.SelectReceive(replacement, pullBatchSize: 1, maxResponseBytes: 1);

        Assert.True(state.ReceiveCancellation.IsCancellationRequested);
        Assert.Same(state, Assert.Single(changes.Removed));
        Assert.True(selection.CanReceive);
        Assert.Equal(newFilter, selection.Filter);
        Assert.Equal(0, selection.Offset);
    }

    [Fact]
    public async Task AdmitAsync_ReplacedState_RejectsStaleResponseWithoutAdvancingCurrentOffset()
    {
        var queue = Queue();
        var positions = CreatePositions(queue, offset: 10, out var oldState);
        var replacement = positions.ReplacePosition(queue, offset: 20);

        var admitted = await positions.AdmitAsync(
            oldState,
            new RemotingPullResult([Message(queue, offset: 10)], NextOffset: 11),
            CancellationToken.None);

        Assert.False(admitted);
        Assert.Equal(10, oldState.FetchOffset);
        Assert.False(oldState.HasReceivedResponse);
        Assert.Equal(20, replacement.FetchOffset);
        Assert.False(replacement.HasReceivedResponse);
        Assert.Equal(20, positions.GetRequiredQueuePosition(queue).Offset);
    }

    [Fact]
    public void ReplacePosition_CurrentState_PreservesFilterForReplacementGeneration()
    {
        var queue = Queue();
        var filter = new FilterExpression("priority");
        var positions = new LitePullDeliveryState(new LitePullBuffer(maxMessages: 8, maxBytes: 64));

        positions.ApplyDesiredTargets([Target(queue, filter)]);
        var current = positions.AddInitializedQueue(Target(queue, filter), offset: 4)!;

        var replacement = positions.ReplacePosition(queue, offset: 12);

        Assert.NotSame(current, replacement);
        Assert.Equal(filter, replacement.Filter);
        var selection = positions.SelectReceive(replacement, pullBatchSize: 1, maxResponseBytes: 1);
        Assert.Equal(filter, selection.Filter);
    }

    [Fact]
    public void SelectReceive_PausedState_WaitsAndResumeMakesReceiveReady()
    {
        var queue = Queue();
        var positions = CreatePositions(queue, offset: 7, out var state);

        positions.SetPaused(queue, paused: true);
        var pausedSelection = positions.SelectReceive(state, pullBatchSize: 3, maxResponseBytes: 12);

        Assert.True(pausedSelection.IsCurrent);
        Assert.False(pausedSelection.CanReceive);
        Assert.False(pausedSelection.StateChanged.IsCompleted);

        positions.SetPaused(queue, paused: false);

        Assert.True(pausedSelection.StateChanged.IsCompleted);
        var resumedSelection = positions.SelectReceive(state, pullBatchSize: 3, maxResponseBytes: 12);
        Assert.True(resumedSelection.IsCurrent);
        Assert.True(resumedSelection.CanReceive);
        Assert.Same(queue, resumedSelection.Queue);
        Assert.Equal(7, resumedSelection.Offset);
        Assert.Equal(3, resumedSelection.MaxMessages);
        Assert.Equal(12, resumedSelection.MaxBytes);
    }

    [Fact]
    public async Task TryDrainBuffer_AdmittedMessages_AdvancesDeliveredPositionOnlyWhenDrained()
    {
        var queue = Queue();
        var positions = CreatePositions(queue, offset: 3, out var state);

        var admitted = await positions.AdmitAsync(
            state,
            new RemotingPullResult(
                [Message(queue, offset: 3), Message(queue, offset: 4)],
                NextOffset: 10),
            CancellationToken.None);

        Assert.True(admitted);
        Assert.Equal(10, state.FetchOffset);
        Assert.Equal(3, state.DeliveredOffset);

        var poll = positions.TryDrainBuffer();

        Assert.Equal(2, poll.Messages.Count);
        Assert.Equal(10, state.FetchOffset);
        Assert.Equal(10, state.DeliveredOffset);
        Assert.Equal(10, positions.GetRequiredQueuePosition(queue).Offset);
    }

    [Fact]
    public async Task ReplacePosition_BufferedOldMessages_AreDiscardedAndOffsetStartsAtReplacement()
    {
        var queue = Queue();
        var positions = CreatePositions(queue, offset: 0, out var oldState);
        Assert.True(
            await positions.AdmitAsync(
                oldState,
                new RemotingPullResult([Message(queue, offset: 0)], NextOffset: 1),
                CancellationToken.None));

        var replacement = positions.ReplacePosition(queue, offset: 10);
        var poll = positions.TryDrainBuffer();

        Assert.True(oldState.ReceiveCancellation.IsCancellationRequested);
        Assert.Empty(poll.Messages);
        Assert.Equal(10, replacement.FetchOffset);
        Assert.Equal(10, replacement.DeliveredOffset);
        Assert.Equal(10, positions.GetRequiredQueuePosition(queue).Offset);
    }

    private static LitePullDeliveryState CreatePositions(
        RemotingConsumerQueue queue,
        long offset,
        out LitePullQueueState state)
    {
        var positions = new LitePullDeliveryState(new LitePullBuffer(maxMessages: 8, maxBytes: 64));
        var target = Target(queue);
        var changes = positions.ApplyDesiredTargets([target]);
        Assert.Same(queue, Assert.Single(changes.Additions).Queue);
        state = positions.AddInitializedQueue(target, offset)!;
        return positions;
    }

    private static LitePullReceiveTarget Target(
        RemotingConsumerQueue queue,
        FilterExpression? filter = null) =>
        new(queue, filter ?? FilterExpression.All);

    private static RemotingConsumerQueue Queue(int queueId = 0) =>
        new("orders", "broker-a", queueId);

    private static RemotingMessageView Message(
        RemotingConsumerQueue queue,
        long offset,
        int bodySize = 1) =>
        new(
            queue.Topic,
            new byte[bodySize],
            $"message-{queue.QueueId}-{offset}",
            $"offset-{queue.QueueId}-{offset}",
            null,
            [],
            new Dictionary<string, string>(),
            1,
            null,
            queue.QueueId,
            queue.BrokerName,
            offset,
            offset,
            DateTimeOffset.UnixEpoch,
            DateTimeOffset.UnixEpoch);
}
