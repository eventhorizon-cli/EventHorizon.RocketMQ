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

using EventHorizon.RocketMQ.Remoting.Consumer.LitePull.Assignment;
using EventHorizon.RocketMQ.Remoting.Consumer.Pull;

namespace EventHorizon.RocketMQ.Remoting.Consumer.LitePull.Receive;

internal sealed class LitePullDeliveryState
{
    private const string QueueNotAssignedMessage =
        "The message queue is not assigned to this Lite Pull consumer.";

    private readonly object _gate = new();
    private readonly Dictionary<QueueKey, LitePullQueueState> _assignments = [];
    private readonly LitePullBuffer _buffer;
    private IReadOnlyDictionary<QueueKey, LitePullReceiveTarget> _desiredTargets =
        new Dictionary<QueueKey, LitePullReceiveTarget>();

    public LitePullDeliveryState(LitePullBuffer buffer)
    {
        ArgumentNullException.ThrowIfNull(buffer);
        _buffer = buffer;
    }

    public IReadOnlyList<RemotingConsumerQueue> GetAssignment()
    {
        lock (_gate)
        {
            return _assignments
                .OrderBy(static entry => entry.Key.Topic, StringComparer.Ordinal)
                .ThenBy(static entry => entry.Key.BrokerName, StringComparer.Ordinal)
                .ThenBy(static entry => entry.Key.QueueId)
                .Select(static entry => entry.Value.Queue)
                .ToArray();
        }
    }

    public AssignmentChanges ApplyDesiredTargets(IReadOnlyCollection<LitePullReceiveTarget> desiredTargets)
    {
        ArgumentNullException.ThrowIfNull(desiredTargets);
        var desired = desiredTargets
            .GroupBy(static target => QueueKey.Create(target.Queue))
            .ToDictionary(static entry => entry.Key, static entry => entry.First());

        lock (_gate)
        {
            _desiredTargets = desired;
            var removed = new List<LitePullQueueState>();
            foreach (var key in _assignments.Keys.Except(desired.Keys).ToArray())
            {
                var state = _assignments[key];
                state.CancelReceive();
                _assignments.Remove(key);
                _buffer.Remove(state);
                removed.Add(state);
            }

            var additions = new List<LitePullReceiveTarget>();
            var replacements = new List<LitePullQueueState>();
            foreach (var (key, target) in desired)
            {
                if (_assignments.TryGetValue(key, out var state))
                {
                    if (state.Filter == target.Filter)
                    {
                        state.Queue = target.Queue;
                        continue;
                    }

                    state.CancelReceive();
                    _buffer.Remove(state);
                    var replacement = new LitePullQueueState(target, state.DeliveredOffset)
                    {
                        Paused = state.Paused
                    };
                    _assignments[key] = replacement;
                    removed.Add(state);
                    replacements.Add(replacement);
                }
                else
                {
                    additions.Add(target);
                }
            }

            _buffer.SignalStateChanged();
            return new AssignmentChanges(additions, replacements, removed);
        }
    }

    public LitePullQueueState? AddInitializedQueue(LitePullReceiveTarget target, long offset)
    {
        ArgumentNullException.ThrowIfNull(target);
        var key = QueueKey.Create(target.Queue);
        lock (_gate)
        {
            if (!_desiredTargets.TryGetValue(key, out var desired) ||
                desired != target ||
                _assignments.ContainsKey(key))
            {
                return null;
            }

            var state = new LitePullQueueState(target, offset);
            _assignments[key] = state;
            _buffer.SignalStateChanged();
            return state;
        }
    }

    public LitePullQueueState ReplacePosition(RemotingConsumerQueue queue, long offset)
    {
        ArgumentNullException.ThrowIfNull(queue);
        lock (_gate)
        {
            var key = QueueKey.Create(queue);
            if (!_assignments.TryGetValue(key, out var current))
            {
                throw new InvalidOperationException(QueueNotAssignedMessage);
            }

            current.CancelReceive();
            _buffer.Remove(current);
            var replacement = new LitePullQueueState(
                new LitePullReceiveTarget(current.Queue, current.Filter),
                offset)
            {
                Paused = current.Paused
            };
            _assignments[key] = replacement;
            _buffer.SignalStateChanged();
            return replacement;
        }
    }

    public void CancelAllAssignments()
    {
        lock (_gate)
        {
            _buffer.Clear();
            _buffer.SignalStateChanged();
        }
    }

    public ReceiveSelection SelectReceive(
        LitePullQueueState state,
        int pullBatchSize,
        int maxResponseBytes)
    {
        ArgumentNullException.ThrowIfNull(state);
        lock (_gate)
        {
            if (!IsCurrentState(state))
            {
                return ReceiveSelection.Stale;
            }

            if (state.Paused)
            {
                return ReceiveSelection.Wait(_buffer.StateChanged);
            }

            var attempt = _buffer.TryStartReceive(
                pullBatchSize,
                maxResponseBytes,
                allowWhenFull: !state.HasReceivedResponse);
            return attempt.CanReceive
                ? ReceiveSelection.Ready(
                    state.Queue,
                    state.Filter,
                    state.FetchOffset,
                    attempt.MaxMessages,
                    attempt.MaxBytes)
                : ReceiveSelection.Wait(attempt.StateChanged);
        }
    }

    public async Task<bool> AdmitAsync(
        LitePullQueueState state,
        RemotingPullResult result,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(state);
        ArgumentNullException.ThrowIfNull(result);
        LitePullBuffer.MessageEntry[] entries;
        lock (_gate)
        {
            if (!IsCurrentState(state))
            {
                return false;
            }

            state.HasReceivedResponse = true;
            if (result.Messages.Count == 0)
            {
                state.FetchOffset = result.NextOffset;
                return true;
            }

            var messages = result.Messages
                .Where(message => message.QueueOffset >= state.FetchOffset)
                .OrderBy(static message => message.QueueOffset)
                .DistinctBy(static message => message.QueueOffset)
                .ToArray();
            entries = new LitePullBuffer.MessageEntry[messages.Length];
            for (var index = 0; index < messages.Length; index++)
            {
                var message = messages[index];
                var messageNextOffset = checked(message.QueueOffset + 1);
                var deliveredOffset = index == messages.Length - 1
                    ? Math.Max(messageNextOffset, result.NextOffset)
                    : messageNextOffset;
                entries[index] = new LitePullBuffer.MessageEntry(
                    state,
                    message,
                    deliveredOffset,
                    message.Body.Length);
            }
        }

        while (true)
        {
            Task stateChanged;
            lock (_gate)
            {
                if (!IsCurrentState(state))
                {
                    return false;
                }

                var attempt = _buffer.TryAdmit(entries);
                if (attempt.Admitted)
                {
                    state.FetchOffset = Math.Max(state.FetchOffset, result.NextOffset);
                    if (entries.Length > 0)
                    {
                        state.FetchOffset = Math.Max(
                            state.FetchOffset,
                            checked(entries[^1].Message.QueueOffset + 1));
                    }

                    return true;
                }

                stateChanged = attempt.StateChanged;
            }

            await stateChanged.WaitAsync(cancellationToken).ConfigureAwait(false);
        }
    }

    public BufferPollAttempt TryDrainBuffer()
    {
        lock (_gate)
        {
            if (_assignments.Count == 0)
            {
                throw new InvalidOperationException("Lite Pull consumer has no assigned queues.");
            }

            var bufferedMessages = _buffer.Drain();
            if (bufferedMessages.Count == 0)
            {
                return new BufferPollAttempt([], _buffer.StateChanged);
            }

            var messages = new List<RemotingMessageView>(bufferedMessages.Count);
            foreach (var buffered in bufferedMessages)
            {
                if (IsCurrentState(buffered.State))
                {
                    buffered.State.DeliveredOffset = buffered.DeliveredOffset;
                    messages.Add(buffered.Message);
                }
            }

            _buffer.SignalStateChanged();
            return new BufferPollAttempt(messages, _buffer.StateChanged);
        }
    }

    public RemotingConsumerQueue GetRequiredQueue(RemotingConsumerQueue queue)
    {
        ArgumentNullException.ThrowIfNull(queue);
        lock (_gate)
        {
            return _assignments.TryGetValue(QueueKey.Create(queue), out var state)
                ? state.Queue
                : throw new InvalidOperationException(QueueNotAssignedMessage);
        }
    }

    public QueuePosition GetRequiredQueuePosition(RemotingConsumerQueue queue)
    {
        ArgumentNullException.ThrowIfNull(queue);
        lock (_gate)
        {
            return _assignments.TryGetValue(QueueKey.Create(queue), out var state)
                ? CreatePosition(state)
                : throw new InvalidOperationException(QueueNotAssignedMessage);
        }
    }

    public void SetPaused(RemotingConsumerQueue queue, bool paused)
    {
        ArgumentNullException.ThrowIfNull(queue);
        lock (_gate)
        {
            if (!_assignments.TryGetValue(QueueKey.Create(queue), out var state))
            {
                throw new InvalidOperationException(QueueNotAssignedMessage);
            }

            state.Paused = paused;
            _buffer.SignalStateChanged();
        }
    }

    public IReadOnlyList<QueuePosition> GetPositions()
    {
        lock (_gate)
        {
            return _assignments.Values.Select(CreatePosition).ToArray();
        }
    }

    public IReadOnlyList<DeliveredPosition> GetDeliveredPositions()
    {
        lock (_gate)
        {
            return _assignments.Values
                .Select(static state => new DeliveredPosition(state.Queue, state.DeliveredOffset))
                .ToArray();
        }
    }

    public bool IsCurrent(QueuePosition position)
    {
        lock (_gate)
        {
            return _assignments.TryGetValue(position.Key, out var current) &&
                   ReferenceEquals(current, position.State) &&
                   !current.ReceiveCancellation.IsCancellationRequested;
        }
    }

    public bool MarkReceiverStoppedUnexpectedly(LitePullQueueState state)
    {
        ArgumentNullException.ThrowIfNull(state);
        lock (_gate)
        {
            var key = QueueKey.Create(state.Queue);
            if (!_assignments.TryGetValue(key, out var current) || !ReferenceEquals(current, state))
            {
                return false;
            }

            state.CancelReceive();
            _assignments.Remove(key);
            _buffer.Remove(state);
            _buffer.SignalStateChanged();
            return true;
        }
    }

    public void Clear()
    {
        lock (_gate)
        {
            _desiredTargets = new Dictionary<QueueKey, LitePullReceiveTarget>();
            _assignments.Clear();
            _buffer.Clear();
            _buffer.SignalStateChanged();
        }
    }

    private static QueuePosition CreatePosition(LitePullQueueState state) =>
        new(
            QueueKey.Create(state.Queue),
            state,
            state.Queue,
            state.DeliveredOffset,
            state.ReceiveCancellation.Token);

    private bool IsCurrentState(LitePullQueueState state) =>
        _assignments.TryGetValue(QueueKey.Create(state.Queue), out var current) &&
        ReferenceEquals(current, state) &&
        !state.ReceiveCancellation.IsCancellationRequested;

    internal readonly record struct QueueKey(string Topic, string BrokerName, int QueueId)
    {
        public static QueueKey Create(RemotingConsumerQueue queue) =>
            new(queue.Topic, queue.BrokerName, queue.QueueId);
    }

    internal sealed record AssignmentChanges(
        IReadOnlyList<LitePullReceiveTarget> Additions,
        IReadOnlyList<LitePullQueueState> Replacements,
        IReadOnlyList<LitePullQueueState> Removed);

    internal readonly record struct QueuePosition(
        QueueKey Key,
        LitePullQueueState State,
        RemotingConsumerQueue Queue,
        long Offset,
        CancellationToken CancellationToken);

    internal readonly record struct DeliveredPosition(RemotingConsumerQueue Queue, long Offset);

    internal readonly record struct BufferPollAttempt(
        IReadOnlyList<RemotingMessageView> Messages,
        Task StateChanged);

    internal readonly record struct ReceiveSelection(
        bool IsCurrent,
        bool CanReceive,
        RemotingConsumerQueue? Queue,
        FilterExpression? Filter,
        long Offset,
        int MaxMessages,
        int MaxBytes,
        Task StateChanged)
    {
        public static ReceiveSelection Stale => new(false, false, null, null, 0, 0, 0, Task.CompletedTask);

        public static ReceiveSelection Wait(Task stateChanged) =>
            new(true, false, null, null, 0, 0, 0, stateChanged);

        public static ReceiveSelection Ready(
            RemotingConsumerQueue queue,
            FilterExpression filter,
            long offset,
            int maxMessages,
            int maxBytes) =>
            new(true, true, queue, filter, offset, maxMessages, maxBytes, Task.CompletedTask);
    }
}
