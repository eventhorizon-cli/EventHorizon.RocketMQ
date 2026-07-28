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

using System.Diagnostics.CodeAnalysis;
using System.Threading.Channels;
using EventHorizon.RocketMQ.Remoting.Consumer.Pull;

namespace EventHorizon.RocketMQ.Remoting.Consumer.Push;

// Client-side consumption state for one Broker message queue. This mirrors the
// role of RocketMQ's ProcessQueue; it is not a Broker-side physical queue.
internal sealed class ProcessQueue(RemotingPullMessageQueue messageQueue, CancellationToken queueCancellation)
{
    private readonly object _gate = new();
    private readonly SortedDictionary<long, MessageEntry> _messages = [];
    private readonly Channel<bool> _commitAvailable = CreateSignalChannel();
    private readonly Channel<bool> _stateChanged = CreateSignalChannel();
    private long _nextPullOffset;
    private long _committableOffset;
    private long _persistedOffset;
    private long? _forcedPersistOffset;
    private long _commitGeneration;
    private bool _initialized;
    private bool _readyQueued;
    private bool _dropped;

    public RemotingPullMessageQueue MessageQueue { get; } = messageQueue;

    public CancellationToken QueueCancellation { get; } = queueCancellation;

    public bool IsDropped
    {
        get
        {
            lock (_gate)
            {
                return _dropped;
            }
        }
    }

    public int CachedMessageCount
    {
        get
        {
            lock (_gate)
            {
                return _messages.Count;
            }
        }
    }

    public void Initialize(long offset, bool forcePersist)
    {
        if (offset < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(offset));
        }

        lock (_gate)
        {
            if (_initialized)
            {
                throw new InvalidOperationException("The process queue has already been initialized.");
            }

            _initialized = true;
            _nextPullOffset = offset;
            _committableOffset = offset;
            _persistedOffset = offset;
            _forcedPersistOffset = forcePersist ? offset : null;
        }
    }

    public IReadOnlyList<MessageEntry> PutMessages(
        IReadOnlyList<RemotingMessageView> messages,
        Action<MessageEntry>? initializeEntry = null)
    {
        ArgumentNullException.ThrowIfNull(messages);
        var added = new List<MessageEntry>(messages.Count);
        lock (_gate)
        {
            EnsureInitialized();
            if (_dropped)
            {
                return [];
            }

            foreach (var message in messages)
            {
                if (message.QueueOffset < _committableOffset)
                {
                    continue;
                }

                var entry = new MessageEntry(message);
                if (_messages.TryAdd(message.QueueOffset, entry))
                {
                    try
                    {
                        initializeEntry?.Invoke(entry);
                        added.Add(entry);
                    }
                    catch
                    {
                        _messages.Remove(message.QueueOffset);
                        throw;
                    }
                }
            }
        }

        return added;
    }

    public void AdvanceNextPullOffset(long nextOffset)
    {
        if (nextOffset < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(nextOffset));
        }

        var commitAvailable = false;
        lock (_gate)
        {
            EnsureInitialized();
            if (_dropped)
            {
                return;
            }

            if (nextOffset < _nextPullOffset)
            {
                throw new InvalidOperationException("The next pull offset cannot move backwards.");
            }

            _nextPullOffset = nextOffset;
            AdvanceCommittableOffsetCore();
            commitAvailable = HasCommitAvailableCore();
        }

        if (commitAvailable)
        {
            Signal(_commitAvailable);
        }

        Signal(_stateChanged);
    }

    public void RequestOffsetReset(long offset)
    {
        if (offset < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(offset));
        }

        lock (_gate)
        {
            EnsureInitialized();
            if (_messages.Count != 0)
            {
                throw new InvalidOperationException("The process queue must be empty before its offset is reset.");
            }

            _commitGeneration++;
            _nextPullOffset = offset;
            _committableOffset = offset;
            _forcedPersistOffset = offset;
            _readyQueued = false;
        }

        Signal(_commitAvailable);
        Signal(_stateChanged);
    }

    public bool TryQueueForDispatch()
    {
        lock (_gate)
        {
            if (_dropped || _readyQueued || !HasDispatchableMessageCore())
            {
                return false;
            }

            _readyQueued = true;
            return true;
        }
    }

    public void CancelQueuedDispatch()
    {
        lock (_gate)
        {
            _readyQueued = false;
        }
    }

    public bool TryCreateConsumeRequest(int batchSize, [NotNullWhen(true)] out ConsumeRequest? request)
    {
        if (batchSize <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(batchSize));
        }

        lock (_gate)
        {
            _readyQueued = false;
            if (_dropped)
            {
                request = null;
                return false;
            }

            var selected = new List<MessageEntry>(batchSize);
            var settlementRetry = false;
            foreach (var entry in _messages.Values)
            {
                if (entry.State == MessageState.SettlementDelayed)
                {
                    break;
                }

                if (!entry.IsDispatchable)
                {
                    continue;
                }

                if (selected.Count == 0)
                {
                    settlementRetry = entry.State == MessageState.PendingSettlement;
                    selected.Add(entry);
                    if (settlementRetry ||
                        !string.IsNullOrEmpty(entry.Message.MessageGroup) ||
                        selected.Count == batchSize)
                    {
                        break;
                    }

                    continue;
                }

                if (entry.State == MessageState.PendingSettlement ||
                    !string.IsNullOrEmpty(entry.Message.MessageGroup))
                {
                    break;
                }

                selected.Add(entry);
                if (selected.Count == batchSize)
                {
                    break;
                }
            }

            if (selected.Count == 0)
            {
                request = null;
                return false;
            }

            var reservedDeliverySlots = 0;
            foreach (var entry in selected)
            {
                entry.State = MessageState.Dispatching;
                if (entry.HasReservedDeliverySlot)
                {
                    entry.HasReservedDeliverySlot = false;
                    reservedDeliverySlots++;
                }
            }

            request = new ConsumeRequest(this, selected, reservedDeliverySlots, settlementRetry);
            return true;
        }
    }

    public bool TryStartConsumeRequest(ConsumeRequest request)
    {
        ArgumentNullException.ThrowIfNull(request);
        if (!ReferenceEquals(request.ProcessQueue, this))
        {
            throw new ArgumentException("The consume request belongs to another process queue.", nameof(request));
        }

        lock (_gate)
        {
            if (_dropped || QueueCancellation.IsCancellationRequested ||
                request.Entries.Any(static entry => entry.State != MessageState.Dispatching))
            {
                return false;
            }

            foreach (var entry in request.Entries)
            {
                entry.State = MessageState.InFlight;
            }

            return true;
        }
    }

    public bool CompleteConsumeRequest(ConsumeRequest request, IReadOnlyList<bool> completed)
    {
        ArgumentNullException.ThrowIfNull(request);
        ArgumentNullException.ThrowIfNull(completed);
        if (!ReferenceEquals(request.ProcessQueue, this))
        {
            throw new ArgumentException("The consume request belongs to another process queue.", nameof(request));
        }

        if (completed.Count != request.Entries.Count)
        {
            throw new ArgumentException("Every consume-request entry requires a completion state.", nameof(completed));
        }

        bool hasPending;
        bool commitAvailable;
        lock (_gate)
        {
            if (_dropped)
            {
                return false;
            }

            for (var index = 0; index < request.Entries.Count; index++)
            {
                var entry = request.Entries[index];
                if (entry.State != MessageState.InFlight)
                {
                    continue;
                }

                entry.State = completed[index]
                    ? MessageState.Completed
                    : entry.PendingSettlementResult.HasValue
                        ? entry.DelaySettlementRetry
                            ? MessageState.SettlementDelayed
                            : MessageState.PendingSettlement
                        : MessageState.Pending;
                entry.DelaySettlementRetry = false;
            }

            AdvanceCommittableOffsetCore();
            hasPending = HasDispatchableMessageCore();
            commitAvailable = HasCommitAvailableCore();
        }

        if (commitAvailable)
        {
            Signal(_commitAvailable);
        }

        Signal(_stateChanged);
        return hasPending;
    }

    public bool AbandonConsumeRequest(ConsumeRequest request)
    {
        ArgumentNullException.ThrowIfNull(request);
        if (!ReferenceEquals(request.ProcessQueue, this))
        {
            throw new ArgumentException("The consume request belongs to another process queue.", nameof(request));
        }

        lock (_gate)
        {
            if (_dropped)
            {
                return false;
            }

            foreach (var entry in request.Entries)
            {
                if (entry.State is MessageState.Dispatching or MessageState.InFlight)
                {
                    entry.State = entry.PendingSettlementResult.HasValue
                        ? MessageState.PendingSettlement
                        : MessageState.Pending;
                }
            }

            return _messages.Values.Any(static entry => entry.IsReady);
        }
    }

    public bool ActivateSettlementRetries(IReadOnlyList<MessageEntry> entries)
    {
        ArgumentNullException.ThrowIfNull(entries);
        lock (_gate)
        {
            if (_dropped)
            {
                return false;
            }

            foreach (var entry in entries)
            {
                if (entry.State == MessageState.SettlementDelayed)
                {
                    entry.State = MessageState.PendingSettlement;
                }
            }

            return HasDispatchableMessageCore();
        }
    }

    public async ValueTask WaitForCommitAsync(CancellationToken cancellationToken) =>
        await _commitAvailable.Reader.ReadAsync(cancellationToken).ConfigureAwait(false);

    public bool TryGetOffsetCommit(out OffsetCommit commit)
    {
        lock (_gate)
        {
            if (_dropped)
            {
                commit = default;
                return false;
            }

            if (_forcedPersistOffset is { } forced)
            {
                commit = new OffsetCommit(forced, _commitGeneration, IsForced: true);
                return true;
            }

            if (_committableOffset <= _persistedOffset)
            {
                commit = default;
                return false;
            }

            commit = new OffsetCommit(_committableOffset, _commitGeneration, IsForced: false);
            return true;
        }
    }

    public void MarkOffsetPersisted(OffsetCommit commit)
    {
        lock (_gate)
        {
            if (_dropped || commit.Generation != _commitGeneration)
            {
                return;
            }

            if (commit.IsForced)
            {
                if (_forcedPersistOffset != commit.Offset)
                {
                    return;
                }

                _persistedOffset = commit.Offset;
                _forcedPersistOffset = null;
                Signal(_stateChanged);
                return;
            }

            if (_forcedPersistOffset.HasValue ||
                commit.Offset > _committableOffset ||
                commit.Offset <= _persistedOffset)
            {
                return;
            }

            _persistedOffset = commit.Offset;
        }

        Signal(_stateChanged);
    }

    public async ValueTask WaitUntilEmptyAsync(CancellationToken cancellationToken)
    {
        while (true)
        {
            lock (_gate)
            {
                if (_messages.Count == 0 || _dropped)
                {
                    return;
                }
            }

            await _stateChanged.Reader.ReadAsync(cancellationToken).ConfigureAwait(false);
        }
    }

    public async ValueTask WaitUntilOffsetPersistedAsync(long offset, CancellationToken cancellationToken)
    {
        while (true)
        {
            lock (_gate)
            {
                if (_dropped || (_forcedPersistOffset is null && _persistedOffset >= offset))
                {
                    return;
                }
            }

            await _stateChanged.Reader.ReadAsync(cancellationToken).ConfigureAwait(false);
        }
    }

    public int MarkDropped(Action<MessageEntry>? dropFifoOrder = null)
    {
        var reservedDeliverySlots = 0;
        List<MessageEntry> fifoEntries = [];
        lock (_gate)
        {
            if (_dropped)
            {
                return 0;
            }

            _dropped = true;
            _readyQueued = false;
            foreach (var entry in _messages.Values)
            {
                if (entry.HasReservedDeliverySlot)
                {
                    entry.HasReservedDeliverySlot = false;
                    reservedDeliverySlots++;
                }

                if (entry.FifoOrder is not null && entry.State != MessageState.InFlight)
                {
                    fifoEntries.Add(entry);
                }
            }

            _messages.Clear();
        }

        foreach (var entry in fifoEntries)
        {
            if (dropFifoOrder is null)
            {
                entry.FifoOrder!.Completion.TrySetResult();
            }
            else
            {
                dropFifoOrder(entry);
            }
        }

        Signal(_commitAvailable);
        Signal(_stateChanged);
        return reservedDeliverySlots;
    }

    private void AdvanceCommittableOffsetCore()
    {
        foreach (var offset in _messages
                     .Where(static entry => entry.Value.State == MessageState.Completed)
                     .Select(static entry => entry.Key)
                     .ToArray())
        {
            _messages.Remove(offset);
        }

        var candidate = _messages.Count == 0
            ? _nextPullOffset
            : Math.Min(_messages.First().Key, _nextPullOffset);
        if (candidate <= _committableOffset)
        {
            return;
        }

        _committableOffset = candidate;
    }

    private bool HasDispatchableMessageCore()
    {
        foreach (var entry in _messages.Values)
        {
            if (entry.State == MessageState.SettlementDelayed)
            {
                return false;
            }

            if (entry.IsDispatchable)
            {
                return true;
            }
        }

        return false;
    }

    private bool HasCommitAvailableCore() =>
        _forcedPersistOffset.HasValue || _committableOffset > _persistedOffset;

    private void EnsureInitialized()
    {
        if (!_initialized)
        {
            throw new InvalidOperationException("The process queue has not been initialized.");
        }
    }

    private static Channel<bool> CreateSignalChannel() =>
        Channel.CreateBounded<bool>(new BoundedChannelOptions(1)
        {
            FullMode = BoundedChannelFullMode.DropWrite,
            SingleReader = true,
            SingleWriter = false,
            AllowSynchronousContinuations = false
        });

    private static void Signal(Channel<bool> signal) => signal.Writer.TryWrite(true);

    internal sealed class MessageEntry(RemotingMessageView message)
    {
        public RemotingMessageView Message { get; } = message;
        public MessageState State { get; set; } = MessageState.Pending;
        public bool HasReservedDeliverySlot { get; set; } = true;
        public FifoOrder? FifoOrder { get; set; }
        public ConsumeResult? PendingSettlementResult { get; set; }
        public int PendingSettlementDelayLevel { get; set; }
        public bool DelaySettlementRetry { get; set; }
        public bool IsReady => State is MessageState.Pending or MessageState.PendingSettlement;
        public bool IsDispatchable => IsReady && (FifoOrder is null || FifoOrder.Predecessor.IsCompleted);
    }

    internal sealed class FifoOrder(string key, Task predecessor)
    {
        public string Key { get; } = key;
        public Task Predecessor { get; } = predecessor;
        public TaskCompletionSource Completion { get; } =
            new(TaskCreationOptions.RunContinuationsAsynchronously);
    }

    internal enum MessageState
    {
        Pending,
        Dispatching,
        InFlight,
        PendingSettlement,
        SettlementDelayed,
        Completed
    }

    internal readonly record struct OffsetCommit(long Offset, long Generation, bool IsForced);
}
