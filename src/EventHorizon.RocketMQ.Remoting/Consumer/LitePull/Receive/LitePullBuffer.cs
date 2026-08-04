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

namespace EventHorizon.RocketMQ.Remoting.Consumer.LitePull.Receive;

internal sealed class LitePullBuffer
{
    private readonly object _sync = new();
    private readonly LinkedList<MessageEntry> _messages = [];
    private readonly int _maxMessages;
    private readonly int _maxBytes;
    private TaskCompletionSource _stateChanged = CreateStateSignal();
    private int _bufferedBytes;

    public LitePullBuffer(int maxMessages, int maxBytes)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(maxMessages);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(maxBytes);

        _maxMessages = maxMessages;
        _maxBytes = maxBytes;
    }

    public ReceiveAttempt TryStartReceive(
        int requestedMessages,
        int requestedBytes,
        bool allowWhenFull = false)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(requestedMessages);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(requestedBytes);

        lock (_sync)
        {
            if (!allowWhenFull && (_messages.Count >= _maxMessages || _bufferedBytes >= _maxBytes))
            {
                return ReceiveAttempt.Wait(_stateChanged.Task);
            }

            return ReceiveAttempt.Ready(
                Math.Min(requestedMessages, _maxMessages),
                Math.Min(requestedBytes, _maxBytes));
        }
    }

    public AdmissionAttempt TryAdmit(IReadOnlyList<MessageEntry> messages)
    {
        ArgumentNullException.ThrowIfNull(messages);
        if (messages.Count == 0)
        {
            return AdmissionAttempt.AdmittedResult;
        }

        if (messages.Count > _maxMessages)
        {
            throw new InvalidDataException(
                $"Broker returned {messages.Count} messages for a cache limit of {_maxMessages}.");
        }

        var actualBytes = messages.Sum(static message => (long)message.BodyBytes);
        var isOversizedSingle = messages.Count == 1 && actualBytes > _maxBytes;
        if (actualBytes > _maxBytes && !isOversizedSingle)
        {
            throw new InvalidDataException(
                $"Broker returned {actualBytes} message bytes for a cache limit of {_maxBytes} bytes.");
        }

        lock (_sync)
        {
            if (_messages.Count + messages.Count > _maxMessages ||
                (!isOversizedSingle && _bufferedBytes + actualBytes > _maxBytes) ||
                (isOversizedSingle && _messages.Count != 0))
            {
                return AdmissionAttempt.Wait(_stateChanged.Task);
            }

            var addedMessages = 0;
            try
            {
                foreach (var message in messages)
                {
                    _messages.AddLast(message);
                    addedMessages++;
                }
            }
            catch
            {
                while (addedMessages-- > 0)
                {
                    _messages.RemoveLast();
                }

                throw;
            }

            _bufferedBytes = checked(_bufferedBytes + (int)actualBytes);
            SignalStateChangedCore();
            return AdmissionAttempt.AdmittedResult;
        }
    }

    public IReadOnlyList<MessageEntry> Drain()
    {
        lock (_sync)
        {
            if (_messages.Count == 0)
            {
                return [];
            }

            var messages = _messages.ToArray();
            _messages.Clear();
            _bufferedBytes = 0;
            SignalStateChangedCore();
            return messages;
        }
    }

    public void Remove(LitePullQueueState state)
    {
        ArgumentNullException.ThrowIfNull(state);
        lock (_sync)
        {
            var node = _messages.First;
            while (node is not null)
            {
                var next = node.Next;
                if (ReferenceEquals(node.Value.State, state))
                {
                    _bufferedBytes -= node.Value.BodyBytes;
                    _messages.Remove(node);
                }

                node = next;
            }

            SignalStateChangedCore();
        }
    }

    public void Clear()
    {
        lock (_sync)
        {
            _messages.Clear();
            _bufferedBytes = 0;
            SignalStateChangedCore();
        }
    }

    public Task StateChanged
    {
        get
        {
            lock (_sync)
            {
                return _stateChanged.Task;
            }
        }
    }

    public void SignalStateChanged()
    {
        lock (_sync)
        {
            SignalStateChangedCore();
        }
    }

    private void SignalStateChangedCore()
    {
        var signal = _stateChanged;
        _stateChanged = CreateStateSignal();
        signal.TrySetResult();
    }

    private static TaskCompletionSource CreateStateSignal() =>
        new(TaskCreationOptions.RunContinuationsAsynchronously);

    internal sealed record MessageEntry(
        LitePullQueueState State,
        RemotingMessageView Message,
        long DeliveredOffset,
        int BodyBytes);

    internal readonly record struct ReceiveAttempt(
        bool CanReceive,
        int MaxMessages,
        int MaxBytes,
        Task StateChanged)
    {
        public static ReceiveAttempt Wait(Task stateChanged) => new(false, 0, 0, stateChanged);

        public static ReceiveAttempt Ready(int maxMessages, int maxBytes) =>
            new(true, maxMessages, maxBytes, Task.CompletedTask);
    }

    internal readonly record struct AdmissionAttempt(bool Admitted, Task StateChanged)
    {
        public static AdmissionAttempt AdmittedResult => new(true, Task.CompletedTask);

        public static AdmissionAttempt Wait(Task stateChanged) => new(false, stateChanged);
    }
}
