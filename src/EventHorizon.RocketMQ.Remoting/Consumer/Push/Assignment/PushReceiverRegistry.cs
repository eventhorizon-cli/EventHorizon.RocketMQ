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

using System.Collections.Concurrent;
using EventHorizon.RocketMQ.Remoting.Consumer.Push.Pull.Dispatch;
using EventHorizon.RocketMQ.Remoting.Consumer.Push.Pull.Receive;

namespace EventHorizon.RocketMQ.Remoting.Consumer.Push.Assignment;

internal sealed class PushReceiverRegistry
{
    private readonly ConcurrentDictionary<PushReceiverKey, IRemotingPushReceiver> _receivers = new();

    public IReadOnlyList<IRemotingPushReceiver> Snapshot() => _receivers.Values.ToArray();

    public IReadOnlyList<KeyValuePair<PushReceiverKey, IRemotingPushReceiver>> SnapshotEntries() =>
        _receivers.ToArray();

    public bool Contains(PushReceiverKey key) => _receivers.ContainsKey(key);

    public bool TryAdd(PushReceiverKey key, IRemotingPushReceiver receiver)
    {
        ArgumentNullException.ThrowIfNull(receiver);
        return _receivers.TryAdd(key, receiver);
    }

    public bool TryRemove(PushReceiverKey key, out IRemotingPushReceiver receiver)
    {
        var removed = _receivers.TryRemove(key, out var value);
        receiver = value!;
        return removed;
    }

    public bool TryRemove(PushReceiverKey key, IRemotingPushReceiver receiver)
    {
        ArgumentNullException.ThrowIfNull(receiver);
        return ((ICollection<KeyValuePair<PushReceiverKey, IRemotingPushReceiver>>)_receivers)
            .Remove(new KeyValuePair<PushReceiverKey, IRemotingPushReceiver>(key, receiver));
    }

    public bool HasPullQueue(RemotingConsumerQueue queue)
    {
        ArgumentNullException.ThrowIfNull(queue);
        return _receivers.Values
            .OfType<PullAssignmentReceiver>()
            .Any(receiver => receiver.Queue == queue);
    }

    public void Drop(IRemotingPushReceiver receiver, PullDeliveryCoordinator? dispatchScheduler)
    {
        ArgumentNullException.ThrowIfNull(receiver);
        if (receiver is PullAssignmentReceiver { PullProcessQueue: { } processQueue })
        {
            (dispatchScheduler ?? throw new InvalidOperationException(
                "A PULL dispatch scheduler is required for a concurrent PullProcessQueue."))
                .DropProcessQueue(processQueue);
        }

        receiver.Stop();
    }

    public IReadOnlyList<IRemotingPushReceiver> StopAll(PullDeliveryCoordinator? dispatchScheduler)
    {
        var receivers = TakeAll();
        foreach (var receiver in receivers)
        {
            Drop(receiver, dispatchScheduler);
        }

        return receivers;
    }

    public IReadOnlyList<IRemotingPushReceiver> TakeAll()
    {
        var removed = new List<IRemotingPushReceiver>();
        foreach (var entry in SnapshotEntries())
        {
            if (TryRemove(entry.Key, entry.Value))
            {
                removed.Add(entry.Value);
            }
        }

        return removed;
    }

    public static async Task AwaitAsync(IEnumerable<IRemotingPushReceiver> receivers)
    {
        ArgumentNullException.ThrowIfNull(receivers);
        await Task.WhenAll(receivers.Select(AwaitReceiverAsync)).ConfigureAwait(false);
    }

    private static async Task AwaitReceiverAsync(IRemotingPushReceiver receiver)
    {
        try
        {
            await receiver.Completion.ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
        }
    }

    public static void DisposeAll(IEnumerable<IRemotingPushReceiver> receivers)
    {
        ArgumentNullException.ThrowIfNull(receivers);
        foreach (var receiver in receivers)
        {
            receiver.Dispose();
        }
    }
}
