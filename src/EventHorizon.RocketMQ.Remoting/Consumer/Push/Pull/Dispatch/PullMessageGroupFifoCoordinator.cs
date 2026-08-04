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

namespace EventHorizon.RocketMQ.Remoting.Consumer.Push.Pull.Dispatch;

internal sealed class PullMessageGroupFifoCoordinator
{
    private readonly object _gate = new();
    private readonly Dictionary<string, Task> _tails = new(StringComparer.Ordinal);

    public void Reserve(
        PullProcessQueue processQueue,
        PullProcessQueue.MessageEntry entry,
        Action<PullProcessQueue> notifyReady)
    {
        ArgumentNullException.ThrowIfNull(processQueue);
        ArgumentNullException.ThrowIfNull(entry);
        ArgumentNullException.ThrowIfNull(notifyReady);
        if (string.IsNullOrEmpty(entry.Message.MessageGroup))
        {
            return;
        }

        var key = $"{entry.Message.Topic}\0{entry.Message.MessageGroup}";
        lock (_gate)
        {
            var predecessor = _tails.TryGetValue(key, out var tail) ? tail : Task.CompletedTask;
            entry.FifoOrder = new PullProcessQueue.FifoOrder(key, predecessor);
            _tails[key] = entry.FifoOrder.Completion.Task;
        }

        if (!entry.FifoOrder.Predecessor.IsCompleted)
        {
            _ = NotifySuccessorAsync(processQueue, entry.FifoOrder.Predecessor, notifyReady);
        }
    }

    public void Complete(PullProcessQueue.MessageEntry entry)
    {
        ArgumentNullException.ThrowIfNull(entry);
        if (entry.FifoOrder is not { } order)
        {
            return;
        }

        order.Completion.TrySetResult();
        lock (_gate)
        {
            if (_tails.TryGetValue(order.Key, out var tail) &&
                ReferenceEquals(tail, order.Completion.Task))
            {
                _tails.Remove(order.Key);
            }
        }
    }

    public void Drop(PullProcessQueue.MessageEntry entry)
    {
        if (entry.FifoOrder is not { } order)
        {
            return;
        }

        if (order.Predecessor.IsCompleted)
        {
            Complete(entry);
        }
        else
        {
            _ = CompleteDroppedAsync(entry, order.Predecessor);
        }
    }

    public void Clear()
    {
        lock (_gate)
        {
            _tails.Clear();
        }
    }

    private static async Task NotifySuccessorAsync(
        PullProcessQueue processQueue,
        Task predecessor,
        Action<PullProcessQueue> notifyReady)
    {
        await predecessor.ConfigureAwait(false);
        processQueue.NotifyFifoPredecessorCompleted();
        notifyReady(processQueue);
    }

    private async Task CompleteDroppedAsync(PullProcessQueue.MessageEntry entry, Task predecessor)
    {
        await predecessor.ConfigureAwait(false);
        Complete(entry);
    }
}
