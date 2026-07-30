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

namespace EventHorizon.RocketMQ.Remoting.Tests.Consumer;

internal sealed class TestRemotingRebalanceService : IRemotingRebalanceService
{
    private readonly object _sync = new();
    private readonly Dictionary<string, Func<CancellationToken, Task<bool>>> _participants =
        new(StringComparer.Ordinal);

    public int ActiveRegistrations
    {
        get
        {
            lock (_sync)
            {
                return _participants.Count;
            }
        }
    }

    public IRemotingRebalanceRegistration Register(
        string consumerGroup,
        Func<CancellationToken, Task<bool>> rebalance)
    {
        lock (_sync)
        {
            if (!_participants.TryAdd(consumerGroup, rebalance))
            {
                throw new InvalidOperationException($"Consumer group '{consumerGroup}' is already registered.");
            }
        }

        return new Registration(this, consumerGroup, rebalance);
    }

    public Task<bool> RebalanceAsync(string consumerGroup, CancellationToken cancellationToken)
    {
        Func<CancellationToken, Task<bool>> rebalance;
        lock (_sync)
        {
            rebalance = _participants[consumerGroup];
        }

        return rebalance(cancellationToken);
    }

    private void Unregister(
        string consumerGroup,
        Func<CancellationToken, Task<bool>> rebalance)
    {
        lock (_sync)
        {
            if (_participants.TryGetValue(consumerGroup, out var current) && current == rebalance)
            {
                _participants.Remove(consumerGroup);
            }
        }
    }

    private sealed class Registration(
        TestRemotingRebalanceService owner,
        string consumerGroup,
        Func<CancellationToken, Task<bool>> rebalance) : IRemotingRebalanceRegistration
    {
        private int _disposed;

        public void Wakeup() => _ = WakeupAsync();

        public ValueTask DisposeAsync()
        {
            if (Interlocked.Exchange(ref _disposed, 1) == 0)
            {
                owner.Unregister(consumerGroup, rebalance);
            }

            return ValueTask.CompletedTask;
        }

        private async Task WakeupAsync()
        {
            try
            {
                await rebalance(CancellationToken.None);
            }
            catch
            {
            }
        }
    }
}
