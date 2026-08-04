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

internal sealed class PullConsumeRequest
{
    public PullConsumeRequest(
        PullProcessQueue processQueue,
        IReadOnlyList<PullProcessQueue.MessageEntry> entries,
        PullCacheReservation cacheReservation,
        bool isSettlementRetry)
    {
        ArgumentNullException.ThrowIfNull(processQueue);
        ArgumentNullException.ThrowIfNull(entries);
        if (entries.Count == 0)
        {
            throw new ArgumentException("At least one message entry is required.", nameof(entries));
        }

        PullProcessQueue = processQueue;
        Entries = entries;
        Messages = entries.Select(static entry => entry.Message).ToArray();
        CacheReservation = cacheReservation;
        IsSettlementRetry = isSettlementRetry;
    }

    public PullProcessQueue PullProcessQueue { get; }

    public IReadOnlyList<PullProcessQueue.MessageEntry> Entries { get; }

    public IReadOnlyList<RemotingMessageView> Messages { get; }

    public PullCacheReservation CacheReservation { get; }

    public bool IsSettlementRetry { get; }

    public CancellationToken QueueCancellation => PullProcessQueue.QueueCancellation;
}
