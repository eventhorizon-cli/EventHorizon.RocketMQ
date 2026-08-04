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

namespace EventHorizon.RocketMQ.Remoting.Consumer.Coordination.Rebalance;

internal static class AverageQueueAllocator
{
    public static IReadOnlyList<RemotingConsumerQueue> Allocate(
        IReadOnlyList<RemotingConsumerQueue> queues,
        IReadOnlyList<string> consumerIds,
        string currentConsumerId)
    {
        ArgumentNullException.ThrowIfNull(queues);
        ArgumentNullException.ThrowIfNull(consumerIds);
        ArgumentException.ThrowIfNullOrWhiteSpace(currentConsumerId);
        if (queues.Count == 0 || consumerIds.Count == 0)
        {
            return Array.Empty<RemotingConsumerQueue>();
        }

        var orderedConsumers = consumerIds
            .Distinct(StringComparer.Ordinal)
            .OrderBy(static value => value, StringComparer.Ordinal)
            .ToArray();
        var index = Array.IndexOf(orderedConsumers, currentConsumerId);
        if (index < 0)
        {
            return Array.Empty<RemotingConsumerQueue>();
        }

        var orderedQueues = queues
            .OrderBy(static queue => queue.Topic, StringComparer.Ordinal)
            .ThenBy(static queue => queue.BrokerName, StringComparer.Ordinal)
            .ThenBy(static queue => queue.QueueId)
            .ToArray();
        var remainder = orderedQueues.Length % orderedConsumers.Length;
        var averageSize = orderedQueues.Length <= orderedConsumers.Length
            ? 1
            : remainder > 0 && index < remainder
                ? orderedQueues.Length / orderedConsumers.Length + 1
                : orderedQueues.Length / orderedConsumers.Length;
        var startIndex = remainder > 0 && index < remainder
            ? index * averageSize
            : index * averageSize + remainder;
        var range = Math.Min(averageSize, orderedQueues.Length - startIndex);
        return range <= 0
            ? Array.Empty<RemotingConsumerQueue>()
            : orderedQueues.AsSpan(startIndex, range).ToArray();
    }
}
