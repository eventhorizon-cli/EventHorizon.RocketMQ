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

namespace EventHorizon.RocketMQ.Remoting.Consumer.LitePull.Assignment;

internal sealed record LitePullManualAssignment(
    IReadOnlyList<RemotingConsumerQueue> Queues,
    IReadOnlyList<KeyValuePair<string, FilterExpression>> Subscriptions)
{
    public IReadOnlyList<LitePullReceiveTarget> GetReceiveTargets()
    {
        var filters = Subscriptions.ToDictionary(
            static entry => entry.Key,
            static entry => entry.Value,
            StringComparer.Ordinal);
        return Queues
            .Select(queue => new LitePullReceiveTarget(queue, filters[queue.Topic]))
            .ToArray();
    }

    public static LitePullManualAssignment? Create(
        IReadOnlyCollection<RemotingConsumerQueue> queues,
        IReadOnlyDictionary<string, FilterExpression> filters)
    {
        ArgumentNullException.ThrowIfNull(queues);
        ArgumentNullException.ThrowIfNull(filters);
        if (queues.Count == 0)
        {
            return null;
        }

        var queueSnapshot = queues.ToArray();
        var subscriptions = queueSnapshot
            .Select(static queue => queue.Topic)
            .Distinct(StringComparer.Ordinal)
            .Order(StringComparer.Ordinal)
            .Select(topic => new KeyValuePair<string, FilterExpression>(
                topic,
                filters.TryGetValue(topic, out var filter) ? filter : FilterExpression.All))
            .ToArray();
        return new LitePullManualAssignment(queueSnapshot, subscriptions);
    }
}
