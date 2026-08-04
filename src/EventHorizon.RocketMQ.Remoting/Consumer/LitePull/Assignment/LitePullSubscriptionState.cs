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

internal sealed class LitePullSubscriptionState
{
    private readonly object _gate = new();
    private readonly Dictionary<string, FilterExpression> _subscriptions;

    public LitePullSubscriptionState(IReadOnlyDictionary<string, FilterExpression> subscriptions)
    {
        ArgumentNullException.ThrowIfNull(subscriptions);
        _subscriptions = new Dictionary<string, FilterExpression>(subscriptions, StringComparer.Ordinal);
    }

    public bool IsEmpty
    {
        get
        {
            lock (_gate)
            {
                return _subscriptions.Count == 0;
            }
        }
    }

    public void Set(string topic, FilterExpression filter)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(topic);
        ArgumentNullException.ThrowIfNull(filter);
        lock (_gate)
        {
            _subscriptions[topic] = filter;
        }
    }

    public bool Remove(string topic)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(topic);
        lock (_gate)
        {
            return _subscriptions.Remove(topic);
        }
    }

    public KeyValuePair<string, FilterExpression>[] Snapshot()
    {
        lock (_gate)
        {
            return _subscriptions
                .OrderBy(static entry => entry.Key, StringComparer.Ordinal)
                .ToArray();
        }
    }
}
