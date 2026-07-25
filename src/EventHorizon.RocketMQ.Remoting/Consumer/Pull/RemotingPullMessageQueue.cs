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

namespace EventHorizon.RocketMQ.Remoting.Consumer.Pull;

/// <summary>
/// Identifies a readable message queue and its owning broker in the classic remoting protocol.
/// </summary>
public sealed class RemotingPullMessageQueue
{
    internal RemotingPullMessageQueue(string topic, int queueId, string brokerName, string brokerAddress)
        : this(topic, queueId, brokerName, new Dictionary<long, string> { [0] = brokerAddress })
    {
    }

    internal RemotingPullMessageQueue(
        string topic,
        int queueId,
        string brokerName,
        IReadOnlyDictionary<long, string> brokerAddresses)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(topic);
        ArgumentException.ThrowIfNullOrWhiteSpace(brokerName);
        ArgumentNullException.ThrowIfNull(brokerAddresses);
        if (brokerAddresses.Count == 0)
        {
            throw new ArgumentException("At least one broker address is required.", nameof(brokerAddresses));
        }

        Topic = topic;
        QueueId = queueId;
        BrokerName = brokerName;
        BrokerAddresses = new Dictionary<long, string>(brokerAddresses);
        BrokerAddress = BrokerAddresses.TryGetValue(0, out var master)
            ? master
            : BrokerAddresses.OrderBy(static entry => entry.Key).First().Value;
    }

    /// <summary>
    /// Gets the topic to which the queue belongs.
    /// </summary>
    public string Topic { get; }

    /// <summary>
    /// Gets the queue identifier within the topic and broker.
    /// </summary>
    public int QueueId { get; }

    /// <summary>
    /// Gets the logical name of the broker that owns the queue.
    /// </summary>
    public string BrokerName { get; }
    internal string BrokerAddress { get; }
    internal IReadOnlyDictionary<long, string> BrokerAddresses { get; }

    internal string GetPullBrokerAddress()
    {
        var preferred = Interlocked.Read(ref _preferredBrokerId);
        return BrokerAddresses.TryGetValue(preferred, out var address) ? address : BrokerAddress;
    }

    internal void SetPreferredBrokerId(long brokerId) =>
        Interlocked.Exchange(ref _preferredBrokerId, brokerId);

    private long _preferredBrokerId;
}
