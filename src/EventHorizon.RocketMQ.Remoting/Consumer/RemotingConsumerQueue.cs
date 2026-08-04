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

namespace EventHorizon.RocketMQ.Remoting.Consumer;

/// <summary>
/// Identifies a readable message queue in the RocketMQ classic remoting protocol.
/// </summary>
public sealed class RemotingConsumerQueue : IEquatable<RemotingConsumerQueue>
{
    /// <summary>
    /// Initializes a new instance of the <see cref="RemotingConsumerQueue"/> class.
    /// </summary>
    /// <param name="topic">The topic to which the queue belongs.</param>
    /// <param name="brokerName">The logical name of the Broker that owns the queue.</param>
    /// <param name="queueId">The non-negative queue identifier within the topic and Broker.</param>
    public RemotingConsumerQueue(string topic, string brokerName, int queueId)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(topic);
        ArgumentException.ThrowIfNullOrWhiteSpace(brokerName);
        ArgumentOutOfRangeException.ThrowIfNegative(queueId);

        Topic = topic;
        BrokerName = brokerName;
        QueueId = queueId;
    }

    /// <summary>
    /// Gets the topic to which the queue belongs.
    /// </summary>
    public string Topic { get; }

    /// <summary>
    /// Gets the logical name of the Broker that owns the queue.
    /// </summary>
    public string BrokerName { get; }

    /// <summary>
    /// Gets the queue identifier within the topic and Broker.
    /// </summary>
    public int QueueId { get; }

    /// <inheritdoc/>
    public bool Equals(RemotingConsumerQueue? other) =>
        other is not null &&
        string.Equals(Topic, other.Topic, StringComparison.Ordinal) &&
        string.Equals(BrokerName, other.BrokerName, StringComparison.Ordinal) &&
        QueueId == other.QueueId;

    /// <inheritdoc/>
    public override bool Equals(object? obj) => obj is RemotingConsumerQueue other && Equals(other);

    /// <inheritdoc/>
    public override int GetHashCode() => HashCode.Combine(
        StringComparer.Ordinal.GetHashCode(Topic),
        StringComparer.Ordinal.GetHashCode(BrokerName),
        QueueId);

    /// <summary>
    /// Determines whether two queue identities are equal.
    /// </summary>
    public static bool operator ==(RemotingConsumerQueue? left, RemotingConsumerQueue? right) =>
        EqualityComparer<RemotingConsumerQueue>.Default.Equals(left, right);

    /// <summary>
    /// Determines whether two queue identities are different.
    /// </summary>
    public static bool operator !=(RemotingConsumerQueue? left, RemotingConsumerQueue? right) => !(left == right);

}
