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

using System.Diagnostics;

namespace EventHorizon.RocketMQ.Remoting.Consumer;

/// <summary>
/// Represents a message received through the RocketMQ classic remoting protocol.
/// </summary>
public sealed class RemotingMessageView
{
    internal RemotingMessageView(
        string topic,
        byte[] body,
        string messageId,
        string offsetMessageId,
        string? tag,
        IReadOnlyList<string> keys,
        IReadOnlyDictionary<string, string> properties,
        int deliveryAttempt,
        string? messageGroup,
        int queueId,
        string? brokerName,
        long queueOffset,
        long commitLogOffset,
        DateTimeOffset bornTimestamp,
        DateTimeOffset storeTimestamp)
    {
        Topic = topic;
        Body = body;
        MessageId = messageId;
        OffsetMessageId = offsetMessageId;
        Tag = tag;
        Keys = keys;
        Properties = properties;
        DeliveryAttempt = deliveryAttempt;
        MessageGroup = messageGroup;
        QueueId = queueId;
        BrokerName = brokerName;
        QueueOffset = queueOffset;
        CommitLogOffset = commitLogOffset;
        BornTimestamp = bornTimestamp;
        StoreTimestamp = storeTimestamp;
    }

    /// <summary>
    /// Gets the topic from which the message was received.
    /// </summary>
    public string Topic { get; }

    /// <summary>
    /// Gets the message body.
    /// </summary>
    public byte[] Body { get; }

    /// <summary>
    /// Gets the producer-assigned unique message identifier, or <see cref="OffsetMessageId"/> when it is unavailable.
    /// </summary>
    public string MessageId { get; }

    /// <summary>
    /// Gets the physical message identifier assigned by the broker.
    /// </summary>
    public string OffsetMessageId { get; }

    /// <summary>
    /// Gets the optional message tag.
    /// </summary>
    public string? Tag { get; }

    /// <summary>
    /// Gets the application-defined message keys.
    /// </summary>
    public IReadOnlyList<string> Keys { get; }

    /// <summary>
    /// Gets the message properties decoded from the classic wire format.
    /// </summary>
    public IReadOnlyDictionary<string, string> Properties { get; }

    /// <summary>
    /// Gets the one-based delivery attempt reported by the broker.
    /// </summary>
    public int DeliveryAttempt { get; }

    /// <summary>
    /// Gets the optional message group used to preserve FIFO ordering.
    /// </summary>
    public string? MessageGroup { get; }

    /// <summary>
    /// Gets the identifier of the queue from which the message was received.
    /// </summary>
    public int QueueId { get; }

    /// <summary>
    /// Gets the logical name of the broker that owns the queue, or <see langword="null"/> when it is unknown.
    /// </summary>
    public string? BrokerName { get; }

    /// <summary>
    /// Gets the message's logical offset within its queue.
    /// </summary>
    public long QueueOffset { get; }

    /// <summary>
    /// Gets the message's physical offset within the broker commit log.
    /// </summary>
    public long CommitLogOffset { get; }

    /// <summary>
    /// Gets the timestamp at which the producer created the message.
    /// </summary>
    public DateTimeOffset BornTimestamp { get; }

    /// <summary>
    /// Gets the timestamp at which the broker stored the message.
    /// </summary>
    public DateTimeOffset StoreTimestamp { get; }

    internal ActivityContext? ReceiveActivityContext { get; set; }

    internal RemotingMessageView WithTopic(string topic)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(topic);
        if (string.Equals(Topic, topic, StringComparison.Ordinal))
        {
            return this;
        }

        return new RemotingMessageView(
            topic,
            Body,
            MessageId,
            OffsetMessageId,
            Tag,
            Keys,
            Properties,
            DeliveryAttempt,
            MessageGroup,
            QueueId,
            BrokerName,
            QueueOffset,
            CommitLogOffset,
            BornTimestamp,
            StoreTimestamp)
        {
            ReceiveActivityContext = ReceiveActivityContext
        };
    }
}
