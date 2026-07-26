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

namespace EventHorizon.RocketMQ.Grpc.Consumer;

/// <summary>
/// Represents a RocketMQ message received through the gRPC protocol together with its delivery metadata.
/// </summary>
public sealed class GrpcMessageView
{
    internal GrpcMessageView(
        string topic,
        byte[] body,
        string messageId,
        string receiptHandle,
        string? tag,
        IReadOnlyList<string> keys,
        IReadOnlyDictionary<string, string> properties,
        int deliveryAttempt,
        string? messageGroup,
        TimeSpan? invisibleDuration,
        string? liteTopic,
        int queueId,
        string brokerName,
        long? queueOffset,
        DateTimeOffset? bornTimestamp,
        DateTimeOffset? storeTimestamp,
        bool isCorrupted,
        string? corruptionReason,
        Uri endpoint)
    {
        Topic = topic;
        Body = body;
        MessageId = messageId;
        ReceiptHandle = receiptHandle;
        Tag = tag;
        Keys = keys;
        Properties = properties;
        DeliveryAttempt = deliveryAttempt;
        MessageGroup = messageGroup;
        InvisibleDuration = invisibleDuration;
        LiteTopic = liteTopic;
        QueueId = queueId;
        BrokerName = brokerName;
        QueueOffset = queueOffset;
        BornTimestamp = bornTimestamp;
        StoreTimestamp = storeTimestamp;
        IsCorrupted = isCorrupted;
        CorruptionReason = corruptionReason;
        Endpoint = endpoint;
    }

    /// <summary>
    /// Gets the topic from which the message was received.
    /// </summary>
    public string Topic { get; }

    /// <summary>
    /// Gets the message body, decoded according to the body metadata when decoding succeeds.
    /// </summary>
    public byte[] Body { get; }

    /// <summary>
    /// Gets the server-assigned message identifier.
    /// </summary>
    public string MessageId { get; }

    /// <summary>
    /// Gets the current receipt handle used to acknowledge the message or change its invisible duration.
    /// </summary>
    public string ReceiptHandle { get; internal set; }

    /// <summary>
    /// Gets the message tag, or <see langword="null"/> when no tag was assigned.
    /// </summary>
    public string? Tag { get; }

    /// <summary>
    /// Gets the message keys.
    /// </summary>
    public IReadOnlyList<string> Keys { get; }

    /// <summary>
    /// Gets the user-defined message properties.
    /// </summary>
    public IReadOnlyDictionary<string, string> Properties { get; }

    /// <summary>
    /// Gets the current delivery attempt number.
    /// </summary>
    public int DeliveryAttempt { get; internal set; }

    /// <summary>
    /// Gets the message group used for ordered delivery, or <see langword="null"/> when none was assigned.
    /// </summary>
    public string? MessageGroup { get; }

    /// <summary>
    /// Gets the current invisible duration, or <see langword="null"/> when it was not reported by the server.
    /// </summary>
    public TimeSpan? InvisibleDuration { get; internal set; }

    /// <summary>
    /// Gets the lightweight topic identifier supplied by the server, or <see langword="null"/> when absent.
    /// </summary>
    public string? LiteTopic { get; }

    /// <summary>
    /// Gets the identifier of the message queue from which the message was received.
    /// </summary>
    public int QueueId { get; }

    /// <summary>
    /// Gets the name of the broker that owns the message queue.
    /// </summary>
    public string BrokerName { get; }

    /// <summary>
    /// Gets the offset of the message in its queue, or <see langword="null"/> when it was not reported.
    /// </summary>
    public long? QueueOffset { get; }

    /// <summary>
    /// Gets the time at which the producer created the message, or <see langword="null"/> when it was not reported.
    /// </summary>
    public DateTimeOffset? BornTimestamp { get; }

    /// <summary>
    /// Gets the time at which the server stored the message, or <see langword="null"/> when it was not reported.
    /// </summary>
    public DateTimeOffset? StoreTimestamp { get; }

    /// <summary>
    /// Gets a value indicating whether body decoding or integrity validation failed.
    /// </summary>
    public bool IsCorrupted { get; }

    /// <summary>
    /// Gets the body corruption description, or <see langword="null"/> when the message is not corrupted.
    /// </summary>
    public string? CorruptionReason { get; }

    internal Uri Endpoint { get; }
    internal ActivityContext? ReceiveActivityContext { get; set; }
    internal object? Owner { get; set; }
}
