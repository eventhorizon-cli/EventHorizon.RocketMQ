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

namespace EventHorizon.RocketMQ.Remoting.Producer;

/// <summary>
/// Represents the broker response to a classic remoting send operation.
/// </summary>
public sealed class RemotingSendResult
{
    internal RemotingSendResult(
        RemotingSendStatus status,
        string messageId,
        string offsetMessageId,
        RemotingMessageQueue messageQueue,
        long queueOffset,
        string? transactionId,
        string? recallHandle,
        string regionId,
        bool traceEnabled)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(messageId);
        ArgumentException.ThrowIfNullOrWhiteSpace(offsetMessageId);
        ArgumentNullException.ThrowIfNull(messageQueue);
        ArgumentException.ThrowIfNullOrWhiteSpace(regionId);

        Status = status;
        MessageId = messageId;
        OffsetMessageId = offsetMessageId;
        MessageQueue = messageQueue;
        QueueOffset = queueOffset;
        TransactionId = transactionId;
        RecallHandle = recallHandle;
        RegionId = regionId;
        TraceEnabled = traceEnabled;
    }

    /// <summary>
    /// Gets the send status reported by the broker.
    /// </summary>
    public RemotingSendStatus Status { get; }

    /// <summary>
    /// Gets the unique message identifier assigned by the producer.
    /// </summary>
    public string MessageId { get; }

    /// <summary>
    /// Gets the physical message identifier assigned by the broker.
    /// </summary>
    /// <remarks>
    /// This differs from <see cref="MessageId"/> and encodes the broker endpoint and commit-log offset.
    /// </remarks>
    public string OffsetMessageId { get; }

    /// <summary>
    /// Gets the message queue selected for the send operation.
    /// </summary>
    public RemotingMessageQueue MessageQueue { get; }

    /// <summary>
    /// Gets the message's logical offset within the selected queue.
    /// </summary>
    public long QueueOffset { get; }

    /// <summary>
    /// Gets the optional transaction identifier returned by the broker.
    /// </summary>
    public string? TransactionId { get; }

    /// <summary>
    /// Gets the handle used to recall a delayed message, or <see langword="null"/> when recall is not available.
    /// </summary>
    public string? RecallHandle { get; }

    /// <summary>
    /// Gets the broker region identifier associated with the send operation.
    /// </summary>
    public string RegionId { get; }

    /// <summary>
    /// Gets a value indicating whether the broker reported message tracing as enabled.
    /// </summary>
    public bool TraceEnabled { get; }
}
