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

namespace EventHorizon.RocketMQ.Remoting.Producer.Transactions;

/// <summary>
/// Represents a transactional message presented to the broker transaction-check callback.
/// </summary>
public sealed class RemotingTransactionMessage
{
    internal RemotingTransactionMessage(
        string? transactionId,
        string messageId,
        string topic,
        byte[] body,
        string? tag,
        IReadOnlyList<string> keys,
        IReadOnlyDictionary<string, string> properties,
        DateTimeOffset bornTimestamp)
    {
        TransactionId = transactionId;
        MessageId = messageId;
        Topic = topic;
        Body = body;
        Tag = tag;
        Keys = keys;
        Properties = properties;
        BornTimestamp = bornTimestamp;
    }

    /// <summary>
    /// Gets the broker-assigned transaction identifier.
    /// </summary>
    public string? TransactionId { get; }

    /// <summary>
    /// Gets the producer-assigned message identifier.
    /// </summary>
    public string MessageId { get; }

    /// <summary>
    /// Gets the topic to which the message was sent.
    /// </summary>
    public string Topic { get; }

    /// <summary>
    /// Gets the message body.
    /// </summary>
    public byte[] Body { get; }

    /// <summary>
    /// Gets the optional message tag.
    /// </summary>
    public string? Tag { get; }

    /// <summary>
    /// Gets the message keys.
    /// </summary>
    public IReadOnlyList<string> Keys { get; }

    /// <summary>
    /// Gets the message properties decoded from the classic wire format.
    /// </summary>
    public IReadOnlyDictionary<string, string> Properties { get; }

    /// <summary>
    /// Gets the time at which the producer created the message.
    /// </summary>
    public DateTimeOffset BornTimestamp { get; }
}
