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
/// Represents a reply delivered to a classic remoting request producer.
/// </summary>
public sealed class RemotingReplyMessage
{
    internal RemotingReplyMessage(
        string topic,
        byte[] body,
        string correlationId,
        string? messageId,
        string? tag,
        IReadOnlyList<string> keys,
        IReadOnlyDictionary<string, string> properties,
        DateTimeOffset bornTimestamp,
        DateTimeOffset storeTimestamp)
    {
        Topic = topic;
        Body = body;
        CorrelationId = correlationId;
        MessageId = messageId;
        Tag = tag;
        Keys = keys;
        Properties = properties;
        BornTimestamp = bornTimestamp;
        StoreTimestamp = storeTimestamp;
    }

    /// <summary>
    /// Gets the topic to which the reply was published.
    /// </summary>
    public string Topic { get; }

    /// <summary>
    /// Gets the reply payload.
    /// </summary>
    public byte[] Body { get; }

    /// <summary>
    /// Gets the request correlation identifier.
    /// </summary>
    public string CorrelationId { get; }

    /// <summary>
    /// Gets the optional reply message identifier reported in its properties.
    /// </summary>
    public string? MessageId { get; }

    /// <summary>
    /// Gets the optional reply tag.
    /// </summary>
    public string? Tag { get; }

    /// <summary>
    /// Gets the application-defined reply keys.
    /// </summary>
    public IReadOnlyList<string> Keys { get; }

    /// <summary>
    /// Gets the decoded classic RocketMQ reply properties.
    /// </summary>
    public IReadOnlyDictionary<string, string> Properties { get; }

    /// <summary>
    /// Gets the timestamp at which the reply producer created the message.
    /// </summary>
    public DateTimeOffset BornTimestamp { get; }

    /// <summary>
    /// Gets the timestamp at which the broker stored the reply message.
    /// </summary>
    public DateTimeOffset StoreTimestamp { get; }
}
