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

using Proto = Apache.Rocketmq.V2;

namespace EventHorizon.RocketMQ.Grpc.Producer.Transactions;

/// <summary>
/// Represents a transactional message presented to the transaction-check callback.
/// </summary>
public sealed class TransactionMessage
{
    internal TransactionMessage(Proto.Message message, string transactionId)
    {
        var systemProperties = message.SystemProperties;
        TransactionId = transactionId;
        MessageId = systemProperties.MessageId;
        Topic = message.Topic.Name;
        Body = message.Body.ToByteArray();
        Tag = systemProperties.HasTag ? systemProperties.Tag : null;
        Keys = systemProperties.Keys.ToArray();
        Properties = new Dictionary<string, string>(message.UserProperties, StringComparer.Ordinal);
        BornTimestamp = systemProperties.BornTimestamp?.ToDateTimeOffset();
        TraceContext = systemProperties.HasTraceContext ? systemProperties.TraceContext : null;
    }

    /// <summary>
    /// Gets the server-assigned transaction identifier.
    /// </summary>
    public string TransactionId { get; }

    /// <summary>
    /// Gets the server-assigned message identifier.
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
    /// Gets the time at which the producer created the message, or <see langword="null"/> when it was not reported.
    /// </summary>
    public DateTimeOffset? BornTimestamp { get; }

    /// <summary>
    /// Gets the distributed-tracing context, or <see langword="null"/> when none was supplied.
    /// </summary>
    public string? TraceContext { get; }
}
