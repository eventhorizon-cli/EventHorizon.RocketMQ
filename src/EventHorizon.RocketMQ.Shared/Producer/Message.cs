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

namespace EventHorizon.RocketMQ.Producer;

/// <summary>
/// Represents a message to publish to RocketMQ.
/// </summary>
/// <remarks>
/// <see cref="MessageGroup"/>, <see cref="DeliveryTimestamp"/>, <see cref="Priority"/>, and
/// <see cref="LiteTopic"/> select mutually exclusive specialized RocketMQ message types.
/// </remarks>
public class Message
{
    /// <summary>
    /// Initializes a new instance of the <see cref="Message"/> class.
    /// </summary>
    /// <param name="topic">The topic to which the message is published.</param>
    /// <param name="body">The message payload.</param>
    /// <exception cref="ArgumentNullException">
    /// <paramref name="topic"/> or <paramref name="body"/> is <see langword="null"/>.
    /// </exception>
    /// <exception cref="ArgumentException"><paramref name="topic"/> is empty or consists only of white-space characters.</exception>
    public Message(string topic, byte[] body)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(topic);
        ArgumentNullException.ThrowIfNull(body);
        Topic = topic;
        Body = body;
    }

    /// <summary>
    /// Gets the topic to which the message is published.
    /// </summary>
    public string Topic { get; }

    /// <summary>
    /// Gets the mutable collection of user-defined message properties.
    /// </summary>
    public IDictionary<string, string> Properties { get; } = new Dictionary<string, string>(StringComparer.Ordinal);

    /// <summary>
    /// Gets the message payload.
    /// </summary>
    public byte[] Body { get; }

    /// <summary>
    /// Gets or initializes the optional tag used to categorize the message.
    /// </summary>
    public string? Tag { get; init; }

    /// <summary>
    /// Gets the mutable collection of keys used to index the message.
    /// </summary>
    public IList<string> Keys { get; } = new List<string>();

    /// <summary>
    /// Gets or initializes the optional grouping key for ordered delivery of FIFO messages.
    /// </summary>
    public string? MessageGroup { get; init; }

    /// <summary>
    /// Gets or initializes the optional time at which a delayed message becomes eligible for delivery.
    /// </summary>
    public DateTimeOffset? DeliveryTimestamp { get; init; }

    /// <summary>
    /// Gets or initializes the optional non-negative priority of a priority message.
    /// </summary>
    public int? Priority { get; init; }

    /// <summary>
    /// Gets or initializes the optional lite topic for a lite message.
    /// </summary>
    public string? LiteTopic { get; init; }
}
