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

using System.Globalization;

namespace EventHorizon.RocketMQ.Remoting.Producer;

/// <summary>
/// Represents a response to a classic RocketMQ request message.
/// </summary>
/// <remarks>
/// Create this value from the <c>CLUSTER</c>, <c>CORRELATION_ID</c>, <c>REPLY_TO_CLIENT</c>, and <c>TTL</c>
/// properties of the received request. <see cref="FromRequestProperties"/> performs that extraction directly.
/// </remarks>
public sealed class RemotingReply : Message
{
    /// <summary>
    /// Initializes a new instance of the <see cref="RemotingReply"/> class.
    /// </summary>
    /// <param name="cluster">The RocketMQ cluster that owns the request.</param>
    /// <param name="correlationId">The correlation identifier copied from the request.</param>
    /// <param name="replyToClient">The requester client identifier copied from the request.</param>
    /// <param name="timeToLive">The remaining broker reply time-to-live copied from the request.</param>
    /// <param name="body">The reply payload.</param>
    /// <exception cref="ArgumentException">
    /// <paramref name="cluster"/>, <paramref name="correlationId"/>, or <paramref name="replyToClient"/> is empty,
    /// or <paramref name="timeToLive"/> is not positive.
    /// </exception>
    public RemotingReply(
        string cluster,
        string correlationId,
        string replyToClient,
        TimeSpan timeToLive,
        byte[] body)
        : base(CreateReplyTopic(cluster), body)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(correlationId);
        ArgumentException.ThrowIfNullOrWhiteSpace(replyToClient);
        if (timeToLive <= TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(nameof(timeToLive), "Reply time-to-live must be positive.");
        }

        Cluster = cluster;
        CorrelationId = correlationId;
        ReplyToClient = replyToClient;
        TimeToLive = timeToLive;
        Properties["MSG_TYPE"] = "reply";
        Properties["CORRELATION_ID"] = correlationId;
        Properties["REPLY_TO_CLIENT"] = replyToClient;
        Properties["TTL"] = ToMilliseconds(timeToLive).ToString(CultureInfo.InvariantCulture);
    }

    /// <summary>
    /// Gets the RocketMQ cluster that owns the request.
    /// </summary>
    public string Cluster { get; }

    /// <summary>
    /// Gets the correlation identifier copied from the request.
    /// </summary>
    public string CorrelationId { get; }

    /// <summary>
    /// Gets the requester client identifier copied from the request.
    /// </summary>
    public string ReplyToClient { get; }

    /// <summary>
    /// Gets the broker reply time-to-live copied from the request.
    /// </summary>
    public TimeSpan TimeToLive { get; }

    /// <summary>
    /// Creates a reply from the required properties of a received request message.
    /// </summary>
    /// <param name="requestProperties">The decoded properties of the received request.</param>
    /// <param name="body">The reply payload.</param>
    /// <returns>A reply ready to send with <see cref="IRemotingProducer.SendReplyAsync"/>.</returns>
    /// <exception cref="ArgumentException">A required request property is missing or contains an invalid TTL.</exception>
    public static RemotingReply FromRequestProperties(
        IReadOnlyDictionary<string, string> requestProperties,
        byte[] body)
    {
        ArgumentNullException.ThrowIfNull(requestProperties);
        return new RemotingReply(
            GetRequiredProperty(requestProperties, "CLUSTER"),
            GetRequiredProperty(requestProperties, "CORRELATION_ID"),
            GetRequiredProperty(requestProperties, "REPLY_TO_CLIENT"),
            ParseTimeToLive(GetRequiredProperty(requestProperties, "TTL")),
            body);
    }

    private static string CreateReplyTopic(string cluster)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(cluster);
        return $"{cluster}_REPLY_TOPIC";
    }

    private static string GetRequiredProperty(
        IReadOnlyDictionary<string, string> properties,
        string name) =>
        properties.TryGetValue(name, out var value) && !string.IsNullOrWhiteSpace(value)
            ? value
            : throw new ArgumentException($"The received request does not contain a valid '{name}' property.", nameof(properties));

    private static TimeSpan ParseTimeToLive(string value)
    {
        if (!long.TryParse(value, NumberStyles.None, CultureInfo.InvariantCulture, out var milliseconds) ||
            milliseconds <= 0)
        {
            throw new ArgumentException("The received request contains an invalid 'TTL' property.", nameof(value));
        }

        try
        {
            return TimeSpan.FromMilliseconds(milliseconds);
        }
        catch (ArgumentOutOfRangeException exception)
        {
            throw new ArgumentException("The received request contains an invalid 'TTL' property.", nameof(value), exception);
        }
    }

    private static long ToMilliseconds(TimeSpan value)
    {
        try
        {
            return checked((long)Math.Ceiling(value.TotalMilliseconds));
        }
        catch (OverflowException)
        {
            throw new ArgumentOutOfRangeException(nameof(value), "Reply time-to-live is too large.");
        }
    }
}
