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

namespace EventHorizon.RocketMQ.Grpc.Consumer.Lite;

/// <summary>
/// Selects the initial offset used when a LiteTopic subscription is created.
/// </summary>
public sealed class GrpcLiteOffsetOption
{
    private readonly OffsetKind _kind;
    private readonly long _value;

    private GrpcLiteOffsetOption(OffsetKind kind, long value = 0)
    {
        _kind = kind;
        _value = value;
    }

    /// <summary>
    /// Gets an option that starts from the service-defined most recent position.
    /// </summary>
    public static GrpcLiteOffsetOption Last { get; } = new(OffsetKind.Last);

    /// <summary>
    /// Gets an option that starts from the earliest available position.
    /// </summary>
    public static GrpcLiteOffsetOption Min { get; } = new(OffsetKind.Min);

    /// <summary>
    /// Gets an option that starts from the latest available position.
    /// </summary>
    public static GrpcLiteOffsetOption Max { get; } = new(OffsetKind.Max);

    /// <summary>
    /// Creates an option that starts at an explicit non-negative queue offset.
    /// </summary>
    /// <param name="offset">The non-negative offset at which consumption begins.</param>
    /// <returns>An offset option for <paramref name="offset"/>.</returns>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="offset"/> is negative.</exception>
    public static GrpcLiteOffsetOption AtOffset(long offset)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(offset);
        return new GrpcLiteOffsetOption(OffsetKind.Offset, offset);
    }

    /// <summary>
    /// Creates an option that starts with the requested number of most recent messages.
    /// </summary>
    /// <param name="messageCount">The positive number of recent messages to include.</param>
    /// <returns>An offset option for the requested tail length.</returns>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="messageCount"/> is not positive.</exception>
    public static GrpcLiteOffsetOption Tail(long messageCount)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(messageCount);
        return new GrpcLiteOffsetOption(OffsetKind.Tail, messageCount);
    }

    /// <summary>
    /// Creates an option that starts at the first message stored at or after a timestamp.
    /// </summary>
    /// <param name="timestamp">The UTC-compatible timestamp from which consumption begins.</param>
    /// <returns>An offset option for <paramref name="timestamp"/>.</returns>
    public static GrpcLiteOffsetOption AtTimestamp(DateTimeOffset timestamp) =>
        new(OffsetKind.Timestamp, timestamp.ToUnixTimeMilliseconds());

    internal Proto.OffsetOption ToProtobuf() => _kind switch
    {
        OffsetKind.Last => new Proto.OffsetOption { Policy = Proto.OffsetOption.Types.Policy.Last },
        OffsetKind.Min => new Proto.OffsetOption { Policy = Proto.OffsetOption.Types.Policy.Min },
        OffsetKind.Max => new Proto.OffsetOption { Policy = Proto.OffsetOption.Types.Policy.Max },
        OffsetKind.Offset => new Proto.OffsetOption { Offset = _value },
        OffsetKind.Tail => new Proto.OffsetOption { TailN = _value },
        OffsetKind.Timestamp => new Proto.OffsetOption { Timestamp = _value },
        _ => throw new InvalidOperationException("Unknown Lite offset option.")
    };

    private enum OffsetKind
    {
        Last,
        Min,
        Max,
        Offset,
        Tail,
        Timestamp
    }
}
