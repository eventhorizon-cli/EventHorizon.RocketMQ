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

namespace EventHorizon.RocketMQ.Grpc.Consumer;

/// <summary>
/// Specifies the outcome of processing a consumed Push or LitePush message.
/// </summary>
/// <remarks>
/// Regular Push normalizes <see cref="Suspend(TimeSpan)"/> to <see cref="Failure"/>. LitePush preserves Suspend as a
/// caller-duration invisibility change with the protocol suspend flag. Failure uses service-owned retry progression
/// for regular non-FIFO Push; FIFO Push and every LitePush mode retry locally before client-owned dead-letter
/// forwarding.
/// </remarks>
/// <seealso href="https://github.com/apache/rocketmq-clients/blob/java-5.2.1/java/client-apis/src/main/java/org/apache/rocketmq/client/apis/consumer/ConsumeResultSuspend.java">
/// Apache RocketMQ Java duration-carrying suspend result.
/// </seealso>
public sealed record ConsumeResult
{
    private static readonly TimeSpan MinimumSuspendDuration = TimeSpan.FromMilliseconds(50);

    private ConsumeResult(string name, TimeSpan? suspendDuration = null)
    {
        Name = name;
        SuspendDuration = suspendDuration;
    }

    /// <summary>
    /// Indicates that the message was processed successfully and can be acknowledged.
    /// </summary>
    public static ConsumeResult Success { get; } = new("Success");

    /// <summary>
    /// Indicates that message processing failed and should follow the effective retry policy.
    /// </summary>
    public static ConsumeResult Failure { get; } = new("Failure");

    /// <summary>
    /// Gets the requested LitePush suspension duration, or <see langword="null"/> for Success and Failure.
    /// </summary>
    public TimeSpan? SuspendDuration { get; }

    internal string Name { get; }

    /// <summary>
    /// Creates a LitePush result that delays redelivery using the protocol suspension operation.
    /// </summary>
    /// <remarks>
    /// Regular Push treats this result as <see cref="Failure"/> and ignores the duration. LitePush sends the protocol
    /// suspend flag, which asks the service not to count the invisibility change as a retry; a later delivery attempt
    /// value remains service-owned. FIFO LitePush also suspends unprocessed messages with the same LiteTopic from the
    /// current receive batch without invoking their handlers.
    /// </remarks>
    /// <param name="duration">The requested invisible duration. The minimum value is 50 milliseconds.</param>
    /// <returns>A duration-carrying suspend result.</returns>
    /// <exception cref="ArgumentOutOfRangeException">
    /// <paramref name="duration"/> is shorter than 50 milliseconds.
    /// </exception>
    public static ConsumeResult Suspend(TimeSpan duration)
    {
        if (duration < MinimumSuspendDuration)
        {
            throw new ArgumentOutOfRangeException(
                nameof(duration),
                duration,
                $"Suspend duration must be at least {MinimumSuspendDuration}.");
        }

        return new ConsumeResult("Suspend", duration);
    }

    /// <inheritdoc />
    public override string ToString() => Name;
}
