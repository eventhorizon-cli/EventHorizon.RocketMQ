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

using EventHorizon.RocketMQ.Remoting.Consumer;

namespace EventHorizon.RocketMQ.Remoting.Consumer.Push;

/// <summary>
/// Provides options for a RocketMQ classic remoting push consumer.
/// </summary>
public sealed class RemotingPushConsumerOptions : ConsumerOptions
{
    /// <summary>
    /// Gets or sets the maximum number of message-handler batch dispatches that may run concurrently.
    /// </summary>
    /// <remarks>
    /// A non-FIFO handler that ignores cancellation after <see cref="ConsumeTimeout"/> expires can continue running
    /// after its dispatch slot is released, so the setting limits scheduled dispatches rather than every in-process
    /// application invocation in that exceptional case.
    /// </remarks>
    public int MaxConcurrency { get; set; } = Environment.ProcessorCount;

    /// <summary>
    /// Gets or sets the maximum number of messages requested by each long-poll operation.
    /// </summary>
    public int BatchSize { get; set; } = 32;

    /// <summary>
    /// Gets or sets the maximum number of messages passed to one batch message-handler invocation.
    /// </summary>
    /// <remarks>
    /// This setting is independent from <see cref="BatchSize"/>, which controls the maximum number of
    /// messages requested from the Broker. Batch handling is used only for concurrent dispatch; orderly
    /// consumption and messages with a <c>MessageGroup</c> continue to be delivered one at a time so that
    /// their ordering and retry semantics remain intact.
    /// </remarks>
    public int ConsumeMessageBatchSize { get; set; } = 1;

    /// <summary>
    /// Gets or sets the maximum number of messages that may wait for local dispatch.
    /// </summary>
    public int MaxCachedMessages { get; set; } = 1024;

    /// <summary>
    /// Gets or sets the maximum message payload size, in bytes, requested in a pull response.
    /// </summary>
    public int MaxMessageBytes { get; set; } = 32 * 1024 * 1024;

    /// <summary>
    /// Gets or sets the maximum delivery attempt after which a retried message is sent to the dead-letter queue.
    /// </summary>
    public int MaxDeliveryAttempts { get; set; } = 16;

    /// <summary>
    /// Gets or sets how long the broker may hold a pull request while waiting for new messages.
    /// </summary>
    public TimeSpan LongPollingTimeout { get; set; } = TimeSpan.FromSeconds(15);

    /// <summary>
    /// Gets or sets the delay applied before retrying message handling or a failed receive operation.
    /// </summary>
    public TimeSpan RetryDelay { get; set; } = TimeSpan.FromSeconds(10);

    /// <summary>
    /// Gets or sets the maximum time a concurrent clustered message batch may occupy a handler before retry is requested.
    /// </summary>
    /// <remarks>
    /// When this timeout elapses, the consumer cancels the handler token and marks the complete non-FIFO batch as
    /// <see cref="ConsumeResult.Retry"/> so it can be returned to the Broker for redelivery. Application code that
    /// does not cooperate with cancellation cannot be forcibly stopped; its late result is ignored and can overlap
    /// a redelivered invocation.
    /// This recovery is intentionally not applied to <see cref="ConsumeOrderly"/> or <c>MessageGroup</c> FIFO delivery,
    /// because automatically releasing those messages could break their ordering guarantees.
    /// </remarks>
    public TimeSpan ConsumeTimeout { get; set; } = TimeSpan.FromMinutes(15);

    /// <summary>
    /// Gets or sets whether this consumer shares queues with its group or receives every queue.
    /// </summary>
    public ConsumerMode ConsumerMode { get; set; } =
        global::EventHorizon.RocketMQ.Remoting.Consumer.Push.ConsumerMode.Clustering;

    /// <summary>
    /// Gets or sets whether the consumer processes each physical message queue serially.
    /// </summary>
    /// <remarks>
    /// In clustering mode, the consumer acquires and renews a classic Broker queue lock before it
    /// pulls or commits that queue. Broadcasting mode serializes each local queue without acquiring
    /// a Broker lock. This is separate from producer <c>MessageGroup</c> FIFO affinity.
    /// </remarks>
    public bool ConsumeOrderly { get; set; }

    /// <summary>
    /// Gets or sets the broadcast offset file. When omitted, a path under local application data is
    /// derived from the client identity and consumer group.
    /// </summary>
    public string? LocalOffsetStorePath { get; set; }

    /// <summary>
    /// Gets or sets where a classic push consumer starts when no committed offset exists.
    /// </summary>
    public ConsumeFromPosition InitialPosition { get; set; } = ConsumeFromPosition.End;

    /// <summary>
    /// Gets or sets the timestamp used when <see cref="InitialPosition"/> is
    /// <see cref="ConsumeFromPosition.Timestamp"/>.
    /// </summary>
    public DateTimeOffset? ConsumeTimestamp { get; set; }

    /// <summary>
    /// Gets or sets the asynchronous handler that processes one delivered message batch and reports its outcome.
    /// </summary>
    /// <remarks>
    /// This delegate is invoked directly. Use the generic <c>AddRemotingPushConsumer&lt;TMessageHandler&gt;</c>
    /// overload to use a dependency-injected handler with an explicit lifetime. When it returns
    /// <see cref="ConsumeResult.Success"/>, use the supplied <see cref="RemotingPushConsumeContext"/> for a
    /// concurrent non-FIFO batch to acknowledge a prefix and retry its remaining messages.
    /// </remarks>
    public Func<IReadOnlyList<RemotingMessageView>, RemotingPushConsumeContext, CancellationToken, ValueTask<ConsumeResult>>? MessageHandler
    {
        get;
        set;
    }
}
