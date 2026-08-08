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

namespace EventHorizon.RocketMQ.Remoting.Consumer.Push;

/// <summary>
/// Provides options for a RocketMQ classic remoting push consumer.
/// </summary>
public sealed class RemotingPushConsumerOptions : ConsumerOptions
{
    internal const int MaximumPopBatchSize = 32;
    internal static readonly TimeSpan MinimumPopInvisibleDuration = TimeSpan.FromSeconds(5);
    internal static readonly TimeSpan MaximumPopInvisibleDuration = TimeSpan.FromMinutes(5);

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
    public int PullBatchSize { get; set; } = 32;

    /// <summary>
    /// Gets or sets the maximum number of messages requested by each Broker-assigned POP operation.
    /// </summary>
    /// <remarks>RocketMQ Brokers accept values from one through 32.</remarks>
    public int PopBatchSize { get; set; } = MaximumPopBatchSize;

    /// <summary>
    /// Gets or sets the initial invisibility lease for a message received through POP.
    /// </summary>
    /// <remarks>
    /// Classic Remoting Push treats this as a fixed processing deadline. It does not renew the receipt while the
    /// message handler is active. If processing outlives the deadline, the client ignores the late result and the
    /// Broker may redeliver the message. A handler retry changes invisibility as a one-shot negative acknowledgement;
    /// that operation is not an active-handler lease renewal. The replacement deadline hides the message from the same
    /// consumer group for the selected retry delay. If it expires without acknowledgement, the Broker makes the message
    /// available through the POP retry path and increments its delivery attempt.
    /// </remarks>
    /// <seealso href="https://github.com/apache/rocketmq/blob/rocketmq-all-5.5.0/client/src/main/java/org/apache/rocketmq/client/impl/consumer/ConsumeMessagePopConcurrentlyService.java#L266-L360">
    /// Apache RocketMQ classic POP result handling and fixed-deadline checks.
    /// </seealso>
    /// <seealso href="https://github.com/apache/rocketmq/blob/rocketmq-all-5.5.0/broker/src/main/java/org/apache/rocketmq/broker/processor/ChangeInvisibleTimeProcessor.java#L172-L180">
    /// Apache RocketMQ Broker replacement of a POP invisibility checkpoint.
    /// </seealso>
    /// <seealso href="https://github.com/apache/rocketmq/blob/rocketmq-all-5.5.0/broker/src/main/java/org/apache/rocketmq/broker/processor/PopReviveService.java#L107-L154">
    /// Apache RocketMQ Broker revival of an expired POP checkpoint into the retry topic.
    /// </seealso>
    public TimeSpan PopInvisibleDuration { get; set; } = TimeSpan.FromSeconds(60);

    /// <summary>
    /// Gets or sets the maximum number of POP messages awaiting settlement for one Broker assignment.
    /// </summary>
    public int PopMaxInflightMessagesPerAssignment { get; set; } = 96;

    /// <summary>
    /// Gets or sets the maximum number of messages passed to one batch message-handler invocation.
    /// </summary>
    /// <remarks>
    /// This setting is independent from <see cref="PullBatchSize"/>, which controls the maximum number of
    /// messages requested from the Broker. Batch handling is used only for concurrent dispatch; orderly
    /// consumption and messages with a <c>MessageGroup</c> continue to be delivered one at a time so that
    /// their ordering and retry semantics remain intact.
    /// </remarks>
    public int ConsumeMessageBatchSize { get; set; } = 1;

    /// <summary>
    /// Gets or sets the maximum number of newly pulled messages that may wait for their first local dispatch.
    /// </summary>
    /// <remarks>
    /// Messages already handed to a handler or retained behind unresolved settlement or an incomplete FIFO
    /// predecessor do not hold admission capacity. A blocked queue also pauses new admission until it can make
    /// progress, so this is not a strict limit on every message resident in the Consumer process.
    /// </remarks>
    public int PullMaxCachedMessages { get; set; } = 1024;

    /// <summary>
    /// Gets or sets the maximum total message-body bytes waiting for first dispatch in PULL receivers.
    /// </summary>
    public int PullMaxCachedMessageBytes { get; set; } = 32 * 1024 * 1024;

    /// <summary>
    /// Gets or sets the maximum message payload size, in bytes, requested in a pull response.
    /// </summary>
    public int MaxMessageBytes { get; set; } = 32 * 1024 * 1024;

    /// <summary>
    /// Gets or sets the maximum delivery attempt at which protocol-specific terminal retry handling begins.
    /// </summary>
    /// <remarks>
    /// PULL sends a retried message to the dead-letter queue at this limit. Classic POP follows the Apache Java
    /// client's age-based terminal handling instead: it continues changing invisibility until the message is older
    /// than twice the final POP retry delay, then acknowledges it without implicit dead-letter forwarding.
    /// </remarks>
    public int MaxDeliveryAttempts { get; set; } = 16;

    /// <summary>
    /// Gets or sets how long the broker may hold a pull request while waiting for new messages.
    /// </summary>
    public TimeSpan LongPollingTimeout { get; set; } = TimeSpan.FromSeconds(15);

    /// <summary>
    /// Gets or sets the local delay before retrying failed receive, settlement, or orderly handling work.
    /// </summary>
    /// <remarks>
    /// This value does not select the Broker redelivery interval. Use
    /// <see cref="RemotingPushConsumeContext.DelayLevelWhenNextConsume"/> for that purpose.
    /// </remarks>
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
    /// Gets or sets whether queue assignments are calculated by the client or queried from the Broker.
    /// </summary>
    /// <remarks>
    /// For concurrent clustered consumption, Broker assignment can return PULL and POP modes independently for each
    /// topic. The receive mode remains internal and is not selected by this option. Broadcasting and orderly
    /// consumption always use client assignment and PULL. The consumer never changes the Broker's administrative
    /// request-mode configuration.
    /// </remarks>
    /// <seealso href="https://github.com/apache/rocketmq/blob/rocketmq-all-5.5.0/client/src/main/java/org/apache/rocketmq/client/impl/consumer/RebalancePushImpl.java#L125-L128">
    /// Apache RocketMQ 5.5.0 effective client-rebalance selection.
    /// </seealso>
    public RemotingPushQueueAssignmentMode QueueAssignmentMode { get; set; } = RemotingPushQueueAssignmentMode.Client;

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
}
