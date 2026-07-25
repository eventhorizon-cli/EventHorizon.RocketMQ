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

using EventHorizon.RocketMQ.Consumer;
using EventHorizon.RocketMQ.Remoting.Consumer;

namespace EventHorizon.RocketMQ.Remoting.Consumer.Push;

/// <summary>
/// Provides options for a RocketMQ classic remoting push consumer.
/// </summary>
public sealed class RemotingPushConsumerOptions : ConsumerOptions
{
    /// <summary>
    /// Gets or sets the maximum number of message-handler invocations that may run concurrently.
    /// </summary>
    public int MaxConcurrency { get; set; } = Environment.ProcessorCount;

    /// <summary>
    /// Gets or sets the maximum number of messages requested by each long-poll operation.
    /// </summary>
    public int BatchSize { get; set; } = 32;

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
    /// Gets or sets the asynchronous handler that processes each delivered message and reports its outcome.
    /// </summary>
    /// <remarks>
    /// This delegate is invoked directly. Use the generic <c>AddRemotingPushConsumer&lt;TMessageHandler&gt;</c>
    /// overload to use a dependency-injected handler with an explicit lifetime.
    /// </remarks>
    public Func<RemotingMessageView, CancellationToken, ValueTask<ConsumeResult>>? MessageHandler { get; set; }
}
