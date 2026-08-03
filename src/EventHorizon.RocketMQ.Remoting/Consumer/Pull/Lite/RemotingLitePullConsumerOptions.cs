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

namespace EventHorizon.RocketMQ.Remoting.Consumer.Pull.Lite;

/// <summary>
/// Provides options for a RocketMQ classic remoting Lite Pull consumer.
/// </summary>
public sealed class RemotingLitePullConsumerOptions : ConsumerOptions
{
    /// <summary>
    /// Gets or sets the maximum number of messages requested by each background PULL operation.
    /// </summary>
    public int PullBatchSize { get; set; } = 32;

    /// <summary>
    /// Gets or sets the maximum number of prefetched messages retained in the shared local delivery buffer.
    /// </summary>
    public int MaxCachedMessages { get; set; } = 1024;

    /// <summary>
    /// Gets or sets the maximum total message-body bytes retained in the shared local delivery buffer.
    /// </summary>
    public int MaxCachedMessageBytes { get; set; } = 32 * 1024 * 1024;

    /// <summary>
    /// Gets or sets the maximum message payload size, in bytes, requested in a pull response.
    /// </summary>
    public int MaxMessageBytes { get; set; } = 32 * 1024 * 1024;

    /// <summary>
    /// Gets or sets how long the Broker may hold a poll request while waiting for new messages.
    /// </summary>
    public TimeSpan LongPollingTimeout { get; set; } = TimeSpan.FromSeconds(15);

    /// <summary>
    /// Gets or sets how long <c>PollAsync</c> waits for locally buffered messages when no timeout is supplied.
    /// </summary>
    public TimeSpan PollTimeout { get; set; } = TimeSpan.FromSeconds(5);

    /// <summary>
    /// Gets or sets whether the consumer periodically commits delivered positions.
    /// </summary>
    public bool EnableAutoCommit { get; set; } = true;

    /// <summary>
    /// Gets or sets how often delivered positions are committed when <see cref="EnableAutoCommit"/> is enabled.
    /// </summary>
    public TimeSpan AutoCommitInterval { get; set; } = TimeSpan.FromSeconds(5);
}
