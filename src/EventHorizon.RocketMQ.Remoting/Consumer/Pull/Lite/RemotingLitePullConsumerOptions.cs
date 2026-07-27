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
    /// Gets or sets the maximum number of messages requested by each poll operation.
    /// </summary>
    public int BatchSize { get; set; } = 32;

    /// <summary>
    /// Gets or sets the maximum message payload size, in bytes, requested in a pull response.
    /// </summary>
    public int MaxMessageBytes { get; set; } = 32 * 1024 * 1024;

    /// <summary>
    /// Gets or sets how long the Broker may hold a poll request while waiting for new messages.
    /// </summary>
    public TimeSpan LongPollingTimeout { get; set; } = TimeSpan.FromSeconds(15);

    /// <summary>
    /// Gets or sets the initial offset policy used for a newly assigned queue with no committed group offset.
    /// </summary>
    public QueryOffsetPolicy InitialOffset { get; set; } = QueryOffsetPolicy.End;

    /// <summary>
    /// Gets or sets the timestamp used when <see cref="InitialOffset"/> is
    /// <see cref="QueryOffsetPolicy.Timestamp"/>.
    /// </summary>
    public DateTimeOffset? InitialOffsetTimestamp { get; set; }
}
