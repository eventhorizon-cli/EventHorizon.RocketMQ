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

namespace EventHorizon.RocketMQ.Remoting.Consumer.Push.Pop;

/// <summary>
/// Provides options for a RocketMQ classic remoting POP consumer.
/// </summary>
/// <remarks>
/// Topic filters configured through <see cref="ConsumerOptions.Subscribe(string, FilterExpression?)"/> are used as
/// defaults by <see cref="IRemotingPopConsumer.PopAsync"/> when a call does not provide a filter. They do not create
/// a subscription, assignment, or background receive loop.
/// </remarks>
public sealed class RemotingPopConsumerOptions : ConsumerOptions
{
    internal const int MaximumBatchSize = 32;

    /// <summary>
    /// Gets or sets the default maximum number of messages requested by each POP operation. RocketMQ Brokers accept
    /// values from one through 32.
    /// </summary>
    public int BatchSize { get; set; } = 32;

    /// <summary>
    /// Gets or sets the maximum POP response body size, in bytes, accepted before message decoding.
    /// </summary>
    public int MaxMessageBytes { get; set; } = 32 * 1024 * 1024;

    /// <summary>
    /// Gets or sets how long returned messages remain invisible to other POP consumers by default.
    /// </summary>
    public TimeSpan InvisibleDuration { get; set; } = TimeSpan.FromSeconds(60);

    /// <summary>
    /// Gets or sets how long the broker may hold a POP request while waiting for new messages.
    /// </summary>
    public TimeSpan LongPollingTimeout { get; set; } = TimeSpan.FromSeconds(15);
}
