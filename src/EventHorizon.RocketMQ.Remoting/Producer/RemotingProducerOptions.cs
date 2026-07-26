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

using EventHorizon.RocketMQ.Remoting.Producer;
using EventHorizon.RocketMQ.Remoting.Producer.Transactions;

namespace EventHorizon.RocketMQ.Remoting.Producer;

/// <summary>
/// Provides options for a RocketMQ classic remoting producer.
/// </summary>
public sealed class RemotingProducerOptions
{
    /// <summary>
    /// Gets or sets the producer group name.
    /// </summary>
    public string GroupName { get; set; } = "DEFAULT_PRODUCER";

    /// <summary>
    /// Gets or sets the number of queues created for an automatically created topic.
    /// </summary>
    public int DefaultTopicQueueNums { get; set; } = 4;

    /// <summary>
    /// Gets or sets the timeout for sending messages.
    /// </summary>
    public TimeSpan SendMsgTimeout { get; set; } = TimeSpan.FromSeconds(3);

    /// <summary>
    /// Gets or sets the message body size at which compression is enabled.
    /// </summary>
    public int CompressMsgBodyOverHowmuch { get; set; } = 4 * 1024;

    /// <summary>
    /// Gets or sets the number of retries before a send operation fails.
    /// </summary>
    /// <remarks>
    /// Retrying may produce duplicate messages. Applications are responsible for idempotent processing.
    /// </remarks>
    public int RetryTimesWhenSendFailed { get; set; } = 2;

    /// <summary>
    /// Gets or sets the callback that executes a local transaction after its half message is stored.
    /// </summary>
    public Func<Message, object?, CancellationToken, ValueTask<RemotingTransactionResolution>>?
        LocalTransactionExecutor
    { get; set; }

    /// <summary>
    /// Gets or sets the callback used to resolve transactions when the broker checks their local outcome.
    /// </summary>
    public Func<RemotingTransactionMessage, CancellationToken, ValueTask<RemotingTransactionResolution>>?
        TransactionChecker
    { get; set; }

    /// <summary>
    /// Gets or sets the maximum number of broker transaction checks processed concurrently.
    /// </summary>
    public int MaxConcurrentTransactionChecks { get; set; } = Environment.ProcessorCount;

    /// <summary>
    /// Gets or sets the maximum allowed message size in bytes.
    /// </summary>
    public int MaxMessageSize { get; set; } = 4 * 1024 * 1024;
}
