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

using EventHorizon.RocketMQ.Grpc.Producer.Transactions;

namespace EventHorizon.RocketMQ.Grpc.Producer;

/// <summary>
/// Configures a RocketMQ gRPC producer.
/// </summary>
public sealed class GrpcProducerOptions
{
    /// <summary>
    /// Gets or sets the timeout for sending messages.
    /// </summary>
    public TimeSpan SendMsgTimeout { get; set; } = TimeSpan.FromSeconds(3);

    /// <summary>
    /// Gets or sets the number of retries before a send operation fails.
    /// </summary>
    /// <remarks>
    /// Retrying may produce duplicate messages. Applications are responsible for idempotent processing.
    /// </remarks>
    public int RetryTimesWhenSendFailed { get; set; } = 2;

    /// <summary>
    /// Gets or sets the maximum allowed message size in bytes.
    /// </summary>
    public int MaxMessageSize { get; set; } = 4 * 1024 * 1024;

    /// <summary>
    /// Gets the topics published by this producer.
    /// </summary>
    /// <remarks>
    /// Declaring topics enables transaction recovery immediately after startup.
    /// </remarks>
    public ISet<string> Topics { get; } = new HashSet<string>(StringComparer.Ordinal);

    /// <summary>
    /// Gets or sets the callback that resolves transactions whose local outcome was not reported in time.
    /// </summary>
    public Func<TransactionMessage, CancellationToken, ValueTask<TransactionResolution>>? TransactionChecker { get; set; }

    /// <summary>
    /// Gets or sets the maximum number of transaction checks processed concurrently.
    /// </summary>
    public int MaxConcurrentTransactionChecks { get; set; } = Environment.ProcessorCount;
}
