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

using EventHorizon.RocketMQ.Grpc.Consumer;

namespace EventHorizon.RocketMQ.Grpc.Consumer.Push;

/// <summary>
/// Configures a RocketMQ gRPC push consumer that receives messages through client-initiated long polling.
/// </summary>
public class GrpcPushConsumerOptions : ConsumerOptions
{
    /// <summary>
    /// Gets or sets the maximum number of messages processed concurrently.
    /// </summary>
    public int MaxConcurrency { get; set; } = Environment.ProcessorCount;

    /// <summary>
    /// Gets or sets the maximum number of messages requested by each receive operation.
    /// </summary>
    public int BatchSize { get; set; } = 32;

    /// <summary>
    /// Gets or sets the maximum number of received messages buffered locally before dispatch.
    /// </summary>
    public int MaxCachedMessages { get; set; } = 1024;

    /// <summary>
    /// Gets or sets the maximum total body size, in bytes, of messages buffered locally before dispatch.
    /// </summary>
    public int MaxCachedMessageBytes { get; set; } = 32 * 1024 * 1024;

    /// <summary>
    /// Gets or sets the fallback maximum number of delivery attempts before a message is dead-lettered.
    /// </summary>
    public int MaxDeliveryAttempts { get; set; } = 16;

    /// <summary>
    /// Gets or sets the initial duration for which a received message remains invisible to other consumers.
    /// </summary>
    public TimeSpan InvisibleDuration { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>
    /// Gets or sets the maximum time the server may wait for messages during a long-poll request.
    /// </summary>
    public TimeSpan LongPollingTimeout { get; set; } = TimeSpan.FromSeconds(15);

    /// <summary>
    /// Gets or sets the fallback delay before a message is made available for retry.
    /// </summary>
    public TimeSpan RetryDelay { get; set; } = TimeSpan.FromSeconds(10);

    /// <summary>
    /// Gets or sets the asynchronous handler invoked for each received message.
    /// </summary>
    /// <remarks>
    /// This delegate is invoked directly. Use the generic <c>AddGrpcPushConsumer&lt;TMessageHandler&gt;</c> or
    /// <c>AddGrpcLitePushConsumer&lt;TMessageHandler&gt;</c> overload to use a dependency-injected handler with an
    /// explicit lifetime.
    /// </remarks>
    public Func<GrpcMessageView, CancellationToken, ValueTask<ConsumeResult>>? MessageHandler { get; set; }
}
