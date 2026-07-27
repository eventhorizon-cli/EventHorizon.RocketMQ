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

using Microsoft.Extensions.DependencyInjection;

namespace EventHorizon.RocketMQ.Remoting.Consumer.Push;

/// <summary>
/// Processes a message batch delivered by a classic remoting Push consumer.
/// </summary>
/// <remarks>
/// A handler registered with <see cref="ServiceLifetime.Singleton"/> can receive concurrent calls and must be
/// thread-safe. Scoped and transient handlers are resolved for each batch handling attempt. A returned
/// <see cref="ConsumeResult.Success"/> can acknowledge a prefix of a concurrent non-FIFO batch through the
/// supplied <see cref="RemotingPushConsumeContext"/>. Other outcomes apply to every message in the batch.
/// </remarks>
public interface IRemotingPushMessageHandler
{
    /// <summary>
    /// Processes received messages and returns the requested delivery outcome for the batch.
    /// </summary>
    /// <param name="messages">The messages to process, in their delivery order.</param>
    /// <param name="context">
    /// The acknowledgement settings for the current concurrent non-FIFO batch. FIFO <c>MessageGroup</c> and
    /// orderly singleton deliveries ignore these settings.
    /// </param>
    /// <param name="cancellationToken">
    /// The token that cancels processing when the consumer stops and, for concurrent clustered non-FIFO batches,
    /// when <see cref="RemotingPushConsumerOptions.ConsumeTimeout"/> expires.
    /// </param>
    /// <returns>The requested delivery outcome for the batch.</returns>
    ValueTask<ConsumeResult> HandleAsync(
        IReadOnlyList<RemotingMessageView> messages,
        RemotingPushConsumeContext context,
        CancellationToken cancellationToken);
}
