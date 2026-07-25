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
using EventHorizon.RocketMQ.Remoting.Consumer.Pull;

namespace EventHorizon.RocketMQ.Remoting.Consumer.Push.Pop;

/// <summary>
/// Represents a RocketMQ classic remoting POP consumer with explicit queue selection and receipt operations.
/// </summary>
/// <remarks>
/// POP requests are initiated by the client. The consumer does not perform background assignment, dispatch, or
/// acknowledgement; callers select a queue, receive messages, and explicitly acknowledge or extend each receipt.
/// </remarks>
public interface IRemotingPopConsumer : IAsyncDisposable
{
    /// <summary>
    /// Starts the consumer.
    /// </summary>
    /// <param name="cancellationToken">The token used to cancel the operation.</param>
    /// <returns>A value task that represents the asynchronous start operation.</returns>
    ValueTask StartAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Stops the consumer.
    /// </summary>
    /// <param name="cancellationToken">The token used to cancel the operation.</param>
    /// <returns>A value task that represents the asynchronous stop operation.</returns>
    ValueTask StopAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Gets the readable message queues for a topic.
    /// </summary>
    /// <param name="topic">The topic whose queues to retrieve.</param>
    /// <param name="cancellationToken">The token used to cancel the operation.</param>
    /// <returns>A task whose result contains the readable message queues.</returns>
    Task<IReadOnlyList<RemotingPullMessageQueue>> GetMessageQueuesAsync(
        string topic,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Pops messages from a queue and returns receipts for the messages accepted by the broker.
    /// </summary>
    /// <param name="queue">The queue from which to receive messages.</param>
    /// <param name="maxMessages">
    /// The maximum number of messages to request, or <see langword="null"/> to use the configured batch size.
    /// </param>
    /// <param name="invisibleDuration">
    /// The duration for which returned messages remain invisible to other POP consumers, or
    /// <see langword="null"/> to use the configured duration.
    /// </param>
    /// <param name="filter">
    /// The filter expression, or <see langword="null"/> to use the configured topic filter when one exists.
    /// When neither is specified, every message in the queue is accepted.
    /// </param>
    /// <param name="cancellationToken">The token used to cancel the operation.</param>
    /// <returns>A task whose result describes the POP response and its message receipts.</returns>
    Task<RemotingPopResult> PopAsync(
        RemotingPullMessageQueue queue,
        int? maxMessages = null,
        TimeSpan? invisibleDuration = null,
        FilterExpression? filter = null,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Acknowledges a POP message receipt so that the broker can complete the delivery.
    /// </summary>
    /// <param name="receipt">The receipt returned with the POP message.</param>
    /// <param name="cancellationToken">The token used to cancel the operation.</param>
    /// <returns>A task that represents the asynchronous acknowledgement operation.</returns>
    Task AcknowledgeAsync(RemotingPopReceipt receipt, CancellationToken cancellationToken = default);

    /// <summary>
    /// Changes the remaining invisible time for a POP message receipt.
    /// </summary>
    /// <param name="receipt">The receipt returned with the POP message.</param>
    /// <param name="invisibleDuration">The requested new invisible duration.</param>
    /// <param name="cancellationToken">The token used to cancel the operation.</param>
    /// <returns>
    /// A task whose result is the replacement receipt that must be used for subsequent receipt operations.
    /// </returns>
    Task<RemotingPopReceipt> ChangeInvisibleTimeAsync(
        RemotingPopReceipt receipt,
        TimeSpan invisibleDuration,
        CancellationToken cancellationToken = default);
}
