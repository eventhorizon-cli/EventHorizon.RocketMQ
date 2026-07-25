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

using EventHorizon.RocketMQ.Producer;
using EventHorizon.RocketMQ.Remoting.Producer.Transactions;

namespace EventHorizon.RocketMQ.Remoting.Producer;

/// <summary>
/// Represents a producer that sends messages through the RocketMQ classic remoting protocol.
/// </summary>
public interface IRemotingProducer : IAsyncDisposable
{
    /// <summary>
    /// Starts the producer.
    /// </summary>
    /// <param name="cancellationToken">The token used to cancel the operation.</param>
    /// <returns>A value task that represents the asynchronous start operation.</returns>
    ValueTask StartAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Stops the producer.
    /// </summary>
    /// <param name="cancellationToken">The token used to cancel the operation.</param>
    /// <returns>A value task that represents the asynchronous stop operation.</returns>
    ValueTask StopAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Gets the currently writable queues for a topic.
    /// </summary>
    /// <param name="topic">The topic whose publish queues to retrieve.</param>
    /// <param name="cancellationToken">The token used to cancel the operation.</param>
    /// <returns>A task whose result contains the writable queues.</returns>
    Task<IReadOnlyList<RemotingMessageQueue>> GetPublishMessageQueuesAsync(
        string topic,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Sends a message and waits for the broker's response.
    /// </summary>
    /// <param name="message">The message to send.</param>
    /// <param name="cancellationToken">The token used to cancel the operation.</param>
    /// <returns>A task whose result contains the broker's send status and queue metadata.</returns>
    /// <remarks>
    /// Inspect <see cref="RemotingSendResult.Status"/> because some non-success durability outcomes are returned
    /// as a result rather than raised as an exception.
    /// </remarks>
    Task<RemotingSendResult> SendAsync(Message message, CancellationToken cancellationToken = default);

    /// <summary>
    /// Sends a message to a specific writable queue.
    /// </summary>
    /// <param name="message">The message to send.</param>
    /// <param name="messageQueue">The target queue.</param>
    /// <param name="cancellationToken">The token used to cancel the operation.</param>
    /// <returns>A task whose result contains the broker's send status and queue metadata.</returns>
    Task<RemotingSendResult> SendAsync(
        Message message,
        RemotingMessageQueue messageQueue,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Selects a writable queue and sends a message to it.
    /// </summary>
    /// <param name="message">The message to send.</param>
    /// <param name="selector">The delegate used to choose a queue.</param>
    /// <param name="argument">An application-defined selector argument.</param>
    /// <param name="cancellationToken">The token used to cancel the operation.</param>
    /// <returns>A task whose result contains the broker's send status and queue metadata.</returns>
    Task<RemotingSendResult> SendAsync(
        Message message,
        RemotingMessageQueueSelector selector,
        object? argument = null,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Sends messages in a single classic RocketMQ batch request.
    /// </summary>
    /// <param name="messages">The messages to send. They must use the same topic and cannot be delayed or retry messages.</param>
    /// <param name="cancellationToken">The token used to cancel the operation.</param>
    /// <returns>A task whose result contains the broker's send status and queue metadata.</returns>
    Task<RemotingSendResult> SendAsync(
        IReadOnlyCollection<Message> messages,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Sends messages in a single classic RocketMQ batch request to a specific writable queue.
    /// </summary>
    /// <param name="messages">The messages to send. They must use the same topic and cannot be delayed or retry messages.</param>
    /// <param name="messageQueue">The target queue.</param>
    /// <param name="cancellationToken">The token used to cancel the operation.</param>
    /// <returns>A task whose result contains the broker's send status and queue metadata.</returns>
    Task<RemotingSendResult> SendAsync(
        IReadOnlyCollection<Message> messages,
        RemotingMessageQueue messageQueue,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Selects a writable queue and sends messages in a single classic RocketMQ batch request.
    /// </summary>
    /// <param name="messages">The messages to send. They must use the same topic and cannot be delayed or retry messages.</param>
    /// <param name="selector">The delegate used to choose a queue.</param>
    /// <param name="argument">An application-defined selector argument.</param>
    /// <param name="cancellationToken">The token used to cancel the operation.</param>
    /// <returns>A task whose result contains the broker's send status and queue metadata.</returns>
    Task<RemotingSendResult> SendAsync(
        IReadOnlyCollection<Message> messages,
        RemotingMessageQueueSelector selector,
        object? argument = null,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Sends a transactional half message, executes the configured local transaction, and reports its outcome.
    /// </summary>
    /// <param name="message">The transactional message to send.</param>
    /// <param name="argument">An optional application-defined value passed to the local transaction executor.</param>
    /// <param name="cancellationToken">The token used to cancel the operation.</param>
    /// <returns>A task whose result contains the send result and local transaction outcome.</returns>
    /// <exception cref="InvalidOperationException">
    /// No local transaction executor or transaction checker is configured, or the classic remoting client cannot receive
    /// transaction checks.
    /// </exception>
    Task<RemotingTransactionSendResult> SendTransactionAsync(
        Message message,
        object? argument = null,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Recalls a delayed message by using the handle returned in its send result.
    /// </summary>
    /// <param name="topic">The topic containing the delayed message.</param>
    /// <param name="recallHandle">The recall handle returned when the message was sent.</param>
    /// <param name="cancellationToken">The token used to cancel the operation.</param>
    /// <returns>A task whose result contains the identifier of the recalled message.</returns>
    Task<string> RecallAsync(
        string topic,
        string recallHandle,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Sends a request message and waits for a broker-delivered reply.
    /// </summary>
    /// <param name="message">The request message to send.</param>
    /// <param name="timeout">The maximum time allowed for sending the request and receiving its reply.</param>
    /// <param name="cancellationToken">The token used to cancel the operation.</param>
    /// <returns>A task whose result contains the reply message.</returns>
    /// <exception cref="TimeoutException">No reply arrived before <paramref name="timeout"/> elapsed.</exception>
    Task<RemotingReplyMessage> RequestAsync(
        Message message,
        TimeSpan timeout,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Sends a request message to a specific writable queue and waits for a broker-delivered reply.
    /// </summary>
    /// <param name="message">The request message to send.</param>
    /// <param name="messageQueue">The target queue.</param>
    /// <param name="timeout">The maximum time allowed for sending the request and receiving its reply.</param>
    /// <param name="cancellationToken">The token used to cancel the operation.</param>
    /// <returns>A task whose result contains the reply message.</returns>
    /// <exception cref="TimeoutException">No reply arrived before <paramref name="timeout"/> elapsed.</exception>
    Task<RemotingReplyMessage> RequestAsync(
        Message message,
        RemotingMessageQueue messageQueue,
        TimeSpan timeout,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Selects a writable queue, sends a request message, and waits for a broker-delivered reply.
    /// </summary>
    /// <param name="message">The request message to send.</param>
    /// <param name="selector">The delegate used to choose a queue.</param>
    /// <param name="argument">An application-defined selector argument.</param>
    /// <param name="timeout">The maximum time allowed for sending the request and receiving its reply.</param>
    /// <param name="cancellationToken">The token used to cancel the operation.</param>
    /// <returns>A task whose result contains the reply message.</returns>
    /// <exception cref="TimeoutException">No reply arrived before <paramref name="timeout"/> elapsed.</exception>
    Task<RemotingReplyMessage> RequestAsync(
        Message message,
        RemotingMessageQueueSelector selector,
        object? argument,
        TimeSpan timeout,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Sends a reply to a classic RocketMQ request message.
    /// </summary>
    /// <param name="reply">The reply created from the received request properties.</param>
    /// <param name="cancellationToken">The token used to cancel the operation.</param>
    /// <returns>A task whose result contains the broker's send status and queue metadata.</returns>
    Task<RemotingSendResult> SendReplyAsync(
        RemotingReply reply,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Sends a message without waiting for a broker response.
    /// </summary>
    /// <param name="message">The message to send.</param>
    /// <param name="cancellationToken">The token used to cancel the operation.</param>
    /// <returns>A task that represents the asynchronous send operation.</returns>
    /// <remarks>A completed task does not confirm that the broker stored the message.</remarks>
    Task SendOnewayAsync(Message message, CancellationToken cancellationToken = default);

    /// <summary>
    /// Sends a message to a specific writable queue without waiting for a broker response.
    /// </summary>
    /// <param name="message">The message to send.</param>
    /// <param name="messageQueue">The target queue.</param>
    /// <param name="cancellationToken">The token used to cancel the operation.</param>
    /// <returns>A task that represents the asynchronous send operation.</returns>
    Task SendOnewayAsync(
        Message message,
        RemotingMessageQueue messageQueue,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Selects a writable queue and sends a message without waiting for a broker response.
    /// </summary>
    /// <param name="message">The message to send.</param>
    /// <param name="selector">The delegate used to choose a queue.</param>
    /// <param name="argument">An application-defined selector argument.</param>
    /// <param name="cancellationToken">The token used to cancel the operation.</param>
    /// <returns>A task that represents the asynchronous send operation.</returns>
    Task SendOnewayAsync(
        Message message,
        RemotingMessageQueueSelector selector,
        object? argument = null,
        CancellationToken cancellationToken = default);
}
