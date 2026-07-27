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

namespace EventHorizon.RocketMQ.Grpc.Consumer.Simple;

/// <summary>
/// Provides user-controlled message reception and acknowledgement through the RocketMQ gRPC protocol.
/// </summary>
public interface IGrpcSimpleConsumer : IAsyncDisposable
{
    /// <summary>
    /// Starts the consumer.
    /// </summary>
    /// <param name="cancellationToken">A token that cancels the start operation.</param>
    /// <returns>A task-like value that represents the asynchronous operation.</returns>
    ValueTask StartAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Stops the consumer.
    /// </summary>
    /// <param name="cancellationToken">A token that cancels the stop operation.</param>
    /// <returns>A task-like value that represents the asynchronous operation.</returns>
    ValueTask StopAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Adds or replaces a topic subscription and synchronizes it with active gRPC sessions.
    /// </summary>
    /// <param name="topic">The topic to subscribe to.</param>
    /// <param name="expression">
    /// The filter expression, or <see langword="null"/> to subscribe to all messages.
    /// </param>
    /// <param name="cancellationToken">A token that cancels the operation.</param>
    /// <returns>A task that represents the asynchronous operation.</returns>
    Task SubscribeAsync(
        string topic,
        FilterExpression? expression = null,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Removes a topic subscription and synchronizes the change with active gRPC sessions.
    /// </summary>
    /// <param name="topic">The topic to unsubscribe from.</param>
    /// <param name="cancellationToken">A token that cancels the operation.</param>
    /// <returns>A task that represents the asynchronous operation.</returns>
    Task UnsubscribeAsync(string topic, CancellationToken cancellationToken = default);

    /// <summary>
    /// Receives up to the requested number of messages from the configured subscriptions.
    /// </summary>
    /// <param name="maxMessages">The maximum number of messages to receive.</param>
    /// <param name="invisibleDuration">
    /// The invisible duration to request, or <see langword="null"/> to use the configured default.
    /// </param>
    /// <param name="cancellationToken">A token that cancels the operation.</param>
    /// <returns>The messages returned by the server, or an empty list when none are available.</returns>
    Task<IReadOnlyList<GrpcMessageView>> ReceiveAsync(int maxMessages, TimeSpan? invisibleDuration = null, CancellationToken cancellationToken = default);

    /// <summary>
    /// Acknowledges successful processing of a received message.
    /// </summary>
    /// <param name="message">The message to acknowledge.</param>
    /// <param name="cancellationToken">A token that cancels the operation.</param>
    /// <returns>A task that represents the asynchronous operation.</returns>
    Task AckAsync(GrpcMessageView message, CancellationToken cancellationToken = default);

    /// <summary>
    /// Changes how long a received message remains invisible to other consumers.
    /// </summary>
    /// <param name="message">The message whose invisible duration is changed.</param>
    /// <param name="invisibleDuration">The new positive invisible duration.</param>
    /// <param name="cancellationToken">A token that cancels the operation.</param>
    /// <returns>A task that represents the asynchronous operation.</returns>
    Task ChangeInvisibleDurationAsync(GrpcMessageView message, TimeSpan invisibleDuration, CancellationToken cancellationToken = default);

    /// <summary>
    /// Forwards a received message to the consumer group's dead-letter queue.
    /// </summary>
    /// <param name="message">The message to forward.</param>
    /// <param name="maxDeliveryAttempts">The positive maximum delivery-attempt threshold.</param>
    /// <param name="cancellationToken">A token that cancels the operation.</param>
    /// <returns>A task that represents the asynchronous operation.</returns>
    Task ForwardToDeadLetterQueueAsync(GrpcMessageView message, int maxDeliveryAttempts, CancellationToken cancellationToken = default);
}
