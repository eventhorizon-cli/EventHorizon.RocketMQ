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
/// Sends messages through the RocketMQ gRPC protocol.
/// </summary>
public interface IGrpcProducer : IAsyncDisposable
{
    /// <summary>
    /// Starts the producer and establishes its telemetry sessions.
    /// </summary>
    /// <param name="cancellationToken">A token that cancels the start operation.</param>
    /// <returns>A task-like value that represents the asynchronous operation.</returns>
    /// <remarks>
    /// A .NET Generic Host invokes this method through the hosted service registered by <c>AddRocketMQGrpc</c>.
    /// Call it explicitly only when resolving the role from a standalone <see cref="IServiceProvider"/>.
    /// </remarks>
    ValueTask StartAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Stops the producer and closes its telemetry sessions.
    /// </summary>
    /// <param name="cancellationToken">A token that cancels the stop operation.</param>
    /// <returns>A task-like value that represents the asynchronous operation.</returns>
    /// <remarks>
    /// A .NET Generic Host invokes this method through the hosted service registered by <c>AddRocketMQGrpc</c>.
    /// Call it explicitly only when manually managing a role resolved from a standalone <see cref="IServiceProvider"/>.
    /// </remarks>
    ValueTask StopAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Sends a message.
    /// </summary>
    /// <param name="message">The message to send.</param>
    /// <param name="cancellationToken">A token that cancels the operation.</param>
    /// <returns>The receipt returned by the RocketMQ service.</returns>
    Task<GrpcSendReceipt> SendAsync(Message message, CancellationToken cancellationToken = default);

    /// <summary>
    /// Recalls a delayed message by using the handle returned in its send receipt.
    /// </summary>
    /// <param name="topic">The topic containing the delayed message.</param>
    /// <param name="recallHandle">The recall handle returned when the message was sent.</param>
    /// <param name="cancellationToken">A token that cancels the operation.</param>
    /// <returns>The identifier of the recalled message.</returns>
    Task<string> RecallAsync(
        string topic,
        string recallHandle,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Sends a transactional message and returns a transaction that must be committed or rolled back.
    /// </summary>
    /// <param name="message">The transactional message to send.</param>
    /// <param name="cancellationToken">A token that cancels the operation.</param>
    /// <returns>The pending transaction associated with the sent message.</returns>
    /// <exception cref="InvalidOperationException">
    /// No transaction checker is configured, or the message topic was not declared before the producer started.
    /// </exception>
    Task<IRocketMQTransaction> SendTransactionAsync(
        Message message,
        CancellationToken cancellationToken = default);
}
