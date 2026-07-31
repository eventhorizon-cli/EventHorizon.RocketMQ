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

namespace EventHorizon.RocketMQ.Grpc.Consumer.Push;

/// <summary>
/// Provides automatic message dispatch using client-initiated assignment queries and long-poll receive requests.
/// </summary>
public interface IGrpcPushConsumer : IAsyncDisposable
{
    /// <summary>
    /// Starts assignment polling, message reception, and automatic dispatch.
    /// </summary>
    /// <param name="cancellationToken">A token that cancels the start operation.</param>
    /// <returns>A task-like value that represents the asynchronous operation.</returns>
    /// <remarks>
    /// A .NET Generic Host invokes this method through the hosted service registered by <c>AddRocketMQGrpc</c>.
    /// Call it explicitly only when resolving the role from a standalone <see cref="IServiceProvider"/>.
    /// </remarks>
    ValueTask StartAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Stops message reception and waits for active dispatch operations to finish.
    /// </summary>
    /// <param name="cancellationToken">A token that cancels the stop operation.</param>
    /// <returns>A task-like value that represents the asynchronous operation.</returns>
    /// <remarks>
    /// A .NET Generic Host invokes this method through the hosted service registered by <c>AddRocketMQGrpc</c>.
    /// Call it explicitly only when manually managing a role resolved from a standalone <see cref="IServiceProvider"/>.
    /// </remarks>
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
}
