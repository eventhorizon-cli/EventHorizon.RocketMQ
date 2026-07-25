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

namespace EventHorizon.RocketMQ.Grpc.Consumer.Lite;

/// <summary>
/// Provides automatic LiteTopic message dispatch through the RocketMQ gRPC Lite Push protocol.
/// </summary>
public interface IGrpcLitePushConsumer : IAsyncDisposable
{
    /// <summary>
    /// Starts the consumer, synchronizes configured LiteTopic subscriptions, and begins automatic dispatch.
    /// </summary>
    /// <param name="cancellationToken">A token that cancels the start operation.</param>
    /// <returns>A task-like value that represents the asynchronous operation.</returns>
    ValueTask StartAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Stops subscription synchronization, message reception, and automatic dispatch.
    /// </summary>
    /// <param name="cancellationToken">A token that cancels the stop operation.</param>
    /// <returns>A task-like value that represents the asynchronous operation.</returns>
    ValueTask StopAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Gets a snapshot of LiteTopics currently subscribed by this consumer.
    /// </summary>
    IReadOnlySet<string> LiteTopics { get; }

    /// <summary>
    /// Adds a LiteTopic subscription and synchronizes it with the RocketMQ service.
    /// </summary>
    /// <param name="liteTopic">The LiteTopic identifier to subscribe to.</param>
    /// <param name="offsetOption">
    /// The initial offset selection for the LiteTopic, or <see langword="null"/> to use the service default.
    /// </param>
    /// <param name="cancellationToken">A token that cancels the operation.</param>
    /// <returns>A task that represents the asynchronous operation.</returns>
    /// <exception cref="InvalidOperationException">The consumer has not been started.</exception>
    Task SubscribeLiteAsync(
        string liteTopic,
        GrpcLiteOffsetOption? offsetOption = null,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Removes a LiteTopic subscription and synchronizes the change with the RocketMQ service.
    /// </summary>
    /// <param name="liteTopic">The LiteTopic identifier to unsubscribe from.</param>
    /// <param name="cancellationToken">A token that cancels the operation.</param>
    /// <returns>A task that represents the asynchronous operation.</returns>
    /// <exception cref="InvalidOperationException">The consumer has not been started.</exception>
    Task UnsubscribeLiteAsync(string liteTopic, CancellationToken cancellationToken = default);
}
