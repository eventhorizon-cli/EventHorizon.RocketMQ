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

namespace EventHorizon.RocketMQ.Remoting.Consumer.Push;

/// <summary>
/// Represents a RocketMQ classic remoting push consumer driven by client-initiated long polling.
/// </summary>
public interface IRemotingPushConsumer : IAsyncDisposable
{
    /// <summary>
    /// Starts the consumer.
    /// </summary>
    /// <param name="cancellationToken">The token used to cancel the operation.</param>
    /// <returns>A value task that represents the asynchronous start operation.</returns>
    /// <remarks>
    /// A .NET Generic Host invokes this method through the hosted service registered by <c>AddRocketMQRemoting</c>.
    /// Call it explicitly only when resolving the role from a standalone <see cref="IServiceProvider"/>.
    /// </remarks>
    ValueTask StartAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Stops the consumer.
    /// </summary>
    /// <param name="cancellationToken">The token used to cancel the operation.</param>
    /// <returns>A value task that represents the asynchronous stop operation.</returns>
    /// <remarks>
    /// A .NET Generic Host invokes this method through the hosted service registered by <c>AddRocketMQRemoting</c>.
    /// Call it explicitly only when manually managing a role resolved from a standalone <see cref="IServiceProvider"/>.
    /// </remarks>
    ValueTask StopAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Adds or replaces a topic subscription and reconciles the active queue assignments.
    /// </summary>
    /// <param name="topic">The topic to subscribe to.</param>
    /// <param name="expression">
    /// The subscription filter, or <see langword="null"/> to accept every message on the topic.
    /// </param>
    /// <param name="cancellationToken">The token used to cancel the operation.</param>
    /// <returns>A task that represents the asynchronous subscription operation.</returns>
    Task SubscribeAsync(
        string topic,
        FilterExpression? expression = null,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Removes a topic subscription and stops receiving from its assigned queues.
    /// </summary>
    /// <param name="topic">The topic to unsubscribe from.</param>
    /// <param name="cancellationToken">The token used to cancel the operation.</param>
    /// <returns>A task that represents the asynchronous unsubscription operation.</returns>
    Task UnsubscribeAsync(string topic, CancellationToken cancellationToken = default);
}
