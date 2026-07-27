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

namespace EventHorizon.RocketMQ.Remoting.Consumer.Pull;

/// <summary>
/// Represents a RocketMQ classic remoting pull consumer with explicit queue and offset control.
/// </summary>
public interface IRemotingPullConsumer : IAsyncDisposable
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
    /// Gets the readable message queues for a subscribed topic.
    /// </summary>
    /// <param name="topic">The topic whose queues to retrieve.</param>
    /// <param name="cancellationToken">The token used to cancel the operation.</param>
    /// <returns>A task whose result contains the readable message queues.</returns>
    Task<IReadOnlyList<RemotingPullMessageQueue>> GetMessageQueuesAsync(
        string topic,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Pulls messages from a queue beginning at the specified offset.
    /// </summary>
    /// <param name="queue">The queue from which to pull messages.</param>
    /// <param name="offset">The first queue offset to request.</param>
    /// <param name="maxMessages">
    /// The maximum number of messages to request, or <see langword="null"/> to use the configured batch size.
    /// </param>
    /// <param name="cancellationToken">The token used to cancel the operation.</param>
    /// <returns>A task whose result describes the messages and the next queue offset.</returns>
    Task<RemotingPullResult> PullAsync(
        RemotingPullMessageQueue queue,
        long offset,
        int? maxMessages = null,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Gets the consumer group's committed offset for a queue.
    /// </summary>
    /// <param name="queue">The queue whose committed offset to retrieve.</param>
    /// <param name="cancellationToken">The token used to cancel the operation.</param>
    /// <returns>
    /// A task whose result is the committed offset, or <c>-1</c> when the consumer group has no committed offset.
    /// </returns>
    Task<long> GetOffsetAsync(
        RemotingPullMessageQueue queue,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Updates the consumer group's committed offset for a queue.
    /// </summary>
    /// <param name="queue">The queue whose committed offset to update.</param>
    /// <param name="offset">The queue offset to commit.</param>
    /// <param name="cancellationToken">The token used to cancel the operation.</param>
    /// <returns>A task that represents the asynchronous update operation.</returns>
    Task UpdateOffsetAsync(
        RemotingPullMessageQueue queue,
        long offset,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Queries a queue offset according to the specified policy.
    /// </summary>
    /// <param name="queue">The queue whose offset to query.</param>
    /// <param name="policy">The policy used to select the offset.</param>
    /// <param name="timestamp">
    /// The timestamp used by <see cref="QueryOffsetPolicy.Timestamp"/>; otherwise, <see langword="null"/>.
    /// </param>
    /// <param name="cancellationToken">The token used to cancel the operation.</param>
    /// <returns>A task whose result is the matching queue offset.</returns>
    Task<long> QueryOffsetAsync(
        RemotingPullMessageQueue queue,
        QueryOffsetPolicy policy,
        DateTimeOffset? timestamp = null,
        CancellationToken cancellationToken = default);
}
