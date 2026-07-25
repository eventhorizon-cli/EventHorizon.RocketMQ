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

using EventHorizon.RocketMQ.Remoting.Consumer;
using EventHorizon.RocketMQ.Remoting.Producer;

namespace EventHorizon.RocketMQ.Remoting.Admin;

/// <summary>
/// Provides read-only administrative operations through the RocketMQ classic remoting protocol.
/// </summary>
public interface IRemotingAdmin
{
    /// <summary>
    /// Gets the writable message queues currently published for a topic.
    /// </summary>
    /// <param name="topic">The topic whose queues to retrieve.</param>
    /// <param name="cancellationToken">The token used to cancel the operation.</param>
    /// <returns>A task whose result contains the message queues ordered by broker and queue identifier.</returns>
    Task<IReadOnlyList<RemotingMessageQueue>> GetMessageQueuesAsync(
        string topic,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Gets a message directly from the broker encoded in a physical offset message identifier.
    /// </summary>
    /// <param name="topic">The topic that owns the message.</param>
    /// <param name="offsetMessageId">The physical identifier returned by <see cref="RemotingSendResult.OffsetMessageId"/>.</param>
    /// <param name="cancellationToken">The token used to cancel the operation.</param>
    /// <returns>A task whose result contains the decoded message.</returns>
    /// <exception cref="ArgumentException">
    /// <paramref name="topic"/> or <paramref name="offsetMessageId"/> is empty.
    /// </exception>
    /// <exception cref="InvalidDataException">
    /// <paramref name="offsetMessageId"/> is malformed, or the broker returns an invalid message record.
    /// </exception>
    /// <remarks>
    /// The physical identifier encodes a broker endpoint and commit-log offset. The client connects directly to that
    /// endpoint, so it must be reachable from the application; NameServer route discovery is not used.
    /// </remarks>
    Task<RemotingMessageView> ViewMessageAsync(
        string topic,
        string offsetMessageId,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Gets the earliest available offset for a message queue.
    /// </summary>
    /// <param name="messageQueue">The message queue to query.</param>
    /// <param name="cancellationToken">The token used to cancel the operation.</param>
    /// <returns>A task whose result is the earliest available offset.</returns>
    Task<long> GetMinOffsetAsync(
        RemotingMessageQueue messageQueue,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Gets the store time of the earliest available message in a message queue.
    /// </summary>
    /// <param name="messageQueue">The message queue to query.</param>
    /// <param name="cancellationToken">The token used to cancel the operation.</param>
    /// <returns>
    /// A task whose result is the earliest message store time in UTC, or <see langword="null"/> when the broker
    /// reports that the queue has no available message.
    /// </returns>
    Task<DateTimeOffset?> GetEarliestMessageStoreTimeAsync(
        RemotingMessageQueue messageQueue,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Gets the maximum offset for a message queue.
    /// </summary>
    /// <param name="messageQueue">The message queue to query.</param>
    /// <param name="committed">
    /// <see langword="true"/> to return the highest committed, consumable offset; otherwise, returns the current
    /// in-flight offset when supported by the broker.
    /// </param>
    /// <param name="cancellationToken">The token used to cancel the operation.</param>
    /// <returns>A task whose result is the maximum offset.</returns>
    Task<long> GetMaxOffsetAsync(
        RemotingMessageQueue messageQueue,
        bool committed = true,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Finds a queue offset for a timestamp.
    /// </summary>
    /// <param name="messageQueue">The message queue to query.</param>
    /// <param name="timestamp">The timestamp for which to search.</param>
    /// <param name="boundary">The matching-offset boundary to return.</param>
    /// <param name="cancellationToken">The token used to cancel the operation.</param>
    /// <returns>A task whose result is the matching queue offset.</returns>
    Task<long> SearchOffsetAsync(
        RemotingMessageQueue messageQueue,
        DateTimeOffset timestamp,
        RemotingOffsetBoundary boundary = RemotingOffsetBoundary.Lower,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Gets a consumer group's committed offset for a message queue.
    /// </summary>
    /// <param name="consumerGroup">The consumer group to query.</param>
    /// <param name="messageQueue">The message queue to query.</param>
    /// <param name="cancellationToken">The token used to cancel the operation.</param>
    /// <returns>
    /// A task whose result is the committed offset, or <see langword="null"/> when the group has no offset for the
    /// queue.
    /// </returns>
    Task<long?> GetConsumerOffsetAsync(
        string consumerGroup,
        RemotingMessageQueue messageQueue,
        CancellationToken cancellationToken = default);
}
