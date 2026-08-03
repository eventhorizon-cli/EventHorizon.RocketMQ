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

namespace EventHorizon.RocketMQ.Remoting.Consumer.Pull.Lite;

/// <summary>
/// Provides assignment-oriented, application-driven polling through the RocketMQ classic remoting protocol.
/// </summary>
/// <remarks>
/// Subscription and manual-assignment modes are mutually exclusive. The consumer receives assigned queues in the
/// background, and <see cref="PollAsync"/> reads from its bounded local buffer. A position becomes committable only
/// after its message has been returned to the application.
/// </remarks>
public interface IRemotingLitePullConsumer : IAsyncDisposable
{
    /// <summary>
    /// Starts the consumer and its assignment, receive, heartbeat, and commit loops.
    /// </summary>
    /// <param name="cancellationToken">The token used to cancel the operation.</param>
    /// <returns>A value task that represents the asynchronous start operation.</returns>
    /// <remarks>
    /// A .NET Generic Host invokes this method through the hosted service registered by <c>AddRocketMQRemoting</c>.
    /// Call it explicitly only when resolving the role from a standalone <see cref="IServiceProvider"/>.
    /// </remarks>
    ValueTask StartAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Stops background work and releases the current assignment.
    /// </summary>
    /// <param name="cancellationToken">The token used to cancel the operation.</param>
    /// <returns>A value task that represents the asynchronous stop operation.</returns>
    ValueTask StopAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Gets a snapshot of the queues currently assigned to this consumer.
    /// </summary>
    IReadOnlyList<RemotingConsumerQueue> Assignment { get; }

    /// <summary>
    /// Discovers the readable queues for a topic so they can be used with manual assignment.
    /// </summary>
    /// <param name="topic">The logical topic name.</param>
    /// <param name="cancellationToken">The token used to cancel the operation.</param>
    /// <returns>The readable queues currently reported by the NameServer.</returns>
    Task<IReadOnlyList<RemotingConsumerQueue>> GetMessageQueuesAsync(
        string topic,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Adds or replaces a topic subscription and reconciles the automatic assignment.
    /// </summary>
    /// <param name="topic">The topic to subscribe to.</param>
    /// <param name="expression">The filter expression, or <see langword="null"/> to match every message.</param>
    /// <param name="cancellationToken">The token used to cancel the operation.</param>
    /// <returns>A task that represents the asynchronous subscription operation.</returns>
    /// <exception cref="InvalidOperationException">The consumer is using manual-assignment mode.</exception>
    Task SubscribeAsync(
        string topic,
        FilterExpression? expression = null,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Removes a topic subscription and its automatically assigned queues.
    /// </summary>
    /// <param name="topic">The topic to unsubscribe from.</param>
    /// <param name="cancellationToken">The token used to cancel the operation.</param>
    /// <returns>A task that represents the asynchronous unsubscription operation.</returns>
    /// <exception cref="InvalidOperationException">The consumer is using manual-assignment mode.</exception>
    Task UnsubscribeAsync(string topic, CancellationToken cancellationToken = default);

    /// <summary>
    /// Replaces the automatic assignment with an explicit set of queues and per-topic filters.
    /// </summary>
    /// <param name="queues">The queues to receive. An empty collection clears manual-assignment mode.</param>
    /// <param name="filters">
    /// The filters keyed by logical topic, or <see langword="null"/> to match every message. A missing topic matches
    /// every message on that topic.
    /// </param>
    /// <param name="cancellationToken">The token used to cancel the operation.</param>
    /// <returns>A task that represents the asynchronous assignment operation.</returns>
    /// <exception cref="InvalidOperationException">
    /// The consumer is not running or still has one or more topic subscriptions.
    /// </exception>
    Task AssignAsync(
        IEnumerable<RemotingConsumerQueue> queues,
        IReadOnlyDictionary<string, FilterExpression>? filters = null,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Reads messages from the local delivery buffer.
    /// </summary>
    /// <param name="timeout">
    /// The maximum local wait time, or <see langword="null"/> to use the configured poll timeout.
    /// </param>
    /// <param name="cancellationToken">The token used to cancel the operation.</param>
    /// <returns>The available messages, or an empty list when the local timeout expires.</returns>
    /// <exception cref="InvalidOperationException">The consumer is not running or has no assignment.</exception>
    Task<IReadOnlyList<RemotingMessageView>> PollAsync(
        TimeSpan? timeout = null,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Gets the next local position for an assigned queue without performing Broker I/O.
    /// </summary>
    /// <param name="queue">The assigned queue whose position to retrieve.</param>
    /// <returns>The next local queue offset.</returns>
    /// <exception cref="InvalidOperationException">The queue is not currently assigned.</exception>
    long Position(RemotingConsumerQueue queue);

    /// <summary>
    /// Sets the next local receive position for an assigned queue without committing it.
    /// </summary>
    /// <param name="queue">The assigned queue whose position is changed.</param>
    /// <param name="offset">The non-negative queue offset to receive next.</param>
    /// <exception cref="InvalidOperationException">The queue is not currently assigned.</exception>
    void Seek(RemotingConsumerQueue queue, long offset);

    /// <summary>
    /// Sets an assigned queue's next local position to its earliest available offset.
    /// </summary>
    /// <param name="queue">The assigned queue whose position is changed.</param>
    /// <param name="cancellationToken">The token used to cancel the operation.</param>
    /// <returns>A task that represents the asynchronous seek operation.</returns>
    Task SeekToBeginningAsync(RemotingConsumerQueue queue, CancellationToken cancellationToken = default);

    /// <summary>
    /// Sets an assigned queue's next local position to its end offset.
    /// </summary>
    /// <param name="queue">The assigned queue whose position is changed.</param>
    /// <param name="cancellationToken">The token used to cancel the operation.</param>
    /// <returns>A task that represents the asynchronous seek operation.</returns>
    Task SeekToEndAsync(RemotingConsumerQueue queue, CancellationToken cancellationToken = default);

    /// <summary>
    /// Sets an assigned queue's next local position to the first offset matching a timestamp.
    /// </summary>
    /// <param name="queue">The assigned queue whose position is changed.</param>
    /// <param name="timestamp">The timestamp for which to search.</param>
    /// <param name="cancellationToken">The token used to cancel the operation.</param>
    /// <returns>A task that represents the asynchronous seek operation.</returns>
    Task SeekToTimestampAsync(
        RemotingConsumerQueue queue,
        DateTimeOffset timestamp,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Gets the consumer group's committed offset for an assigned queue.
    /// </summary>
    /// <param name="queue">The assigned queue whose committed offset to retrieve.</param>
    /// <param name="cancellationToken">The token used to cancel the operation.</param>
    /// <returns>The committed offset, or <see langword="null"/> when no offset has been committed.</returns>
    Task<long?> GetCommittedOffsetAsync(
        RemotingConsumerQueue queue,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Stops new receives for the specified assigned queues. Already buffered messages may still be returned.
    /// </summary>
    /// <param name="queues">The assigned queues to pause.</param>
    void Pause(IEnumerable<RemotingConsumerQueue> queues);

    /// <summary>
    /// Restarts receives for the specified paused queues.
    /// </summary>
    /// <param name="queues">The assigned queues to resume.</param>
    void Resume(IEnumerable<RemotingConsumerQueue> queues);

    /// <summary>
    /// Commits delivered positions for all assigned queues to the Broker.
    /// </summary>
    /// <param name="cancellationToken">The token used to cancel the operation.</param>
    /// <returns>A task that represents the asynchronous commit operation.</returns>
    Task CommitAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Commits the delivered position for one assigned queue to the Broker.
    /// </summary>
    /// <param name="queue">The assigned queue whose delivered position is committed.</param>
    /// <param name="cancellationToken">The token used to cancel the operation.</param>
    /// <returns>A task that represents the asynchronous commit operation.</returns>
    /// <exception cref="InvalidOperationException">The queue is not currently assigned.</exception>
    Task CommitAsync(RemotingConsumerQueue queue, CancellationToken cancellationToken = default);
}
