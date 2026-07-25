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

namespace EventHorizon.RocketMQ.Remoting.Consumer.Pull.Lite;

/// <summary>
/// Provides assignment-oriented, caller-driven message polling through the RocketMQ classic remoting protocol.
/// </summary>
/// <remarks>
/// Subscription mode and manual-assignment mode are mutually exclusive. Calls to <see cref="PollAsync"/> advance
/// local queue positions only; use <see cref="CommitAsync(CancellationToken)"/> or
/// <see cref="CommitAsync(RemotingPullMessageQueue, CancellationToken)"/> to persist positions to the Broker.
/// </remarks>
public interface IRemotingLitePullConsumer : IAsyncDisposable
{
    /// <summary>
    /// Starts the consumer and resolves any topic subscriptions configured before startup.
    /// </summary>
    /// <param name="cancellationToken">The token used to cancel the operation.</param>
    /// <returns>A value task that represents the asynchronous start operation.</returns>
    ValueTask StartAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Stops the consumer and cancels any active polling operation.
    /// </summary>
    /// <param name="cancellationToken">The token used to cancel the operation.</param>
    /// <returns>A value task that represents the asynchronous stop operation.</returns>
    ValueTask StopAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Gets a snapshot of the queues currently assigned to this consumer.
    /// </summary>
    IReadOnlyList<RemotingPullMessageQueue> Assignment { get; }

    /// <summary>
    /// Discovers the readable queues for a topic so they can be used with manual assignment.
    /// </summary>
    /// <param name="topic">The logical topic name.</param>
    /// <param name="cancellationToken">The token used to cancel the operation.</param>
    /// <returns>The readable queues currently reported by the NameServer.</returns>
    /// <exception cref="InvalidOperationException">The consumer is not running.</exception>
    Task<IReadOnlyList<RemotingPullMessageQueue>> GetMessageQueuesAsync(
        string topic,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Adds or replaces a topic subscription and refreshes the automatic queue assignment when the consumer is
    /// running.
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
    /// Replaces the automatic assignment with an explicit set of queues.
    /// </summary>
    /// <param name="queues">The queues to poll. An empty collection clears manual-assignment mode.</param>
    /// <param name="cancellationToken">The token used to cancel the operation.</param>
    /// <returns>A task that represents the asynchronous assignment operation.</returns>
    /// <exception cref="InvalidOperationException">
    /// The consumer is not running or still has one or more topic subscriptions.
    /// </exception>
    Task AssignAsync(IEnumerable<RemotingPullMessageQueue> queues, CancellationToken cancellationToken = default);

    /// <summary>
    /// Polls the next active assigned queue and advances its local position to the returned next offset.
    /// </summary>
    /// <param name="timeout">
    /// The maximum local wait time, or <see langword="null"/> to use the configured broker long-poll timeout.
    /// </param>
    /// <param name="cancellationToken">The token used to cancel the operation.</param>
    /// <returns>The messages and offset reported by the Broker.</returns>
    /// <exception cref="InvalidOperationException">The consumer is not running or has no assignment.</exception>
    Task<RemotingPullResult> PollAsync(
        TimeSpan? timeout = null,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Sets the next local polling position for an assigned queue without committing it to the Broker.
    /// </summary>
    /// <param name="queue">The assigned queue whose position is changed.</param>
    /// <param name="offset">The non-negative queue offset to use for the next poll.</param>
    /// <exception cref="InvalidOperationException">The queue is not currently assigned.</exception>
    void Seek(RemotingPullMessageQueue queue, long offset);

    /// <summary>
    /// Sets the next local polling position for an assigned queue to its earliest available offset.
    /// </summary>
    /// <param name="queue">The assigned queue whose position is changed.</param>
    /// <param name="cancellationToken">The token used to cancel the operation.</param>
    /// <returns>A task that represents the asynchronous seek operation.</returns>
    Task SeekToBeginningAsync(RemotingPullMessageQueue queue, CancellationToken cancellationToken = default);

    /// <summary>
    /// Sets the next local polling position for an assigned queue to its end offset.
    /// </summary>
    /// <param name="queue">The assigned queue whose position is changed.</param>
    /// <param name="cancellationToken">The token used to cancel the operation.</param>
    /// <returns>A task that represents the asynchronous seek operation.</returns>
    Task SeekToEndAsync(RemotingPullMessageQueue queue, CancellationToken cancellationToken = default);

    /// <summary>
    /// Prevents future polls from selecting the specified assigned queues.
    /// </summary>
    /// <param name="queues">The assigned queues to pause.</param>
    void Pause(IEnumerable<RemotingPullMessageQueue> queues);

    /// <summary>
    /// Allows future polls to select the specified assigned queues.
    /// </summary>
    /// <param name="queues">The assigned queues to resume.</param>
    void Resume(IEnumerable<RemotingPullMessageQueue> queues);

    /// <summary>
    /// Commits the current local positions for all assigned queues to the Broker.
    /// </summary>
    /// <param name="cancellationToken">The token used to cancel the operation.</param>
    /// <returns>A task that represents the asynchronous commit operation.</returns>
    Task CommitAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Commits the current local position for an assigned queue to the Broker.
    /// </summary>
    /// <param name="queue">The assigned queue whose position is committed.</param>
    /// <param name="cancellationToken">The token used to cancel the operation.</param>
    /// <returns>A task that represents the asynchronous commit operation.</returns>
    /// <exception cref="InvalidOperationException">The queue is not currently assigned.</exception>
    Task CommitAsync(RemotingPullMessageQueue queue, CancellationToken cancellationToken = default);
}
