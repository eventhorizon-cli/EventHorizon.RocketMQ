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
using EventHorizon.RocketMQ.Grpc.Consumer;
using Microsoft.Extensions.Options;

namespace EventHorizon.RocketMQ.Grpc.Consumer.Simple;

internal sealed class GrpcSimpleConsumer : IGrpcSimpleConsumer
{
    private readonly GrpcSimpleConsumerOptions _options;
    private readonly IGrpcReceiveConsumerEngine _engine;
    private int _topicIndex;
    private int _queueIndex;

    public GrpcSimpleConsumer(
        IOptions<GrpcSimpleConsumerOptions> options,
        IGrpcReceiveConsumerEngine engine)
    {
        _options = options.Value;
        _engine = engine;
    }

    public ValueTask StartAsync(CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);
        return _engine.StartAsync(cancellationToken);
    }

    public ValueTask StopAsync(CancellationToken cancellationToken = default) => _engine.StopAsync(cancellationToken);

    public Task SubscribeAsync(
        string topic,
        FilterExpression? expression = null,
        CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);
        return _engine.SubscribeAsync(topic, expression, cancellationToken);
    }

    public Task UnsubscribeAsync(string topic, CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);
        return _engine.UnsubscribeAsync(topic, cancellationToken);
    }

    public async Task<IReadOnlyList<GrpcMessageView>> ReceiveAsync(int maxMessages, TimeSpan? invisibleDuration = null, CancellationToken cancellationToken = default)
    {
        if (maxMessages <= 0) throw new ArgumentOutOfRangeException(nameof(maxMessages));
        if (invisibleDuration <= TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(nameof(invisibleDuration), "Invisible duration must be positive.");
        }

        var subscriptions = _engine.GetSubscriptions();
        if (subscriptions.Length == 0) throw new InvalidOperationException("At least one subscription is required.");
        var topicIndex = (Interlocked.Increment(ref _topicIndex) & int.MaxValue) % subscriptions.Length;
        var subscription = subscriptions[topicIndex];
        var assignments = await _engine.GetAssignmentsAsync(subscription.Key, cancellationToken).ConfigureAwait(false);
        if (assignments.Count == 0)
        {
            return Array.Empty<GrpcMessageView>();
        }

        var queueIndex = (Interlocked.Increment(ref _queueIndex) & int.MaxValue) % assignments.Count;
        return await _engine.ReceiveAsync(
            assignments[queueIndex].MessageQueue,
            subscription.Value,
            maxMessages,
            invisibleDuration ?? _options.InvisibleDuration,
            _options.AwaitDuration,
            false,
            cancellationToken).ConfigureAwait(false);
    }

    public Task AckAsync(GrpcMessageView message, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(message);
        return _engine.AckAsync(message, cancellationToken);
    }

    public Task ChangeInvisibleDurationAsync(GrpcMessageView message, TimeSpan invisibleDuration, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(message);
        if (invisibleDuration <= TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(nameof(invisibleDuration), "Invisible duration must be positive.");
        }

        return _engine.ChangeInvisibleDurationAsync(message, invisibleDuration, cancellationToken);
    }

    public Task ForwardToDeadLetterQueueAsync(GrpcMessageView message, int maxDeliveryAttempts, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(message);
        if (maxDeliveryAttempts <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(maxDeliveryAttempts), "Maximum delivery attempts must be positive.");
        }

        return _engine.ForwardToDeadLetterQueueAsync(message, maxDeliveryAttempts, cancellationToken);
    }

    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
        {
            return;
        }

        await _engine.DisposeAsync().ConfigureAwait(false);
    }

    private int _disposed;
}
