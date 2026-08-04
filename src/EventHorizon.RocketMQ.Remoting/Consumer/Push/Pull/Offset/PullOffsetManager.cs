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

using System.Collections.Concurrent;
using EventHorizon.RocketMQ.Remoting.Consumer.Offset;
using EventHorizon.RocketMQ.Remoting.Exceptions;
using EventHorizon.RocketMQ.Remoting.Protocol;
using Microsoft.Extensions.Logging;

namespace EventHorizon.RocketMQ.Remoting.Consumer.Push.Pull.Offset;

internal sealed class PullOffsetManager
{
    private readonly RemotingPushConsumerOptions _options;
    private readonly IRemotingConsumerOffsetClient _offsetClient;
    private readonly ILogger _logger;
    private readonly BroadcastOffsetStore? _broadcastOffsetStore;
    private readonly ConcurrentDictionary<QueueIdentity, long> _resetBoundaries = new();
    private readonly ConcurrentDictionary<QueueIdentity, long> _retryBoundaries = new();
    private readonly string _retryTopic;

    public PullOffsetManager(
        RemotingPushConsumerOptions options,
        IRemotingConsumerOffsetClient offsetClient,
        string clientId,
        string consumerGroup,
        ILogger logger)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(offsetClient);
        ArgumentException.ThrowIfNullOrWhiteSpace(clientId);
        ArgumentException.ThrowIfNullOrWhiteSpace(consumerGroup);
        ArgumentNullException.ThrowIfNull(logger);

        _options = options;
        _offsetClient = offsetClient;
        _logger = logger;
        _retryTopic = $"%RETRY%{options.GroupName}";
        if (options.ConsumerMode == ConsumerMode.Broadcasting)
        {
            _broadcastOffsetStore = new BroadcastOffsetStore(
                options.LocalOffsetStorePath,
                clientId,
                consumerGroup);
        }
    }

    public bool HasResetBoundary(RemotingConsumerQueue queue) =>
        _resetBoundaries.ContainsKey(QueueIdentity.Create(queue));

    public void BeginRun() => _retryBoundaries.Clear();

    public async Task<long> InitializeAsync(
        RemotingConsumerQueue queue,
        CancellationToken cancellationToken)
    {
        if (_resetBoundaries.TryGetValue(QueueIdentity.Create(queue), out var resetOffset))
        {
            return resetOffset;
        }

        while (true)
        {
            try
            {
                if (_options.ConsumerMode == ConsumerMode.Broadcasting)
                {
                    var localOffset = await _broadcastOffsetStore!.ReadAsync(queue, cancellationToken)
                        .ConfigureAwait(false);
                    if (localOffset.HasValue)
                    {
                        return localOffset.Value;
                    }

                    var initialBroadcastOffset = await QueryInitialOffsetAsync(queue, cancellationToken)
                        .ConfigureAwait(false);
                    await _broadcastOffsetStore.UpdateAsync(queue, initialBroadcastOffset, cancellationToken)
                        .ConfigureAwait(false);
                    return initialBroadcastOffset;
                }

                var committed = await _offsetClient.GetOffsetAsync(queue, cancellationToken).ConfigureAwait(false);
                if (committed >= 0)
                {
                    return committed;
                }

                var initial = string.Equals(queue.Topic, _retryTopic, StringComparison.Ordinal)
                    ? _retryBoundaries.TryGetValue(QueueIdentity.Create(queue), out var retryBoundary)
                        ? retryBoundary
                        : 0
                    : await QueryInitialOffsetAsync(queue, cancellationToken).ConfigureAwait(false);
                await _offsetClient.UpdateOffsetAsync(queue, initial, cancellationToken).ConfigureAwait(false);
                return initial;
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception exception)
            {
                _logger.LogDebug(
                    exception,
                    "Legacy queue {Topic}/{BrokerName}/{QueueId} is not available yet",
                    queue.Topic,
                    queue.BrokerName,
                    queue.QueueId);
                await Task.Delay(TimeSpan.FromSeconds(1), cancellationToken).ConfigureAwait(false);
            }
        }
    }

    public async Task PersistAsync(
        RemotingConsumerQueue queue,
        long offset,
        CancellationToken cancellationToken)
    {
        var key = QueueIdentity.Create(queue);
        var hasResetBoundary = _resetBoundaries.TryGetValue(key, out var resetBoundary);
        if (_options.ConsumerMode == ConsumerMode.Broadcasting)
        {
            await _broadcastOffsetStore!.UpdateAsync(queue, offset, cancellationToken).ConfigureAwait(false);
        }
        else
        {
            await _offsetClient.UpdateOffsetAsync(queue, offset, cancellationToken).ConfigureAwait(false);
        }

        if (hasResetBoundary)
        {
            TryRemoveResetBoundary(key, resetBoundary);
        }
    }

    public void SetResetBoundaries(string logicalTopic, IEnumerable<LegacyResetOffset> offsets)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(logicalTopic);
        ArgumentNullException.ThrowIfNull(offsets);
        foreach (var offset in offsets)
        {
            _resetBoundaries[new QueueIdentity(logicalTopic, offset.BrokerName, offset.QueueId)] = offset.Offset;
        }
    }

    public void RemoveResetBoundaries(string logicalTopic)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(logicalTopic);
        foreach (var entry in _resetBoundaries)
        {
            if (string.Equals(entry.Key.Topic, logicalTopic, StringComparison.Ordinal))
            {
                ((ICollection<KeyValuePair<QueueIdentity, long>>)_resetBoundaries).Remove(entry);
            }
        }
    }

    public async Task PersistBroadcastResetAsync(
        string logicalTopic,
        LegacyResetOffset offset,
        CancellationToken cancellationToken)
    {
        if (_broadcastOffsetStore is null)
        {
            throw new InvalidOperationException("Only a broadcasting consumer has a local offset store.");
        }

        var key = new QueueIdentity(logicalTopic, offset.BrokerName, offset.QueueId);
        await _broadcastOffsetStore.UpdateAsync(
            logicalTopic,
            offset.BrokerName,
            offset.QueueId,
            offset.Offset,
            cancellationToken).ConfigureAwait(false);
        TryRemoveResetBoundary(key, offset.Offset);
    }

    public RemotingConsumerQueue CreateRetryQueue(RemotingConsumerQueue sourceQueue) => new(
        _retryTopic,
        sourceQueue.BrokerName,
        0);

    public async Task<RetryQueueOffsetInitialization?> PrepareRetryAsync(
        RemotingConsumerQueue retryQueue,
        CancellationToken cancellationToken)
    {
        var key = QueueIdentity.Create(retryQueue);
        if (_retryBoundaries.TryGetValue(key, out var pendingBoundary))
        {
            return new RetryQueueOffsetInitialization(retryQueue, pendingBoundary);
        }

        try
        {
            var committed = await _offsetClient.GetOffsetAsync(retryQueue, cancellationToken).ConfigureAwait(false);
            if (committed >= 0)
            {
                return null;
            }

            var end = await _offsetClient.QueryOffsetAsync(
                retryQueue,
                ConsumeFromPosition.End,
                cancellationToken: cancellationToken).ConfigureAwait(false);
            var boundary = _retryBoundaries.GetOrAdd(key, end);
            return new RetryQueueOffsetInitialization(retryQueue, boundary);
        }
        catch (RemotingCommandException exception) when (exception.ResponseCode == ResponseCodes.ResTopicNotExist)
        {
            var boundary = _retryBoundaries.GetOrAdd(key, 0);
            return new RetryQueueOffsetInitialization(retryQueue, boundary);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
    }

    public async Task PersistRetryAsync(
        RetryQueueOffsetInitialization initialization,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(initialization);
        var committed = await _offsetClient.GetOffsetAsync(initialization.Queue, cancellationToken)
            .ConfigureAwait(false);
        if (committed < 0)
        {
            await _offsetClient.UpdateOffsetAsync(
                initialization.Queue,
                initialization.Offset,
                cancellationToken).ConfigureAwait(false);
        }
    }

    public Task FlushAsync(CancellationToken cancellationToken) =>
        _broadcastOffsetStore?.FlushAsync(cancellationToken) ?? Task.CompletedTask;

    private Task<long> QueryInitialOffsetAsync(
        RemotingConsumerQueue queue,
        CancellationToken cancellationToken) =>
        _offsetClient.QueryOffsetAsync(
            queue,
            _options.InitialPosition,
            _options.InitialPosition == ConsumeFromPosition.Timestamp ? _options.ConsumeTimestamp : null,
            cancellationToken);

    private bool TryRemoveResetBoundary(QueueIdentity key, long expectedOffset) =>
        ((ICollection<KeyValuePair<QueueIdentity, long>>)_resetBoundaries).Remove(
            new KeyValuePair<QueueIdentity, long>(key, expectedOffset));

    private readonly record struct QueueIdentity(string Topic, string BrokerName, int QueueId)
    {
        public static QueueIdentity Create(RemotingConsumerQueue queue) =>
            new(queue.Topic, queue.BrokerName, queue.QueueId);
    }
}
