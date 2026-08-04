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

using System.Threading.Channels;
using Microsoft.Extensions.Logging;

namespace EventHorizon.RocketMQ.Remoting.Consumer.Push.Pull.Dispatch;

internal sealed class ConcurrentPullConsumeDispatcher
{
    private readonly Channel<PullProcessQueue> _readyProcessQueues;
    private readonly int _consumeMessageBatchSize;
    private readonly Func<PullConsumeRequest, CancellationToken, ValueTask> _consumeRequest;
    private readonly Action<PullCacheReservation> _releaseCacheReservation;
    private readonly CancellationToken _stopping;
    private readonly ILogger _logger;

    public ConcurrentPullConsumeDispatcher(
        int maxConcurrency,
        int consumeMessageBatchSize,
        Func<PullConsumeRequest, CancellationToken, ValueTask> consumeRequest,
        Action<PullCacheReservation> releaseCacheReservation,
        CancellationToken stopping,
        ILogger logger)
    {
        if (maxConcurrency <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(maxConcurrency));
        }

        if (consumeMessageBatchSize <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(consumeMessageBatchSize));
        }

        ArgumentNullException.ThrowIfNull(consumeRequest);
        ArgumentNullException.ThrowIfNull(releaseCacheReservation);
        ArgumentNullException.ThrowIfNull(logger);

        _consumeMessageBatchSize = consumeMessageBatchSize;
        _consumeRequest = consumeRequest;
        _releaseCacheReservation = releaseCacheReservation;
        _stopping = stopping;
        _logger = logger;
        _readyProcessQueues = Channel.CreateUnbounded<PullProcessQueue>(new UnboundedChannelOptions
        {
            SingleReader = maxConcurrency == 1,
            SingleWriter = false,
            AllowSynchronousContinuations = false
        });
        ConsumeLoopTasks = Enumerable.Range(0, maxConcurrency)
            .Select(_ => RunConsumeLoopAsync())
            .ToArray();
    }

    public Task[] ConsumeLoopTasks { get; }

    public void NotifyReady(PullProcessQueue processQueue)
    {
        ArgumentNullException.ThrowIfNull(processQueue);
        if (!processQueue.TryQueueForDispatch())
        {
            return;
        }

        if (!_readyProcessQueues.Writer.TryWrite(processQueue))
        {
            processQueue.CancelQueuedDispatch();
        }
    }

    public void Complete() => _readyProcessQueues.Writer.TryComplete();

    private async Task RunConsumeLoopAsync()
    {
        try
        {
            await foreach (var processQueue in _readyProcessQueues.Reader.ReadAllAsync(_stopping).ConfigureAwait(false))
            {
                if (!processQueue.TryCreateConsumeRequest(_consumeMessageBatchSize, out var request))
                {
                    continue;
                }

                _releaseCacheReservation(request.CacheReservation);
                NotifyReady(processQueue);

                using var linked = CancellationTokenSource.CreateLinkedTokenSource(
                    _stopping,
                    request.QueueCancellation);
                if (!processQueue.TryStartConsumeRequest(request))
                {
                    if (processQueue.AbandonConsumeRequest(request) &&
                        !_stopping.IsCancellationRequested &&
                        !request.QueueCancellation.IsCancellationRequested)
                    {
                        NotifyReady(processQueue);
                    }

                    if (_stopping.IsCancellationRequested)
                    {
                        return;
                    }

                    continue;
                }

                try
                {
                    await _consumeRequest(request, linked.Token).ConfigureAwait(false);
                }
                catch (OperationCanceledException) when (linked.IsCancellationRequested)
                {
                    if (processQueue.AbandonConsumeRequest(request) &&
                        !_stopping.IsCancellationRequested &&
                        !request.QueueCancellation.IsCancellationRequested)
                    {
                        NotifyReady(processQueue);
                    }

                    if (_stopping.IsCancellationRequested)
                    {
                        return;
                    }
                }
                catch (Exception exception)
                {
                    _logger.LogError(
                        exception,
                        "Legacy consume request failed for {Topic}/{BrokerName}/{QueueId}",
                        processQueue.MessageQueue.Topic,
                        processQueue.MessageQueue.BrokerName,
                        processQueue.MessageQueue.QueueId);
                    if (processQueue.AbandonConsumeRequest(request))
                    {
                        NotifyReady(processQueue);
                    }
                }
            }
        }
        catch (OperationCanceledException) when (_stopping.IsCancellationRequested)
        {
        }
    }
}
