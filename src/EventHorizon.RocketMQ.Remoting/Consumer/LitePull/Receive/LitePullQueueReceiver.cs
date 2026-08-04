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

using EventHorizon.RocketMQ.Remoting.Consumer.Pull;
using Microsoft.Extensions.Logging;

namespace EventHorizon.RocketMQ.Remoting.Consumer.LitePull.Receive;

internal sealed class LitePullQueueReceiver
{
    private static readonly TimeSpan FailureRetryDelay = TimeSpan.FromSeconds(1);

    private readonly IRemotingPullWireClient _pullClient;
    private readonly int _pullBatchSize;
    private readonly int _maxMessageBytes;
    private readonly ILogger _logger;

    public LitePullQueueReceiver(
        IRemotingPullWireClient pullClient,
        int pullBatchSize,
        int maxMessageBytes,
        ILogger logger)
    {
        ArgumentNullException.ThrowIfNull(pullClient);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(pullBatchSize);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(maxMessageBytes);
        ArgumentNullException.ThrowIfNull(logger);

        _pullClient = pullClient;
        _pullBatchSize = pullBatchSize;
        _maxMessageBytes = maxMessageBytes;
        _logger = logger;
    }

    public async Task ReceiveAsync(
        RemotingLitePullConsumerRun run,
        LitePullQueueState state)
    {
        ArgumentNullException.ThrowIfNull(run);
        ArgumentNullException.ThrowIfNull(state);

        using var receiveCancellation = CancellationTokenSource.CreateLinkedTokenSource(
            run.Stopping.Token,
            state.ReceiveCancellation.Token);
        var cancellationToken = receiveCancellation.Token;
        try
        {
            while (true)
            {
                var selection = run.DeliveryState.SelectReceive(state, _pullBatchSize, _maxMessageBytes);
                if (!selection.IsCurrent)
                {
                    return;
                }

                if (!selection.CanReceive)
                {
                    await selection.StateChanged.WaitAsync(cancellationToken).ConfigureAwait(false);
                    continue;
                }

                RemotingPullResult result;
                try
                {
                    result = await _pullClient.PullAsync(
                        selection.Queue!,
                        selection.Offset,
                        selection.Filter!,
                        selection.MaxMessages,
                        selection.MaxBytes,
                        cancellationToken).ConfigureAwait(false);
                }
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                {
                    return;
                }
                catch (Exception exception)
                {
                    _logger.LogWarning(
                        exception,
                        "Legacy Lite Pull receive failed for {Topic}/{BrokerName}/{QueueId}; retrying",
                        state.Queue.Topic,
                        state.Queue.BrokerName,
                        state.Queue.QueueId);
                    await Task.Delay(FailureRetryDelay, cancellationToken).ConfigureAwait(false);
                    continue;
                }

                try
                {
                    if (!await run.DeliveryState.AdmitAsync(state, result, cancellationToken).ConfigureAwait(false))
                    {
                        return;
                    }
                }
                catch (InvalidDataException exception)
                {
                    _logger.LogWarning(
                        exception,
                        "Legacy Lite Pull response exceeded the configured shared cache limit for " +
                        "{Topic}/{BrokerName}/{QueueId}; retrying",
                        state.Queue.Topic,
                        state.Queue.BrokerName,
                        state.Queue.QueueId);
                    await Task.Delay(FailureRetryDelay, cancellationToken).ConfigureAwait(false);
                    continue;
                }

                await Task.Yield();
            }
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
        }
    }
}
