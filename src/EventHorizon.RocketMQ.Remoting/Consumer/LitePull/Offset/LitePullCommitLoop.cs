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

using EventHorizon.RocketMQ.Remoting.Consumer.LitePull.Receive;
using EventHorizon.RocketMQ.Remoting.Consumer.Offset;
using Microsoft.Extensions.Logging;

namespace EventHorizon.RocketMQ.Remoting.Consumer.LitePull.Offset;

internal sealed class LitePullCommitLoop
{
    private readonly IRemotingConsumerOffsetClient _offsetClient;
    private readonly TimeSpan _interval;
    private readonly TimeProvider _timeProvider;
    private readonly string _groupName;
    private readonly ILogger _logger;

    public LitePullCommitLoop(
        IRemotingConsumerOffsetClient offsetClient,
        TimeSpan interval,
        TimeProvider timeProvider,
        string groupName,
        ILogger logger)
    {
        ArgumentNullException.ThrowIfNull(offsetClient);
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(interval, TimeSpan.Zero);
        ArgumentNullException.ThrowIfNull(timeProvider);
        ArgumentException.ThrowIfNullOrWhiteSpace(groupName);
        ArgumentNullException.ThrowIfNull(logger);

        _offsetClient = offsetClient;
        _interval = interval;
        _timeProvider = timeProvider;
        _groupName = groupName;
        _logger = logger;
    }

    public async Task RunAsync(RemotingLitePullConsumerRun run)
    {
        ArgumentNullException.ThrowIfNull(run);
        while (!run.Stopping.IsCancellationRequested)
        {
            try
            {
                await Task.Delay(_interval, _timeProvider, run.Stopping.Token).ConfigureAwait(false);
                await CommitAllAsync(run, run.Stopping.Token).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (run.Stopping.IsCancellationRequested)
            {
                return;
            }
            catch (Exception exception)
            {
                _logger.LogWarning(
                    exception,
                    "Unable to auto-commit legacy Lite Pull positions for group {GroupName}",
                    _groupName);
            }
        }
    }

    public Task CommitAllAsync(
        RemotingLitePullConsumerRun run,
        CancellationToken cancellationToken) =>
        CommitAsync(run, static state => state.DeliveryState.GetPositions(), cancellationToken);

    public Task CommitQueueAsync(
        RemotingLitePullConsumerRun run,
        RemotingConsumerQueue queue,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(queue);
        return CommitAsync(
            run,
            state => [state.DeliveryState.GetRequiredQueuePosition(queue)],
            cancellationToken);
    }

    public Task CommitFinalAsync(RemotingLitePullConsumerRun run)
    {
        ArgumentNullException.ThrowIfNull(run);
        return Task.WhenAll(run.DeliveryState.GetDeliveredPositions().Select(UpdateFinalOffsetAsync));
    }

    private async Task CommitAsync(
        RemotingLitePullConsumerRun run,
        Func<RemotingLitePullConsumerRun, IReadOnlyList<LitePullDeliveryState.QueuePosition>> getPositions,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(run);
        using var operationCancellation = CancellationTokenSource.CreateLinkedTokenSource(
            cancellationToken,
            run.Stopping.Token);
        using var operation = await run.OperationGate.EnterAsync(operationCancellation.Token).ConfigureAwait(false);
        var positions = getPositions(run);
        await Task.WhenAll(positions.Select(position => UpdateOffsetAsync(
            run,
            position,
            operationCancellation.Token))).ConfigureAwait(false);
    }

    private async Task UpdateOffsetAsync(
        RemotingLitePullConsumerRun run,
        LitePullDeliveryState.QueuePosition position,
        CancellationToken cancellationToken)
    {
        using var queueCancellation = CancellationTokenSource.CreateLinkedTokenSource(
            cancellationToken,
            position.CancellationToken);
        await _offsetClient.UpdateOffsetAsync(
            position.Queue,
            position.Offset,
            queueCancellation.Token).ConfigureAwait(false);
        if (!run.DeliveryState.IsCurrent(position))
        {
            throw new OperationCanceledException(queueCancellation.Token);
        }
    }

    private async Task UpdateFinalOffsetAsync(LitePullDeliveryState.DeliveredPosition position)
    {
        try
        {
            await _offsetClient.UpdateOffsetAsync(
                position.Queue,
                position.Offset,
                CancellationToken.None).ConfigureAwait(false);
        }
        catch (Exception exception)
        {
            _logger.LogWarning(
                exception,
                "Unable to commit the final legacy Lite Pull position for group {GroupName} and " +
                "{Topic}/{BrokerName}/{QueueId}",
                _groupName,
                position.Queue.Topic,
                position.Queue.BrokerName,
                position.Queue.QueueId);
        }
    }
}
