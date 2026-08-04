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

namespace EventHorizon.RocketMQ.Remoting.Consumer.LitePull.Assignment;

internal sealed class LitePullAssignmentReconciler
{
    private readonly RemotingLitePullConsumerOptions _options;
    private readonly IRemotingConsumerOffsetClient _offsetClient;
    private readonly LitePullQueueReceiver _queueReceiver;
    private readonly ILogger _logger;

    public LitePullAssignmentReconciler(
        RemotingLitePullConsumerOptions options,
        IRemotingConsumerOffsetClient offsetClient,
        LitePullQueueReceiver queueReceiver,
        ILogger logger)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(offsetClient);
        ArgumentNullException.ThrowIfNull(queueReceiver);
        ArgumentNullException.ThrowIfNull(logger);

        _options = options;
        _offsetClient = offsetClient;
        _queueReceiver = queueReceiver;
        _logger = logger;
    }

    public async Task ReconcileAsync(
        RemotingLitePullConsumerRun run,
        IReadOnlyCollection<LitePullReceiveTarget> desiredTargets,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(run);
        ArgumentNullException.ThrowIfNull(desiredTargets);
        using var operation = await run.OperationGate.EnterAsync(cancellationToken).ConfigureAwait(false);
        var changes = run.DeliveryState.ApplyDesiredTargets(desiredTargets);
        await LitePullReceiverRegistry.AwaitAsync(changes.Removed).ConfigureAwait(false);
        if (!run.OperationGate.IsOpen)
        {
            return;
        }

        foreach (var replacement in changes.Replacements)
        {
            StartReceiver(run, replacement);
        }

        foreach (var target in changes.Additions)
        {
            var offset = await InitializeOffsetAsync(target.Queue, cancellationToken).ConfigureAwait(false);
            if (!run.OperationGate.IsOpen)
            {
                return;
            }

            var state = run.DeliveryState.AddInitializedQueue(target, offset);
            if (state is not null)
            {
                StartReceiver(run, state);
            }
        }
    }

    public void StartReceiver(RemotingLitePullConsumerRun run, LitePullQueueState state)
    {
        ArgumentNullException.ThrowIfNull(run);
        ArgumentNullException.ThrowIfNull(state);
        if (!run.OperationGate.IsOpen || run.Stopping.IsCancellationRequested)
        {
            return;
        }

        run.Receivers.Start(
            state,
            receiverState => _queueReceiver.ReceiveAsync(run, receiverState),
            (receiverState, exception) => ReceiverStoppedUnexpectedly(run, receiverState, exception));
    }

    private async Task<long> InitializeOffsetAsync(
        RemotingConsumerQueue queue,
        CancellationToken cancellationToken)
    {
        var committed = await _offsetClient.GetOffsetAsync(queue, cancellationToken).ConfigureAwait(false);
        if (committed >= 0)
        {
            return committed;
        }

        return await _offsetClient.QueryOffsetAsync(
            queue,
            _options.InitialPosition,
            _options.ConsumeTimestamp,
            cancellationToken).ConfigureAwait(false);
    }

    private void ReceiverStoppedUnexpectedly(
        RemotingLitePullConsumerRun run,
        LitePullQueueState state,
        Exception? exception)
    {
        if (!run.DeliveryState.MarkReceiverStoppedUnexpectedly(state))
        {
            return;
        }

        if (exception is null)
        {
            _logger.LogWarning(
                "Legacy Lite Pull receiver stopped unexpectedly for {Topic}/{BrokerName}/{QueueId}; reconciling a replacement generation",
                state.Queue.Topic,
                state.Queue.BrokerName,
                state.Queue.QueueId);
        }
        else
        {
            _logger.LogWarning(
                exception,
                "Legacy Lite Pull receiver failed for {Topic}/{BrokerName}/{QueueId}; reconciling a replacement generation",
                state.Queue.Topic,
                state.Queue.BrokerName,
                state.Queue.QueueId);
        }

        run.GroupSession.Wakeup();
    }
}
