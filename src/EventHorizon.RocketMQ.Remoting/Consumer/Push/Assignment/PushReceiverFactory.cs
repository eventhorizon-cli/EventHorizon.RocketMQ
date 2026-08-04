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

using EventHorizon.RocketMQ.Remoting.Consumer.Push.Pop;
using EventHorizon.RocketMQ.Remoting.Consumer.Push.Processing;
using EventHorizon.RocketMQ.Remoting.Consumer.Push.Pull.Receive;
using EventHorizon.RocketMQ.Remoting.Consumer.Settlement;
using Microsoft.Extensions.Logging;

namespace EventHorizon.RocketMQ.Remoting.Consumer.Push.Assignment;

internal sealed class PushReceiverFactory
{
    private readonly RemotingPushConsumerOptions _options;
    private readonly PopWireClient _popClient;
    private readonly IRemotingSettlementClient _settlementClient;
    private readonly TimeProvider _timeProvider;
    private readonly ILogger _logger;

    public PushReceiverFactory(
        RemotingPushConsumerOptions options,
        PopWireClient popClient,
        IRemotingSettlementClient settlementClient,
        TimeProvider timeProvider,
        ILogger logger)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(popClient);
        ArgumentNullException.ThrowIfNull(settlementClient);
        ArgumentNullException.ThrowIfNull(timeProvider);
        ArgumentNullException.ThrowIfNull(logger);

        _options = options;
        _popClient = popClient;
        _settlementClient = settlementClient;
        _timeProvider = timeProvider;
        _logger = logger;
    }

    public IRemotingPushReceiver Create(
        PushReceiveTarget target,
        RemotingPushConsumerRun run)
    {
        ArgumentNullException.ThrowIfNull(run);
        return target.Assignment.Mode == RemotingPushReceiveMode.Pop
            ? new PopAssignmentReceiver(
                target,
                _options,
                _popClient,
                _timeProvider,
                _logger,
                target.Filter,
                new PopDeliveryProcessor(
                    _options,
                    _popClient,
                    _settlementClient,
                    _timeProvider,
                    _logger,
                    (messages, canInvoke, token) => HandlePopMessagesAsync(run, messages, canInvoke, token)),
                run.Stopping.Token)
            : new PullAssignmentReceiver(
                target,
                run.Stopping.Token,
                createProcessQueue: !_options.ConsumeOrderly);
    }

    public void Start(
        IRemotingPushReceiver receiver,
        RemotingPushConsumerRun run,
        bool brokerLockAcquired)
    {
        ArgumentNullException.ThrowIfNull(receiver);
        ArgumentNullException.ThrowIfNull(run);
        if (brokerLockAcquired && receiver is PullAssignmentReceiver lockedPullReceiver)
        {
            lockedPullReceiver.MarkBrokerLockAcquired(_timeProvider.GetUtcNow());
        }

        if (receiver is PopAssignmentReceiver popReceiver)
        {
            popReceiver.Start();
        }
        else
        {
            var pullReceiver = (PullAssignmentReceiver)receiver;
            pullReceiver.Start(_ => run.RunPullReceiverAsync(pullReceiver));
        }
    }

    private async ValueTask<IReadOnlyList<PushHandlerMessageOutcome>?> HandlePopMessagesAsync(
        RemotingPushConsumerRun run,
        IReadOnlyList<RemotingMessageView> messages,
        Func<bool> canInvoke,
        CancellationToken cancellationToken)
    {
        PushHandlerBatchResult result;
        try
        {
            var admittedResult = await run.MessageHandlerInvoker.InvokeIfAsync(
                messages,
                enableTimeoutRecovery: false,
                canInvoke,
                cancellationToken).ConfigureAwait(false);
            if (admittedResult is null)
            {
                return null;
            }

            result = admittedResult.Value;
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception exception)
        {
            _logger.LogError(exception, "Legacy POP message handler failed; the batch will be retried");
            result = PushHandlerBatchResult.Retry(
                messages.Count,
                delayLevelWhenNextConsume: 0);
        }

        return Enumerable.Range(0, messages.Count)
            .Select(result.GetOutcome)
            .ToArray();
    }
}
