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

using EventHorizon.RocketMQ.Remoting.Consumer.Push.Pull.Offset;
using EventHorizon.RocketMQ.Remoting.Consumer.Settlement;
using Microsoft.Extensions.Logging;

namespace EventHorizon.RocketMQ.Remoting.Consumer.Push.Pull.Settlement;

internal sealed class PullSendBackSettlement
{
    private readonly RemotingPushConsumerOptions _options;
    private readonly IRemotingSettlementClient _settlementClient;
    private readonly PullOffsetManager _offsetManager;
    private readonly ILogger _logger;
    private readonly Func<RemotingConsumerQueue, bool> _receiverExists;
    private readonly Action _requireRetryTopic;
    private readonly Action _wakeupRebalance;

    public PullSendBackSettlement(
        RemotingPushConsumerOptions options,
        IRemotingSettlementClient settlementClient,
        PullOffsetManager offsetManager,
        ILogger logger,
        Func<RemotingConsumerQueue, bool> receiverExists,
        Action requireRetryTopic,
        Action wakeupRebalance)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(settlementClient);
        ArgumentNullException.ThrowIfNull(offsetManager);
        ArgumentNullException.ThrowIfNull(logger);
        ArgumentNullException.ThrowIfNull(receiverExists);
        ArgumentNullException.ThrowIfNull(requireRetryTopic);
        ArgumentNullException.ThrowIfNull(wakeupRebalance);

        _options = options;
        _settlementClient = settlementClient;
        _offsetManager = offsetManager;
        _logger = logger;
        _receiverExists = receiverExists;
        _requireRetryTopic = requireRetryTopic;
        _wakeupRebalance = wakeupRebalance;
    }

    public async Task SendAsync(
        RemotingConsumerQueue sourceQueue,
        RemotingMessageView message,
        bool deadLetter,
        int delayLevel,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(sourceQueue);
        ArgumentNullException.ThrowIfNull(message);
        var retryOffset = deadLetter
            ? null
            : await PrepareRetryOffsetAsync(sourceQueue, cancellationToken).ConfigureAwait(false);
        await _settlementClient.SendBackAsync(
            sourceQueue,
            message,
            deadLetter ? -1 : delayLevel,
            _options.MaxDeliveryAttempts,
            cancellationToken).ConfigureAwait(false);
        if (deadLetter)
        {
            return;
        }

        _requireRetryTopic();
        if (retryOffset is { } initialization)
        {
            try
            {
                await _offsetManager.PersistRetryAsync(initialization, CancellationToken.None)
                    .ConfigureAwait(false);
            }
            catch (Exception exception)
            {
                _logger.LogWarning(
                    exception,
                    "Unable to initialize retry-queue offset for {Topic}/{BrokerName}/{QueueId}",
                    initialization.Queue.Topic,
                    initialization.Queue.BrokerName,
                    initialization.Queue.QueueId);
            }
        }

        _wakeupRebalance();
    }

    private async Task<RetryQueueOffsetInitialization?> PrepareRetryOffsetAsync(
        RemotingConsumerQueue sourceQueue,
        CancellationToken cancellationToken)
    {
        var retryQueue = _offsetManager.CreateRetryQueue(sourceQueue);
        return _receiverExists(retryQueue)
            ? null
            : await _offsetManager.PrepareRetryAsync(retryQueue, cancellationToken).ConfigureAwait(false);
    }
}
