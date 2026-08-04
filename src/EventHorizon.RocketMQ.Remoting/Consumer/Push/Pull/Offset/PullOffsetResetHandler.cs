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

using EventHorizon.RocketMQ.Remoting.Consumer.Coordination;
using EventHorizon.RocketMQ.Remoting.Consumer.Push.Assignment;
using EventHorizon.RocketMQ.Remoting.Consumer.Push.Pull.Receive;
using EventHorizon.RocketMQ.Remoting.Protocol;
using Microsoft.Extensions.Logging;

namespace EventHorizon.RocketMQ.Remoting.Consumer.Push.Pull.Offset;

internal sealed class PullOffsetResetHandler : IDisposable
{
    private readonly RemotingPushConsumerOptions _options;
    private readonly RemotingClientOptions _clientOptions;
    private readonly RemotingConsumerGroupClient _groupClient;
    private readonly PullOffsetManager _pullOffsetManager;
    private readonly PushSubscriptionState _subscriptions;
    private readonly SemaphoreSlim _lifecycle;
    private readonly Func<RemotingPushConsumerRun?> _getRun;
    private readonly ILogger _logger;
    private readonly IDisposable? _registration;

    public PullOffsetResetHandler(
        RemotingPushConsumerOptions options,
        RemotingClientOptions clientOptions,
        RemotingConsumerGroupClient groupClient,
        PullOffsetManager pullOffsetManager,
        PushSubscriptionState subscriptions,
        SemaphoreSlim lifecycle,
        Func<RemotingPushConsumerRun?> getRun,
        IRemotingClient remotingClient,
        ILogger logger)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(clientOptions);
        ArgumentNullException.ThrowIfNull(groupClient);
        ArgumentNullException.ThrowIfNull(pullOffsetManager);
        ArgumentNullException.ThrowIfNull(subscriptions);
        ArgumentNullException.ThrowIfNull(lifecycle);
        ArgumentNullException.ThrowIfNull(getRun);
        ArgumentNullException.ThrowIfNull(remotingClient);
        ArgumentNullException.ThrowIfNull(logger);

        _options = options;
        _clientOptions = clientOptions;
        _groupClient = groupClient;
        _pullOffsetManager = pullOffsetManager;
        _subscriptions = subscriptions;
        _lifecycle = lifecycle;
        _getRun = getRun;
        _logger = logger;
        if (remotingClient is IRemotingRequestHandlerRegistry requestHandlers)
        {
            _registration = requestHandlers.RegisterRequestHandler(
                RequestCode.ResetConsumerOffset,
                HandleAsync);
        }
    }

    public void Dispose() => _registration?.Dispose();

    private async ValueTask<RemotingRequestResult> HandleAsync(RemotingRequestContext context)
    {
        if (!context.Request.ExtFields.TryGetValue("group", out var groupValue) ||
            groupValue is not string group ||
            !string.Equals(group, _groupClient.ConsumerGroup, StringComparison.Ordinal) ||
            !context.Request.ExtFields.TryGetValue("topic", out var topicValue) ||
            topicValue is not string wireTopic)
        {
            return RemotingRequestResult.NotHandled;
        }

        var logicalTopic = LegacyNamespace.WithoutNamespace(_clientOptions.Namespace, wireTopic);
        if (!string.Equals(GetWireTopic(logicalTopic), wireTopic, StringComparison.Ordinal) ||
            !_subscriptions.Contains(logicalTopic))
        {
            return RemotingRequestResult.NotHandled;
        }

        var offsets = LegacyResetOffsetDecoder.Decode(context.Request.Body);
        if (offsets.Any(offset => !string.Equals(offset.Topic, wireTopic, StringComparison.Ordinal)))
        {
            throw new InvalidDataException(
                $"The reset-offset body contains a queue outside the requested topic '{wireTopic}'.");
        }

        await _lifecycle.WaitAsync(context.CancellationToken).ConfigureAwait(false);
        try
        {
            if (!_subscriptions.Contains(logicalTopic))
            {
                return RemotingRequestResult.NotHandled;
            }

            if (offsets.Count == 0)
            {
                return RemotingRequestResult.NoResponse;
            }

            if (_getRun() is { } run)
            {
                await ResetConsumerOffsetsAsync(
                    run,
                    logicalTopic,
                    offsets,
                    context.CancellationToken).ConfigureAwait(false);
            }
            else
            {
                _pullOffsetManager.SetResetBoundaries(logicalTopic, offsets);
            }
        }
        finally
        {
            _lifecycle.Release();
        }

        return RemotingRequestResult.NoResponse;
    }

    private async Task ResetConsumerOffsetsAsync(
        RemotingPushConsumerRun run,
        string logicalTopic,
        IReadOnlyList<LegacyResetOffset> offsets,
        CancellationToken cancellationToken)
    {
        var desiredOffsets = offsets.ToDictionary(
            static offset => (offset.BrokerName, offset.QueueId),
            static offset => offset.Offset);
        await run.CoordinationGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        var removed = new List<ResetReceiver>();
        try
        {
            foreach (var entry in run.Receivers.SnapshotEntries())
            {
                if (entry.Value is not PullAssignmentReceiver ||
                    !string.Equals(entry.Key.Topic, logicalTopic, StringComparison.Ordinal) ||
                    !desiredOffsets.TryGetValue((entry.Key.BrokerName, entry.Key.QueueId), out var offset) ||
                    !run.Receivers.TryRemove(entry.Key, out var receiver) ||
                    receiver is not PullAssignmentReceiver pullReceiver)
                {
                    continue;
                }

                run.Receivers.Drop(pullReceiver, run.PullDeliveryCoordinator);
                removed.Add(new ResetReceiver(pullReceiver, offset));
            }

            foreach (var reset in removed)
            {
                try
                {
                    await reset.Receiver.Completion.ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                }
                catch (Exception exception)
                {
                    _logger.LogWarning(
                        exception,
                        "Legacy receiver failed while resetting {Topic}/{BrokerName}/{QueueId}",
                        reset.Receiver.Queue.Topic,
                        reset.Receiver.Queue.BrokerName,
                        reset.Receiver.Queue.QueueId);
                }
            }

            _pullOffsetManager.SetResetBoundaries(logicalTopic, offsets);
            if (_options.ConsumerMode == ConsumerMode.Broadcasting)
            {
                foreach (var offset in offsets)
                {
                    await _pullOffsetManager.PersistBroadcastResetAsync(
                        logicalTopic,
                        offset,
                        cancellationToken).ConfigureAwait(false);
                }
            }
            else
            {
                foreach (var reset in removed)
                {
                    await _pullOffsetManager.PersistAsync(
                        reset.Receiver.Queue,
                        reset.Offset,
                        cancellationToken).ConfigureAwait(false);
                }
            }
        }
        finally
        {
            foreach (var reset in removed)
            {
                reset.Receiver.Dispose();
            }

            if (offsets.Count > 0)
            {
                run.GroupSession.Wakeup();
            }

            run.CoordinationGate.Release();
        }
    }

    private string GetWireTopic(string topic) => LegacyNamespace.Wrap(_clientOptions.Namespace, topic);

    private sealed record ResetReceiver(PullAssignmentReceiver Receiver, long Offset);
}
