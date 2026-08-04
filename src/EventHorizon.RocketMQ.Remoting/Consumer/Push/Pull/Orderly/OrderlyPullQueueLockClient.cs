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

using System.Text;
using EventHorizon.RocketMQ.Remoting.Consumer.Coordination;
using EventHorizon.RocketMQ.Remoting.Exceptions;
using EventHorizon.RocketMQ.Remoting.Protocol;
using Microsoft.Extensions.Logging;

namespace EventHorizon.RocketMQ.Remoting.Consumer.Push.Pull.Orderly;

/// <summary>
/// Acquires, renews, and releases Broker-owned queue leases for orderly Push consumption.
/// A successful batch lock makes one consumer-group member the active processor for each returned queue;
/// repeating the lock request renews that ownership until reassignment or shutdown releases it.
/// </summary>
internal sealed class OrderlyPullQueueLockClient
{
    private readonly IRemotingClient _remotingClient;
    private readonly RemotingClientOptions _options;
    private readonly RemotingConsumerGroupClient _groupClient;
    private readonly ILogger _logger;

    public OrderlyPullQueueLockClient(
        IRemotingClient remotingClient,
        RemotingClientOptions options,
        RemotingConsumerGroupClient groupClient,
        ILogger logger)
    {
        ArgumentNullException.ThrowIfNull(remotingClient);
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(groupClient);
        ArgumentNullException.ThrowIfNull(logger);

        _remotingClient = remotingClient;
        _options = options;
        _groupClient = groupClient;
        _logger = logger;
    }

    public async Task<OrderlyPullQueueLockResult> LockAsync(
        IEnumerable<OrderlyPullQueueLockTarget> targets,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(targets);

        var candidates = targets.DistinctBy(static target => target.Queue).ToArray();
        var result = new OrderlyPullQueueLockResult();
        foreach (var broker in candidates.GroupBy(static target => new QueueBrokerKey(
                     target.Broker.BrokerName,
                     target.Broker.Address)))
        {
            var queuesByReference = broker.ToDictionary(
                target => CreateQueueLockReference(target.Queue),
                static target => target.Queue);
            var request = new RemotingCommand(RequestCode.LockBatchMq)
            {
                Body = Encoding.UTF8.GetBytes(new QueueLockRequestBody
                {
                    ConsumerGroup = _groupClient.ConsumerGroup,
                    ClientId = _groupClient.ClientId,
                    MqSet = queuesByReference.Keys.ToArray()
                }.ToJson())
            };
            try
            {
                var response = await _remotingClient.InvokeAsync(
                    EndpointParser.Parse(broker.Key.Address),
                    request,
                    _options.RequestTimeout,
                    cancellationToken).ConfigureAwait(false);
                if (response.Code != ResponseCodes.ResSuccess)
                {
                    _logger.LogWarning(
                        "Broker {BrokerName} rejected the orderly queue-lock request: {ResponseCode} {Remark}",
                        broker.Key.Name,
                        response.Code,
                        response.Remark);
                    continue;
                }

                var responseBody = response.Body is { Length: > 0 }
                    ? Encoding.UTF8.GetString(response.Body).FromJson<QueueLockResponseBody>()
                    : null;
                if (responseBody is null)
                {
                    _logger.LogWarning(
                        "Broker {BrokerName} returned an empty orderly queue-lock response",
                        broker.Key.Name);
                    continue;
                }

                result.RefreshedQueues.UnionWith(broker.Select(static target => target.Queue));
                foreach (var queueReference in responseBody.LockOKMQSet ?? [])
                {
                    if (queuesByReference.TryGetValue(queueReference, out var queue))
                    {
                        result.LockedQueues.Add(queue);
                    }
                }
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception exception)
            {
                _logger.LogWarning(
                    exception,
                    "Unable to acquire orderly queue locks from Broker {BrokerName} at {BrokerAddress}",
                    broker.Key.Name,
                    broker.Key.Address);
            }
        }

        return result;
    }

    public async Task UnlockAsync(
        IEnumerable<OrderlyPullQueueLockTarget> targets,
        bool oneway,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(targets);

        var candidates = targets.DistinctBy(static target => target.Queue).ToArray();
        foreach (var broker in candidates.GroupBy(static target => new QueueBrokerKey(
                     target.Broker.BrokerName,
                     target.Broker.Address)))
        {
            var request = new RemotingCommand(RequestCode.UnlockBatchMq)
            {
                Body = Encoding.UTF8.GetBytes(new QueueLockRequestBody
                {
                    ConsumerGroup = _groupClient.ConsumerGroup,
                    ClientId = _groupClient.ClientId,
                    MqSet = broker.Select(target => CreateQueueLockReference(target.Queue)).ToArray()
                }.ToJson())
            };
            try
            {
                if (oneway)
                {
                    await _remotingClient.InvokeOnewayAsync(
                        EndpointParser.Parse(broker.Key.Address),
                        request,
                        _options.RequestTimeout,
                        cancellationToken).ConfigureAwait(false);
                }
                else
                {
                    var response = await _remotingClient.InvokeAsync(
                        EndpointParser.Parse(broker.Key.Address),
                        request,
                        _options.RequestTimeout,
                        cancellationToken).ConfigureAwait(false);
                    EnsureSuccess(response, "unlock orderly message queues");
                }
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception exception)
            {
                _logger.LogWarning(
                    exception,
                    "Unable to release orderly queue locks at Broker {BrokerName} at {BrokerAddress}",
                    broker.Key.Name,
                    broker.Key.Address);
            }
        }
    }

    private QueueLockReference CreateQueueLockReference(RemotingConsumerQueue queue) => new(
        LegacyNamespace.Wrap(_options.Namespace, queue.Topic),
        queue.BrokerName,
        queue.QueueId);

    private static void EnsureSuccess(RemotingCommand response, string operation)
    {
        if (response.Code != ResponseCodes.ResSuccess)
        {
            throw new RemotingCommandException(response.Code, response.Remark ?? $"Unable to {operation}.");
        }
    }

    private sealed record QueueBrokerKey(string Name, string Address);

    private sealed record QueueLockReference(string Topic, string BrokerName, int QueueId);

    private sealed class QueueLockRequestBody
    {
        public string ConsumerGroup { get; init; } = string.Empty;

        public string ClientId { get; init; } = string.Empty;

        public bool OnlyThisBroker { get; init; }

        public QueueLockReference[] MqSet { get; init; } = [];
    }

    private sealed class QueueLockResponseBody
    {
        public QueueLockReference[]? LockOKMQSet { get; init; }
    }

    internal sealed class OrderlyPullQueueLockResult
    {
        public HashSet<RemotingConsumerQueue> LockedQueues { get; } = [];

        public HashSet<RemotingConsumerQueue> RefreshedQueues { get; } = [];
    }
}
