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
using System.Runtime.ExceptionServices;
using EventHorizon.RocketMQ.Remoting.Protocol;
using EventHorizon.RocketMQ.Remoting.Protocol.Route;
using Microsoft.Extensions.Options;

namespace EventHorizon.RocketMQ.Remoting.Consumer.Route;

internal sealed class RemotingConsumerRouteResolver
{
    private const int ReadPermission = 1 << 2;

    private readonly RemotingClientOptions _clientOptions;
    private readonly ITopicRouteService _routes;
    private readonly ConcurrentDictionary<string, RemotingConsumerRouteSnapshot> _lastTopicRoutes =
        new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<string, IReadOnlyDictionary<long, string>> _lastBrokerRoutes =
        new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<RemotingConsumerQueue, long> _suggestedBrokerIds = new();

    public RemotingConsumerRouteResolver(
        IOptions<RemotingClientOptions> clientOptions,
        ITopicRouteService routes)
    {
        ArgumentNullException.ThrowIfNull(clientOptions);
        ArgumentNullException.ThrowIfNull(routes);

        _clientOptions = clientOptions.Value;
        _routes = routes;
    }

    public async Task<RemotingConsumerRouteSnapshot> GetRouteAsync(
        string topic,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(topic);
        var logicalTopic = LegacyNamespace.WithoutNamespace(_clientOptions.Namespace, topic);
        try
        {
            return await RefreshAsync(logicalTopic, cancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception) when (_lastTopicRoutes.TryGetValue(logicalTopic, out _))
        {
            return _lastTopicRoutes[logicalTopic];
        }
    }

    public async Task<RemotingBrokerEndpoint> ResolveBrokerAsync(
        RemotingConsumerQueue queue,
        bool useSuggestedBroker,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(queue);
        return await ResolveBrokerAsync(
            queue.Topic,
            queue.BrokerName,
            queue,
            useSuggestedBroker,
            cancellationToken).ConfigureAwait(false);
    }

    public Task<RemotingBrokerEndpoint> ResolveBrokerAsync(
        string topic,
        string brokerName,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(topic);
        ArgumentException.ThrowIfNullOrWhiteSpace(brokerName);
        return ResolveBrokerAsync(topic, brokerName, queue: null, useSuggestedBroker: false, cancellationToken);
    }

    private async Task<RemotingBrokerEndpoint> ResolveBrokerAsync(
        string topic,
        string brokerName,
        RemotingConsumerQueue? queue,
        bool useSuggestedBroker,
        CancellationToken cancellationToken)
    {
        RemotingConsumerRouteSnapshot? currentRoute = null;
        Exception? refreshException = null;
        try
        {
            currentRoute = await RefreshAsync(topic, cancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception exception)
        {
            refreshException = exception;
        }

        IReadOnlyDictionary<long, string>? addresses = null;
        if (currentRoute is not null)
        {
            currentRoute.TryGetBrokerAddresses(brokerName, out addresses!);
        }

        if (addresses is null)
        {
            _lastBrokerRoutes.TryGetValue(brokerName, out addresses);
        }

        if (addresses is null)
        {
            if (refreshException is not null)
            {
                ExceptionDispatchInfo.Capture(refreshException).Throw();
            }

            throw new InvalidOperationException(
                $"No current or previously resolved route for topic '{topic}' contains Broker '{brokerName}'.");
        }

        var address = SelectAddress(addresses, queue, useSuggestedBroker);
        return new RemotingBrokerEndpoint(brokerName, address);
    }

    public void RecordSuggestedBrokerId(RemotingConsumerQueue queue, long brokerId)
    {
        ArgumentNullException.ThrowIfNull(queue);
        _suggestedBrokerIds[queue] = brokerId;
    }

    private async Task<RemotingConsumerRouteSnapshot> RefreshAsync(
        string logicalTopic,
        CancellationToken cancellationToken)
    {
        var route = await _routes.GetAsync(
            LegacyNamespace.Wrap(_clientOptions.Namespace, logicalTopic),
            cancellationToken: cancellationToken).ConfigureAwait(false);
        var brokers = route.BrokerDatas
            .Where(static broker => broker.BrokerAddrs.Count > 0)
            .ToDictionary(
                static broker => broker.BrokerName,
                static broker => (IReadOnlyDictionary<long, string>)new Dictionary<long, string>(broker.BrokerAddrs),
                StringComparer.Ordinal);
        var queues = route.QueueDatas
            .Where(static queue => (queue.Perm & ReadPermission) == ReadPermission)
            .Where(queue => brokers.ContainsKey(queue.BrokerName))
            .OrderBy(static queue => queue.BrokerName, StringComparer.Ordinal)
            .SelectMany(
                static queue => Enumerable.Range(0, queue.ReadQueueNums),
                (queue, queueId) => new RemotingConsumerQueue(logicalTopic, queue.BrokerName, queueId))
            .ToArray();
        var snapshot = new RemotingConsumerRouteSnapshot(logicalTopic, queues, brokers);
        _lastTopicRoutes[logicalTopic] = snapshot;
        foreach (var broker in brokers)
        {
            _lastBrokerRoutes[broker.Key] = broker.Value;
        }

        return snapshot;
    }

    private string SelectAddress(
        IReadOnlyDictionary<long, string> addresses,
        RemotingConsumerQueue? queue,
        bool useSuggestedBroker)
    {
        if (useSuggestedBroker &&
            queue is not null &&
            _suggestedBrokerIds.TryGetValue(queue, out var brokerId) &&
            addresses.TryGetValue(brokerId, out var suggested))
        {
            return suggested;
        }

        return addresses.TryGetValue(0, out var primary)
            ? primary
            : addresses.OrderBy(static address => address.Key).First().Value;
    }
}
