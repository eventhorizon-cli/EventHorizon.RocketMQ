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

using System.Globalization;
using System.Net;
using EventHorizon.RocketMQ.Remoting.Consumer;
using EventHorizon.RocketMQ.Remoting.Exceptions;
using EventHorizon.RocketMQ.Remoting.Producer;
using EventHorizon.RocketMQ.Remoting.Protocol;
using EventHorizon.RocketMQ.Remoting.Protocol.Route;
using Microsoft.Extensions.Options;

namespace EventHorizon.RocketMQ.Remoting.Admin;

internal sealed class RemotingAdmin : IRemotingAdmin
{
    private const int WritePermission = 1 << 1;

    private readonly RemotingClientOptions _clientOptions;
    private readonly ITopicRouteService _routeService;
    private readonly IRemotingClient _remotingClient;

    public RemotingAdmin(
        IOptions<RemotingClientOptions> clientOptions,
        ITopicRouteService routeService,
        IRemotingClient remotingClient)
    {
        _clientOptions = clientOptions.Value;
        _routeService = routeService;
        _remotingClient = remotingClient;
    }

    public async Task<IReadOnlyList<RemotingMessageQueue>> GetMessageQueuesAsync(
        string topic,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(topic);
        var logicalTopic = LegacyNamespace.WithoutNamespace(_clientOptions.Namespace, topic);
        var route = await _routeService.GetAsync(
            LegacyNamespace.Wrap(_clientOptions.Namespace, logicalTopic),
            cancellationToken: cancellationToken).ConfigureAwait(false);
        var queues = route.QueueDatas
            .Where(static queue => (queue.Perm & WritePermission) == WritePermission)
            .SelectMany(
                static queue => Enumerable.Range(0, queue.WriteQueueNums),
                (queue, queueId) => new { queue.BrokerName, QueueId = queueId })
            .Where(queue => HasBrokerAddress(route, queue.BrokerName))
            .OrderBy(static queue => queue.BrokerName, StringComparer.Ordinal)
            .ThenBy(static queue => queue.QueueId)
            .Select(queue => new RemotingMessageQueue(logicalTopic, queue.BrokerName, queue.QueueId))
            .ToArray();
        return queues.Length > 0
            ? queues
            : throw new RocketMQClientException($"No writable message queue is available for topic '{logicalTopic}'.");
    }

    public async Task<RemotingMessageView> ViewMessageAsync(
        string topic,
        string offsetMessageId,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(topic);
        ArgumentException.ThrowIfNullOrWhiteSpace(offsetMessageId);
        var messageId = LegacyOffsetMessageId.Parse(offsetMessageId);
        var response = await _remotingClient.InvokeAsync(
            messageId.BrokerEndpoint,
            new RemotingCommand(RequestCode.ViewMessageById, new ViewMessageRequestHeader
            {
                Topic = GetWireTopic(topic),
                Offset = messageId.CommitLogOffset
            }),
            _clientOptions.RequestTimeout,
            cancellationToken).ConfigureAwait(false);
        EnsureSuccess(response, "view message");
        return response.Body is { Length: > 0 }
            ? LegacyMessageDecoder.DecodeSingle(response.Body, brokerName: null, @namespace: _clientOptions.Namespace)
            : throw new InvalidDataException("Broker response did not contain a message body.");
    }

    public Task<long> GetMinOffsetAsync(
        RemotingMessageQueue messageQueue,
        CancellationToken cancellationToken = default) =>
        GetQueueOffsetAsync(
            messageQueue,
            RequestCode.GetMinOffset,
            wireTopic => new GetMinOffsetRequestHeader
            {
                Topic = wireTopic,
                QueueId = messageQueue.QueueId,
                Bname = messageQueue.BrokerName
            },
            "get minimum queue offset",
            cancellationToken);

    public async Task<DateTimeOffset?> GetEarliestMessageStoreTimeAsync(
        RemotingMessageQueue messageQueue,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(messageQueue);
        var wireTopic = GetWireTopic(messageQueue.Topic);
        var response = await InvokeAsync(
            messageQueue,
            new RemotingCommand(RequestCode.GetEarliestMessageStoreTime, new GetEarliestMessageStoreTimeRequestHeader
            {
                Topic = wireTopic,
                QueueId = messageQueue.QueueId,
                Bname = messageQueue.BrokerName
            }),
            cancellationToken).ConfigureAwait(false);
        EnsureSuccess(response, "get earliest message store time");
        return GetOptionalTimestamp(response);
    }

    public Task<long> GetMaxOffsetAsync(
        RemotingMessageQueue messageQueue,
        bool committed = true,
        CancellationToken cancellationToken = default) =>
        GetQueueOffsetAsync(
            messageQueue,
            RequestCode.GetMaxOffset,
            wireTopic => new GetMaxOffsetRequestHeader
            {
                Topic = wireTopic,
                QueueId = messageQueue.QueueId,
                Committed = committed,
                Bname = messageQueue.BrokerName
            },
            "get maximum queue offset",
            cancellationToken);

    public Task<long> SearchOffsetAsync(
        RemotingMessageQueue messageQueue,
        DateTimeOffset timestamp,
        RemotingOffsetBoundary boundary = RemotingOffsetBoundary.Lower,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(messageQueue);
        var boundaryType = GetBoundaryType(boundary);
        return GetQueueOffsetAsync(
            messageQueue,
            RequestCode.SearchOffsetByTimestamp,
            wireTopic => new SearchOffsetRequestHeader
            {
                Topic = wireTopic,
                QueueId = messageQueue.QueueId,
                Timestamp = timestamp.ToUnixTimeMilliseconds(),
                BoundaryType = boundaryType,
                Bname = messageQueue.BrokerName
            },
            "search queue offset",
            cancellationToken);
    }

    public async Task<long?> GetConsumerOffsetAsync(
        string consumerGroup,
        RemotingMessageQueue messageQueue,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(consumerGroup);
        ArgumentNullException.ThrowIfNull(messageQueue);
        var wireTopic = GetWireTopic(messageQueue.Topic);
        var response = await InvokeAsync(
            messageQueue,
            new RemotingCommand(RequestCode.QueryConsumerOffset, new QueryConsumerOffsetRequestHeader
            {
                ConsumerGroup = LegacyNamespace.Wrap(_clientOptions.Namespace, consumerGroup),
                Topic = wireTopic,
                QueueId = messageQueue.QueueId,
                SetZeroIfNotFound = false,
                Bname = messageQueue.BrokerName
            }),
            cancellationToken).ConfigureAwait(false);
        if (response.Code == ResponseCodes.ResQueryNotFound)
        {
            return null;
        }

        EnsureSuccess(response, "query consumer offset");
        return GetRequiredOffset(response);
    }

    private async Task<long> GetQueueOffsetAsync(
        RemotingMessageQueue messageQueue,
        int requestCode,
        Func<string, CommandCustomHeader> headerFactory,
        string operation,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(messageQueue);
        var wireTopic = GetWireTopic(messageQueue.Topic);
        var response = await InvokeAsync(
            messageQueue,
            new RemotingCommand(requestCode, headerFactory(wireTopic)),
            cancellationToken).ConfigureAwait(false);
        EnsureSuccess(response, operation);
        return GetRequiredOffset(response);
    }

    private async Task<RemotingCommand> InvokeAsync(
        RemotingMessageQueue messageQueue,
        RemotingCommand request,
        CancellationToken cancellationToken)
    {
        var endpoint = await GetBrokerEndpointAsync(messageQueue, cancellationToken).ConfigureAwait(false);
        return await _remotingClient.InvokeAsync(
            endpoint,
            request,
            _clientOptions.RequestTimeout,
            cancellationToken).ConfigureAwait(false);
    }

    private async Task<EndPoint> GetBrokerEndpointAsync(
        RemotingMessageQueue messageQueue,
        CancellationToken cancellationToken)
    {
        var wireTopic = GetWireTopic(messageQueue.Topic);
        var route = await _routeService.GetAsync(
            wireTopic,
            cancellationToken: cancellationToken).ConfigureAwait(false);
        var brokerAddress = GetBrokerAddress(route, messageQueue.BrokerName);
        if (brokerAddress is null)
        {
            route = await _routeService.GetAsync(
                wireTopic,
                forceRefresh: true,
                cancellationToken: cancellationToken).ConfigureAwait(false);
            brokerAddress = GetBrokerAddress(route, messageQueue.BrokerName);
        }

        return brokerAddress is not null
            ? EndpointParser.Parse(brokerAddress)
            : throw new RocketMQClientException(
                $"No broker endpoint is available for queue '{messageQueue.BrokerName}:{messageQueue.QueueId}' " +
                $"of topic '{messageQueue.Topic}'.");
    }

    private string GetWireTopic(string topic) => LegacyNamespace.Wrap(_clientOptions.Namespace, topic);

    private static bool HasBrokerAddress(TopicRouteData route, string brokerName) =>
        GetBrokerAddress(route, brokerName) is not null;

    private static string? GetBrokerAddress(TopicRouteData route, string brokerName)
    {
        var broker = route.BrokerDatas.FirstOrDefault(candidate =>
            string.Equals(candidate.BrokerName, brokerName, StringComparison.Ordinal));
        if (broker is null || broker.BrokerAddrs.Count == 0)
        {
            return null;
        }

        if (broker.BrokerAddrs.TryGetValue(0, out var master) && !string.IsNullOrWhiteSpace(master))
        {
            return master;
        }

        return broker.BrokerAddrs
            .OrderBy(static address => address.Key)
            .Select(static address => address.Value)
            .FirstOrDefault(static address => !string.IsNullOrWhiteSpace(address));
    }

    private static string GetBoundaryType(RemotingOffsetBoundary boundary) => boundary switch
    {
        RemotingOffsetBoundary.Lower => "lower",
        RemotingOffsetBoundary.Upper => "upper",
        _ => throw new ArgumentOutOfRangeException(nameof(boundary), boundary, "Unknown offset boundary.")
    };

    private static void EnsureSuccess(RemotingCommand response, string operation)
    {
        if (response.Code != ResponseCodes.ResSuccess)
        {
            throw new RemotingCommandException(
                response.Code,
                response.Remark ?? $"Broker rejected the {operation} request.");
        }
    }

    private static long GetRequiredOffset(RemotingCommand response)
    {
        if (response.ExtFields.TryGetValue("offset", out var raw) &&
            long.TryParse(
                Convert.ToString(raw, CultureInfo.InvariantCulture),
                NumberStyles.Integer,
                CultureInfo.InvariantCulture,
                out var offset))
        {
            return offset;
        }

        throw new InvalidDataException("Broker response did not contain a valid 'offset' field.");
    }

    private static DateTimeOffset? GetOptionalTimestamp(RemotingCommand response)
    {
        if (!response.ExtFields.TryGetValue("timestamp", out var raw) ||
            !long.TryParse(
                Convert.ToString(raw, CultureInfo.InvariantCulture),
                NumberStyles.Integer,
                CultureInfo.InvariantCulture,
                out var timestamp))
        {
            throw new InvalidDataException("Broker response did not contain a valid 'timestamp' field.");
        }

        if (timestamp < 0)
        {
            return null;
        }

        try
        {
            return DateTimeOffset.FromUnixTimeMilliseconds(timestamp);
        }
        catch (ArgumentOutOfRangeException exception)
        {
            throw new InvalidDataException("Broker response contained an out-of-range 'timestamp' field.", exception);
        }
    }
}
