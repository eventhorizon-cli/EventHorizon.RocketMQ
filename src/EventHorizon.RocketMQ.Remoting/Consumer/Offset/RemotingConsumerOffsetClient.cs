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
using EventHorizon.RocketMQ.Remoting.Consumer.Route;
using EventHorizon.RocketMQ.Remoting.Exceptions;
using EventHorizon.RocketMQ.Remoting.Instrumentation;
using EventHorizon.RocketMQ.Remoting.Protocol;

namespace EventHorizon.RocketMQ.Remoting.Consumer.Offset;

internal sealed class RemotingConsumerOffsetClient : IRemotingConsumerOffsetClient
{
    private readonly RemotingConsumerSettings _settings;
    private readonly RemotingClientOptions _clientOptions;
    private readonly RemotingConsumerRouteResolver _routes;
    private readonly IRemotingClient _remotingClient;
    private readonly IRemotingRocketMQTelemetry _telemetry;

    public RemotingConsumerOffsetClient(
        RemotingConsumerSettings settings,
        RemotingClientOptions clientOptions,
        RemotingConsumerRouteResolver routes,
        IRemotingClient remotingClient,
        IRemotingRocketMQTelemetry telemetry)
    {
        ArgumentNullException.ThrowIfNull(settings);
        ArgumentNullException.ThrowIfNull(clientOptions);
        ArgumentNullException.ThrowIfNull(routes);
        ArgumentNullException.ThrowIfNull(remotingClient);
        ArgumentNullException.ThrowIfNull(telemetry);

        _settings = settings;
        _clientOptions = clientOptions;
        _routes = routes;
        _remotingClient = remotingClient;
        _telemetry = telemetry;
    }

    public async Task<long> GetOffsetAsync(
        RemotingConsumerQueue queue,
        CancellationToken cancellationToken = default)
    {
        var response = await InvokeAsync(
            RequestCode.QueryConsumerOffset,
            queue,
            new ConsumerOffsetRequestHeader
            {
                ConsumerGroup = GetConsumerGroup(),
                Topic = GetWireTopic(queue.Topic),
                QueueId = queue.QueueId,
                SetZeroIfNotFound = false,
                Bname = queue.BrokerName
            },
            cancellationToken).ConfigureAwait(false);
        if (response.Code == ResponseCodes.ResQueryNotFound)
        {
            return -1;
        }

        EnsureSuccess(response, "query consumer offset");
        return GetRequiredInt64(response.ExtFields, "offset");
    }

    public async Task UpdateOffsetAsync(
        RemotingConsumerQueue queue,
        long offset,
        CancellationToken cancellationToken = default)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(offset);
        using var telemetry = _telemetry.StartSettle(
            "commit",
            queue.Topic,
            _settings.GroupName,
            null,
            queue.QueueId,
            null);
        try
        {
            var response = await InvokeAsync(
                RequestCode.UpdateConsumerOffset,
                queue,
                new ConsumerOffsetRequestHeader
                {
                    ConsumerGroup = GetConsumerGroup(),
                    Topic = GetWireTopic(queue.Topic),
                    QueueId = queue.QueueId,
                    CommitOffset = offset,
                    Bname = queue.BrokerName
                },
                cancellationToken).ConfigureAwait(false);
            EnsureSuccess(response, "update consumer offset");
            telemetry.Complete();
        }
        catch (Exception exception)
        {
            telemetry.Complete(exception);
            throw;
        }
    }

    public async Task<long> QueryOffsetAsync(
        RemotingConsumerQueue queue,
        ConsumeFromPosition position,
        DateTimeOffset? timestamp = null,
        CancellationToken cancellationToken = default)
    {
        if (position == ConsumeFromPosition.Timestamp && timestamp is null)
        {
            throw new ArgumentException("A timestamp is required for timestamp-based offset queries.", nameof(timestamp));
        }

        var requestCode = position switch
        {
            ConsumeFromPosition.End => RequestCode.GetMaxOffset,
            ConsumeFromPosition.Timestamp => RequestCode.SearchOffsetByTimestamp,
            _ => RequestCode.GetMinOffset
        };
        var response = await InvokeAsync(
            requestCode,
            queue,
            new QueryQueueOffsetRequestHeader
            {
                Topic = GetWireTopic(queue.Topic),
                QueueId = queue.QueueId,
                Timestamp = timestamp?.ToUnixTimeMilliseconds() ?? 0,
                Bname = queue.BrokerName
            },
            cancellationToken).ConfigureAwait(false);
        EnsureSuccess(response, "query queue offset");
        return GetRequiredInt64(response.ExtFields, "offset");
    }

    private async Task<RemotingCommand> InvokeAsync(
        int requestCode,
        RemotingConsumerQueue queue,
        CommandCustomHeader header,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(queue);
        var broker = await _routes.ResolveBrokerAsync(
            queue,
            useSuggestedBroker: false,
            cancellationToken).ConfigureAwait(false);
        return await _remotingClient.InvokeAsync(
            EndpointParser.Parse(broker.Address),
            new RemotingCommand(requestCode, header),
            _clientOptions.RequestTimeout,
            cancellationToken).ConfigureAwait(false);
    }

    private static void EnsureSuccess(RemotingCommand response, string operation)
    {
        if (response.Code != ResponseCodes.ResSuccess)
        {
            throw new RemotingCommandException(
                response.Code,
                response.Remark ?? $"Broker rejected the {operation} request.");
        }
    }

    private static long GetRequiredInt64(IReadOnlyDictionary<string, object> fields, string name)
    {
        var value = fields.TryGetValue(name, out var raw) &&
                    long.TryParse(
                        Convert.ToString(raw, CultureInfo.InvariantCulture),
                        NumberStyles.Integer,
                        CultureInfo.InvariantCulture,
                        out var parsed)
            ? parsed
            : long.MinValue;
        return value != long.MinValue
            ? value
            : throw new InvalidDataException($"Broker response did not contain a valid '{name}' field.");
    }

    private string GetConsumerGroup() => LegacyNamespace.Wrap(_clientOptions.Namespace, _settings.GroupName);

    private string GetWireTopic(string topic) => LegacyNamespace.Wrap(_clientOptions.Namespace, topic);
}
