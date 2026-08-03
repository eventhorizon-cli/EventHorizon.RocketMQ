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

using EventHorizon.RocketMQ.Remoting.Exceptions;
using EventHorizon.RocketMQ.Remoting.Protocol;
using Microsoft.Extensions.Logging;

namespace EventHorizon.RocketMQ.Remoting.Consumer;

internal sealed class RemotingConsumerGroupSession
{
    private readonly IRemotingClient _remotingClient;
    private readonly RemotingClientOptions _options;
    private readonly ILogger _logger;

    public RemotingConsumerGroupSession(
        IRemotingClient remotingClient,
        RemotingClientOptions options,
        string consumerGroup,
        ILogger logger)
    {
        ArgumentNullException.ThrowIfNull(remotingClient);
        ArgumentNullException.ThrowIfNull(options);
        ArgumentException.ThrowIfNullOrWhiteSpace(consumerGroup);
        ArgumentNullException.ThrowIfNull(logger);

        _remotingClient = remotingClient;
        _options = options;
        _logger = logger;
        ClientId = options.BuildRemotingClientId();
        ConsumerGroup = LegacyNamespace.Wrap(options.Namespace, consumerGroup);
    }

    public string ClientId { get; }

    public string ConsumerGroup { get; }

    public async Task SendHeartbeatsAsync(
        IEnumerable<RemotingBrokerEndpoint> brokers,
        byte[] heartbeat,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(brokers);
        ArgumentNullException.ThrowIfNull(heartbeat);

        foreach (var broker in DistinctBrokers(brokers))
        {
            try
            {
                var response = await _remotingClient.InvokeAsync(
                    EndpointParser.Parse(broker.Address),
                    new RemotingCommand(RequestCode.HeartBeat, new HeartbeatRequestHeader()) { Body = heartbeat },
                    _options.RequestTimeout,
                    cancellationToken).ConfigureAwait(false);
                EnsureSuccess(response, "send heartbeat");
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception exception)
            {
                _logger.LogWarning(
                    exception,
                    "Unable to send legacy consumer heartbeat to Broker {BrokerName} at {BrokerAddress}",
                    broker.BrokerName,
                    broker.Address);
            }
        }
    }

    public async Task<IReadOnlyList<string>> GetConsumerIdsAsync(
        RemotingConsumerQueue queue,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(queue);

        var response = await _remotingClient.InvokeAsync(
            EndpointParser.Parse(queue.BrokerAddress),
            new RemotingCommand(RequestCode.GetConsumerListByGroup, new GetConsumerListByGroupRequestHeader
            {
                ConsumerGroup = ConsumerGroup,
                Bname = queue.BrokerName
            }),
            _options.RequestTimeout,
            cancellationToken).ConfigureAwait(false);
        EnsureSuccess(response, "query consumer list");
        return LegacyConsumerProtocol.DecodeConsumerIds(response.Body);
    }

    public async Task UnregisterAsync(IEnumerable<RemotingBrokerEndpoint> brokers)
    {
        ArgumentNullException.ThrowIfNull(brokers);

        foreach (var broker in DistinctBrokers(brokers))
        {
            try
            {
                var response = await _remotingClient.InvokeAsync(
                    EndpointParser.Parse(broker.Address),
                    new RemotingCommand(RequestCode.UnregisterClient, new UnregisterClientRequestHeader
                    {
                        ClientID = ClientId,
                        ConsumerGroup = ConsumerGroup,
                        Bname = broker.BrokerName
                    }),
                    _options.RequestTimeout,
                    CancellationToken.None).ConfigureAwait(false);
                EnsureSuccess(response, "unregister consumer");
            }
            catch (Exception exception)
            {
                _logger.LogDebug(
                    exception,
                    "Unable to unregister legacy consumer group {ConsumerGroup} from Broker {BrokerName} at {BrokerAddress}",
                    ConsumerGroup,
                    broker.BrokerName,
                    broker.Address);
            }
        }
    }

    private static IEnumerable<RemotingBrokerEndpoint> DistinctBrokers(
        IEnumerable<RemotingBrokerEndpoint> brokers) =>
        brokers.DistinctBy(static broker => broker.Address, StringComparer.Ordinal);

    private static void EnsureSuccess(RemotingCommand response, string operation)
    {
        if (response.Code != ResponseCodes.ResSuccess)
        {
            throw new RemotingCommandException(response.Code, response.Remark ?? $"Unable to {operation}.");
        }
    }
}
