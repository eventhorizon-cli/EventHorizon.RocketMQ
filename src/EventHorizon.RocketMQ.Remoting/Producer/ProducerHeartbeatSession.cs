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
using EventHorizon.RocketMQ.Remoting.Protocol.Route;
using Microsoft.Extensions.Logging;

namespace EventHorizon.RocketMQ.Remoting.Producer;

internal sealed class ProducerHeartbeatSession
{
    private readonly IRemotingClient _client;
    private readonly RemotingClientOptions _options;
    private readonly TimeProvider _timeProvider;
    private readonly ILogger _logger;
    private readonly string _clientId;
    private readonly string _producerGroup;
    private readonly byte[] _heartbeatBody;
    private readonly object _stateGate = new();
    private readonly Dictionary<string, BrokerEndpoint> _brokers = new(StringComparer.Ordinal);
    private readonly Dictionary<string, BrokerEndpoint> _knownEndpoints = new(StringComparer.Ordinal);
    private readonly SemaphoreSlim _heartbeatGate = new(1, 1);
    private readonly CancellationTokenSource _shutdown = new();
    private readonly CancellationToken _shutdownToken;
    private readonly Task _heartbeatLoop;
    private bool _stopping;

    public ProducerHeartbeatSession(
        IRemotingClient client,
        RemotingClientOptions options,
        TimeProvider timeProvider,
        ILogger logger,
        string clientId,
        string producerGroup)
    {
        _client = client;
        _options = options;
        _timeProvider = timeProvider;
        _logger = logger;
        _clientId = clientId;
        _producerGroup = producerGroup;
        _heartbeatBody = ProducerHeartbeatCodec.Encode(clientId, producerGroup);
        _shutdownToken = _shutdown.Token;
        _heartbeatLoop = RunAsync();
    }

    public void ObserveRoute(TopicRouteData route)
    {
        lock (_stateGate)
        {
            if (_stopping)
            {
                return;
            }

            // Java rocketmq-all-5.5.1 and Go v2.1.2 heartbeat known Masters for Producer-only membership.
            foreach (var broker in route.BrokerDatas)
            {
                if (broker.BrokerAddrs.TryGetValue(0, out var address) && !string.IsNullOrWhiteSpace(address))
                {
                    RememberBroker(new BrokerEndpoint(broker.BrokerName, address));
                }
            }
        }
    }

    public async Task EnsureRegisteredAsync(string address, string brokerName, CancellationToken cancellationToken)
    {
        _shutdownToken.ThrowIfCancellationRequested();
        var broker = new BrokerEndpoint(brokerName, address);
        lock (_stateGate)
        {
            if (_stopping)
            {
                throw new OperationCanceledException("The producer heartbeat session is stopping.", _shutdownToken);
            }

            RememberBroker(broker);
        }

        using var linkedCancellation = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, _shutdownToken);
        await SendHeartbeatAsync(broker, linkedCancellation.Token).ConfigureAwait(false);
    }

    public async Task StopAsync()
    {
        lock (_stateGate)
        {
            _stopping = true;
        }

        _shutdown.Cancel();
        await _heartbeatLoop.ConfigureAwait(false);
        // An immediate request/reply heartbeat may still be completing outside the periodic loop.
        await _heartbeatGate.WaitAsync().ConfigureAwait(false);
        _heartbeatGate.Release();

        BrokerEndpoint[] endpoints;
        lock (_stateGate)
        {
            endpoints = _knownEndpoints.Values.ToArray();
            _brokers.Clear();
            _knownEndpoints.Clear();
        }

        foreach (var broker in endpoints)
        {
            await UnregisterAsync(broker).ConfigureAwait(false);
        }

        _shutdown.Dispose();
    }

    private void RememberBroker(BrokerEndpoint broker)
    {
        _brokers[broker.BrokerName] = broker;
        _knownEndpoints[broker.Key] = broker;
    }

    private async Task RunAsync()
    {
        try
        {
            while (true)
            {
                await Task.Delay(_options.HeartbeatBrokerInterval, _timeProvider, _shutdownToken).ConfigureAwait(false);
                BrokerEndpoint[] brokers;
                lock (_stateGate)
                {
                    brokers = _brokers.Values.ToArray();
                }

                foreach (var broker in brokers)
                {
                    try
                    {
                        await SendHeartbeatAsync(broker, _shutdownToken).ConfigureAwait(false);
                    }
                    catch (OperationCanceledException) when (_shutdownToken.IsCancellationRequested)
                    {
                        return;
                    }
                    catch (Exception exception)
                    {
                        _logger.LogWarning(
                            exception,
                            "Unable to renew producer heartbeat with broker {BrokerName} at {EndPoint}",
                            broker.BrokerName,
                            broker.Address);
                    }
                }
            }
        }
        catch (OperationCanceledException) when (_shutdownToken.IsCancellationRequested)
        {
        }
    }

    private async Task SendHeartbeatAsync(BrokerEndpoint broker, CancellationToken cancellationToken)
    {
        await _heartbeatGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            cancellationToken.ThrowIfCancellationRequested();
            var response = await _client.InvokeAsync(
                EndpointParser.Parse(broker.Address),
                new RemotingCommand(RequestCode.HeartBeat) { Body = _heartbeatBody },
                _options.RequestTimeout,
                cancellationToken).ConfigureAwait(false);
            if (response.Code != ResponseCodes.ResSuccess)
            {
                throw new RemotingCommandException(response.Code, response.Remark ?? "Broker rejected the producer heartbeat.");
            }
        }
        finally
        {
            _heartbeatGate.Release();
        }
    }

    private async Task UnregisterAsync(BrokerEndpoint broker)
    {
        try
        {
            var response = await _client.InvokeAsync(
                EndpointParser.Parse(broker.Address),
                new RemotingCommand(RequestCode.UnregisterClient, new UnregisterClientRequestHeader
                {
                    ClientID = _clientId,
                    ProducerGroup = _producerGroup,
                    Bname = broker.BrokerName
                }),
                _options.RequestTimeout,
                CancellationToken.None).ConfigureAwait(false);
            if (response.Code != ResponseCodes.ResSuccess)
            {
                _logger.LogDebug(
                    "Broker {BrokerName} rejected producer unregister with code {ResponseCode}: {Remark}",
                    broker.BrokerName,
                    response.Code,
                    response.Remark);
            }
        }
        catch (Exception exception)
        {
            _logger.LogDebug(exception, "Unable to unregister producer from broker {BrokerName} at {EndPoint}",
                broker.BrokerName, broker.Address);
        }
    }

    private sealed record BrokerEndpoint(string BrokerName, string Address)
    {
        public string Key => $"{BrokerName}|{Address}";
    }
}
