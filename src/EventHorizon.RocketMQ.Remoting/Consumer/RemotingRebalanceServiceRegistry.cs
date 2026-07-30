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

using EventHorizon.RocketMQ.Remoting.Protocol;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace EventHorizon.RocketMQ.Remoting.Consumer;

internal sealed class RemotingRebalanceServiceRegistry : IAsyncDisposable
{
    private static readonly TimeSpan MaximumRebalanceInterval = TimeSpan.FromSeconds(20);
    private static readonly TimeSpan RebalanceRetryInterval = TimeSpan.FromSeconds(1);
    private readonly object _sync = new();
    private readonly RemotingClientRegistry _clients;
    private readonly ILogger<RemotingRebalanceService> _logger;
    private readonly TimeSpan _rebalanceInterval;

    // The registry attaches one Broker NotifyConsumerIdsChanged handler and service to each physical client.
    // Different groups sharing the shared client therefore share one service, while a same-group replica with an
    // isolated client gets its own. The service periodically wakes every registered participant; the participant's
    // active Push or LitePull group session then creates the group-specific heartbeat on its assigned client.
    private readonly Dictionary<IRemotingClient, RemotingRebalanceService> _services =
        new(ReferenceEqualityComparer.Instance);
    private int _disposed;

    public RemotingRebalanceServiceRegistry(
        RemotingClientRegistry clients,
        IOptions<RemotingClientOptions> options,
        ILogger<RemotingRebalanceService> logger)
    {
        _clients = clients;
        _logger = logger;
        _rebalanceInterval = GetRebalanceInterval(options.Value);
    }

    public RemotingGroupMemberClient GetGroupMember(string consumerGroup)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(consumerGroup);
        lock (_sync)
        {
            ObjectDisposedException.ThrowIf(_disposed != 0, this);
            var client = _clients.GetGroupMemberClient(consumerGroup);
            if (!_services.TryGetValue(client, out var service))
            {
                // Cache by client reference rather than group: one service owns the inbound handler for that
                // physical channel and independently tracks every group that legitimately shares it.
                service = new RemotingRebalanceService(
                    client,
                    _logger,
                    _rebalanceInterval,
                    RebalanceRetryInterval);
                _services.Add(client, service);
            }

            return new RemotingGroupMemberClient(client, service);
        }
    }

    public async ValueTask DisposeAsync()
    {
        RemotingRebalanceService[] services;
        lock (_sync)
        {
            if (Interlocked.Exchange(ref _disposed, 1) != 0)
            {
                return;
            }

            services = _services.Values.ToArray();
            _services.Clear();
        }

        foreach (var service in services)
        {
            await service.DisposeAsync().ConfigureAwait(false);
        }
    }

    private static TimeSpan GetRebalanceInterval(RemotingClientOptions options)
    {
        var configured = new[] { options.PollNameServerInterval, options.HeartbeatBrokerInterval }
            .Where(static interval => interval > TimeSpan.Zero)
            .DefaultIfEmpty(MaximumRebalanceInterval)
            .Min();
        return configured < MaximumRebalanceInterval ? configured : MaximumRebalanceInterval;
    }
}
