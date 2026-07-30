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

using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace EventHorizon.RocketMQ.Remoting.Protocol;

internal sealed class RemotingClientRegistry : IAsyncDisposable
{
    private readonly object _sync = new();
    private readonly IRemoteCommandSerializer _serializer;
    private readonly IOptions<RemotingClientOptions> _options;
    private readonly ILogger<RemotingClient> _logger;

    // One client is sufficient for producer, admin, pull, POP, and the first active member of every
    // Push or LitePull group in this AddRocketMQRemoting registration. Constructing a RemotingClient does
    // not open a socket; its endpoint connections are created lazily when a role first makes a request.
    private readonly IRemotingClient _sharedClient;

    // A physical RemotingClient has no Broker-visible group identity and therefore does not originate a generic
    // consumer heartbeat. Each active Push or LitePull group session sends its own heartbeats through the client
    // selected here, both during initial coordination and when its Rebalance participant wakes up.
    // The group name is already namespace-qualified and therefore matches the Broker-visible identity.
    // Only configured Push and LitePull group members resolve a client through this registry.
    private readonly HashSet<string> _configuredConsumerGroups = new(StringComparer.Ordinal);

    // A repeated active member of one group needs its own Broker channel and therefore its own client.
    // These clients are owned by this registration-scoped registry and are disposed with the shared client.
    private readonly List<IRemotingClient> _isolatedClients = [];
    private int _disposed;

    public RemotingClientRegistry(
        IRemoteCommandSerializer serializer,
        IOptions<RemotingClientOptions> options,
        ILogger<RemotingClient> logger)
    {
        _serializer = serializer;
        _options = options;
        _logger = logger;
        _sharedClient = CreateClient();
    }

    public IRemotingClient SharedClient => _sharedClient;

    public IRemotingClient GetGroupMemberClient(string groupName)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(groupName);
        lock (_sync)
        {
            ObjectDisposedException.ThrowIf(_disposed != 0, this);

            // Different consumer groups do not need different channels, so they all reuse the shared client.
            // A classic Broker identifies simultaneous members of one group by their connection channel. Reusing
            // the shared client for a second local member would make the Broker observe one member instead of two.
            if (_configuredConsumerGroups.Add(groupName))
            {
                return _sharedClient;
            }

            // Keep allocation bounded by the fixed DI registration graph: for N configured members in G distinct
            // groups, the registry owns exactly one shared client plus N - G isolated clients. An isolated client is
            // created only for a required same-group replica, never per role or per Broker endpoint. Consumer roles
            // are host-lifetime singletons and acquire once, so isolated clients need no release-and-reuse path.
            var client = CreateClient();
            _isolatedClients.Add(client);
            return client;
        }
    }

    public async ValueTask DisposeAsync()
    {
        IRemotingClient[] clients;
        lock (_sync)
        {
            if (Interlocked.Exchange(ref _disposed, 1) != 0)
            {
                return;
            }

            clients = [_sharedClient, .. _isolatedClients];
            _isolatedClients.Clear();
            _configuredConsumerGroups.Clear();
        }

        await Task.WhenAll(clients.Select(static client => client.DisposeAsync().AsTask())).ConfigureAwait(false);
    }

    private IRemotingClient CreateClient() => new RemotingClient(_serializer, _options, _logger);
}
