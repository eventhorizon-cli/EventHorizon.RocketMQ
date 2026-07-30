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

internal sealed class RemotingClientPool : IAsyncDisposable
{
    private readonly object _sync = new();
    private readonly IRemoteCommandSerializer _serializer;
    private readonly IOptions<RemotingClientOptions> _options;
    private readonly ILogger<RemotingClient> _logger;
    private readonly IRemotingClient _sharedClient;
    private readonly HashSet<string> _activeConsumerGroups = new(StringComparer.Ordinal);
    private readonly List<IRemotingClient> _isolatedClients = [];
    private int _disposed;

    public RemotingClientPool(
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

    public IRemotingClient AcquireGroupMemberClient(string groupName)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(groupName);
        lock (_sync)
        {
            ObjectDisposedException.ThrowIf(_disposed != 0, this);
            if (_activeConsumerGroups.Add(groupName))
            {
                return _sharedClient;
            }

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
            _activeConsumerGroups.Clear();
        }

        await Task.WhenAll(clients.Select(static client => client.DisposeAsync().AsTask())).ConfigureAwait(false);
    }

    private IRemotingClient CreateClient() => new RemotingClient(_serializer, _options, _logger);
}
