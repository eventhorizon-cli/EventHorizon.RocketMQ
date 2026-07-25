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
using Microsoft.Extensions.Options;

namespace EventHorizon.RocketMQ.Remoting.Protocol.Route;

internal sealed class TopicRouteService : ITopicRouteService
{
    private readonly INameServer _nameServer;
    private readonly RemotingClientOptions _options;
    private readonly TimeProvider _timeProvider;
    private readonly ConcurrentDictionary<string, CacheEntry> _cache = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<string, SemaphoreSlim> _locks = new(StringComparer.Ordinal);

    public TopicRouteService(
        INameServer nameServer,
        IOptions<RemotingClientOptions> options,
        TimeProvider timeProvider)
    {
        _nameServer = nameServer;
        _options = options.Value;
        _timeProvider = timeProvider;
    }

    public async Task<TopicRouteData> GetAsync(
        string topic,
        bool forceRefresh = false,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(topic);
        var now = _timeProvider.GetUtcNow();
        if (!forceRefresh && _cache.TryGetValue(topic, out var cached) && cached.ExpiresAt > now)
        {
            return cached.Route;
        }

        var gate = _locks.GetOrAdd(topic, static _ => new SemaphoreSlim(1, 1));
        await gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            now = _timeProvider.GetUtcNow();
            if (!forceRefresh && _cache.TryGetValue(topic, out cached) && cached.ExpiresAt > now)
            {
                return cached.Route;
            }

            try
            {
                var route = await _nameServer.GetTopicRouteInfoAsync(
                    topic,
                    _options.NameServerRequestTimeout,
                    cancellationToken: cancellationToken).ConfigureAwait(false);
                _cache[topic] = new CacheEntry(route, now + _options.PollNameServerInterval);
                return route;
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception) when (_cache.TryGetValue(topic, out cached) &&
                                    cached.ExpiresAt > _timeProvider.GetUtcNow())
            {
                return cached.Route;
            }
        }
        finally
        {
            gate.Release();
        }
    }

    private sealed record CacheEntry(TopicRouteData Route, DateTimeOffset ExpiresAt);
}
