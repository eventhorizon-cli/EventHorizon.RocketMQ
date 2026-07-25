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
using EventHorizon.RocketMQ.Grpc.Protocol;
using Microsoft.Extensions.Options;
using Proto = Apache.Rocketmq.V2;

namespace EventHorizon.RocketMQ.Grpc.Protocol.Route;

internal sealed class GrpcRouteService : IGrpcRouteService
{
    private readonly IRocketMQGrpcClient _client;
    private readonly GrpcClientOptions _options;
    private readonly TimeProvider _timeProvider;
    private readonly ConcurrentDictionary<string, CacheEntry> _cache = new(StringComparer.Ordinal);

    public GrpcRouteService(IRocketMQGrpcClient client, IOptions<GrpcClientOptions> options, TimeProvider timeProvider)
    {
        _client = client;
        _options = options.Value;
        _timeProvider = timeProvider;
    }

    public async Task<IReadOnlyList<Proto.MessageQueue>> GetAsync(string topic, bool forceRefresh, CancellationToken cancellationToken)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(topic);
        if (!forceRefresh && _cache.TryGetValue(topic, out var cached) &&
            _timeProvider.GetUtcNow() - cached.CreatedAt < _options.RouteCacheDuration)
        {
            return cached.Queues;
        }

        var response = await _client.QueryRouteAsync(new Proto.QueryRouteRequest
        {
            Topic = Resource(topic),
            Endpoints = _client.AccessPointProtobuf
        }, cancellationToken).ConfigureAwait(false);
        GrpcStatus.EnsureSuccess(response.Status);
        if (response.MessageQueues.Count == 0)
        {
            throw new InvalidOperationException($"RocketMQ returned no message queues for topic '{topic}'.");
        }

        var queues = response.MessageQueues.Select(static queue => queue.Clone()).ToArray();
        _cache[topic] = new CacheEntry(_timeProvider.GetUtcNow(), queues);
        return queues;
    }

    private Proto.Resource Resource(string name) => new()
    {
        ResourceNamespace = _options.Namespace ?? string.Empty,
        Name = name
    };

    private sealed record CacheEntry(DateTimeOffset CreatedAt, IReadOnlyList<Proto.MessageQueue> Queues);
}
