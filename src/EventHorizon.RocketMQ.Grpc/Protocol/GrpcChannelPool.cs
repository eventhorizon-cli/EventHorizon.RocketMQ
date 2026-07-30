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

using Grpc.Net.Client;
using Proto = Apache.Rocketmq.V2;

namespace EventHorizon.RocketMQ.Grpc.Protocol;

internal sealed class GrpcChannelPool : IAsyncDisposable
{
    private readonly object _gate = new();
    private readonly Dictionary<string, GrpcChannel> _channels =
        new(StringComparer.OrdinalIgnoreCase);
    private bool _disposed;

    public Proto.MessagingService.MessagingServiceClient GetClient(Uri endpoint)
    {
        ArgumentNullException.ThrowIfNull(endpoint);
        lock (_gate)
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
            if (!_channels.TryGetValue(endpoint.AbsoluteUri, out var channel))
            {
                channel = GrpcChannel.ForAddress(
                    endpoint.AbsoluteUri,
                    new GrpcChannelOptions { MaxRetryAttempts = 0 });
                _channels.Add(endpoint.AbsoluteUri, channel);
            }

            return new Proto.MessagingService.MessagingServiceClient(channel);
        }
    }

    public ValueTask DisposeAsync()
    {
        GrpcChannel[] channels;
        lock (_gate)
        {
            if (_disposed)
            {
                return ValueTask.CompletedTask;
            }

            _disposed = true;
            channels = [.. _channels.Values];
            _channels.Clear();
        }

        foreach (var channel in channels)
        {
            channel.Dispose();
        }

        return ValueTask.CompletedTask;
    }
}
