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

using EventHorizon.RocketMQ.Grpc.Consumer.Push;

namespace EventHorizon.RocketMQ.Grpc.Consumer.Lite;

internal sealed class GrpcLitePushConsumer : IGrpcLitePushConsumer
{
    private readonly IGrpcPushConsumer _dispatcher;
    private readonly GrpcLiteSubscriptionManager _subscriptions;
    private readonly SemaphoreSlim _lifecycleGate = new(1, 1);
    private int _started;
    private int _disposed;

    public GrpcLitePushConsumer(
        IGrpcPushConsumer dispatcher,
        GrpcLiteSubscriptionManager subscriptions)
    {
        _dispatcher = dispatcher;
        _subscriptions = subscriptions;
    }

    public IReadOnlySet<string> LiteTopics => _subscriptions.LiteTopics;

    public async ValueTask StartAsync(CancellationToken cancellationToken = default)
    {
        await _lifecycleGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);
            if (Volatile.Read(ref _started) != 0)
            {
                return;
            }

            await _dispatcher.StartAsync(cancellationToken).ConfigureAwait(false);
            try
            {
                await _subscriptions.StartAsync(cancellationToken).ConfigureAwait(false);
                Volatile.Write(ref _started, 1);
            }
            catch
            {
                await _dispatcher.StopAsync(CancellationToken.None).ConfigureAwait(false);
                throw;
            }
        }
        finally
        {
            _lifecycleGate.Release();
        }
    }

    public async ValueTask StopAsync(CancellationToken cancellationToken = default)
    {
        await _lifecycleGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (Volatile.Read(ref _started) == 0)
            {
                return;
            }

            await _subscriptions.StopAsync(cancellationToken).ConfigureAwait(false);
            await _dispatcher.StopAsync(cancellationToken).ConfigureAwait(false);
            Volatile.Write(ref _started, 0);
        }
        finally
        {
            _lifecycleGate.Release();
        }
    }

    public Task SubscribeLiteAsync(
        string liteTopic,
        GrpcLiteOffsetOption? offsetOption = null,
        CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);
        return _subscriptions.SubscribeAsync(liteTopic, offsetOption, cancellationToken);
    }

    public Task UnsubscribeLiteAsync(string liteTopic, CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);
        return _subscriptions.UnsubscribeAsync(liteTopic, cancellationToken);
    }

    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
        {
            return;
        }

        await _lifecycleGate.WaitAsync(CancellationToken.None).ConfigureAwait(false);
        try
        {
            if (Volatile.Read(ref _started) != 0)
            {
                Volatile.Write(ref _started, 0);
                await _subscriptions.StopAsync(CancellationToken.None).ConfigureAwait(false);
                await _dispatcher.StopAsync(CancellationToken.None).ConfigureAwait(false);
            }

            await _subscriptions.DisposeAsync().ConfigureAwait(false);
            await _dispatcher.DisposeAsync().ConfigureAwait(false);
        }
        finally
        {
            _lifecycleGate.Release();
        }
    }
}
