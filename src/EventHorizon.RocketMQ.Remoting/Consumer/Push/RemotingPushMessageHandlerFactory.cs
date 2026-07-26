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

using EventHorizon.RocketMQ.Remoting.Consumer;
using Microsoft.Extensions.DependencyInjection;

namespace EventHorizon.RocketMQ.Remoting.Consumer.Push;

internal static class RemotingPushMessageHandlerFactory
{
    internal static void Register<TMessageHandler>(
        IServiceCollection services,
        RemotingRocketMQRoleKey roleKey,
        ServiceLifetime lifetime)
        where TMessageHandler : class, IRemotingPushMessageHandler
    {
        ArgumentNullException.ThrowIfNull(services);

        switch (lifetime)
        {
            case ServiceLifetime.Singleton:
                services.AddKeyedSingleton<IRemotingPushMessageHandler, TMessageHandler>(roleKey);
                return;
            case ServiceLifetime.Scoped:
                services.AddKeyedScoped<IRemotingPushMessageHandler, TMessageHandler>(roleKey);
                return;
            case ServiceLifetime.Transient:
                services.AddKeyedTransient<IRemotingPushMessageHandler, TMessageHandler>(roleKey);
                return;
            default:
                throw new ArgumentOutOfRangeException(nameof(lifetime), lifetime, "Unsupported message handler lifetime.");
        }
    }

    internal static Func<IReadOnlyList<RemotingMessageView>, CancellationToken, ValueTask<ConsumeResult>> Create(
        IServiceProvider provider,
        RemotingRocketMQRoleKey roleKey,
        ServiceLifetime lifetime)
    {
        ArgumentNullException.ThrowIfNull(provider);

        return lifetime switch
        {
            ServiceLifetime.Singleton => provider.GetRequiredKeyedService<IRemotingPushMessageHandler>(roleKey).HandleAsync,
            ServiceLifetime.Scoped or ServiceLifetime.Transient => CreateScopedHandler(
                provider.GetRequiredService<IServiceScopeFactory>(),
                roleKey),
            _ => throw new ArgumentOutOfRangeException(nameof(lifetime), lifetime, "Unsupported message handler lifetime.")
        };
    }

    private static Func<IReadOnlyList<RemotingMessageView>, CancellationToken, ValueTask<ConsumeResult>> CreateScopedHandler(
        IServiceScopeFactory scopeFactory,
        RemotingRocketMQRoleKey roleKey) =>
        (messages, cancellationToken) => HandleScopedAsync(scopeFactory, roleKey, messages, cancellationToken);

    private static async ValueTask<ConsumeResult> HandleScopedAsync(
        IServiceScopeFactory scopeFactory,
        RemotingRocketMQRoleKey roleKey,
        IReadOnlyList<RemotingMessageView> messages,
        CancellationToken cancellationToken)
    {
        await using var scope = scopeFactory.CreateAsyncScope();
        var handler = scope.ServiceProvider.GetRequiredKeyedService<IRemotingPushMessageHandler>(roleKey);
        return await handler.HandleAsync(messages, cancellationToken).ConfigureAwait(false);
    }
}
