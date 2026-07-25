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

using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Options;

namespace EventHorizon.RocketMQ.Grpc;

internal static class GrpcRocketMQRegistration
{
    internal static GrpcRocketMQRoleKey RegisterRole(GrpcRocketMQBuilder builder, GrpcRocketMQRole role)
    {
        var roleKey = new GrpcRocketMQRoleKey(builder.OptionsName, role);
        if (builder.Services.Any(descriptor =>
                descriptor.ServiceType == typeof(GrpcRocketMQRoleRegistration) &&
                descriptor.ImplementationInstance is GrpcRocketMQRoleRegistration registration &&
                registration.RoleKey == roleKey))
        {
            var name = builder.ServiceKey is null ? "the default profile" : $"profile '{builder.ServiceKey}'";
            throw new InvalidOperationException($"A {roleKey.DisplayRole} is already registered for {name}.");
        }

        builder.Services.AddSingleton(new GrpcRocketMQRoleRegistration(roleKey));
        builder.Services.AddKeyedSingleton<IOptions<GrpcClientOptions>>(roleKey, (provider, _) =>
        {
            var configured = provider.GetRequiredService<IOptionsMonitor<GrpcClientOptions>>()
                .Get(roleKey.OptionsName);
            return Options.Create(configured.ForLogicalClient(roleKey.LogicalClientName));
        });
        return roleKey;
    }

    internal static IOptions<GrpcClientOptions> GetClientOptions(
        IServiceProvider provider,
        GrpcRocketMQRoleKey roleKey) =>
        provider.GetRequiredKeyedService<IOptions<GrpcClientOptions>>(roleKey);

    internal static IOptions<TOptions> GetNamedOptions<TOptions>(IServiceProvider provider, string name)
        where TOptions : class =>
        Options.Create(provider.GetRequiredService<IOptionsMonitor<TOptions>>().Get(name));

    internal static void AddRoleSingleton<TService>(
        GrpcRocketMQBuilder builder,
        Func<IServiceProvider, TService> factory)
        where TService : class
    {
        if (builder.ServiceKey is null)
        {
            builder.Services.TryAddSingleton(factory);
            return;
        }

        builder.Services.TryAddKeyedSingleton<TService>(
            builder.ServiceKey,
            (provider, _) => factory(provider));
    }

    internal static TService GetRoleService<TService>(IServiceProvider provider, string? serviceKey)
        where TService : notnull =>
        serviceKey is null
            ? provider.GetRequiredService<TService>()
            : provider.GetRequiredKeyedService<TService>(serviceKey);
}
