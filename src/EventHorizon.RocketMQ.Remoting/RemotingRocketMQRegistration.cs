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

namespace EventHorizon.RocketMQ.Remoting;

internal static class RemotingRocketMQRegistration
{
    internal static RemotingRocketMQRoleRegistration RegisterRole(
        RemotingRocketMQBuilder builder,
        RemotingRocketMQRole role)
    {
        var registrations = builder.Services
            .Where(descriptor =>
                descriptor.ServiceType == typeof(RemotingRocketMQRoleRegistration) &&
                descriptor.ImplementationInstance is RemotingRocketMQRoleRegistration registration &&
                registration.RoleKey.OptionsName == builder.OptionsName &&
                registration.RoleKey.Role == role)
            .ToArray();
        var roleKey = new RemotingRocketMQRoleKey(builder.OptionsName, role, registrations.Length);
        if (registrations.Length > 0 && role is RemotingRocketMQRole.Admin or RemotingRocketMQRole.Producer)
        {
            var registrationDescription = builder.RegistrationName is null
                ? "the default client registration"
                : $"registration name '{builder.RegistrationName}'";
            throw new InvalidOperationException(
                $"A {roleKey.DisplayRole} is already registered for {registrationDescription}.");
        }

        var roleOptionsName = roleKey.InstanceIndex == 0
            ? roleKey.RoleOptionsName
            : $"\0EventHorizon.RocketMQ.Remoting:{roleKey.RoleOptionsName}";
        var registration = new RemotingRocketMQRoleRegistration(roleKey, roleOptionsName);
        builder.Services.AddSingleton(registration);
        builder.Services.AddKeyedSingleton<IOptions<RemotingClientOptions>>(roleKey, (provider, _) =>
        {
            var configured = provider.GetRequiredService<IOptionsMonitor<RemotingClientOptions>>()
                .Get(roleKey.OptionsName);
            return Options.Create(configured.ForLogicalClient(roleKey.LogicalClientName));
        });
        return registration;
    }

    internal static IOptions<RemotingClientOptions> GetClientOptions(
        IServiceProvider provider,
        RemotingRocketMQRoleKey roleKey) =>
        provider.GetRequiredKeyedService<IOptions<RemotingClientOptions>>(roleKey);

    internal static IOptions<TOptions> GetNamedOptions<TOptions>(IServiceProvider provider, string name)
        where TOptions : class =>
        Options.Create(provider.GetRequiredService<IOptionsMonitor<TOptions>>().Get(name));

    internal static void AddRoleSingleton<TService>(
        RemotingRocketMQBuilder builder,
        Func<IServiceProvider, TService> factory)
        where TService : class
    {
        if (builder.RegistrationName is null)
        {
            builder.Services.TryAddSingleton(factory);
            return;
        }

        builder.Services.TryAddKeyedSingleton<TService>(
            builder.RegistrationName,
            (provider, _) => factory(provider));
    }

    internal static Func<IServiceProvider, TService> AddConsumerSingleton<TService>(
        RemotingRocketMQBuilder builder,
        Func<IServiceProvider, TService> factory)
        where TService : class
    {
        if (builder.RegistrationName is null)
        {
            var index = builder.Services.Count(descriptor =>
                descriptor.ServiceType == typeof(TService) && !descriptor.IsKeyedService);
            builder.Services.AddSingleton(factory);
            return provider => provider.GetServices<TService>().ElementAt(index);
        }

        var registrationName = builder.RegistrationName;
        var keyedIndex = builder.Services.Count(descriptor =>
            descriptor.ServiceType == typeof(TService) &&
            descriptor.IsKeyedService &&
            Equals(descriptor.ServiceKey, registrationName));
        builder.Services.AddKeyedSingleton<TService>(
            registrationName,
            (provider, _) => factory(provider));
        return provider => provider.GetKeyedServices<TService>(registrationName).ElementAt(keyedIndex);
    }

    internal static TService GetRoleService<TService>(IServiceProvider provider, string? registrationName)
        where TService : notnull =>
        registrationName is null
            ? provider.GetRequiredService<TService>()
            : provider.GetRequiredKeyedService<TService>(registrationName);
}
