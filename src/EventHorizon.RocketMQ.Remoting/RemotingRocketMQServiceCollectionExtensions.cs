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

/// <summary>
/// Provides dependency-injection registration methods for RocketMQ classic remoting client profiles.
/// </summary>
public static class RemotingRocketMQServiceCollectionExtensions
{
    /// <summary>
    /// Adds the default RocketMQ classic remoting client profile.
    /// </summary>
    /// <param name="services">The service collection to which the client profile is added.</param>
    /// <param name="configure">The delegate used to configure the client.</param>
    /// <returns>A builder used to add producer or consumer roles to the profile.</returns>
    public static RemotingRocketMQBuilder AddRocketMQRemoting(
        this IServiceCollection services,
        Action<RemotingClientOptions> configure) =>
        AddRocketMQRemotingCore(services, Options.DefaultName, null, configure);

    /// <summary>
    /// Adds a named RocketMQ classic remoting client profile. Roles added through the returned builder are
    /// resolved with <c>GetRequiredKeyedService&lt;T&gt;(serviceKey)</c>.
    /// </summary>
    /// <param name="services">The service collection to which the client profile is added.</param>
    /// <param name="serviceKey">The key used to identify and resolve the client profile.</param>
    /// <param name="configure">The delegate used to configure the client.</param>
    /// <returns>A builder used to add producer or consumer roles to the profile.</returns>
    public static RemotingRocketMQBuilder AddRocketMQRemoting(
        this IServiceCollection services,
        string serviceKey,
        Action<RemotingClientOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentException.ThrowIfNullOrWhiteSpace(serviceKey);
        ArgumentNullException.ThrowIfNull(configure);
        return AddRocketMQRemotingCore(services, serviceKey, serviceKey, configure);
    }

    private static RemotingRocketMQBuilder AddRocketMQRemotingCore(
        IServiceCollection services,
        string optionsName,
        string? serviceKey,
        Action<RemotingClientOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(configure);
        services.AddOptions<RemotingClientOptions>(optionsName)
            .Configure(configure)
            .Validate(
                static options => !string.IsNullOrWhiteSpace(options.NamesrvAddr),
                "At least one NameServer address is required.")
            .Validate(
                static options => string.IsNullOrWhiteSpace(options.AccessKey) ==
                                  string.IsNullOrWhiteSpace(options.AccessSecret),
                "RocketMQ ACL requires both an access key and an access secret.")
            .Validate(static options => options.RequestTimeout > TimeSpan.Zero, "Request timeout must be positive.")
            .Validate(
                static options => options.PollNameServerInterval > TimeSpan.Zero,
                "NameServer polling interval must be positive.")
            .Validate(
                static options => options.NameServerRequestTimeout > TimeSpan.Zero,
                "NameServer request timeout must be positive.")
            .Validate(
                static options => options.HeartbeatBrokerInterval > TimeSpan.Zero,
                "Broker heartbeat interval must be positive.")
            .Validate(
                static options => options.MaxRemotingFrameSize >= RemotingClientOptions.MinimumRemotingFrameSize,
                $"Maximum classic remoting frame size must be at least " +
                $"{RemotingClientOptions.MinimumRemotingFrameSize} bytes.");

        services.TryAddSingleton(TimeProvider.System);
        return new RemotingRocketMQBuilder(services, optionsName, serviceKey);
    }
}
