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

using EventHorizon.RocketMQ.Remoting.Instrumentation;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Options;

namespace EventHorizon.RocketMQ.Remoting;

/// <summary>
/// Provides dependency-injection methods for registering RocketMQ classic remoting clients.
/// </summary>
public static class RemotingRocketMQServiceCollectionExtensions
{
    /// <summary>
    /// Registers the default RocketMQ classic remoting client.
    /// </summary>
    /// <param name="services">The service collection to which the client is registered.</param>
    /// <param name="configure">The delegate used to configure the client.</param>
    /// <returns>A builder used to add producer or consumer roles to the client registration.</returns>
    /// <exception cref="ArgumentNullException">
    /// <paramref name="services"/> or <paramref name="configure"/> is <see langword="null"/>.
    /// </exception>
    public static RemotingRocketMQBuilder AddRocketMQRemoting(
        this IServiceCollection services,
        Action<RemotingClientOptions> configure) =>
        AddRocketMQRemotingCore(services, Options.DefaultName, null, configure);

    /// <summary>
    /// Registers a RocketMQ classic remoting client under the specified registration name.
    /// Roles added through the returned builder are resolved as keyed services with
    /// <c>GetRequiredKeyedService&lt;T&gt;(registrationName)</c>.
    /// </summary>
    /// <param name="services">The service collection to which the client is registered.</param>
    /// <param name="registrationName">
    /// The name that identifies the client registration, its named options, and its keyed services.
    /// </param>
    /// <param name="configure">The delegate used to configure the client.</param>
    /// <returns>A builder used to add producer or consumer roles to the keyed client registration.</returns>
    /// <exception cref="ArgumentNullException">
    /// <paramref name="services"/> or <paramref name="configure"/> is <see langword="null"/>.
    /// </exception>
    /// <exception cref="ArgumentException">
    /// <paramref name="registrationName"/> is empty or consists only of white-space characters.
    /// </exception>
    public static RemotingRocketMQBuilder AddRocketMQRemoting(
        this IServiceCollection services,
        string registrationName,
        Action<RemotingClientOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentException.ThrowIfNullOrWhiteSpace(registrationName);
        ArgumentNullException.ThrowIfNull(configure);
        return AddRocketMQRemotingCore(services, registrationName, registrationName, configure);
    }

    private static RemotingRocketMQBuilder AddRocketMQRemotingCore(
        IServiceCollection services,
        string optionsName,
        string? registrationName,
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
            .Validate(
                static options => string.IsNullOrWhiteSpace(options.SecurityToken) ||
                                  (!string.IsNullOrWhiteSpace(options.AccessKey) &&
                                   !string.IsNullOrWhiteSpace(options.AccessSecret)),
                "A RocketMQ security token requires both an access key and an access secret.")
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

        services.AddLogging();
        services.AddMetrics();
        services.TryAddSingleton<IRemotingRocketMQTelemetry, RemotingRocketMQTelemetry>();
        services.TryAddSingleton(TimeProvider.System);
        return new RemotingRocketMQBuilder(services, optionsName, registrationName);
    }
}
