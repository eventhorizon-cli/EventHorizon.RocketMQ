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

/// <summary>
/// Provides dependency-injection registration methods for RocketMQ gRPC client profiles.
/// </summary>
public static class GrpcRocketMQServiceCollectionExtensions
{
    /// <summary>
    /// Adds the default RocketMQ gRPC client profile.
    /// </summary>
    /// <param name="services">The service collection to which the profile is added.</param>
    /// <param name="configure">The delegate used to configure the gRPC client.</param>
    /// <returns>A builder used to add producer and consumer roles to the profile.</returns>
    /// <exception cref="ArgumentNullException">
    /// <paramref name="services"/> or <paramref name="configure"/> is <see langword="null"/>.
    /// </exception>
    public static GrpcRocketMQBuilder AddRocketMQGrpc(
        this IServiceCollection services,
        Action<GrpcClientOptions> configure) =>
        AddRocketMQGrpcCore(services, Options.DefaultName, null, configure);

    /// <summary>
    /// Adds a named RocketMQ gRPC client profile. Roles added through the returned builder are
    /// resolved with <c>GetRequiredKeyedService&lt;T&gt;(serviceKey)</c>.
    /// </summary>
    /// <param name="services">The service collection to which the profile is added.</param>
    /// <param name="serviceKey">The key used to register and resolve services for the profile.</param>
    /// <param name="configure">The delegate used to configure the gRPC client.</param>
    /// <returns>A builder used to add producer and consumer roles to the named profile.</returns>
    /// <exception cref="ArgumentNullException">
    /// <paramref name="services"/> or <paramref name="configure"/> is <see langword="null"/>.
    /// </exception>
    /// <exception cref="ArgumentException"><paramref name="serviceKey"/> is empty or consists only of white-space characters.</exception>
    public static GrpcRocketMQBuilder AddRocketMQGrpc(
        this IServiceCollection services,
        string serviceKey,
        Action<GrpcClientOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentException.ThrowIfNullOrWhiteSpace(serviceKey);
        ArgumentNullException.ThrowIfNull(configure);
        return AddRocketMQGrpcCore(services, serviceKey, serviceKey, configure);
    }

    private static GrpcRocketMQBuilder AddRocketMQGrpcCore(
        IServiceCollection services,
        string optionsName,
        string? serviceKey,
        Action<GrpcClientOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(configure);
        services.AddOptions<GrpcClientOptions>(optionsName)
            .Configure(configure)
            .Validate(
                static options => !string.IsNullOrWhiteSpace(options.Endpoint),
                "A gRPC proxy endpoint is required.")
            .Validate(
                static options => string.IsNullOrWhiteSpace(options.AccessKey) ==
                                  string.IsNullOrWhiteSpace(options.AccessSecret),
                "RocketMQ ACL requires both an access key and an access secret.")
            .Validate(static options => options.RequestTimeout > TimeSpan.Zero, "Request timeout must be positive.")
            .Validate(
                static options => options.RouteCacheDuration > TimeSpan.Zero,
                "Route cache duration must be positive.")
            .Validate(
                static options => options.HeartbeatInterval > TimeSpan.Zero,
                "Heartbeat interval must be positive.");

        services.TryAddSingleton(TimeProvider.System);
        return new GrpcRocketMQBuilder(services, optionsName, serviceKey);
    }
}
