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

using EventHorizon.RocketMQ.Grpc.Instrumentation;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Options;

namespace EventHorizon.RocketMQ.Grpc;

/// <summary>
/// Provides dependency-injection methods for registering RocketMQ gRPC clients.
/// </summary>
public static class GrpcRocketMQServiceCollectionExtensions
{
    /// <summary>
    /// Registers the default RocketMQ gRPC client.
    /// </summary>
    /// <param name="services">The service collection to which the client is registered.</param>
    /// <param name="configure">The delegate used to configure the gRPC client.</param>
    /// <returns>A builder used to add producer and consumer roles to the client registration.</returns>
    /// <exception cref="ArgumentNullException">
    /// <paramref name="services"/> or <paramref name="configure"/> is <see langword="null"/>.
    /// </exception>
    public static GrpcRocketMQBuilder AddRocketMQGrpc(
        this IServiceCollection services,
        Action<GrpcClientOptions> configure) =>
        AddRocketMQGrpcCore(services, Options.DefaultName, null, configure);

    /// <summary>
    /// Registers a RocketMQ gRPC client under the specified registration name.
    /// Roles added through the returned builder are resolved as keyed services with
    /// <c>GetRequiredKeyedService&lt;T&gt;(registrationName)</c>.
    /// </summary>
    /// <param name="services">The service collection to which the client is registered.</param>
    /// <param name="registrationName">
    /// The name that identifies the client registration, its named options, and its keyed services.
    /// </param>
    /// <param name="configure">The delegate used to configure the gRPC client.</param>
    /// <returns>A builder used to add producer and consumer roles to the keyed client registration.</returns>
    /// <exception cref="ArgumentNullException">
    /// <paramref name="services"/> or <paramref name="configure"/> is <see langword="null"/>.
    /// </exception>
    /// <exception cref="ArgumentException">
    /// <paramref name="registrationName"/> is empty, consists only of white-space characters, or contains a null
    /// character.
    /// </exception>
    public static GrpcRocketMQBuilder AddRocketMQGrpc(
        this IServiceCollection services,
        string registrationName,
        Action<GrpcClientOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentException.ThrowIfNullOrWhiteSpace(registrationName);
        if (registrationName.Contains('\0'))
        {
            throw new ArgumentException("The registration name cannot contain a null character.", nameof(registrationName));
        }

        ArgumentNullException.ThrowIfNull(configure);
        return AddRocketMQGrpcCore(services, registrationName, registrationName, configure);
    }

    private static GrpcRocketMQBuilder AddRocketMQGrpcCore(
        IServiceCollection services,
        string optionsName,
        string? registrationName,
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
            .Validate(
                static options => string.IsNullOrWhiteSpace(options.SecurityToken) ||
                                  (!string.IsNullOrWhiteSpace(options.AccessKey) &&
                                   !string.IsNullOrWhiteSpace(options.AccessSecret)),
                "A RocketMQ security token requires both an access key and an access secret.")
            .Validate(static options => options.RequestTimeout > TimeSpan.Zero, "Request timeout must be positive.")
            .Validate(
                static options => options.RouteCacheDuration > TimeSpan.Zero,
                "Route cache duration must be positive.")
            .Validate(
                static options => options.HeartbeatInterval > TimeSpan.Zero,
                "Heartbeat interval must be positive.");

        services.AddLogging();
        services.AddMetrics();
        services.TryAddSingleton<IGrpcRocketMQTelemetry, GrpcRocketMQTelemetry>();
        services.TryAddSingleton(TimeProvider.System);
        return new GrpcRocketMQBuilder(services, optionsName, registrationName);
    }
}
