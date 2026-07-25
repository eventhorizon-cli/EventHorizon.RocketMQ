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

namespace EventHorizon.RocketMQ.Grpc;

/// <summary>
/// Configures protocol-specific roles for a RocketMQ gRPC client profile.
/// </summary>
public sealed class GrpcRocketMQBuilder
{
    internal GrpcRocketMQBuilder(IServiceCollection services, string optionsName, string? serviceKey)
    {
        Services = services;
        OptionsName = optionsName;
        ServiceKey = serviceKey;
    }

    /// <summary>
    /// Gets the service collection to which the client profile is being added.
    /// </summary>
    public IServiceCollection Services { get; }

    /// <summary>
    /// Gets the key used to resolve services registered by this builder, or <see langword="null"/>
    /// for the default unkeyed registration.
    /// </summary>
    public string? ServiceKey { get; }

    internal string OptionsName { get; }
}
