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

namespace EventHorizon.RocketMQ.Grpc;

/// <summary>
/// Configures a RocketMQ 5 gRPC client.
/// </summary>
public sealed class GrpcClientOptions
{
    /// <summary>
    /// Gets or sets the RocketMQ 5 proxy endpoint.
    /// </summary>
    public string Endpoint { get; set; } = "localhost:8081";

    /// <summary>
    /// Gets or sets the base timeout for an individual request to RocketMQ.
    /// </summary>
    public TimeSpan RequestTimeout { get; set; } = TimeSpan.FromSeconds(3);

    /// <summary>
    /// Gets or sets how long topic routes remain cached before they are refreshed.
    /// </summary>
    public TimeSpan RouteCacheDuration { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>
    /// Gets or sets the interval between gRPC heartbeats sent to RocketMQ endpoints.
    /// </summary>
    public TimeSpan HeartbeatInterval { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>
    /// Gets or sets the access key used for access-control authentication.
    /// </summary>
    public string? AccessKey { get; set; }

    /// <summary>
    /// Gets or sets the access secret used for access-control authentication.
    /// </summary>
    public string? AccessSecret { get; set; }

    /// <summary>
    /// Gets or sets the temporary security token included with authenticated requests.
    /// </summary>
    /// <remarks>
    /// Configuring a security token also requires <see cref="AccessKey"/> and <see cref="AccessSecret"/>.
    /// </remarks>
    public string? SecurityToken { get; set; }

    /// <summary>
    /// Gets or sets the resource namespace applied to RocketMQ topics and consumer groups.
    /// </summary>
    public string? Namespace { get; set; }

    /// <summary>
    /// Gets or sets whether client transport connections use TLS.
    /// </summary>
    public bool UseTLS { get; set; }
}
