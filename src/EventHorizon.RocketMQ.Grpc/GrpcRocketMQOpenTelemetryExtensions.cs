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

using OpenTelemetry.Metrics;
using OpenTelemetry.Trace;

namespace EventHorizon.RocketMQ.Grpc;

/// <summary>
/// Provides OpenTelemetry builder extensions for the RocketMQ gRPC client.
/// </summary>
public static class GrpcRocketMQOpenTelemetryExtensions
{
    /// <summary>
    /// Adds the RocketMQ gRPC activity source to a tracing provider builder.
    /// </summary>
    /// <param name="builder">The tracing provider builder to configure.</param>
    /// <returns>The supplied <paramref name="builder"/>.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="builder"/> is <see langword="null"/>.</exception>
    public static TracerProviderBuilder AddRocketMQGrpcInstrumentation(this TracerProviderBuilder builder)
    {
        ArgumentNullException.ThrowIfNull(builder);
        return builder.AddSource(GrpcRocketMQInstrumentation.ActivitySourceName);
    }

    /// <summary>
    /// Adds the RocketMQ gRPC meter to a metrics provider builder.
    /// </summary>
    /// <param name="builder">The metrics provider builder to configure.</param>
    /// <returns>The supplied <paramref name="builder"/>.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="builder"/> is <see langword="null"/>.</exception>
    public static MeterProviderBuilder AddRocketMQGrpcInstrumentation(this MeterProviderBuilder builder)
    {
        ArgumentNullException.ThrowIfNull(builder);
        return builder.AddMeter(GrpcRocketMQInstrumentation.MeterName);
    }
}
