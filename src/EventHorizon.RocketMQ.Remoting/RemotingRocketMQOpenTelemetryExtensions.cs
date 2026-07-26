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

namespace EventHorizon.RocketMQ.Remoting;

/// <summary>
/// Provides OpenTelemetry builder extensions for the RocketMQ classic remoting client.
/// </summary>
public static class RemotingRocketMQOpenTelemetryExtensions
{
    /// <summary>
    /// Adds the RocketMQ classic remoting activity source to a tracing provider builder.
    /// </summary>
    /// <param name="builder">The tracing provider builder to configure.</param>
    /// <returns>The supplied <paramref name="builder"/>.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="builder"/> is <see langword="null"/>.</exception>
    public static TracerProviderBuilder AddRocketMQRemotingInstrumentation(this TracerProviderBuilder builder)
    {
        ArgumentNullException.ThrowIfNull(builder);
        return builder.AddSource(RemotingRocketMQInstrumentation.ActivitySourceName);
    }

    /// <summary>
    /// Adds the RocketMQ classic remoting meter to a metrics provider builder.
    /// </summary>
    /// <param name="builder">The metrics provider builder to configure.</param>
    /// <returns>The supplied <paramref name="builder"/>.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="builder"/> is <see langword="null"/>.</exception>
    public static MeterProviderBuilder AddRocketMQRemotingInstrumentation(this MeterProviderBuilder builder)
    {
        ArgumentNullException.ThrowIfNull(builder);
        return builder.AddMeter(RemotingRocketMQInstrumentation.MeterName);
    }
}
