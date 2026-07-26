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

using System.Diagnostics;

namespace EventHorizon.RocketMQ.Remoting;

/// <summary>
/// Provides the telemetry names emitted by the RocketMQ classic remoting client.
/// </summary>
public static class RemotingRocketMQInstrumentation
{
    /// <summary>
    /// Gets the name of the activity source emitted by the RocketMQ classic remoting client.
    /// </summary>
    public const string ActivitySourceName = "EventHorizon.RocketMQ.Remoting";

    /// <summary>
    /// Gets the name of the meter emitted by the RocketMQ classic remoting client.
    /// </summary>
    public const string MeterName = "EventHorizon.RocketMQ.Remoting";

    /// <summary>
    /// Gets the activity source emitted by the RocketMQ classic remoting client.
    /// </summary>
    internal static ActivitySource ActivitySource { get; } = new(ActivitySourceName);
}
