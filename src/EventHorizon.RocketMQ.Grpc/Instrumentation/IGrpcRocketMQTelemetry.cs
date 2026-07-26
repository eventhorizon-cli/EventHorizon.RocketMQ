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

namespace EventHorizon.RocketMQ.Grpc.Instrumentation;

internal interface IGrpcRocketMQTelemetry
{
    IGrpcRocketMQTelemetryOperation StartSend(string topic, int messageCount, long bodySize);

    IGrpcRocketMQTelemetryOperation StartReceive(
        string topic,
        string consumerGroup,
        int messageCount,
        ActivityContext? parentContext,
        DateTimeOffset startTime,
        long startTimestamp,
        int? queueId = null,
        bool createActivity = true,
        IEnumerable<IReadOnlyDictionary<string, string>>? messageProperties = null);

    IGrpcRocketMQTelemetryOperation StartProcess(
        string topic,
        string consumerGroup,
        string messageId,
        int queueId,
        long bodySize,
        IReadOnlyDictionary<string, string> properties,
        ActivityContext? receiveContext);

    IGrpcRocketMQTelemetryOperation StartSettle(
        string operationName,
        string topic,
        string consumerGroup,
        string? messageId,
        int? queueId,
        IReadOnlyDictionary<string, string>? properties);

    void InjectContext(Activity? activity, IDictionary<string, string> properties);
}
