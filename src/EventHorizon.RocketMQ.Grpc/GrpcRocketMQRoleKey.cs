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

internal readonly record struct GrpcRocketMQRoleKey(
    string OptionsName,
    GrpcRocketMQRole Role,
    int InstanceIndex = 0)
{
    public string LogicalClientName =>
        $"{Normalize(string.IsNullOrEmpty(OptionsName) ? "default" : OptionsName)}-{RoleToken}{InstanceToken}";

    public string DisplayRole => Role switch
    {
        GrpcRocketMQRole.Producer => "gRPC producer",
        GrpcRocketMQRole.SimpleConsumer => "gRPC simple consumer",
        GrpcRocketMQRole.PushConsumer => "gRPC push consumer",
        GrpcRocketMQRole.LitePushConsumer => "gRPC lite push consumer",
        _ => Role.ToString()
    };

    private string RoleToken => Role switch
    {
        GrpcRocketMQRole.Producer => "grpc-producer",
        GrpcRocketMQRole.SimpleConsumer => "grpc-simple-consumer",
        GrpcRocketMQRole.PushConsumer => "grpc-push-consumer",
        GrpcRocketMQRole.LitePushConsumer => "grpc-lite-push-consumer",
        _ => Role.ToString().ToLowerInvariant()
    };

    private string InstanceToken => InstanceIndex == 0 ? string.Empty : $"-{InstanceIndex + 1}";

    private static string Normalize(string value) =>
        new(value.Select(static character =>
            char.IsLetterOrDigit(character) || character is '-' or '_' or '.' ? character : '-').ToArray());
}
