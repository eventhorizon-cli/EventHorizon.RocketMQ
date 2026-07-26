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

using EventHorizon.RocketMQ.Grpc;
using Xunit;

namespace EventHorizon.RocketMQ.Grpc.Tests;

public sealed class GrpcRocketMQRoleKeyTests
{
    [Theory]
    [InlineData((int)GrpcRocketMQRole.Producer, "gRPC producer", "grpc-producer")]
    [InlineData((int)GrpcRocketMQRole.SimpleConsumer, "gRPC simple consumer", "grpc-simple-consumer")]
    [InlineData((int)GrpcRocketMQRole.PushConsumer, "gRPC push consumer", "grpc-push-consumer")]
    [InlineData((int)GrpcRocketMQRole.LitePushConsumer, "gRPC lite push consumer", "grpc-lite-push-consumer")]
    public void RoleKey_UsesTheExpectedKnownRoleLabels(int role, string displayRole, string roleToken)
    {
        var key = new GrpcRocketMQRoleKey(string.Empty, (GrpcRocketMQRole)role);

        Assert.Equal(displayRole, key.DisplayRole);
        Assert.Equal($"default-{roleToken}", key.LogicalClientName);
    }

    [Fact]
    public void RoleKey_NormalizesNamesAndProvidesStableFallbackForUnknownRoles()
    {
        var known = new GrpcRocketMQRoleKey("west region", GrpcRocketMQRole.LitePushConsumer);
        var unknown = new GrpcRocketMQRoleKey("custom/name", (GrpcRocketMQRole)999);

        Assert.Equal("west-region-grpc-lite-push-consumer", known.LogicalClientName);
        Assert.Equal("gRPC lite push consumer", known.DisplayRole);
        Assert.Equal("999", unknown.DisplayRole);
        Assert.Equal("custom-name-999", unknown.LogicalClientName);
    }
}
