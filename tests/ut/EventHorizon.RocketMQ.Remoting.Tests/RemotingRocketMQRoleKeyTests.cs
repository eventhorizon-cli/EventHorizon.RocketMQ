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

using Xunit;

namespace EventHorizon.RocketMQ.Remoting.Tests;

public sealed class RemotingRocketMQRoleKeyTests
{
    [Theory]
    [InlineData((int)RemotingRocketMQRole.Admin, "remoting administrator", "remoting-admin")]
    [InlineData((int)RemotingRocketMQRole.Producer, "remoting producer", "remoting-producer")]
    [InlineData((int)RemotingRocketMQRole.PullConsumer, "remoting pull consumer", "remoting-pull-consumer")]
    [InlineData((int)RemotingRocketMQRole.LitePullConsumer, "remoting lite pull consumer", "remoting-lite-pull-consumer")]
    [InlineData((int)RemotingRocketMQRole.PushConsumer, "remoting push consumer", "remoting-push-consumer")]
    [InlineData((int)RemotingRocketMQRole.PopConsumer, "remoting POP consumer", "remoting-pop-consumer")]
    public void RoleKey_UsesTheExpectedKnownRoleLabels(int role, string displayRole, string roleToken)
    {
        var key = new RemotingRocketMQRoleKey(string.Empty, (RemotingRocketMQRole)role);

        Assert.Equal(displayRole, key.DisplayRole);
        Assert.Equal($"default-{roleToken}", key.LogicalClientName);
    }

    [Fact]
    public void RoleKey_NormalizesNamesAndProvidesStableFallbackForUnknownRoles()
    {
        var known = new RemotingRocketMQRoleKey("west region", RemotingRocketMQRole.PopConsumer);
        var unknown = new RemotingRocketMQRoleKey("custom/name", (RemotingRocketMQRole)999);

        Assert.Equal("west-region-remoting-pop-consumer", known.LogicalClientName);
        Assert.Equal("remoting POP consumer", known.DisplayRole);
        Assert.Equal("999", unknown.DisplayRole);
        Assert.Equal("custom-name-999", unknown.LogicalClientName);
    }
}
