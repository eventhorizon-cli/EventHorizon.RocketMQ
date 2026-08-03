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

using EventHorizon.RocketMQ.Remoting.Protocol;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Xunit;

namespace EventHorizon.RocketMQ.Remoting.Tests.Protocol;

public sealed class RemotingClientRegistryTests
{
    [Fact]
    public async Task GetGroupMemberClient_DistinctGroupsAndSameGroupReplicas_SharesAndIsolates()
    {
        await using var registry = new RemotingClientRegistry(
            new RemoteCommandSerializer(),
            Options.Create(new RemotingClientOptions()),
            NullLogger<RemotingClient>.Instance);

        var sharedClient = registry.SharedClient;
        var firstOrdersMember = registry.GetGroupMemberClient("orders");
        var auditMember = registry.GetGroupMemberClient("audit");
        var secondOrdersMember = registry.GetGroupMemberClient("orders");
        var thirdOrdersMember = registry.GetGroupMemberClient("orders");

        Assert.Same(sharedClient, firstOrdersMember);
        Assert.Same(sharedClient, auditMember);
        Assert.NotSame(sharedClient, secondOrdersMember);
        Assert.NotSame(sharedClient, thirdOrdersMember);
        Assert.NotSame(secondOrdersMember, thirdOrdersMember);
    }
}
