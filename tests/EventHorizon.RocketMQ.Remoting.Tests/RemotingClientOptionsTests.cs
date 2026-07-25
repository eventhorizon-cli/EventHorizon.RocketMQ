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

public sealed class RemotingClientOptionsTests
{
    [Fact]
    public void BuildRemotingClientId_AppendsUnitName()
    {
        var options = new RemotingClientOptions
        {
            ClientIP = "192.0.2.10",
            InstanceName = "client-a",
            UnitName = "unit-a"
        };

        var clientId = options.BuildRemotingClientId();

        Assert.Equal("192.0.2.10@client-a@unit-a", clientId);
    }

    [Fact]
    public void LogicalClientIdentity_PreservesRemotingOptionsAndAppendsUnitNameLast()
    {
        var options = new RemotingClientOptions
        {
            ClientIP = "192.0.2.10",
            InstanceName = "client-a",
            NamesrvAddr = "192.0.2.20:9876",
            UnitName = "unit-a"
        };

        var clone = options.ForLogicalClient("producer");

        Assert.Equal("192.0.2.20:9876", clone.NamesrvAddr);
        Assert.Equal("client-a@producer", clone.InstanceName);
        Assert.Equal("192.0.2.10@client-a@producer@unit-a", clone.BuildRemotingClientId());
    }
}
