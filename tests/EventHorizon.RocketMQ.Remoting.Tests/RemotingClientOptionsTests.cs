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
            AccessKey = "access-key",
            AccessSecret = "access-secret",
            SecurityToken = "security-token",
            ClientIP = "192.0.2.10",
            InstanceName = "client-a",
            NamesrvAddr = "192.0.2.20:9876",
            Namespace = "tenant-a",
            RequestTimeout = TimeSpan.FromSeconds(12),
            PollNameServerInterval = TimeSpan.FromMinutes(2),
            NameServerRequestTimeout = TimeSpan.FromSeconds(5),
            HeartbeatBrokerInterval = TimeSpan.FromSeconds(45),
            UnitMode = true,
            UnitName = "unit-a",
            UseTLS = true,
            MaxRemotingFrameSize = RemotingClientOptions.DefaultRemotingFrameSize * 2
        };

        var clone = options.ForLogicalClient("producer");

        Assert.NotSame(options, clone);
        Assert.Equal("access-key", clone.AccessKey);
        Assert.Equal("access-secret", clone.AccessSecret);
        Assert.Equal("security-token", clone.SecurityToken);
        Assert.Equal("192.0.2.10", clone.ClientIP);
        Assert.Equal("192.0.2.20:9876", clone.NamesrvAddr);
        Assert.Equal("tenant-a", clone.Namespace);
        Assert.Equal(TimeSpan.FromSeconds(12), clone.RequestTimeout);
        Assert.Equal(TimeSpan.FromMinutes(2), clone.PollNameServerInterval);
        Assert.Equal(TimeSpan.FromSeconds(5), clone.NameServerRequestTimeout);
        Assert.Equal(TimeSpan.FromSeconds(45), clone.HeartbeatBrokerInterval);
        Assert.True(clone.UnitMode);
        Assert.True(clone.UseTLS);
        Assert.Equal(RemotingClientOptions.DefaultRemotingFrameSize * 2, clone.MaxRemotingFrameSize);
        Assert.Equal("client-a@producer", clone.InstanceName);
        Assert.Equal("192.0.2.10@client-a@producer@unit-a", clone.BuildRemotingClientId());
        Assert.Equal("client-a", options.InstanceName);
    }

    [Theory]
    [InlineData("")]
    [InlineData(" ")]
    public void ForLogicalClient_RejectsBlankLogicalName(string logicalClient)
    {
        var options = new RemotingClientOptions();

        Assert.Throws<ArgumentException>(() => options.ForLogicalClient(logicalClient));
    }

    [Fact]
    public void ForLogicalClient_RejectsNullLogicalName()
    {
        var options = new RemotingClientOptions();

        Assert.Throws<ArgumentNullException>(() => options.ForLogicalClient(null!));
    }
}
