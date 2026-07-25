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

namespace EventHorizon.RocketMQ.Shared.Tests;

public sealed class RocketMQClientOptionsTests
{
    [Fact]
    public void BuildClientId_UsesClientIpAndInstanceName()
    {
        var options = new TestClientOptions
        {
            ClientIP = "192.0.2.10",
            InstanceName = "client-a"
        };

        var clientId = options.GetClientId();

        Assert.Equal("192.0.2.10@client-a", clientId);
    }

    [Fact]
    public void ForLogicalClient_ClonesConcreteOptionsAndAppendsLogicalName()
    {
        var options = new TestClientOptions
        {
            RequestTimeout = TimeSpan.FromSeconds(12),
            AccessKey = "access-key",
            AccessSecret = "access-secret",
            SecurityToken = "security-token",
            ClientIP = "192.0.2.10",
            InstanceName = "client-a",
            Namespace = "tenant-a",
            UseTLS = true,
            ProtocolSetting = "protocol-value"
        };

        var clone = options.CloneFor("producer");

        Assert.IsType<TestClientOptions>(clone);
        Assert.NotSame(options, clone);
        Assert.Equal(TimeSpan.FromSeconds(12), clone.RequestTimeout);
        Assert.Equal("access-key", clone.AccessKey);
        Assert.Equal("access-secret", clone.AccessSecret);
        Assert.Equal("security-token", clone.SecurityToken);
        Assert.Equal("192.0.2.10", clone.ClientIP);
        Assert.Equal("client-a@producer", clone.InstanceName);
        Assert.Equal("tenant-a", clone.Namespace);
        Assert.True(clone.UseTLS);
        Assert.Equal("protocol-value", clone.ProtocolSetting);
        Assert.Equal("client-a", options.InstanceName);
    }

    [Theory]
    [InlineData("")]
    [InlineData(" ")]
    public void ForLogicalClient_RejectsBlankLogicalName(string logicalClient)
    {
        var options = new TestClientOptions();

        Assert.Throws<ArgumentException>(() => options.CloneFor(logicalClient));
    }

    private sealed class TestClientOptions : RocketMQClientOptions
    {
        public string? ProtocolSetting { get; set; }

        public string GetClientId() => BuildClientId();

        public TestClientOptions CloneFor(string logicalClient) =>
            (TestClientOptions)CloneForLogicalClient(logicalClient);
    }
}
