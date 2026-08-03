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

using EventHorizon.RocketMQ.Remoting.Consumer;
using Xunit;

namespace EventHorizon.RocketMQ.Remoting.Tests.Consumer;

public sealed class RemotingConsumerQueueTests
{
    [Fact]
    public void LogicalEquality_BrokerAddressesDiffer_UsesOnlyQueueIdentity()
    {
        var discovered = new RemotingConsumerQueue(
            "orders",
            3,
            "broker-a",
            new Dictionary<long, string>
            {
                [0] = "127.0.0.1:10911",
                [1] = "127.0.0.1:10912"
            });
        var reconstructed = new RemotingConsumerQueue("orders", "broker-a", 3);

        Assert.Equal(discovered, reconstructed);
        Assert.True(discovered == reconstructed);
        Assert.Equal(discovered.GetHashCode(), reconstructed.GetHashCode());
    }

    [Theory]
    [InlineData(null, "broker-a", 0)]
    [InlineData("orders", null, 0)]
    [InlineData("orders", "broker-a", -1)]
    public void Constructor_IdentityIsInvalid_ThrowsArgumentException(string? topic, string? brokerName, int queueId)
    {
        Assert.ThrowsAny<ArgumentException>(() =>
            new RemotingConsumerQueue(topic!, brokerName!, queueId));
    }
}
