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
    public void LogicalEquality_EquivalentIdentity_UsesTopicBrokerAndQueueId()
    {
        var first = new RemotingConsumerQueue("orders", "broker-a", 3);
        var equivalent = new RemotingConsumerQueue("orders", "broker-a", 3);

        Assert.Equal(first, equivalent);
        Assert.True(first == equivalent);
        Assert.Equal(first.GetHashCode(), equivalent.GetHashCode());
    }

    [Fact]
    public void PublicContract_QueueType_ExposesOnlyQueueIdentity()
    {
        var queueType = typeof(RemotingConsumerQueue);
        var constructor = Assert.Single(queueType.GetConstructors());
        Assert.Equal(
            [typeof(string), typeof(string), typeof(int)],
            constructor.GetParameters().Select(static parameter => parameter.ParameterType));
        Assert.Empty(
            queueType.GetConstructors(
                System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic));

        var properties = queueType
            .GetProperties(System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.Instance)
            .Select(static property => property.Name)
            .Order()
            .ToArray();

        Assert.Equal(["BrokerName", "QueueId", "Topic"], properties);
        Assert.DoesNotContain(
            queueType.GetMembers(System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.Instance),
            static member => member.Name is "BrokerAddresses" or "BrokerAddress");
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
