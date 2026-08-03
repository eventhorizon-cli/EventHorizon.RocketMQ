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

using System.Text;
using EventHorizon.RocketMQ.Remoting.Consumer;
using EventHorizon.RocketMQ.Remoting.Consumer.Push.Assignment;
using Newtonsoft.Json.Linq;
using Xunit;

namespace EventHorizon.RocketMQ.Remoting.Tests.Consumer.Push.Assignment;

public sealed class LegacyAssignmentProtocolTests
{
    [Fact]
    public void EncodeQueryAssignment_ClusteringMode_WritesOfficialRequestBody()
    {
        var body = LegacyConsumerProtocol.EncodeQueryAssignment(
            "tenant-a%orders",
            "tenant-a%orders-group",
            "client-a",
            "AVG");

        var json = JObject.Parse(Encoding.UTF8.GetString(body));

        Assert.Equal("tenant-a%orders", (string?)json["topic"]);
        Assert.Equal("tenant-a%orders-group", (string?)json["consumerGroup"]);
        Assert.Equal("client-a", (string?)json["clientId"]);
        Assert.Equal("AVG", (string?)json["strategyName"]);
        Assert.Equal("CLUSTERING", (string?)json["messageModel"]);
    }

    [Fact]
    public void DecodeQueryAssignment_ResponseContainsNullSet_PreservesPreviousAssignment()
    {
        var update = LegacyConsumerProtocol.DecodeQueryAssignment(
            Encoding.UTF8.GetBytes("{\"messageQueueAssignments\":null}"));

        Assert.False(update.ShouldReplace);
        Assert.Empty(update.Assignments);
    }

    [Fact]
    public void DecodeQueryAssignment_ResponseContainsEmptySet_ClearsPreviousAssignment()
    {
        var update = LegacyConsumerProtocol.DecodeQueryAssignment(
            Encoding.UTF8.GetBytes("{\"messageQueueAssignments\":[]}"));

        Assert.True(update.ShouldReplace);
        Assert.Empty(update.Assignments);
    }

    [Fact]
    public void DecodeQueryAssignment_WildcardPopResponse_DecodesPopAndDefaultsMissingModeToPull()
    {
        const string json = """
            {
              "messageQueueAssignments": [
                {
                  "messageQueue": { "topic": "orders", "brokerName": "broker-a", "queueId": -1 },
                  "mode": "POP",
                  "attachments": { "source": "broker" }
                },
                {
                  "messageQueue": { "topic": "payments", "brokerName": "broker-a", "queueId": 2 }
                }
              ]
            }
            """;

        var update = LegacyConsumerProtocol.DecodeQueryAssignment(Encoding.UTF8.GetBytes(json));

        Assert.True(update.ShouldReplace);
        Assert.Collection(
            update.Assignments,
            assignment =>
            {
                Assert.Equal("orders", assignment.Topic);
                Assert.Equal("broker-a", assignment.BrokerName);
                Assert.Equal(-1, assignment.QueueId);
                Assert.Equal(RemotingPushReceiveMode.Pop, assignment.Mode);
                Assert.Equal("broker", assignment.Attachments["source"]);
            },
            assignment =>
            {
                Assert.Equal("payments", assignment.Topic);
                Assert.Equal(2, assignment.QueueId);
                Assert.Equal(RemotingPushReceiveMode.Pull, assignment.Mode);
                Assert.Empty(assignment.Attachments);
            });
    }

    [Fact]
    public void DecodeQueryAssignment_ResponseContainsUnknownMode_ThrowsInvalidDataException()
    {
        const string json = """
            {
              "messageQueueAssignments": [
                {
                  "messageQueue": { "topic": "orders", "brokerName": "broker-a", "queueId": 0 },
                  "mode": "UNKNOWN"
                }
              ]
            }
            """;

        Assert.Throws<InvalidDataException>(() =>
            LegacyConsumerProtocol.DecodeQueryAssignment(Encoding.UTF8.GetBytes(json)));
    }
}
