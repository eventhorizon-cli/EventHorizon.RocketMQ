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

using System.Text.Json;
using EventHorizon.RocketMQ.Remoting.Consumer;
using EventHorizon.RocketMQ.Remoting.Consumer.Coordination;
using Xunit;

namespace EventHorizon.RocketMQ.Remoting.Tests.Consumer.Coordination;

public sealed class LegacyHeartbeatProtocolTests
{
    [Theory]
    [InlineData("CLUSTERING")]
    [InlineData("BROADCASTING")]
    public void Encode_WireValues_UsesConfiguredValues(string messageModel)
    {
        var body = LegacyHeartbeatProtocol.Encode(
            "client-a",
            "group-a",
            new Dictionary<string, FilterExpression> { ["orders"] = FilterExpression.All },
            123,
            "CONSUME_PASSIVELY",
            messageModel,
            "CONSUME_FROM_TIMESTAMP",
            false);

        using var heartbeat = JsonDocument.Parse(body);
        var consumerData = Assert.Single(
            heartbeat.RootElement.GetProperty("consumerDataSet").EnumerateArray());
        Assert.Equal("CONSUME_PASSIVELY", consumerData.GetProperty("consumeType").GetString());
        Assert.Equal(messageModel, consumerData.GetProperty("messageModel").GetString());
        Assert.Equal("CONSUME_FROM_TIMESTAMP", consumerData.GetProperty("consumeFromWhere").GetString());
    }
}
