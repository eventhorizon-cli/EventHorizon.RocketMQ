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

namespace EventHorizon.RocketMQ.Remoting.Tests.Consumer.Push.Pop;

public sealed class PopRequestHeaderTests
{
    [Fact]
    public void PopMessageRequestHeader_EncodesClassicPopFields()
    {
        var fields = new global::EventHorizon.RocketMQ.Remoting.Consumer.Push.Pop.PopMessageRequestHeader
        {
            ConsumerGroup = "pop-group",
            Topic = "orders",
            QueueId = 3,
            MaxMsgNums = 16,
            InvisibleTime = 60_000,
            PollTime = 15_000,
            BornTime = 1_700_000_000_000,
            InitMode = 1,
            ExpType = "SQL92",
            Exp = "priority >= 5",
            Order = false,
            AttemptId = "attempt-1",
            Bname = "broker-a"
        }.Encode();

        Assert.Equal("pop-group", fields["consumerGroup"]);
        Assert.Equal("orders", fields["topic"]);
        Assert.Equal(3, fields["queueId"]);
        Assert.Equal(16, fields["maxMsgNums"]);
        Assert.Equal(60_000L, fields["invisibleTime"]);
        Assert.Equal(15_000L, fields["pollTime"]);
        Assert.Equal(1_700_000_000_000L, fields["bornTime"]);
        Assert.Equal(1, fields["initMode"]);
        Assert.Equal("SQL92", fields["expType"]);
        Assert.Equal("priority >= 5", fields["exp"]);
        Assert.Equal(false, fields["order"]);
        Assert.Equal("attempt-1", fields["attemptId"]);
        Assert.Equal("broker-a", fields["bname"]);
    }

    [Fact]
    public void AckMessageRequestHeader_EncodesReceiptFields()
    {
        var fields = new global::EventHorizon.RocketMQ.Remoting.Consumer.Push.Pop.AckMessageRequestHeader
        {
            ConsumerGroup = "pop-group",
            Topic = "orders",
            QueueId = 3,
            Offset = 42,
            ExtraInfo = "40 1700000000000 60000 2 0 broker-a 3 42",
            Bname = "broker-a"
        }.Encode();

        Assert.Equal("pop-group", fields["consumerGroup"]);
        Assert.Equal("orders", fields["topic"]);
        Assert.Equal(3, fields["queueId"]);
        Assert.Equal(42L, fields["offset"]);
        Assert.Equal("40 1700000000000 60000 2 0 broker-a 3 42", fields["extraInfo"]);
        Assert.Equal("broker-a", fields["bname"]);
    }

    [Fact]
    public void ChangeInvisibleTimeRequestHeader_EncodesReceiptAndDurationFields()
    {
        var fields = new global::EventHorizon.RocketMQ.Remoting.Consumer.Push.Pop.ChangeInvisibleTimeRequestHeader
        {
            ConsumerGroup = "pop-group",
            Topic = "orders",
            QueueId = 3,
            Offset = 42,
            ExtraInfo = "40 1700000000000 60000 2 0 broker-a 3 42",
            InvisibleTime = 120_000,
            Bname = "broker-a"
        }.Encode();

        Assert.Equal("pop-group", fields["consumerGroup"]);
        Assert.Equal("orders", fields["topic"]);
        Assert.Equal(3, fields["queueId"]);
        Assert.Equal(42L, fields["offset"]);
        Assert.Equal("40 1700000000000 60000 2 0 broker-a 3 42", fields["extraInfo"]);
        Assert.Equal(120_000L, fields["invisibleTime"]);
        Assert.Equal("broker-a", fields["bname"]);
    }
}
