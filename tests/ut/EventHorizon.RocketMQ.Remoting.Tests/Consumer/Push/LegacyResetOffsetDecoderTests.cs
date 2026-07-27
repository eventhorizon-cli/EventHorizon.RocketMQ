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
using EventHorizon.RocketMQ.Remoting.Consumer.Push;
using Xunit;

namespace EventHorizon.RocketMQ.Remoting.Tests.Consumer.Push;

public sealed class LegacyResetOffsetDecoderTests
{
    [Fact]
    public void Decode_ReturnsEmptyForMissingOrNullOffsetTable()
    {
        Assert.Empty(LegacyResetOffsetDecoder.Decode(null));
        Assert.Empty(LegacyResetOffsetDecoder.Decode([]));
        Assert.Empty(Decode("""{"offsetTable":null}"""));
    }

    [Fact]
    public void Decode_DecodesStringKeyedObjectAndNumericStrings()
    {
        var offsets = Decode(
            """
            {"offsetTable":{"{\"topic\":\"orders\",\"brokerName\":\"broker-a\",\"queueId\":\"2\"}":"9"}}
            """);

        Assert.Equal(new LegacyResetOffset("orders", "broker-a", 2, 9), Assert.Single(offsets));
    }

    [Fact]
    public void Decode_DecodesFastJsonEmptyMapAndEscapedStringValues()
    {
        Assert.Empty(Decode("""{"offsetTable":{},}"""));

        var offsets = Decode(
            """
            {"offsetTable":{{"topic":"ord\"ers","brokerName":"broker-a","queueId":"2"}:"9"}}
            """);

        Assert.Equal(new LegacyResetOffset("ord\"ers", "broker-a", 2, 9), Assert.Single(offsets));
    }

    [Theory]
    [InlineData("{}")]
    [InlineData("{\"offsetTable\":1}")]
    [InlineData("{\"offsetTable\":[1]}")]
    [InlineData("{\"offsetTable\":[[1,7]]}")]
    [InlineData("{\"offsetTable\":[{\"brokerName\":\"broker-a\",\"queueId\":0,\"offset\":7}]}")]
    [InlineData("{\"offsetTable\":[{\"topic\":\"orders\",\"brokerName\":\"broker-a\",\"queueId\":-1,\"offset\":7}]}")]
    [InlineData("{\"offsetTable\":[{\"topic\":\"orders\",\"brokerName\":\"broker-a\",\"queueId\":0,\"offset\":-1}]}")]
    [InlineData("{\"offsetTable\":[[{\"topic\":\"orders\",\"brokerName\":\"broker-a\",\"queueId\":0},7],[{\"topic\":\"orders\",\"brokerName\":\"broker-a\",\"queueId\":0},8]]}")]
    [InlineData("{\"offsetTable\":{\"not-json\":7}}")]
    [InlineData("not-json")]
    [InlineData("{\"offsetTable\":x}")]
    [InlineData("{\"offsetTable\":{x:7}}")]
    [InlineData("{\"offsetTable\":{{\"topic\":\"orders\"")]
    [InlineData("{\"offsetTable\":{{\"topic\":\"orders\",\"brokerName\":\"broker-a\",\"queueId\":0}:}}")]
    [InlineData("{\"offsetTable\":{{\"topic\":\"orders\",\"brokerName\":\"broker-a\",\"queueId\":0}:\"7\";}}")]
    [InlineData("{\"offsetTable\":{{\"topic\":\"orders\",\"brokerName\":\"broker-a\",\"queueId\":0}:\"7}}")]
    public void Decode_RejectsInvalidPayloads(string json)
    {
        Assert.Throws<InvalidDataException>(() => Decode(json));
    }

    [Fact]
    public void Decode_RejectsInvalidUtf8()
    {
        Assert.Throws<InvalidDataException>(() => LegacyResetOffsetDecoder.Decode([0xff]));
    }

    private static IReadOnlyList<LegacyResetOffset> Decode(string json) =>
        LegacyResetOffsetDecoder.Decode(Encoding.UTF8.GetBytes(json));
}
