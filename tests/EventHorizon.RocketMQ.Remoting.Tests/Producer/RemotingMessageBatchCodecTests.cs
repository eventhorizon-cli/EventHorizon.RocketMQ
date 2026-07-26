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

using System.Buffers.Binary;
using System.Text;
using EventHorizon.RocketMQ.Remoting.Producer;
using EventHorizon.RocketMQ.Remoting.Protocol;
using Xunit;

namespace EventHorizon.RocketMQ.Remoting.Tests.Producer;

public sealed class RemotingMessageBatchCodecTests
{
    [Fact]
    public void Encode_UsesClassicMiniMessageRecords()
    {
        var first = new Message("orders", "first"u8.ToArray());
        first.Properties["UNIQ_KEY"] = "first-id";
        first.Properties["TAGS"] = "created";
        var second = new Message("orders", "second"u8.ToArray());
        second.Properties["UNIQ_KEY"] = "second-id";

        var encoded = RemotingMessageBatchCodec.Encode(
            [first, second],
            static message => message.Properties);

        var expectedFirstProperties = MessagePropertyCodec.Serialize(first.Properties);
        var expectedFirstPropertiesLength = Encoding.UTF8.GetByteCount(expectedFirstProperties);
        var firstLength = BinaryPrimitives.ReadInt32BigEndian(encoded);
        Assert.Equal(4 + 4 + 4 + 4 + 4 + first.Body.Length + 2 + expectedFirstPropertiesLength, firstLength);
        Assert.Equal(0, BinaryPrimitives.ReadInt32BigEndian(encoded.AsSpan(4)));
        Assert.Equal(0, BinaryPrimitives.ReadInt32BigEndian(encoded.AsSpan(8)));
        Assert.Equal(0, BinaryPrimitives.ReadInt32BigEndian(encoded.AsSpan(12)));
        Assert.Equal(first.Body.Length, BinaryPrimitives.ReadInt32BigEndian(encoded.AsSpan(16)));
        Assert.Equal(first.Body, encoded.AsSpan(20, first.Body.Length).ToArray());
        var firstPropertiesOffset = 20 + first.Body.Length;
        var firstPropertiesLength = BinaryPrimitives.ReadInt16BigEndian(encoded.AsSpan(firstPropertiesOffset));
        Assert.Equal(expectedFirstPropertiesLength, firstPropertiesLength);
        var firstProperties = Encoding.UTF8.GetString(
            encoded,
            firstPropertiesOffset + sizeof(short),
            firstPropertiesLength);
        var decodedProperties = MessagePropertyCodec.Deserialize(firstProperties);
        Assert.Equal("created", decodedProperties["TAGS"]);
        Assert.Equal("first-id", decodedProperties["UNIQ_KEY"]);

        var secondOffset = firstLength;
        var secondLength = BinaryPrimitives.ReadInt32BigEndian(encoded.AsSpan(secondOffset));
        Assert.Equal(secondLength, encoded.Length - secondOffset);
        Assert.Equal(second.Body.Length, BinaryPrimitives.ReadInt32BigEndian(encoded.AsSpan(secondOffset + 16)));
    }
}
