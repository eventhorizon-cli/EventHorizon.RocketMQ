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
using System.Net;
using EventHorizon.RocketMQ.Remoting.Protocol;
using Xunit;

namespace EventHorizon.RocketMQ.Remoting.Tests.Protocol;

public sealed class LegacyOffsetMessageIdTests
{
    [Fact]
    public void Parse_DecodesIpv4BrokerEndpointAndCommitLogOffset()
    {
        var value = CreateOffsetMessageId(IPAddress.Parse("192.0.2.10"), 10911, 1234).ToLowerInvariant();

        var messageId = LegacyOffsetMessageId.Parse(value);

        Assert.Equal(IPAddress.Parse("192.0.2.10"), messageId.BrokerEndpoint.Address);
        Assert.Equal(10911, messageId.BrokerEndpoint.Port);
        Assert.Equal(1234, messageId.CommitLogOffset);
    }

    [Fact]
    public void Parse_DecodesIpv6BrokerEndpointAndCommitLogOffset()
    {
        var value = CreateOffsetMessageId(IPAddress.Parse("2001:db8::1"), 10911, -42);

        var messageId = LegacyOffsetMessageId.Parse(value);

        Assert.Equal(IPAddress.Parse("2001:db8::1"), messageId.BrokerEndpoint.Address);
        Assert.Equal(10911, messageId.BrokerEndpoint.Port);
        Assert.Equal(-42, messageId.CommitLogOffset);
    }

    [Theory]
    [InlineData("0000000000000000000000000000000")]
    [InlineData("GG000000000000000000000000000000")]
    public void Parse_RejectsMalformedIdentifier(string value)
    {
        Assert.Throws<InvalidDataException>(() => LegacyOffsetMessageId.Parse(value));
    }

    [Fact]
    public void Parse_RejectsPortOutsideSocketRange()
    {
        var bytes = new byte[16];
        BinaryPrimitives.WriteInt32BigEndian(bytes.AsSpan(4), 65536);

        Assert.Throws<InvalidDataException>(() => LegacyOffsetMessageId.Parse(Convert.ToHexString(bytes)));
    }

    private static string CreateOffsetMessageId(IPAddress address, int port, long offset)
    {
        var addressBytes = address.GetAddressBytes();
        var bytes = new byte[addressBytes.Length + sizeof(int) + sizeof(long)];
        addressBytes.CopyTo(bytes, 0);
        BinaryPrimitives.WriteInt32BigEndian(bytes.AsSpan(addressBytes.Length), port);
        BinaryPrimitives.WriteInt64BigEndian(bytes.AsSpan(addressBytes.Length + sizeof(int)), offset);
        return Convert.ToHexString(bytes);
    }
}
