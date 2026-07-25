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

namespace EventHorizon.RocketMQ.Remoting.Protocol;

internal readonly record struct LegacyOffsetMessageId(IPEndPoint BrokerEndpoint, long CommitLogOffset)
{
    public static LegacyOffsetMessageId Parse(string value)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(value);
        if (value.Length is not (32 or 56))
        {
            throw new InvalidDataException(
                "The offset message ID must contain 32 hexadecimal characters for IPv4 or 56 for IPv6.");
        }

        byte[] bytes;
        try
        {
            bytes = Convert.FromHexString(value);
        }
        catch (FormatException exception)
        {
            throw new InvalidDataException("The offset message ID is not valid hexadecimal.", exception);
        }

        var addressLength = bytes.Length - sizeof(int) - sizeof(long);
        var port = BinaryPrimitives.ReadInt32BigEndian(bytes.AsSpan(addressLength, sizeof(int)));
        if (port is < 0 or > ushort.MaxValue)
        {
            throw new InvalidDataException("The offset message ID contains an invalid broker port.");
        }

        var endpoint = new IPEndPoint(new IPAddress(bytes.AsSpan(0, addressLength)), port);
        var offset = BinaryPrimitives.ReadInt64BigEndian(bytes.AsSpan(addressLength + sizeof(int), sizeof(long)));
        return new LegacyOffsetMessageId(endpoint, offset);
    }
}
