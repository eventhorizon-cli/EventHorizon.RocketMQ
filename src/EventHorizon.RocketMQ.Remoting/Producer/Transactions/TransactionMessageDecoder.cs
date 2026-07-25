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
using EventHorizon.RocketMQ.Remoting.Protocol;

namespace EventHorizon.RocketMQ.Remoting.Producer.Transactions;

internal static class TransactionMessageDecoder
{
    private const int MessageMagicCodeV1 = -626843481;
    private const int MessageMagicCodeV2 = -626843477;

    public static RemotingTransactionMessage Decode(
        byte[]? body,
        string? transactionId,
        string fallbackMessageId,
        string? @namespace)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(fallbackMessageId);
        if (body is not { Length: > 0 })
        {
            throw new InvalidDataException("The transaction check did not contain a message body.");
        }

        var reader = new SpanReader(body);
        var totalSize = reader.ReadInt32();
        if (totalSize != body.Length)
        {
            throw new InvalidDataException(
                $"The transaction check contains a message size of {totalSize}, but its body is {body.Length} bytes.");
        }

        var magicCode = reader.ReadInt32();
        if (magicCode is not (MessageMagicCodeV1 or MessageMagicCodeV2))
        {
            throw new InvalidDataException($"The transaction check has unknown message magic code {magicCode}.");
        }

        _ = reader.ReadInt32(); // body CRC
        _ = reader.ReadInt32(); // queue ID
        _ = reader.ReadInt32(); // application flag
        _ = reader.ReadInt64(); // queue offset
        _ = reader.ReadInt64(); // physical offset
        var sysFlag = reader.ReadInt32();
        var bornTimestamp = DateTimeOffset.FromUnixTimeMilliseconds(reader.ReadInt64());
        reader.Skip((sysFlag & MessageSysFlag.BornhostV6Flag) == 0 ? 8 : 20);
        _ = reader.ReadInt64(); // store timestamp
        reader.Skip((sysFlag & MessageSysFlag.StorehostaddressV6Flag) == 0 ? 8 : 20);
        _ = reader.ReadInt32(); // reconsume times
        _ = reader.ReadInt64(); // prepared transaction offset

        var bodyLength = reader.ReadInt32();
        if (bodyLength < 0)
        {
            throw new InvalidDataException($"The transaction check has an invalid body size of {bodyLength} bytes.");
        }

        var messageBody = reader.ReadBytes(bodyLength).ToArray();
        messageBody = MessageBodyCodec.Decompress(messageBody, sysFlag);

        var topicLength = magicCode == MessageMagicCodeV2 ? reader.ReadUInt16() : reader.ReadByte();
        var topic = Encoding.UTF8.GetString(reader.ReadBytes(topicLength));
        var propertiesLength = reader.ReadUInt16();
        var properties = MessagePropertyCodec.Deserialize(Encoding.UTF8.GetString(reader.ReadBytes(propertiesLength)));
        if (reader.Remaining != 0)
        {
            throw new InvalidDataException(
                $"The transaction check contains {reader.Remaining} unexpected trailing message bytes.");
        }

        var messageId = Get(properties, "UNIQ_KEY") ?? fallbackMessageId;
        var keys = (Get(properties, "KEYS") ?? string.Empty)
            .Split(' ', StringSplitOptions.RemoveEmptyEntries);
        return new RemotingTransactionMessage(
            transactionId,
            messageId,
            LegacyNamespace.WithoutNamespace(@namespace, topic),
            messageBody,
            Get(properties, "TAGS"),
            keys,
            properties,
            bornTimestamp);
    }

    private static string? Get(IReadOnlyDictionary<string, string> properties, string name) =>
        properties.TryGetValue(name, out var value) ? value : null;

    private ref struct SpanReader
    {
        private ReadOnlySpan<byte> _remaining;

        public SpanReader(ReadOnlySpan<byte> input) => _remaining = input;

        public int Remaining => _remaining.Length;

        public byte ReadByte()
        {
            Ensure(sizeof(byte));
            var value = _remaining[0];
            _remaining = _remaining[1..];
            return value;
        }

        public ushort ReadUInt16()
        {
            Ensure(sizeof(ushort));
            var value = BinaryPrimitives.ReadUInt16BigEndian(_remaining);
            _remaining = _remaining[sizeof(ushort)..];
            return value;
        }

        public int ReadInt32()
        {
            Ensure(sizeof(int));
            var value = BinaryPrimitives.ReadInt32BigEndian(_remaining);
            _remaining = _remaining[sizeof(int)..];
            return value;
        }

        public long ReadInt64()
        {
            Ensure(sizeof(long));
            var value = BinaryPrimitives.ReadInt64BigEndian(_remaining);
            _remaining = _remaining[sizeof(long)..];
            return value;
        }

        public ReadOnlySpan<byte> ReadBytes(int length)
        {
            Ensure(length);
            var value = _remaining[..length];
            _remaining = _remaining[length..];
            return value;
        }

        public void Skip(int length) => _ = ReadBytes(length);

        private readonly void Ensure(int length)
        {
            if (length < 0 || _remaining.Length < length)
            {
                throw new InvalidDataException("The transaction check message is truncated.");
            }
        }
    }
}
