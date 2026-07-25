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
using EventHorizon.RocketMQ.Remoting.Consumer;

namespace EventHorizon.RocketMQ.Remoting.Protocol;

internal static class LegacyMessageDecoder
{
    private const int MessageMagicCodeV1 = -626843481;
    private const int MessageMagicCodeV2 = -626843477;

    public static IReadOnlyList<RemotingMessageView> Decode(
        ReadOnlySpan<byte> input,
        string? brokerName,
        string? @namespace = null)
    {
        var messages = new List<RemotingMessageView>();
        while (!input.IsEmpty)
        {
            if (input.Length < sizeof(int))
            {
                throw new InvalidDataException("The RocketMQ message response ended before the message size field.");
            }

            var totalSize = BinaryPrimitives.ReadInt32BigEndian(input);
            if (totalSize < sizeof(int) * 2 || totalSize > input.Length)
            {
                throw new InvalidDataException($"The RocketMQ message has an invalid total size of {totalSize} bytes.");
            }

            messages.Add(DecodeMessage(input[..totalSize], brokerName, @namespace));
            input = input[totalSize..];
        }

        return messages;
    }

    public static RemotingMessageView DecodeSingle(
        ReadOnlySpan<byte> input,
        string? brokerName,
        string? @namespace = null)
    {
        var messages = Decode(input, brokerName, @namespace);
        return messages.Count == 1
            ? messages[0]
            : throw new InvalidDataException("The RocketMQ response must contain exactly one message.");
    }

    private static RemotingMessageView DecodeMessage(
        ReadOnlySpan<byte> record,
        string? brokerName,
        string? @namespace)
    {
        var reader = new SpanReader(record);
        _ = reader.ReadInt32(); // total size
        var magicCode = reader.ReadInt32();
        if (magicCode is not (MessageMagicCodeV1 or MessageMagicCodeV2))
        {
            throw new InvalidDataException($"The RocketMQ message has unknown magic code {magicCode}.");
        }

        _ = reader.ReadInt32(); // body CRC
        var queueId = reader.ReadInt32();
        _ = reader.ReadInt32(); // application flag
        var queueOffset = reader.ReadInt64();
        var physicalOffset = reader.ReadInt64();
        var sysFlag = reader.ReadInt32();
        var bornTimestamp = DateTimeOffset.FromUnixTimeMilliseconds(reader.ReadInt64());
        reader.Skip((sysFlag & MessageSysFlag.BornhostV6Flag) == 0 ? 8 : 20);
        var storeTimestamp = DateTimeOffset.FromUnixTimeMilliseconds(reader.ReadInt64());
        var storeHost = reader.ReadBytes((sysFlag & MessageSysFlag.StorehostaddressV6Flag) == 0 ? 8 : 20);
        var deliveryAttempt = checked(reader.ReadInt32() + 1);
        _ = reader.ReadInt64(); // prepared transaction offset

        var bodyLength = reader.ReadInt32();
        if (bodyLength < 0)
        {
            throw new InvalidDataException($"The RocketMQ message has an invalid body size of {bodyLength} bytes.");
        }

        var body = reader.ReadBytes(bodyLength).ToArray();
        body = MessageBodyCodec.Decompress(body, sysFlag);

        var topicLength = magicCode == MessageMagicCodeV2 ? reader.ReadUInt16() : reader.ReadByte();
        var topic = Encoding.UTF8.GetString(reader.ReadBytes(topicLength));
        var propertiesLength = reader.ReadUInt16();
        var properties = MessagePropertyCodec.Deserialize(
            Encoding.UTF8.GetString(reader.ReadBytes(propertiesLength)));
        if (reader.Remaining != 0)
        {
            throw new InvalidDataException($"The RocketMQ message contains {reader.Remaining} unexpected trailing bytes.");
        }

        var offsetMessageId = CreateOffsetMessageId(storeHost, physicalOffset);
        var messageId = Get(properties, "UNIQ_KEY") ?? offsetMessageId;
        var keys = (Get(properties, "KEYS") ?? string.Empty)
            .Split(' ', StringSplitOptions.RemoveEmptyEntries);
        var messageGroup = Get(properties, "__SHARDINGKEY") ?? Get(properties, "SHARDING_KEY");
        var deliveryTopic = LegacyNamespace.WithoutNamespace(
            @namespace,
            Get(properties, "RETRY_TOPIC") ?? topic);
        return new RemotingMessageView(
            deliveryTopic,
            body,
            messageId,
            offsetMessageId,
            Get(properties, "TAGS"),
            keys,
            properties,
            deliveryAttempt,
            messageGroup,
            queueId,
            brokerName,
            queueOffset,
            physicalOffset,
            bornTimestamp,
            storeTimestamp);
    }

    private static string? Get(IReadOnlyDictionary<string, string> properties, string name) =>
        properties.TryGetValue(name, out var value) ? value : null;

    private static string CreateOffsetMessageId(ReadOnlySpan<byte> storeHost, long physicalOffset)
    {
        var bytes = new byte[storeHost.Length + sizeof(long)];
        storeHost.CopyTo(bytes);
        BinaryPrimitives.WriteInt64BigEndian(bytes.AsSpan(storeHost.Length), physicalOffset);
        return Convert.ToHexString(bytes);
    }

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
                throw new InvalidDataException("The RocketMQ message body is truncated.");
            }
        }
    }
}
