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
using System.IO.Compression;
using System.Text;
using EventHorizon.RocketMQ.Remoting.Protocol;
using K4os.Compression.LZ4.Streams;
using Xunit;
using ZstdSharp;

namespace EventHorizon.RocketMQ.Remoting.Tests.Protocol;

public sealed class LegacyMessageDecoderTests
{
    [Fact]
    public void LegacyMessageDecoder_RocketMqV1Message_DecodesMessage()
    {
        const string topic = "tenant-a%orders";
        var body = "legacy pull"u8.ToArray();
        var properties = Encoding.UTF8.GetBytes(
            "UNIQ_KEY\u0001message-id\u0002" +
            "KEYS\u0001order-1 order-2\u0002" +
            "TAGS\u0001paid\u0002" +
            "__SHARDINGKEY\u0001customer-7\u0002");
        using var stream = new MemoryStream();
        WriteInt32(stream, 0);
        WriteInt32(stream, -626843481);
        WriteInt32(stream, 0);
        WriteInt32(stream, 3);
        WriteInt32(stream, 7);
        WriteInt64(stream, 42);
        WriteInt64(stream, 1234);
        WriteInt32(stream, 0);
        WriteInt64(stream, 1_700_000_000_000);
        stream.Write([127, 0, 0, 1, 0, 0, 0x2A, 0x9F]);
        WriteInt64(stream, 1_700_000_000_100);
        stream.Write([127, 0, 0, 1, 0, 0, 0x2A, 0x9F]);
        WriteInt32(stream, 2);
        WriteInt64(stream, 0);
        WriteInt32(stream, body.Length);
        stream.Write(body);
        stream.WriteByte((byte)Encoding.UTF8.GetByteCount(topic));
        stream.Write(Encoding.UTF8.GetBytes(topic));
        WriteUInt16(stream, checked((ushort)properties.Length));
        stream.Write(properties);

        var bytes = stream.ToArray();
        BinaryPrimitives.WriteInt32BigEndian(bytes, bytes.Length);
        var message = Assert.Single(LegacyMessageDecoder.Decode(bytes, "broker-a", "tenant-a"));

        Assert.Equal("orders", message.Topic);
        Assert.Equal(body, message.Body);
        Assert.Equal("message-id", message.MessageId);
        Assert.Equal("7F00000100002A9F00000000000004D2", message.OffsetMessageId);
        Assert.Equal("paid", message.Tag);
        Assert.Equal(new[] { "order-1", "order-2" }, message.Keys);
        Assert.Equal("customer-7", message.MessageGroup);
        Assert.Equal(3, message.DeliveryAttempt);
        Assert.Equal(42, message.QueueOffset);
        Assert.Equal(1234, message.CommitLogOffset);
        Assert.Equal("broker-a", message.BrokerName);
        Assert.Equal(DateTimeOffset.FromUnixTimeMilliseconds(1_700_000_000_000), message.BornTimestamp);
        Assert.Equal(DateTimeOffset.FromUnixTimeMilliseconds(1_700_000_000_100), message.StoreTimestamp);
    }

    [Fact]
    public void LegacyMessageDecoder_TruncatedMessage_Rejects()
    {
        Assert.Throws<InvalidDataException>(() => LegacyMessageDecoder.Decode([0, 0, 0, 20], "broker-a"));
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(3)]
    public void LegacyMessageDecoder_RocketMqCompressionTypes_DecodesCompressionTypes(int compressionType)
    {
        var body = Encoding.UTF8.GetBytes(string.Concat(Enumerable.Repeat("RocketMQ compression ", 128)));
        var storedBody = Compress(body, compressionType);
        var record = CreateRecord(storedBody, MessageSysFlag.CompressedFlag | (compressionType << 8));
        var message = Assert.Single(LegacyMessageDecoder.Decode(record, "broker-a"));

        Assert.Equal(body, message.Body);
    }

    [Fact]
    public void LegacyMessageDecoder_UnknownCompressionType_Rejects()
    {
        var record = CreateRecord([1, 2, 3], MessageSysFlag.CompressedFlag | (7 << 8));
        var exception = Assert.Throws<InvalidDataException>(() => LegacyMessageDecoder.Decode(record, "broker-a"));
        Assert.Contains("compression type 7", exception.Message, StringComparison.Ordinal);
    }

    private static byte[] Compress(byte[] body, int compressionType)
    {
        if (compressionType == 2)
        {
            using var compressor = new Compressor();
            return compressor.Wrap(body).ToArray();
        }

        using var output = new MemoryStream();
        using (Stream compressor = compressionType switch
        {
            0 or 3 => new ZLibStream(output, CompressionMode.Compress, leaveOpen: true),
            1 => LZ4Stream.Encode(output, leaveOpen: true),
            _ => throw new ArgumentOutOfRangeException(nameof(compressionType))
        })
        {
            compressor.Write(body);
        }

        return output.ToArray();
    }

    private static byte[] CreateRecord(byte[] storedBody, int sysFlag)
    {
        const string topic = "orders";
        using var stream = new MemoryStream();
        WriteInt32(stream, 0);
        WriteInt32(stream, -626843481);
        WriteInt32(stream, 0);
        WriteInt32(stream, 0);
        WriteInt32(stream, 0);
        WriteInt64(stream, 0);
        WriteInt64(stream, 0);
        WriteInt32(stream, sysFlag);
        WriteInt64(stream, 1_700_000_000_000);
        stream.Write([127, 0, 0, 1, 0, 0, 0x2A, 0x9F]);
        WriteInt64(stream, 1_700_000_000_100);
        stream.Write([127, 0, 0, 1, 0, 0, 0x2A, 0x9F]);
        WriteInt32(stream, 0);
        WriteInt64(stream, 0);
        WriteInt32(stream, storedBody.Length);
        stream.Write(storedBody);
        stream.WriteByte((byte)topic.Length);
        stream.Write(Encoding.UTF8.GetBytes(topic));
        WriteUInt16(stream, 0);

        var record = stream.ToArray();
        BinaryPrimitives.WriteInt32BigEndian(record, record.Length);
        return record;
    }

    private static void WriteInt32(Stream stream, int value)
    {
        Span<byte> bytes = stackalloc byte[sizeof(int)];
        BinaryPrimitives.WriteInt32BigEndian(bytes, value);
        stream.Write(bytes);
    }

    private static void WriteInt64(Stream stream, long value)
    {
        Span<byte> bytes = stackalloc byte[sizeof(long)];
        BinaryPrimitives.WriteInt64BigEndian(bytes, value);
        stream.Write(bytes);
    }

    private static void WriteUInt16(Stream stream, ushort value)
    {
        Span<byte> bytes = stackalloc byte[sizeof(ushort)];
        BinaryPrimitives.WriteUInt16BigEndian(bytes, value);
        stream.Write(bytes);
    }
}
