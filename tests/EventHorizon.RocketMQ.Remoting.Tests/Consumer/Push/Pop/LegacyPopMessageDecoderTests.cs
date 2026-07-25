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
using EventHorizon.RocketMQ.Remoting.Consumer.Pull;
using EventHorizon.RocketMQ.Remoting.Protocol;
using Xunit;

namespace EventHorizon.RocketMQ.Remoting.Tests.Consumer.Push.Pop;

public sealed class LegacyPopMessageDecoderTests
{
    private const string Checkpoint = "40 1700000000000 60000 2 0 broker-a 3 42";

    [Fact]
    public void DecodeMessage_CreatesReceiptForMatchingPhysicalQueue()
    {
        var queue = CreateQueue();
        var message = CreateMessage(Checkpoint);

        var popMessage = global::EventHorizon.RocketMQ.Remoting.Consumer.Push.Pop.LegacyPopMessageDecoder.DecodeMessage(
            message,
            queue);

        Assert.Same(message, popMessage.Message);
        Assert.Equal("orders", popMessage.Receipt.Topic);
        Assert.Equal("broker-a", popMessage.Receipt.BrokerName);
        Assert.Equal("127.0.0.1:10911", popMessage.Receipt.BrokerAddress);
        Assert.Equal(3, popMessage.Receipt.QueueId);
        Assert.Equal(40, popMessage.Receipt.CheckpointOffset);
        Assert.Equal(42, popMessage.Receipt.QueueOffset);
        Assert.Equal(DateTimeOffset.FromUnixTimeMilliseconds(1_700_000_000_000), popMessage.Receipt.PopTime);
        Assert.Equal(TimeSpan.FromSeconds(60), popMessage.Receipt.InvisibleTime);
        Assert.Equal(2, popMessage.Receipt.ReviveQueueId);
        Assert.Equal(Checkpoint, popMessage.Receipt.ExtraInfo);
    }

    [Fact]
    public void DecodeMessage_UsesTheCapturedBrokerAddress()
    {
        var queue = new RemotingPullMessageQueue(
            "orders",
            3,
            "broker-a",
            new Dictionary<long, string>
            {
                [0] = "127.0.0.1:10911",
                [1] = "127.0.0.1:10912"
            });
        queue.SetPreferredBrokerId(1);

        var popMessage = global::EventHorizon.RocketMQ.Remoting.Consumer.Push.Pop.LegacyPopMessageDecoder.DecodeMessage(
            CreateMessage(Checkpoint),
            queue,
            "127.0.0.1:10911");

        Assert.Equal("127.0.0.1:10911", popMessage.Receipt.BrokerAddress);
    }

    [Fact]
    public void ReceiptRenew_ReplacesOnlyBrokerControlledCheckpointFields()
    {
        var receipt = global::EventHorizon.RocketMQ.Remoting.Consumer.Push.Pop.LegacyPopMessageDecoder.DecodeMessage(
            CreateMessage(Checkpoint),
            CreateQueue()).Receipt;

        var renewed = receipt.Renew(1_700_000_120_000, 120_000, 5);

        Assert.Equal(receipt.Topic, renewed.Topic);
        Assert.Equal(receipt.BrokerName, renewed.BrokerName);
        Assert.Equal(receipt.BrokerAddress, renewed.BrokerAddress);
        Assert.Equal(receipt.QueueId, renewed.QueueId);
        Assert.Equal(42, renewed.CheckpointOffset);
        Assert.Equal(receipt.QueueOffset, renewed.QueueOffset);
        Assert.Equal(DateTimeOffset.FromUnixTimeMilliseconds(1_700_000_120_000), renewed.PopTime);
        Assert.Equal(TimeSpan.FromSeconds(120), renewed.InvisibleTime);
        Assert.Equal(5, renewed.ReviveQueueId);
        Assert.Equal("42 1700000120000 120000 5 0 broker-a 3 42", renewed.ExtraInfo);
    }

    [Theory]
    [InlineData("40 1700000000000 60000 2 1 broker-a 3 42")]
    [InlineData("40 1700000000000 60000 2 0 broker-b 3 42")]
    [InlineData("40 1700000000000 60000 2 0 broker-a 4 42")]
    [InlineData("40 1700000000000 60000 2 0 broker-a 3 41")]
    [InlineData("43 1700000000000 60000 2 0 broker-a 3 42")]
    [InlineData("40 1700000000000 60000 2 0 broker-a 3")]
    public void DecodeMessage_RejectsUnsupportedOrMalformedCheckpoint(string checkpoint)
    {
        var exception = Assert.Throws<InvalidDataException>(() =>
            global::EventHorizon.RocketMQ.Remoting.Consumer.Push.Pop.LegacyPopMessageDecoder.DecodeMessage(
                CreateMessage(checkpoint),
                CreateQueue()));

        Assert.Contains("POP_CK", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void DecodeMessage_RejectsMissingCheckpoint()
    {
        var exception = Assert.Throws<InvalidDataException>(() =>
            global::EventHorizon.RocketMQ.Remoting.Consumer.Push.Pop.LegacyPopMessageDecoder.DecodeMessage(
                CreateMessage(checkpoint: null),
                CreateQueue()));

        Assert.Contains("POP_CK", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Decode_SynthesizesReceiptForAnOrdinaryMessageWithoutCheckpoint()
    {
        var result = DecodeSuccess(CreateNormalResponseFields());

        var receipt = Assert.Single(result.Messages).Receipt;
        Assert.Equal(global::EventHorizon.RocketMQ.Remoting.Consumer.Push.Pop.RemotingPopStatus.Found, result.Status);
        Assert.Equal("127.0.0.1:10912", receipt.BrokerAddress);
        Assert.Equal(42, receipt.CheckpointOffset);
        Assert.Equal(42, receipt.QueueOffset);
        Assert.Equal(DateTimeOffset.FromUnixTimeMilliseconds(1_700_000_000_000), receipt.PopTime);
        Assert.Equal(TimeSpan.FromSeconds(60), receipt.InvisibleTime);
        Assert.Equal(2, receipt.ReviveQueueId);
        Assert.Equal("42 1700000000000 60000 2 0 broker-a 3 42", receipt.ExtraInfo);
    }

    [Fact]
    public void Decode_SynthesizesReceiptWhenStartOffsetInfoIsEmpty()
    {
        var fields = CreateNormalResponseFields();
        fields["startOffsetInfo"] = string.Empty;

        var receipt = Assert.Single(DecodeSuccess(fields).Messages).Receipt;

        Assert.Equal(42, receipt.CheckpointOffset);
        Assert.Equal(42, receipt.QueueOffset);
    }

    [Theory]
    [InlineData("popTime", "invalid")]
    [InlineData("invisibleTime", "0")]
    [InlineData("reviveQid", "-1")]
    public void Decode_RejectsMalformedSynthesizedReceiptMetadata(string name, string value)
    {
        var fields = CreateNormalResponseFields();
        fields[name] = value;

        var exception = Assert.Throws<InvalidDataException>(() => DecodeSuccess(fields));

        Assert.Contains(name, exception.Message, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("popTime")]
    [InlineData("invisibleTime")]
    [InlineData("reviveQid")]
    public void Decode_RejectsMissingSynthesizedReceiptMetadata(string name)
    {
        var fields = CreateNormalResponseFields();
        fields.Remove(name);

        var exception = Assert.Throws<InvalidDataException>(() => DecodeSuccess(fields));

        Assert.Contains(name, exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Decode_UsesPhysicalOffsetMappingsToSynthesizeReceipts()
    {
        var fields = CreateNormalResponseFields();
        fields["startOffsetInfo"] = "1 0 99;0 3 40";
        fields["msgOffsetInfo"] = "1 0 100;0 3 41,42";

        var result = DecodeSuccess(fields, body: CreateEncodedMessages(41, 42));

        Assert.Equal(2, result.Messages.Count);
        Assert.Equal(40, result.Messages[0].Receipt.CheckpointOffset);
        Assert.Equal(41, result.Messages[0].Receipt.QueueOffset);
        Assert.Equal("40 1700000000000 60000 2 0 broker-a 3 41", result.Messages[0].Receipt.ExtraInfo);
        Assert.Equal(40, result.Messages[1].Receipt.CheckpointOffset);
        Assert.Equal(42, result.Messages[1].Receipt.QueueOffset);
        Assert.Equal("40 1700000000000 60000 2 0 broker-a 3 42", result.Messages[1].Receipt.ExtraInfo);
    }

    [Fact]
    public void Decode_RejectsIncompletePhysicalOffsetMapping()
    {
        var fields = CreateNormalResponseFields();
        fields["startOffsetInfo"] = "0 3 40";

        var exception = Assert.Throws<InvalidDataException>(() => DecodeSuccess(fields));

        Assert.Contains("msgOffsetInfo", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Decode_RejectsPhysicalOffsetMappingThatDoesNotMatchMessages()
    {
        var fields = CreateNormalResponseFields();
        fields["startOffsetInfo"] = "0 3 40";
        fields["msgOffsetInfo"] = "0 3 43";

        var exception = Assert.Throws<InvalidDataException>(() => DecodeSuccess(fields));

        Assert.Contains("msgOffsetInfo", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Decode_RejectsPhysicalOffsetMappingWithCheckpointAfterMessage()
    {
        var fields = CreateNormalResponseFields();
        fields["startOffsetInfo"] = "0 3 43";
        fields["msgOffsetInfo"] = "0 3 42";

        var exception = Assert.Throws<InvalidDataException>(() => DecodeSuccess(fields));

        Assert.Contains("startOffsetInfo", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Decode_RejectsSynthesizedReceiptForRetryMessage()
    {
        var exception = Assert.Throws<InvalidDataException>(() =>
            DecodeSuccess(CreateNormalResponseFields(), retryTopic: "orders"));

        Assert.Contains("POP_CK", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Decode_RejectsSynthesizedReceiptForLogicalQueueMessage()
    {
        var queue = CreateQueue("LOGICAL_QUEUE_MOCK_BROKER_orders");

        var exception = Assert.Throws<InvalidDataException>(() =>
            DecodeSuccess(CreateNormalResponseFields(), queue: queue));

        Assert.Contains("POP_CK", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Decode_MapsPollingNotFoundAndRestNum()
    {
        var result = global::EventHorizon.RocketMQ.Remoting.Consumer.Push.Pop.LegacyPopMessageDecoder.Decode(
            new RemotingCommand
            {
                Code = ResponseCodes.ResPullNotFound,
                ExtFields = new Dictionary<string, object> { ["restNum"] = "9" }
            },
            CreateQueue());

        Assert.Equal(global::EventHorizon.RocketMQ.Remoting.Consumer.Push.Pop.RemotingPopStatus.NoNewMessage, result.Status);
        Assert.Empty(result.Messages);
        Assert.Equal(9, result.RestNum);
    }

    [Fact]
    public void Decode_RejectsInvalidRestNum()
    {
        var exception = Assert.Throws<InvalidDataException>(() =>
            global::EventHorizon.RocketMQ.Remoting.Consumer.Push.Pop.LegacyPopMessageDecoder.Decode(
                new RemotingCommand
                {
                    Code = ResponseCodes.ResPullNotFound,
                    ExtFields = new Dictionary<string, object> { ["restNum"] = "-1" }
                },
                CreateQueue()));

        Assert.Contains("restNum", exception.Message, StringComparison.Ordinal);
    }

    private static global::EventHorizon.RocketMQ.Remoting.Consumer.Push.Pop.RemotingPopResult DecodeSuccess(
        Dictionary<string, object> fields,
        string? retryTopic = null,
        RemotingPullMessageQueue? queue = null,
        byte[]? body = null)
    {
        queue ??= CreateQueue();
        return global::EventHorizon.RocketMQ.Remoting.Consumer.Push.Pop.LegacyPopMessageDecoder.Decode(
            new RemotingCommand
            {
                Code = ResponseCodes.ResSuccess,
                Body = body ?? CreateEncodedMessage(retryTopic),
                ExtFields = fields
            },
            queue,
            "127.0.0.1:10912",
            @namespace: null);
    }

    private static Dictionary<string, object> CreateNormalResponseFields() =>
        new()
        {
            ["popTime"] = "1700000000000",
            ["invisibleTime"] = "60000",
            ["reviveQid"] = "2"
        };

    private static RemotingPullMessageQueue CreateQueue(string brokerName = "broker-a") =>
        new("orders", 3, brokerName, "127.0.0.1:10911");

    private static RemotingMessageView CreateMessage(string? checkpoint) =>
        new(
            "orders",
            [1, 2, 3],
            "message-id",
            "offset-message-id",
            null,
            Array.Empty<string>(),
            checkpoint is null
                ? new Dictionary<string, string>(StringComparer.Ordinal)
                : new Dictionary<string, string>(StringComparer.Ordinal) { ["POP_CK"] = checkpoint },
            1,
            null,
            3,
            "broker-a",
            42,
            100,
            DateTimeOffset.FromUnixTimeMilliseconds(1_700_000_000_000),
            DateTimeOffset.FromUnixTimeMilliseconds(1_700_000_000_001));

    private static byte[] CreateEncodedMessages(params long[] queueOffsets)
    {
        using var stream = new MemoryStream();
        foreach (var queueOffset in queueOffsets)
        {
            stream.Write(CreateEncodedMessage(retryTopic: null, queueOffset: queueOffset));
        }

        return stream.ToArray();
    }

    private static byte[] CreateEncodedMessage(string? retryTopic, long queueOffset = 42)
    {
        const string topic = "orders";
        var properties = new StringBuilder("UNIQ_KEY\u0001message-id\u0002");
        if (retryTopic is not null)
        {
            properties.Append("RETRY_TOPIC\u0001");
            properties.Append(retryTopic);
            properties.Append('\u0002');
        }

        var encodedProperties = Encoding.UTF8.GetBytes(properties.ToString());
        using var stream = new MemoryStream();
        WriteInt32(stream, 0);
        WriteInt32(stream, -626843481);
        WriteInt32(stream, 0);
        WriteInt32(stream, 3);
        WriteInt32(stream, 0);
        WriteInt64(stream, queueOffset);
        WriteInt64(stream, 100);
        WriteInt32(stream, 0);
        WriteInt64(stream, 1_700_000_000_000);
        stream.Write([127, 0, 0, 1, 0x2A, 0x9F, 0, 0]);
        WriteInt64(stream, 1_700_000_000_001);
        stream.Write([127, 0, 0, 1, 0x2A, 0x9F, 0, 0]);
        WriteInt32(stream, 0);
        WriteInt64(stream, 0);
        WriteInt32(stream, 0);
        stream.WriteByte((byte)Encoding.UTF8.GetByteCount(topic));
        stream.Write(Encoding.UTF8.GetBytes(topic));
        WriteUInt16(stream, checked((ushort)encodedProperties.Length));
        stream.Write(encodedProperties);

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
