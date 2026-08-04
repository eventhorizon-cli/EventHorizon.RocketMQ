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
using EventHorizon.RocketMQ.Remoting.Consumer.Push.Assignment;
using EventHorizon.RocketMQ.Remoting.Consumer.Push.Pop;
using EventHorizon.RocketMQ.Remoting.Exceptions;
using EventHorizon.RocketMQ.Remoting.Protocol;
using Xunit;

namespace EventHorizon.RocketMQ.Remoting.Tests.Consumer.Push.Pop;

public sealed class LegacyPopMessageDecoderTests
{
    private const string Checkpoint = "40 1700000000000 60000 2 0 broker-a 3 42";

    [Fact]
    public void DecodeMessage_ReceiptForMatchingPhysicalQueue_CreatesReceipt()
    {
        var queue = CreateQueue();
        var message = CreateMessage(Checkpoint);

        var popMessage = global::EventHorizon.RocketMQ.Remoting.Consumer.Push.Pop.LegacyPopMessageDecoder.DecodeMessage(
            message,
            queue);

        Assert.Same(message, popMessage.Message);
        Assert.Equal("orders", popMessage.Receipt.Topic);
        Assert.Equal("broker-a", popMessage.Receipt.BrokerName);
        Assert.Equal(3, popMessage.Receipt.QueueId);
        Assert.Equal(40, popMessage.Receipt.CheckpointOffset);
        Assert.Equal(42, popMessage.Receipt.QueueOffset);
        Assert.Equal(DateTimeOffset.FromUnixTimeMilliseconds(1_700_000_000_000), popMessage.Receipt.PopTime);
        Assert.Equal(TimeSpan.FromSeconds(60), popMessage.Receipt.InvisibleTime);
        Assert.Equal(2, popMessage.Receipt.ReviveQueueId);
        Assert.Equal(Checkpoint, popMessage.Receipt.ExtraInfo);
    }

    [Fact]
    public void WithChangedInvisibleTime_BrokerReturnsCheckpointFields_ReplacesBrokerFields()
    {
        var receipt = global::EventHorizon.RocketMQ.Remoting.Consumer.Push.Pop.LegacyPopMessageDecoder.DecodeMessage(
            CreateMessage(Checkpoint),
            CreateQueue()).Receipt;

        var changed = receipt.WithChangedInvisibleTime(1_700_000_120_000, 120_000, 5);

        Assert.Equal(receipt.Topic, changed.Topic);
        Assert.Equal(receipt.BrokerName, changed.BrokerName);
        Assert.Equal(receipt.QueueId, changed.QueueId);
        Assert.Equal(42, changed.CheckpointOffset);
        Assert.Equal(receipt.QueueOffset, changed.QueueOffset);
        Assert.Equal(DateTimeOffset.FromUnixTimeMilliseconds(1_700_000_120_000), changed.PopTime);
        Assert.Equal(TimeSpan.FromSeconds(120), changed.InvisibleTime);
        Assert.Equal(5, changed.ReviveQueueId);
        Assert.Equal("42 1700000120000 120000 5 0 broker-a 3 42", changed.ExtraInfo);
    }

    [Theory]
    [InlineData("40 1700000000000 60000 2 0 broker-b 3 42")]
    [InlineData("40 1700000000000 60000 2 0 broker-a 4 42")]
    [InlineData("40 1700000000000 60000 2 0 broker-a 3 41")]
    [InlineData("43 1700000000000 60000 2 0 broker-a 3 42")]
    [InlineData("40 1700000000000 60000 2 0 broker-a 3")]
    public void DecodeMessage_UnsupportedOrMalformedCheckpoint_Rejects(string checkpoint)
    {
        var exception = Assert.Throws<InvalidDataException>(() =>
            global::EventHorizon.RocketMQ.Remoting.Consumer.Push.Pop.LegacyPopMessageDecoder.DecodeMessage(
                CreateMessage(checkpoint),
                CreateQueue()));

        Assert.Contains("POP_CK", exception.Message, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("1", RemotingPopRetryTopicKind.RetryV1)]
    [InlineData("2", RemotingPopRetryTopicKind.RetryV2)]
    public void DecodeMessage_RetryTopicMessage_PreservesRetryTopicMarker(
        string marker,
        object expectedKind)
    {
        var popMessage = global::EventHorizon.RocketMQ.Remoting.Consumer.Push.Pop.LegacyPopMessageDecoder.DecodeMessage(
            CreateMessage($"40 1700000000000 60000 2 {marker} broker-a 3 42"),
            CreateQueue());

        Assert.Equal((RemotingPopRetryTopicKind)expectedKind, popMessage.Receipt.RetryTopicKind);
    }

    [Fact]
    public void WithChangedInvisibleTime_RetryTopicMessage_PreservesRetryTopicMarker()
    {
        var receipt = new RemotingPopReceipt(
            "orders",
            "broker-a",
            3,
            40,
            42,
            DateTimeOffset.FromUnixTimeMilliseconds(1_700_000_000_000),
            TimeSpan.FromSeconds(60),
            2,
            "40 1700000000000 60000 2 1 broker-a 3 42",
            RemotingPopRetryTopicKind.RetryV1);

        var changed = receipt.WithChangedInvisibleTime(1_700_000_120_000, 120_000, 5);

        Assert.Equal(RemotingPopRetryTopicKind.RetryV1, changed.RetryTopicKind);
        Assert.Equal("42 1700000120000 120000 5 1 broker-a 3 42", changed.ExtraInfo);
    }

    [Fact]
    public void DecodeMessage_MissingCheckpoint_Rejects()
    {
        var exception = Assert.Throws<InvalidDataException>(() =>
            global::EventHorizon.RocketMQ.Remoting.Consumer.Push.Pop.LegacyPopMessageDecoder.DecodeMessage(
                CreateMessage(checkpoint: null),
                CreateQueue()));

        Assert.Contains("POP_CK", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Decode_OrdinaryMessageWithoutCheckpoint_SynthesizesReceipt()
    {
        var result = DecodeSuccess(CreateNormalResponseFields());

        var receipt = Assert.Single(result.Messages).Receipt;
        Assert.Equal(global::EventHorizon.RocketMQ.Remoting.Consumer.Push.Pop.RemotingPopStatus.Found, result.Status);
        Assert.Equal(42, receipt.CheckpointOffset);
        Assert.Equal(42, receipt.QueueOffset);
        Assert.Equal(DateTimeOffset.FromUnixTimeMilliseconds(1_700_000_000_000), receipt.PopTime);
        Assert.Equal(TimeSpan.FromSeconds(60), receipt.InvisibleTime);
        Assert.Equal(2, receipt.ReviveQueueId);
        Assert.Equal("42 1700000000000 60000 2 0 broker-a 3 42", receipt.ExtraInfo);
    }

    [Fact]
    public void Decode_EmptyStartOffsetInfo_SynthesizesReceipt()
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
    public void Decode_MalformedSynthesizedReceiptMetadata_Rejects(string name, string value)
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
    public void Decode_MissingSynthesizedReceiptMetadata_Rejects(string name)
    {
        var fields = CreateNormalResponseFields();
        fields.Remove(name);

        var exception = Assert.Throws<InvalidDataException>(() => DecodeSuccess(fields));

        Assert.Contains(name, exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Decode_PhysicalOffsetMappings_SynthesizesReceipts()
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
    public void Decode_IncompletePhysicalOffsetMapping_Rejects()
    {
        var fields = CreateNormalResponseFields();
        fields["startOffsetInfo"] = "0 3 40";

        var exception = Assert.Throws<InvalidDataException>(() => DecodeSuccess(fields));

        Assert.Contains("msgOffsetInfo", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Decode_MismatchedPhysicalOffsetMapping_Rejects()
    {
        var fields = CreateNormalResponseFields();
        fields["startOffsetInfo"] = "0 3 40";
        fields["msgOffsetInfo"] = "0 3 43";

        var exception = Assert.Throws<InvalidDataException>(() => DecodeSuccess(fields));

        Assert.Contains("msgOffsetInfo", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Decode_PhysicalOffsetMappingWithCheckpointAfterMessage_Rejects()
    {
        var fields = CreateNormalResponseFields();
        fields["startOffsetInfo"] = "0 3 43";
        fields["msgOffsetInfo"] = "0 3 42";

        var exception = Assert.Throws<InvalidDataException>(() => DecodeSuccess(fields));

        Assert.Contains("startOffsetInfo", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Decode_RetryMessageHasNoCheckpoint_SynthesizesReceipt()
    {
        var fields = CreateNormalResponseFields();
        fields["startOffsetInfo"] = "1 3 40";
        fields["msgOffsetInfo"] = "1 3 42";

        var receipt = Assert.Single(DecodeSuccess(fields, retryTopic: "orders").Messages).Receipt;

        Assert.Equal(RemotingPopRetryTopicKind.RetryV1, receipt.RetryTopicKind);
        Assert.Equal("40 1700000000000 60000 2 1 broker-a 3 42", receipt.ExtraInfo);
    }

    [Fact]
    public void Decode_WildcardPopResponse_UsesEachMessagePhysicalQueue()
    {
        var fields = CreateNormalResponseFields();
        fields["startOffsetInfo"] = "0 1 10;0 3 40";
        fields["msgOffsetInfo"] = "0 1 12;0 3 42";
        var assignment = new RemotingPushAssignment(
            "orders",
            "broker-a",
            -1,
            RemotingPushReceiveMode.Pop,
            new Dictionary<string, string>());

        var result = global::EventHorizon.RocketMQ.Remoting.Consumer.Push.Pop.LegacyPopMessageDecoder.Decode(
            new RemotingCommand
            {
                Code = ResponseCodes.ResSuccess,
                Body = CreateEncodedMessages((1, 12), (3, 42)),
                ExtFields = fields
            },
            assignment,
            @namespace: null);

        Assert.Collection(
            result.Messages,
            message =>
            {
                Assert.Equal(1, message.Message.QueueId);
                Assert.Equal(1, message.Receipt.QueueId);
                Assert.Equal(10, message.Receipt.CheckpointOffset);
                Assert.Equal(12, message.Receipt.QueueOffset);
            },
            message =>
            {
                Assert.Equal(3, message.Message.QueueId);
                Assert.Equal(3, message.Receipt.QueueId);
                Assert.Equal(40, message.Receipt.CheckpointOffset);
                Assert.Equal(42, message.Receipt.QueueOffset);
            });
    }

    [Fact]
    public void Decode_SynthesizedReceiptForLogicalQueueMessage_Rejects()
    {
        var queue = CreateQueue("LOGICAL_QUEUE_MOCK_BROKER_orders");

        var exception = Assert.Throws<InvalidDataException>(() =>
            DecodeSuccess(CreateNormalResponseFields(), queue: queue));

        Assert.Contains("POP_CK", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Decode_PollingNotFoundAndRestNum_MapsPollingResult()
    {
        var result = global::EventHorizon.RocketMQ.Remoting.Consumer.Push.Pop.LegacyPopMessageDecoder.Decode(
            new RemotingCommand
            {
                Code = ResponseCodes.ResPullNotFound,
                ExtFields = new Dictionary<string, object> { ["restNum"] = "9" }
            },
            CreateQueue(),
            @namespace: null);

        Assert.Equal(global::EventHorizon.RocketMQ.Remoting.Consumer.Push.Pop.RemotingPopStatus.NoNewMessage, result.Status);
        Assert.Empty(result.Messages);
        Assert.Equal(9, result.RestNum);
    }

    [Fact]
    public void Decode_InvalidRestNum_Rejects()
    {
        var exception = Assert.Throws<InvalidDataException>(() =>
            global::EventHorizon.RocketMQ.Remoting.Consumer.Push.Pop.LegacyPopMessageDecoder.Decode(
                new RemotingCommand
                {
                    Code = ResponseCodes.ResPullNotFound,
                    ExtFields = new Dictionary<string, object> { ["restNum"] = "-1" }
                },
                CreateQueue(),
                @namespace: null));

        Assert.Contains("restNum", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Decode_PollingFullAndUnknownResponseCodes_MapsAndRejects()
    {
        var pollingFull = global::EventHorizon.RocketMQ.Remoting.Consumer.Push.Pop.LegacyPopMessageDecoder.Decode(
            new RemotingCommand { Code = ResponseCodes.ResPollingFull },
            CreateQueue(),
            @namespace: null);

        Assert.Equal(global::EventHorizon.RocketMQ.Remoting.Consumer.Push.Pop.RemotingPopStatus.PollingFull, pollingFull.Status);
        Assert.Empty(pollingFull.Messages);

        var exception = Assert.Throws<RemotingCommandException>(() =>
            global::EventHorizon.RocketMQ.Remoting.Consumer.Push.Pop.LegacyPopMessageDecoder.Decode(
                new RemotingCommand { Code = 999_999, Remark = "unsupported" },
                CreateQueue(),
                @namespace: null));

        Assert.Equal(999_999, exception.ResponseCode);
    }

    [Fact]
    public void DecodeMessages_EmptyCollectionsAndMissingArguments_HandlesAndRejects()
    {
        var queue = CreateQueue();

        Assert.Empty(global::EventHorizon.RocketMQ.Remoting.Consumer.Push.Pop.LegacyPopMessageDecoder.DecodeMessages(
            Array.Empty<RemotingMessageView>(),
            queue));
        Assert.Throws<ArgumentNullException>(() =>
            global::EventHorizon.RocketMQ.Remoting.Consumer.Push.Pop.LegacyPopMessageDecoder.DecodeMessages(
                null!,
                queue));
        Assert.Throws<ArgumentNullException>(() =>
            global::EventHorizon.RocketMQ.Remoting.Consumer.Push.Pop.LegacyPopMessageDecoder.DecodeMessages(
                Array.Empty<RemotingMessageView>(),
                null!));
    }

    [Fact]
    public void DecodeMessage_WhitespaceAndMismatchedQueues_Rejects()
    {
        var whitespace = Assert.Throws<InvalidDataException>(() =>
            global::EventHorizon.RocketMQ.Remoting.Consumer.Push.Pop.LegacyPopMessageDecoder.DecodeMessage(
                CreateMessage(" "),
                CreateQueue()));
        Assert.Contains("valid", whitespace.Message, StringComparison.OrdinalIgnoreCase);

        var mismatchedQueue = Assert.Throws<InvalidDataException>(() =>
            global::EventHorizon.RocketMQ.Remoting.Consumer.Push.Pop.LegacyPopMessageDecoder.DecodeMessage(
                CreateMessage(Checkpoint, queueId: 4),
                CreateQueue()));
        Assert.Contains("physical queue", mismatchedQueue.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Theory]
    [InlineData("popTime", "9223372036854775807")]
    [InlineData("invisibleTime", "9223372036854775807")]
    public void Decode_OutOfRangeSynthesizedReceiptValues_Rejects(string name, string value)
    {
        var fields = CreateNormalResponseFields();
        fields[name] = value;

        var exception = Assert.Throws<InvalidDataException>(() => DecodeSuccess(fields));

        Assert.Contains("out-of-range", exception.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Theory]
    [InlineData("0 3", "0 3 42")]
    [InlineData("0 3 invalid", "0 3 42")]
    [InlineData("0 3 40", "0 3")]
    [InlineData("0 3 40", "0 3 invalid")]
    [InlineData("0 3 40;0 3 40", "0 3 42")]
    [InlineData("0 3 40", "0 3 42,43")]
    public void Decode_MalformedPhysicalQueueMappings_Rejects(string startOffsetInfo, string messageOffsetInfo)
    {
        var fields = CreateNormalResponseFields();
        fields["startOffsetInfo"] = startOffsetInfo;
        fields["msgOffsetInfo"] = messageOffsetInfo;

        var exception = Assert.Throws<InvalidDataException>(() => DecodeSuccess(fields));

        Assert.Contains("mapping", exception.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Theory]
    [InlineData("40 9223372036854775807 60000 2 0 broker-a 3 42")]
    [InlineData("40 1700000000000 9223372036854775807 2 0 broker-a 3 42")]
    [InlineData("invalid 1700000000000 60000 2 0 broker-a 3 42")]
    [InlineData("40 1700000000000 60000 invalid 0 broker-a 3 42")]
    public void DecodeMessage_OutOfRangeAndNonNumericCheckpointValues_Rejects(string checkpoint)
    {
        var exception = Assert.Throws<InvalidDataException>(() =>
            global::EventHorizon.RocketMQ.Remoting.Consumer.Push.Pop.LegacyPopMessageDecoder.DecodeMessage(
                CreateMessage(checkpoint),
                CreateQueue()));

        Assert.Contains("POP_CK", exception.Message, StringComparison.Ordinal);
    }

    private static global::EventHorizon.RocketMQ.Remoting.Consumer.Push.Pop.RemotingPopResult DecodeSuccess(
        Dictionary<string, object> fields,
        string? retryTopic = null,
        RemotingConsumerQueue? queue = null,
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
            @namespace: null);
    }

    private static Dictionary<string, object> CreateNormalResponseFields() =>
        new()
        {
            ["popTime"] = "1700000000000",
            ["invisibleTime"] = "60000",
            ["reviveQid"] = "2"
        };

    private static RemotingConsumerQueue CreateQueue(string brokerName = "broker-a") =>
        new("orders", brokerName, 3);

    private static RemotingMessageView CreateMessage(string? checkpoint, int queueId = 3) =>
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
            queueId,
            "broker-a",
            42,
            100,
            DateTimeOffset.FromUnixTimeMilliseconds(1_700_000_000_000),
            DateTimeOffset.FromUnixTimeMilliseconds(1_700_000_000_001));

    private static byte[] CreateEncodedMessages(params long[] queueOffsets) =>
        CreateEncodedMessages(queueOffsets.Select(static offset => (QueueId: 3, Offset: offset)).ToArray());

    private static byte[] CreateEncodedMessages(params (int QueueId, long Offset)[] queues)
    {
        using var stream = new MemoryStream();
        foreach (var (queueId, queueOffset) in queues)
        {
            stream.Write(CreateEncodedMessage(retryTopic: null, queueOffset: queueOffset, queueId: queueId));
        }

        return stream.ToArray();
    }

    private static byte[] CreateEncodedMessage(string? retryTopic, long queueOffset = 42, int queueId = 3)
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
        WriteInt32(stream, queueId);
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
