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

using System.Buffers;
using System.Buffers.Binary;
using EventHorizon.RocketMQ.Remoting.Protocol;
using Xunit;

namespace EventHorizon.RocketMQ.Remoting.Tests.Protocol;

public sealed class RemoteCommandSerializerTests
{
    [Fact]
    public void TryParseMessage_OnlyOneFrame_Consumes()
    {
        var serializer = new RemoteCommandSerializer();
        var first = new RemotingCommand(RequestCode.GetRouteInfoByTopic, new TestHeader { Topic = "orders" })
        {
            Body = [1, 2, 3]
        };
        var second = new RemotingCommand(RequestCode.GetRouteInfoByTopic, new TestHeader { Topic = "payments" });
        var writer = new ArrayBufferWriter<byte>();
        serializer.WriteMessage(first, writer);
        var firstLength = writer.WrittenCount;
        serializer.WriteMessage(second, writer);

        var input = new ReadOnlySequence<byte>(writer.WrittenMemory);
        var consumed = input.Start;
        var examined = input.Start;

        var parsed = serializer.TryParseMessage(input, ref consumed, ref examined, out var command);

        Assert.True(parsed);
        Assert.Equal(first.Opaque, command.Opaque);
        Assert.Equal<byte>([1, 2, 3], command.Body!);
        Assert.Equal(firstLength, input.Slice(0, consumed).Length);
    }

    [Fact]
    public void TryParseMessage_HeaderAcrossSequenceSegments_ReadsHeaderAcrossSegments()
    {
        var serializer = new RemoteCommandSerializer();
        var expected = new RemotingCommand(RequestCode.GetRouteInfoByTopic, new TestHeader
        {
            Topic = new string('x', 256)
        })
        {
            Body = [4, 5, 6]
        };
        var writer = new ArrayBufferWriter<byte>();
        serializer.WriteMessage(expected, writer);
        var bytes = writer.WrittenMemory.ToArray();
        var input = CreateSequence(bytes.AsMemory(0, 37), bytes.AsMemory(37, 29), bytes.AsMemory(66));
        var consumed = input.Start;
        var examined = input.Start;

        var parsed = serializer.TryParseMessage(input, ref consumed, ref examined, out var command);

        Assert.True(parsed);
        Assert.Equal(expected.Opaque, command.Opaque);
        Assert.Equal<byte>([4, 5, 6], command.Body!);
        Assert.Equal(input.End, consumed);
    }

    [Fact]
    public void RocketMqBinaryHeader_UnicodeAndExtensionFields_RoundTrip()
    {
        var serializer = new RemoteCommandSerializer(SerializeType.RocketMQ);
        var expected = new RemotingCommand
        {
            Code = RequestCode.GetRouteInfoByTopic,
            LanguageCode = LanguageCode.Dotnet,
            Version = 317,
            Opaque = 42,
            Flag = 1,
            Remark = "route-ready",
            ExtFields = new Dictionary<string, object>
            {
                ["topic"] = "orders-\u4e2d\u56fd",
                ["enabled"] = true,
                ["count"] = 3
            },
            Body = [7, 8, 9]
        };
        var writer = new ArrayBufferWriter<byte>();
        serializer.WriteMessage(expected, writer);
        var input = new ReadOnlySequence<byte>(writer.WrittenMemory);
        var consumed = input.Start;
        var examined = input.Start;

        var parsed = serializer.TryParseMessage(input, ref consumed, ref examined, out var actual);

        Assert.True(parsed);
        Assert.Equal(expected.Code, actual.Code);
        Assert.Equal(expected.LanguageCode, actual.LanguageCode);
        Assert.Equal(expected.Version, actual.Version);
        Assert.Equal(expected.Opaque, actual.Opaque);
        Assert.Equal(expected.Flag, actual.Flag);
        Assert.Equal(expected.Remark, actual.Remark);
        Assert.Equal("orders-\u4e2d\u56fd", actual.ExtFields["topic"]);
        Assert.Equal("true", actual.ExtFields["enabled"]);
        Assert.Equal("3", actual.ExtFields["count"]);
        Assert.Equal<byte>([7, 8, 9], actual.Body!);
        Assert.Equal(input.End, consumed);
    }

    [Fact]
    public void JsonHeader_CommandCodeAboveInt16_RoundTrip()
    {
        const int popMessageRequestCode = 200050;
        var serializer = new RemoteCommandSerializer();
        var expected = new RemotingCommand
        {
            Code = popMessageRequestCode,
            Body = [1, 2, 3]
        };
        var writer = new ArrayBufferWriter<byte>();

        serializer.WriteMessage(expected, writer);

        var input = new ReadOnlySequence<byte>(writer.WrittenMemory);
        var consumed = input.Start;
        var examined = input.Start;
        var parsed = serializer.TryParseMessage(input, ref consumed, ref examined, out var actual);

        Assert.True(parsed);
        Assert.Equal(popMessageRequestCode, actual.Code);
        Assert.Equal<byte>([1, 2, 3], actual.Body!);
    }

    [Fact]
    public void RocketMqBinaryHeader_CommandCodeAboveInt16_Rejects()
    {
        const int popMessageRequestCode = 200050;
        var serializer = new RemoteCommandSerializer(SerializeType.RocketMQ);
        var writer = new ArrayBufferWriter<byte>();

        var exception = Assert.Throws<InvalidDataException>(() => serializer.WriteMessage(
            new RemotingCommand { Code = popMessageRequestCode },
            writer));

        Assert.Contains(popMessageRequestCode.ToString(), exception.Message);
        Assert.Contains("fixed code field", exception.Message);
        Assert.Equal(0, writer.WrittenCount);
    }

    [Fact]
    public void TryParseMessage_FrameAboveRocketMqLimitFromPrefixAlone_Rejects()
    {
        var serializer = new RemoteCommandSerializer();
        var prefix = new byte[sizeof(int)];
        BinaryPrimitives.WriteInt32BigEndian(prefix, RemotingClientOptions.DefaultRemotingFrameSize);
        var input = new ReadOnlySequence<byte>(prefix);
        var consumed = input.Start;
        var examined = input.Start;

        var exception = Assert.Throws<InvalidDataException>(() =>
            serializer.TryParseMessage(input, ref consumed, ref examined, out _));

        Assert.Contains(RemotingClientOptions.DefaultRemotingFrameSize.ToString(), exception.Message);
    }

    [Fact]
    public void WriteMessage_FrameAboveRocketMqLimit_Rejects()
    {
        var serializer = new RemoteCommandSerializer();
        var command = new RemotingCommand
        {
            Code = RequestCode.GetRouteInfoByTopic,
            Body = GC.AllocateUninitializedArray<byte>(RemotingClientOptions.DefaultRemotingFrameSize)
        };
        var output = new ArrayBufferWriter<byte>();

        var exception = Assert.Throws<InvalidDataException>(() => serializer.WriteMessage(command, output));

        Assert.Contains(RemotingClientOptions.DefaultRemotingFrameSize.ToString(), exception.Message);
        Assert.Equal(0, output.WrittenCount);
    }

    [Fact]
    public void WriteMessage_ConfiguredFrameLimit_UsesLimit()
    {
        const int maximumFrameLength = 128 * 1024;
        var serializer = new RemoteCommandSerializer();
        var command = new RemotingCommand
        {
            Code = RequestCode.GetRouteInfoByTopic,
            Body = GC.AllocateUninitializedArray<byte>(maximumFrameLength)
        };
        var output = new ArrayBufferWriter<byte>();

        var exception = Assert.Throws<InvalidDataException>(() =>
            serializer.WriteMessage(command, output, maximumFrameLength));

        Assert.Contains(maximumFrameLength.ToString(), exception.Message);
        Assert.Equal(0, output.WrittenCount);
    }

    [Fact]
    public void TryParseMessage_FrameAboveConfiguredLimitFromPrefixAlone_Rejects()
    {
        const int maximumFrameLength = 128 * 1024;
        var serializer = new RemoteCommandSerializer();
        var prefix = new byte[sizeof(int)];
        BinaryPrimitives.WriteInt32BigEndian(prefix, maximumFrameLength);
        var input = new ReadOnlySequence<byte>(prefix);
        var consumed = input.Start;
        var examined = input.Start;

        var exception = Assert.Throws<InvalidDataException>(() =>
            serializer.TryParseMessage(
                input,
                ref consumed,
                ref examined,
                out _,
                maximumFrameLength));

        Assert.Contains(maximumFrameLength.ToString(), exception.Message);
    }

    [Fact]
    public void WriteMessage_LargeFramesWithoutRequestingOneContiguousBuffer_WritesLargeFrames()
    {
        const int bodyLength = 512 * 1024;
        var serializer = new RemoteCommandSerializer();
        var body = Enumerable.Range(0, bodyLength).Select(static value => (byte)value).ToArray();
        var command = new RemotingCommand(RequestCode.GetRouteInfoByTopic, new TestHeader { Topic = "orders" })
        {
            Body = body
        };
        var output = new TrackingBufferWriter();

        serializer.WriteMessage(command, output);

        Assert.True(output.MaximumSizeHint <= 64 * 1024);
        var input = new ReadOnlySequence<byte>(output.WrittenMemory);
        var consumed = input.Start;
        var examined = input.Start;
        Assert.True(serializer.TryParseMessage(input, ref consumed, ref examined, out var parsed));
        Assert.Equal(body, parsed.Body);
    }

    [Fact]
    public void ConstructorAndOperations_UnsupportedSerializerAndFrameLimits_Reject()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => new RemoteCommandSerializer((SerializeType)99));

        var serializer = new RemoteCommandSerializer();
        var input = new ReadOnlySequence<byte>(ReadOnlyMemory<byte>.Empty);
        var consumed = input.Start;
        var examined = input.Start;
        var maximumFrameLength = RemotingClientOptions.MinimumRemotingFrameSize - 1;

        Assert.Throws<ArgumentOutOfRangeException>(() =>
            serializer.TryParseMessage(input, ref consumed, ref examined, out _, maximumFrameLength));
        Assert.Throws<ArgumentOutOfRangeException>(() => serializer.WriteMessage(
            new RemotingCommand(),
            new ArrayBufferWriter<byte>(),
            maximumFrameLength));
    }

    [Fact]
    public void TryParseMessage_InvalidFrameAndHeaderLengths_Rejects()
    {
        var serializer = new RemoteCommandSerializer();
        var invalidFrame = CreateFrame(3, 0);
        var invalidFrameConsumed = invalidFrame.Start;
        var invalidFrameExamined = invalidFrame.Start;

        var frameException = Assert.Throws<InvalidDataException>(() => serializer.TryParseMessage(
            invalidFrame,
            ref invalidFrameConsumed,
            ref invalidFrameExamined,
            out _));

        Assert.Contains("frame size", frameException.Message, StringComparison.OrdinalIgnoreCase);

        var invalidHeader = CreateFrame(4, 1);
        var invalidHeaderConsumed = invalidHeader.Start;
        var invalidHeaderExamined = invalidHeader.Start;

        var headerException = Assert.Throws<InvalidDataException>(() => serializer.TryParseMessage(
            invalidHeader,
            ref invalidHeaderConsumed,
            ref invalidHeaderExamined,
            out _));

        Assert.Contains("header length", headerException.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void TryParseMessage_UnsupportedHeaderSerializationType_Rejects()
    {
        var serializer = new RemoteCommandSerializer();
        var input = CreateFrame(6, (2 << 24) | 2, [0, 0]);
        var consumed = input.Start;
        var examined = input.Start;

        var exception = Assert.Throws<InvalidDataException>(() =>
            serializer.TryParseMessage(input, ref consumed, ref examined, out _));

        Assert.Contains("Unsupported", exception.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void TryParseMessage_MalformedRocketMqBinaryHeaders_Rejects()
    {
        var serializer = new RemoteCommandSerializer();
        var shortHeader = CreateFrame(5, (1 << 24) | 1, [0]);
        var shortConsumed = shortHeader.Start;
        var shortExamined = shortHeader.Start;

        var shortException = Assert.Throws<InvalidDataException>(() => serializer.TryParseMessage(
            shortHeader,
            ref shortConsumed,
            ref shortExamined,
            out _));

        Assert.Contains("at least", shortException.Message, StringComparison.OrdinalIgnoreCase);

        var trailingHeader = new byte[22];
        var trailing = CreateFrame(26, (1 << 24) | trailingHeader.Length, trailingHeader);
        var trailingConsumed = trailing.Start;
        var trailingExamined = trailing.Start;

        var trailingException = Assert.Throws<InvalidDataException>(() => serializer.TryParseMessage(
            trailing,
            ref trailingConsumed,
            ref trailingExamined,
            out _));

        Assert.Contains("trailing", trailingException.Message, StringComparison.OrdinalIgnoreCase);

        var invalidExtensionLength = new byte[21];
        BinaryPrimitives.WriteInt32BigEndian(invalidExtensionLength.AsSpan(17), -1);
        var invalidExtension = CreateFrame(25, (1 << 24) | invalidExtensionLength.Length, invalidExtensionLength);
        var extensionConsumed = invalidExtension.Start;
        var extensionExamined = invalidExtension.Start;

        var extensionException = Assert.Throws<InvalidDataException>(() => serializer.TryParseMessage(
            invalidExtension,
            ref extensionConsumed,
            ref extensionExamined,
            out _));

        Assert.Contains("extension fields", extensionException.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void RocketMqBinaryHeader_NullAndCustomExtensionValues_FormatsExtensions()
    {
        var serializer = new RemoteCommandSerializer(SerializeType.RocketMQ);
        var writer = new ArrayBufferWriter<byte>();
        serializer.WriteMessage(new RemotingCommand
        {
            Code = RequestCode.GetRouteInfoByTopic,
            ExtFields = new Dictionary<string, object>
            {
                ["empty"] = null!,
                ["custom"] = new CustomValue()
            }
        }, writer);
        var input = new ReadOnlySequence<byte>(writer.WrittenMemory);
        var consumed = input.Start;
        var examined = input.Start;

        Assert.True(serializer.TryParseMessage(input, ref consumed, ref examined, out var command));
        Assert.Equal(string.Empty, command.ExtFields["empty"]);
        Assert.Equal("custom-value", command.ExtFields["custom"]);
    }

    [Fact]
    public void RocketMqBinaryHeader_ExtensionKeysAboveTheWireLimit_Rejects()
    {
        var serializer = new RemoteCommandSerializer(SerializeType.RocketMQ);
        var output = new ArrayBufferWriter<byte>();
        var command = new RemotingCommand
        {
            Code = RequestCode.GetRouteInfoByTopic,
            ExtFields = new Dictionary<string, object>
            {
                [new string('x', short.MaxValue + 1)] = "value"
            }
        };

        var exception = Assert.Throws<InvalidDataException>(() => serializer.WriteMessage(command, output));

        Assert.Contains("extension key", exception.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Equal(0, output.WrittenCount);
    }

    private static ReadOnlySequence<byte> CreateSequence(params ReadOnlyMemory<byte>[] segments)
    {
        var first = new TestSegment(segments[0]);
        var last = first;
        foreach (var segment in segments.Skip(1))
        {
            last = last.Append(segment);
        }

        return new ReadOnlySequence<byte>(first, 0, last, last.Memory.Length);
    }

    private static ReadOnlySequence<byte> CreateFrame(int frameSize, int markedHeaderLength, byte[]? header = null)
    {
        var frame = new byte[sizeof(int) + frameSize];
        BinaryPrimitives.WriteInt32BigEndian(frame, frameSize);
        if (frameSize >= sizeof(int))
        {
            BinaryPrimitives.WriteInt32BigEndian(frame.AsSpan(sizeof(int)), markedHeaderLength);
            header?.CopyTo(frame, 2 * sizeof(int));
        }

        return new ReadOnlySequence<byte>(frame);
    }

    private sealed class CustomValue
    {
        public override string ToString() => "custom-value";
    }

    private sealed class TestHeader : CommandCustomHeader
    {
        public required string Topic { get; init; }
    }

    private sealed class TestSegment : ReadOnlySequenceSegment<byte>
    {
        public TestSegment(ReadOnlyMemory<byte> memory)
        {
            Memory = memory;
        }

        public TestSegment Append(ReadOnlyMemory<byte> memory)
        {
            var segment = new TestSegment(memory)
            {
                RunningIndex = RunningIndex + Memory.Length
            };
            Next = segment;
            return segment;
        }
    }

    private sealed class TrackingBufferWriter : IBufferWriter<byte>
    {
        private readonly ArrayBufferWriter<byte> _inner = new();

        public int MaximumSizeHint { get; private set; }

        public ReadOnlyMemory<byte> WrittenMemory => _inner.WrittenMemory;

        public void Advance(int count) => _inner.Advance(count);

        public Memory<byte> GetMemory(int sizeHint = 0)
        {
            MaximumSizeHint = Math.Max(MaximumSizeHint, sizeHint);
            return _inner.GetMemory(sizeHint);
        }

        public Span<byte> GetSpan(int sizeHint = 0)
        {
            MaximumSizeHint = Math.Max(MaximumSizeHint, sizeHint);
            return _inner.GetSpan(sizeHint);
        }
    }
}
