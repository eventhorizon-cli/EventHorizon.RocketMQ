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
    public void TryParseMessage_ConsumesOnlyOneFrame()
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
    public void TryParseMessage_ReadsHeaderAcrossSequenceSegments()
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
    public void RocketMqBinaryHeader_RoundTripsUnicodeAndExtensionFields()
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
    public void JsonHeader_RoundTripsCommandCodeAboveInt16()
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
    public void RocketMqBinaryHeader_RejectsCommandCodeAboveInt16()
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
    public void TryParseMessage_RejectsFrameAboveRocketMqLimitFromPrefixAlone()
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
    public void WriteMessage_RejectsFrameAboveRocketMqLimit()
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
    public void WriteMessage_UsesConfiguredFrameLimit()
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
    public void TryParseMessage_RejectsFrameAboveConfiguredLimitFromPrefixAlone()
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
    public void WriteMessage_WritesLargeFramesWithoutRequestingOneContiguousBuffer()
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
