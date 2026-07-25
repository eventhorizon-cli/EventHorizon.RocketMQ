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

using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Globalization;
using System.IO;
using System.Text;

namespace EventHorizon.RocketMQ.Remoting.Protocol;
// Frame format:
// ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// + item | frame size | header length |     header body     |     body     +
// ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// + len  |   4bytes   |     4bytes    | header length bytes | remain bytes +
// ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
internal class RemoteCommandSerializer : IRemoteCommandSerializer
{
    private const int MaximumHeaderLength = 0x00FFFFFF;
    private const int MaximumWriteChunkLength = 64 * 1024;
    private const int RocketMQHeaderFixedLength = 21;
    private readonly SerializeType _serializeType;

    public RemoteCommandSerializer()
        : this(SerializeType.Json)
    {
    }

    internal RemoteCommandSerializer(SerializeType serializeType)
    {
        if (!Enum.IsDefined(serializeType))
        {
            throw new ArgumentOutOfRangeException(nameof(serializeType), serializeType, null);
        }

        _serializeType = serializeType;
    }

    public bool TryParseMessage(
        in ReadOnlySequence<byte> input,
        ref SequencePosition consumed,
        ref SequencePosition examined,
        out RemotingCommand command,
        int maximumFrameLength = RemotingClientOptions.DefaultRemotingFrameSize)
    {
        ValidateMaximumFrameLength(maximumFrameLength);
        command = null!;
        if (input.Length < 4)
        {
            examined = input.End;
            return false;
        }

        Span<byte> frameSizeBuffer = stackalloc byte[4];
        input.Slice(0, 4).CopyTo(frameSizeBuffer);
        int frameSize = BinaryPrimitives.ReadInt32BigEndian(frameSizeBuffer);
        if (frameSize < 4)
        {
            throw new InvalidDataException($"Invalid RocketMQ frame size: {frameSize}.");
        }

        var totalFrameLength = 4L + frameSize;
        if (totalFrameLength > maximumFrameLength)
        {
            throw new InvalidDataException(
                $"RocketMQ frame length {totalFrameLength} exceeds the {maximumFrameLength}-byte limit.");
        }

        if (input.Length < totalFrameLength)
        {
            examined = input.End;
            return false;
        }

        const int framePrefixLength = 4;

        // header length
        Span<byte> headerLengthBuffer = stackalloc byte[4];
        input.Slice(framePrefixLength, 4).CopyTo(headerLengthBuffer);
        int oriHeaderLength = BinaryPrimitives.ReadInt32BigEndian(headerLengthBuffer);
        int headerLength = oriHeaderLength & 0xFFFFFF;
        if (headerLength > frameSize - 4)
        {
            throw new InvalidDataException(
                $"RocketMQ header length {headerLength} exceeds frame payload {frameSize - 4}.");
        }

        // serialize type
        var serializeType = (SerializeType)((oriHeaderLength >> 24) & 0xFF);

        // header body
        var headerData = input.Slice(framePrefixLength + 4, headerLength).ToArray();
        command = serializeType switch
        {
            SerializeType.Json => JsonHeaderDecode(headerData),
            SerializeType.RocketMQ => RocketMQHeaderDecode(headerData),
            _ => throw new InvalidDataException($"Unsupported RocketMQ header serialization type: {serializeType}.")
        };

        // body
        command.Body = input.Slice(
            framePrefixLength + 4 + headerLength,
            frameSize - 4 - headerLength).ToArray();
        consumed = input.GetPosition(4 + frameSize);
        examined = consumed;
        return true;

        static RemotingCommand JsonHeaderDecode(ReadOnlySpan<byte> headerData)
        {
            var headerJson = Encoding.UTF8.GetString(headerData);
            var header = headerJson.FromJson<RemotingCommand>();
            return header!;
        }

    }

    public void WriteMessage(
        RemotingCommand command,
        IBufferWriter<byte> output,
        int maximumFrameLength = RemotingClientOptions.DefaultRemotingFrameSize)
    {
        ValidateMaximumFrameLength(maximumFrameLength);
        var headerData = _serializeType switch
        {
            SerializeType.Json => JsonHeaderEncode(command),
            SerializeType.RocketMQ => RocketMQHeaderEncode(command),
            _ => throw new ArgumentOutOfRangeException()
        };

        if (headerData.Length > MaximumHeaderLength)
        {
            throw new InvalidDataException(
                $"RocketMQ header length {headerData.Length} exceeds the " +
                $"{MaximumHeaderLength}-byte protocol limit.");
        }

        var bodyLength = command.Body?.Length ?? 0;
        var totalFrameLength = 8L + headerData.Length + bodyLength;
        if (totalFrameLength > maximumFrameLength)
        {
            throw new InvalidDataException(
                $"RocketMQ frame length {totalFrameLength} exceeds the {maximumFrameLength}-byte limit.");
        }

        var frameSize = checked((int)totalFrameLength - sizeof(int));
        var prefix = output.GetSpan(2 * sizeof(int));
        BinaryPrimitives.WriteInt32BigEndian(prefix, frameSize);
        var markedHeaderLength = headerData.Length | ((int)_serializeType << 24);
        BinaryPrimitives.WriteInt32BigEndian(prefix[sizeof(int)..], markedHeaderLength);
        output.Advance(2 * sizeof(int));

        WriteBytes(output, headerData);
        if (command.Body is not null)
        {
            WriteBytes(output, command.Body);
        }

        static byte[] JsonHeaderEncode(RemotingCommand command) =>
            Encoding.UTF8.GetBytes(command.ToJson());

    }

    private static void WriteBytes(IBufferWriter<byte> output, ReadOnlySpan<byte> source)
    {
        while (!source.IsEmpty)
        {
            var length = Math.Min(source.Length, MaximumWriteChunkLength);
            source[..length].CopyTo(output.GetSpan(length));
            output.Advance(length);
            source = source[length..];
        }
    }

    private static void ValidateMaximumFrameLength(int maximumFrameLength)
    {
        if (maximumFrameLength < RemotingClientOptions.MinimumRemotingFrameSize)
        {
            throw new ArgumentOutOfRangeException(
                nameof(maximumFrameLength),
                maximumFrameLength,
                $"The maximum RocketMQ frame length must be at least " +
                $"{RemotingClientOptions.MinimumRemotingFrameSize} bytes.");
        }
    }

    private static RemotingCommand RocketMQHeaderDecode(ReadOnlySpan<byte> headerData)
    {
        if (headerData.Length < RocketMQHeaderFixedLength)
        {
            throw new InvalidDataException(
                $"RocketMQ binary header is {headerData.Length} bytes; " +
                $"at least {RocketMQHeaderFixedLength} are required.");
        }

        var reader = new HeaderReader(headerData);
        var command = new RemotingCommand
        {
            Code = reader.ReadInt16(),
            LanguageCode = (LanguageCode)unchecked((sbyte)reader.ReadByte()),
            Version = reader.ReadInt16(),
            Opaque = reader.ReadInt32(),
            Flag = reader.ReadInt32()
        };

        var remarkLength = reader.ReadInt32();
        command.Remark = remarkLength == 0 ? null : reader.ReadString(remarkLength, "remark");
        var extensionLength = reader.ReadInt32();
        var extensionReader = new HeaderReader(reader.ReadBytes(extensionLength, "extension fields"));
        while (extensionReader.Remaining > 0)
        {
            var key = extensionReader.ReadString(extensionReader.ReadInt16(), "extension key");
            var value = extensionReader.ReadString(extensionReader.ReadInt32(), "extension value");
            command.ExtFields[key] = value;
        }

        if (reader.Remaining != 0)
        {
            throw new InvalidDataException(
                $"RocketMQ binary header contains {reader.Remaining} unexpected trailing bytes.");
        }

        return command;
    }

    private static byte[] RocketMQHeaderEncode(RemotingCommand command)
    {
        if (command.Code is < short.MinValue or > short.MaxValue)
        {
            throw new InvalidDataException(
                $"RocketMQ binary header cannot encode command code {command.Code}; " +
                $"the fixed code field supports values from {short.MinValue} to {short.MaxValue}.");
        }

        var remark = Encoding.UTF8.GetBytes(command.Remark ?? string.Empty);
        var fields = command.ExtFields.Select(static entry => new EncodedField(
            Encoding.UTF8.GetBytes(entry.Key),
            Encoding.UTF8.GetBytes(FormatValue(entry.Value)))).ToArray();
        foreach (var field in fields)
        {
            if (field.Key.Length > short.MaxValue)
            {
                throw new InvalidDataException(
                    $"RocketMQ extension key exceeds the {short.MaxValue}-byte binary header limit.");
            }
        }

        var extensionLength = fields.Aggregate(
            0,
            static (length, field) => checked(
                length + sizeof(short) + field.Key.Length + sizeof(int) + field.Value.Length));
        var header = new byte[checked(RocketMQHeaderFixedLength + remark.Length + extensionLength)];
        var span = header.AsSpan();
        var offset = 0;
        BinaryPrimitives.WriteInt16BigEndian(span[offset..], (short)command.Code);
        offset += sizeof(short);
        span[offset++] = unchecked((byte)(sbyte)command.LanguageCode);
        BinaryPrimitives.WriteInt16BigEndian(span[offset..], checked((short)command.Version));
        offset += sizeof(short);
        BinaryPrimitives.WriteInt32BigEndian(span[offset..], command.Opaque);
        offset += sizeof(int);
        BinaryPrimitives.WriteInt32BigEndian(span[offset..], command.Flag);
        offset += sizeof(int);
        BinaryPrimitives.WriteInt32BigEndian(span[offset..], remark.Length);
        offset += sizeof(int);
        remark.CopyTo(span[offset..]);
        offset += remark.Length;
        BinaryPrimitives.WriteInt32BigEndian(span[offset..], extensionLength);
        offset += sizeof(int);

        foreach (var field in fields)
        {
            BinaryPrimitives.WriteInt16BigEndian(span[offset..], checked((short)field.Key.Length));
            offset += sizeof(short);
            field.Key.CopyTo(span[offset..]);
            offset += field.Key.Length;
            BinaryPrimitives.WriteInt32BigEndian(span[offset..], field.Value.Length);
            offset += sizeof(int);
            field.Value.CopyTo(span[offset..]);
            offset += field.Value.Length;
        }

        return header;
    }

    private static string FormatValue(object? value) => value switch
    {
        null => string.Empty,
        bool boolean => boolean ? "true" : "false",
        string text => text,
        IFormattable formattable => formattable.ToString(null, CultureInfo.InvariantCulture) ?? string.Empty,
        _ => value.ToString() ?? string.Empty
    };

    private readonly record struct EncodedField(byte[] Key, byte[] Value);

    private ref struct HeaderReader
    {
        private ReadOnlySpan<byte> _remaining;

        public HeaderReader(ReadOnlySpan<byte> value) => _remaining = value;

        public readonly int Remaining => _remaining.Length;

        public byte ReadByte()
        {
            Ensure(sizeof(byte), "byte");
            var value = _remaining[0];
            _remaining = _remaining[sizeof(byte)..];
            return value;
        }

        public short ReadInt16()
        {
            Ensure(sizeof(short), "16-bit integer");
            var value = BinaryPrimitives.ReadInt16BigEndian(_remaining);
            _remaining = _remaining[sizeof(short)..];
            return value;
        }

        public int ReadInt32()
        {
            Ensure(sizeof(int), "32-bit integer");
            var value = BinaryPrimitives.ReadInt32BigEndian(_remaining);
            _remaining = _remaining[sizeof(int)..];
            return value;
        }

        public string ReadString(int length, string field) =>
            Encoding.UTF8.GetString(ReadBytes(length, field));

        public ReadOnlySpan<byte> ReadBytes(int length, string field)
        {
            Ensure(length, field);
            var value = _remaining[..length];
            _remaining = _remaining[length..];
            return value;
        }

        private readonly void Ensure(int length, string field)
        {
            if (length < 0 || length > _remaining.Length)
            {
                throw new InvalidDataException(
                    $"RocketMQ binary header has an invalid {field} length of {length} bytes " +
                    $"with {_remaining.Length} bytes remaining.");
            }
        }
    }
}
