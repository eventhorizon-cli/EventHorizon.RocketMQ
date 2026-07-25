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

using System.Diagnostics;
using System.Globalization;
using System.IO.Compression;
using System.Security.Cryptography;
using Proto = Apache.Rocketmq.V2;

namespace EventHorizon.RocketMQ.Grpc.Protocol;

internal static class MessageBodyCodec
{
    public static byte[] Decode(
        byte[] encodedBody,
        Proto.Encoding encoding,
        Proto.Digest? digest,
        string topic,
        string messageId)
    {
        ArgumentNullException.ThrowIfNull(encodedBody);

        ValidateDigest(encodedBody, digest, topic, messageId);
        return encoding switch
        {
            Proto.Encoding.Unspecified or Proto.Encoding.Identity => encodedBody,
            Proto.Encoding.Gzip => DecodeGzip(encodedBody, topic, messageId),
            _ => throw new InvalidDataException(
                $"RocketMQ message '{messageId}' on topic '{topic}' uses unsupported body encoding '{encoding}' ({(int)encoding}).")
        };
    }

    private static void ValidateDigest(
        ReadOnlySpan<byte> encodedBody,
        Proto.Digest? digest,
        string topic,
        string messageId)
    {
        if (digest is null ||
            (digest.Type == Proto.DigestType.Unspecified && string.IsNullOrEmpty(digest.Checksum)))
        {
            return;
        }

        if (digest.Type == Proto.DigestType.Unspecified)
        {
            throw new InvalidDataException(
                $"RocketMQ message '{messageId}' on topic '{topic}' declares a checksum without a body digest algorithm.");
        }

        var algorithm = digest.Type switch
        {
            Proto.DigestType.Crc32 => "CRC32",
            Proto.DigestType.Md5 => "MD5",
            Proto.DigestType.Sha1 => "SHA1",
            _ => throw new InvalidDataException(
                $"RocketMQ message '{messageId}' on topic '{topic}' uses unsupported body digest algorithm '{digest.Type}' ({(int)digest.Type}).")
        };

        if (string.IsNullOrEmpty(digest.Checksum))
        {
            throw new InvalidDataException(
                $"RocketMQ message '{messageId}' on topic '{topic}' has no checksum for body digest algorithm '{algorithm}'.");
        }

        var actual = digest.Type switch
        {
            Proto.DigestType.Crc32 => ComputeCrc32(encodedBody).ToString("X", CultureInfo.InvariantCulture),
            Proto.DigestType.Md5 => Convert.ToHexString(MD5.HashData(encodedBody)),
            Proto.DigestType.Sha1 => Convert.ToHexString(SHA1.HashData(encodedBody)),
            _ => throw new UnreachableException()
        };

        if (!string.Equals(actual, digest.Checksum, StringComparison.OrdinalIgnoreCase))
        {
            throw new InvalidDataException(
                $"RocketMQ message '{messageId}' on topic '{topic}' failed {algorithm} body digest validation: " +
                $"expected '{digest.Checksum}', calculated '{actual}'.");
        }
    }

    private static byte[] DecodeGzip(byte[] encodedBody, string topic, string messageId)
    {
        try
        {
            using var input = new MemoryStream(encodedBody, writable: false);
            using var gzip = new GZipStream(input, CompressionMode.Decompress);
            using var output = new MemoryStream();
            gzip.CopyTo(output);
            return output.ToArray();
        }
        catch (Exception exception) when (exception is InvalidDataException or IOException)
        {
            throw new InvalidDataException(
                $"RocketMQ message '{messageId}' on topic '{topic}' contains invalid GZIP body data.",
                exception);
        }
    }

    private static uint ComputeCrc32(ReadOnlySpan<byte> data)
    {
        var crc = uint.MaxValue;
        foreach (var value in data)
        {
            crc ^= value;
            for (var bit = 0; bit < 8; bit++)
            {
                crc = (crc >> 1) ^ ((crc & 1) == 0 ? 0 : 0xEDB88320u);
            }
        }

        return ~crc;
    }
}
