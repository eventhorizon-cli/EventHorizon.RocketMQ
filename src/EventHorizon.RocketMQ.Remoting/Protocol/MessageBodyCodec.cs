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

using System.IO.Compression;
using K4os.Compression.LZ4.Streams;
using ZstdSharp;

namespace EventHorizon.RocketMQ.Remoting.Protocol;

internal static class MessageBodyCodec
{
    private const int CompressionTypeMask = 0x7 << 8;

    public static byte[] Decompress(byte[] body, int sysFlag)
    {
        ArgumentNullException.ThrowIfNull(body);
        if ((sysFlag & MessageSysFlag.CompressedFlag) == 0)
        {
            return body;
        }

        var compressionType = (sysFlag & CompressionTypeMask) >> 8;
        if (compressionType == 2)
        {
            using var decompressor = new Decompressor();
            return decompressor.Unwrap(body).ToArray();
        }

        using var compressed = new MemoryStream(body, writable: false);
        using Stream decompressionStream = compressionType switch
        {
            0 or 3 => new ZLibStream(compressed, CompressionMode.Decompress),
            1 => LZ4Stream.Decode(compressed, leaveOpen: false),
            _ => throw new InvalidDataException($"The message uses unknown compression type {compressionType}.")
        };
        using var decompressed = new MemoryStream();
        decompressionStream.CopyTo(decompressed);
        return decompressed.ToArray();
    }
}
