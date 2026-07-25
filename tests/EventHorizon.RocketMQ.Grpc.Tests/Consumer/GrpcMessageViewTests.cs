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
using System.Security.Cryptography;
using EventHorizon.RocketMQ.Grpc.Consumer;
using Google.Protobuf;
using Xunit;
using Proto = Apache.Rocketmq.V2;

namespace EventHorizon.RocketMQ.Grpc.Tests.Consumer;

public sealed class GrpcMessageViewTests
{
    private static readonly byte[] Body = "foobar"u8.ToArray();

    [Fact]
    public void IdentityEncoding_ReturnsOriginalBodyWhenDigestIsAbsent()
    {
        var view = CreateView(Body, Proto.Encoding.Identity);

        Assert.Equal(Body, view.Body);
        Assert.False(view.IsCorrupted);
        Assert.Null(view.CorruptionReason);
    }

    [Fact]
    public void UnspecifiedEncodingAndDigest_PreserveBackwardCompatibleIdentityBody()
    {
        var view = CreateView(Body, Proto.Encoding.Unspecified);

        Assert.Equal(Body, view.Body);
    }

    [Theory]
    [InlineData((int)Proto.DigestType.Crc32, "9EF61F95")]
    [InlineData((int)Proto.DigestType.Md5, "3858F62230AC3C915F300C664312C63F")]
    [InlineData((int)Proto.DigestType.Sha1, "8843D7F92416211DE9EBB963FF4CE28125932878")]
    public void SupportedDigest_ValidatesEncodedBody(int digestType, string checksum)
    {
        var view = CreateView(Body, Proto.Encoding.Identity, new Proto.Digest
        {
            Type = (Proto.DigestType)digestType,
            Checksum = checksum
        });

        Assert.Equal(Body, view.Body);
    }

    [Fact]
    public void LowercaseChecksum_IsAcceptedBecauseProtoDoesNotSpecifyHexCasing()
    {
        var view = CreateView(Body, Proto.Encoding.Identity, new Proto.Digest
        {
            Type = Proto.DigestType.Sha1,
            Checksum = "8843d7f92416211de9ebb963ff4ce28125932878"
        });

        Assert.Equal(Body, view.Body);
    }

    [Fact]
    public void GzipEncoding_ValidatesWireBodyThenDecompresses()
    {
        var encoded = Compress(Body);
        var checksum = Convert.ToHexString(SHA1.HashData(encoded));

        var view = CreateView(encoded, Proto.Encoding.Gzip, new Proto.Digest
        {
            Type = Proto.DigestType.Sha1,
            Checksum = checksum
        });

        Assert.Equal(Body, view.Body);
    }

    [Fact]
    public void DigestMismatch_MarksMessageCorruptedAndRetainsWireBody()
    {
        var view = CreateView(
            Body,
            Proto.Encoding.Identity,
            new Proto.Digest { Type = Proto.DigestType.Crc32, Checksum = "00000000" });

        Assert.True(view.IsCorrupted);
        Assert.Equal(Body, view.Body);
        Assert.Contains("failed CRC32 body digest validation", view.CorruptionReason);
        Assert.Contains("message-1", view.CorruptionReason);
        Assert.Contains("orders", view.CorruptionReason);
    }

    [Fact]
    public void MissingChecksum_MarksMessageCorrupted()
    {
        var view = CreateView(
            Body,
            Proto.Encoding.Identity,
            new Proto.Digest { Type = Proto.DigestType.Md5 });

        Assert.True(view.IsCorrupted);
        Assert.Contains("has no checksum", view.CorruptionReason);
        Assert.Contains("MD5", view.CorruptionReason);
    }

    [Fact]
    public void UnsupportedDigest_MarksMessageCorrupted()
    {
        var view = CreateView(
            Body,
            Proto.Encoding.Identity,
            new Proto.Digest { Type = (Proto.DigestType)99, Checksum = "checksum" });

        Assert.True(view.IsCorrupted);
        Assert.Contains("unsupported body digest algorithm", view.CorruptionReason);
        Assert.Contains("99", view.CorruptionReason);
    }

    [Fact]
    public void InvalidGzipBody_MarksMessageCorruptedAndRetainsCompressedBytes()
    {
        var view = CreateView(
            Body,
            Proto.Encoding.Gzip);

        Assert.True(view.IsCorrupted);
        Assert.Equal(Body, view.Body);
        Assert.Contains("invalid GZIP body data", view.CorruptionReason);
    }

    [Fact]
    public void UnsupportedEncoding_MarksMessageCorrupted()
    {
        var view = CreateView(
            Body,
            (Proto.Encoding)99);

        Assert.True(view.IsCorrupted);
        Assert.Contains("unsupported body encoding", view.CorruptionReason);
        Assert.Contains("99", view.CorruptionReason);
    }

    private static GrpcMessageView CreateView(byte[] body, Proto.Encoding encoding, Proto.Digest? digest = null)
    {
        var systemProperties = new Proto.SystemProperties
        {
            MessageId = "message-1",
            BodyEncoding = encoding
        };
        if (digest is not null)
        {
            systemProperties.BodyDigest = digest;
        }

        var message = new Proto.Message
        {
            Topic = new Proto.Resource { Name = "orders" },
            Body = ByteString.CopyFrom(body),
            SystemProperties = systemProperties
        };
        var queue = new Proto.MessageQueue
        {
            Topic = message.Topic,
            Id = 3,
            Broker = new Proto.Broker { Name = "broker-a" }
        };
        return GrpcMessageViewFactory.Create(message, queue, new Uri("http://127.0.0.1:8081"));
    }

    private static byte[] Compress(byte[] body)
    {
        using var output = new MemoryStream();
        using (var gzip = new GZipStream(output, CompressionLevel.SmallestSize, leaveOpen: true))
        {
            gzip.Write(body);
        }

        return output.ToArray();
    }
}
