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

using System.Security.Cryptography;
using System.Text;
using EventHorizon.RocketMQ.Grpc.Protocol;
using Grpc.Core;
using Microsoft.Extensions.Options;
using Xunit;

namespace EventHorizon.RocketMQ.Grpc.Tests.Protocol;

public sealed class GrpcMetadataFactoryTests
{
    [Fact]
    public void Create_OfficialProtocolAndUtf8SignatureFormat_UsesSignature()
    {
        var options = Options.Create(new GrpcClientOptions
        {
            AccessKey = "access-key",
            AccessSecret = "secret-\u5bc6\u94a5",
            SecurityToken = "session-token"
        });
        var factory = new GrpcMetadataFactory(options, "producer");

        var metadata = factory.Create();
        var timestamp = Value(metadata, "x-mq-date-time");
        using var hmac = new HMACSHA1(Encoding.UTF8.GetBytes(options.Value.AccessSecret!));
        var signature = Convert.ToHexString(hmac.ComputeHash(Encoding.UTF8.GetBytes(timestamp)))
            .ToLowerInvariant();
        var clientId = Value(metadata, "x-mq-client-id");

        Assert.Equal("v2", Value(metadata, "x-mq-protocol"));
        Assert.Contains("@producer@", clientId, StringComparison.Ordinal);
        Assert.Equal(
            $"MQv2-HMAC-SHA1 Credential=access-key//Rocketmq, SignedHeaders=x-mq-date-time, Signature={signature}",
            Value(metadata, "authorization"));
        Assert.Equal("session-token", Value(metadata, "x-mq-session-token"));
    }

    [Fact]
    public void ClientId_FactoryAndLogicalClients_IsStableAndUnique()
    {
        var options = Options.Create(new GrpcClientOptions());
        var producer = new GrpcMetadataFactory(options, "producer");
        var consumer = new GrpcMetadataFactory(options, "consumer");
        var producerClientId = Value(producer.Create(), "x-mq-client-id");

        Assert.Equal(producerClientId, Value(producer.Create(), "x-mq-client-id"));
        Assert.NotEqual(producerClientId, Value(consumer.Create(), "x-mq-client-id"));
    }

    [Fact]
    public void Create_WhitespaceCredentials_OmitsCredentials()
    {
        var options = Options.Create(new GrpcClientOptions
        {
            AccessKey = " ",
            AccessSecret = "\t",
            SecurityToken = "\r\n"
        });
        var factory = new GrpcMetadataFactory(options, "producer");

        var metadata = factory.Create();

        Assert.DoesNotContain(metadata, entry =>
            string.Equals(entry.Key, "authorization", StringComparison.Ordinal));
        Assert.DoesNotContain(metadata, entry =>
            string.Equals(entry.Key, "x-mq-session-token", StringComparison.Ordinal));
    }

    private static string Value(Metadata metadata, string key) =>
        metadata.Single(entry => string.Equals(entry.Key, key, StringComparison.Ordinal)).Value;
}
