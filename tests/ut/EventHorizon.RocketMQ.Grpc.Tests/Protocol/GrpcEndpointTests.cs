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

using EventHorizon.RocketMQ.Grpc.Protocol;
using Microsoft.Extensions.Options;
using Xunit;
using Proto = Apache.Rocketmq.V2;

namespace EventHorizon.RocketMQ.Grpc.Tests.Protocol;

public sealed class GrpcEndpointTests
{
    [Theory]
    [InlineData("broker.example:8081", false, "http://broker.example:8081/")]
    [InlineData("broker.example:8081", true, "https://broker.example:8081/")]
    [InlineData("http://broker.example:8081", false, "http://broker.example:8081/")]
    [InlineData("https://broker.example:8081", true, "https://broker.example:8081/")]
    public void Parse_UsesConsistentTransportSecurity(string value, bool useTls, string expected)
    {
        Assert.Equal(new Uri(expected), GrpcEndpoint.Parse(value, useTls));
    }

    [Theory]
    [InlineData("http://broker.example:8081", true)]
    [InlineData("https://broker.example:8081", false)]
    [InlineData("ftp://broker.example:8081", false)]
    public void Parse_RejectsConflictingOrUnsupportedSchemes(string value, bool useTls)
    {
        Assert.Throws<ArgumentException>(() => GrpcEndpoint.Parse(value, useTls));
    }

    [Fact]
    public void ParseMany_PreservesEverySemicolonSeparatedAccessPoint()
    {
        var endpoints = GrpcEndpoint.ParseMany(
            "127.0.0.1:8081; 127.0.0.2:8082;",
            false);
        var protobuf = GrpcEndpoint.ToProtobuf(endpoints);

        Assert.Equal(
            [new Uri("http://127.0.0.1:8081"), new Uri("http://127.0.0.2:8082")],
            endpoints);
        Assert.Equal(Proto.AddressScheme.Ipv4, protobuf.Scheme);
        Assert.Equal([8081, 8082], protobuf.Addresses.Select(static address => address.Port));
    }

    [Fact]
    public async Task GrpcClient_AdvertisesAllConfiguredAccessPoints()
    {
        var options = Options.Create(new GrpcClientOptions
        {
            Endpoint = "proxy-a.example:8081;proxy-b.example:8082"
        });
        await using var client = new RocketMQGrpcClient(
            options,
            new GrpcMetadataFactory(options, "test"));

        Assert.Equal(new Uri("http://proxy-a.example:8081"), client.AccessPoint);
        Assert.Equal(
            ["proxy-a.example", "proxy-b.example"],
            client.AccessPointProtobuf.Addresses.Select(static address => address.Host));
    }

    [Fact]
    public void FromProtobuf_RotatesAcrossEveryAdvertisedBrokerAddress()
    {
        var endpoints = GrpcEndpoint.ToProtobuf(
        [
            new Uri("http://127.0.0.10:8081"),
            new Uri("http://127.0.0.11:8082")
        ]);

        var first = GrpcEndpoint.FromProtobuf(endpoints, false);
        var second = GrpcEndpoint.FromProtobuf(endpoints, false);
        var third = GrpcEndpoint.FromProtobuf(endpoints, false);

        Assert.Equal(new Uri("http://127.0.0.10:8081"), first);
        Assert.Equal(new Uri("http://127.0.0.11:8082"), second);
        Assert.Equal(first, third);
    }

    [Fact]
    public void Conversion_RejectsEmptyEndpointCollections()
    {
        Assert.Throws<ArgumentException>(() => GrpcEndpoint.ToProtobuf([]));
        Assert.Throws<InvalidOperationException>(() => GrpcEndpoint.FromProtobufAll(new Proto.Endpoints(), false));
    }
}
