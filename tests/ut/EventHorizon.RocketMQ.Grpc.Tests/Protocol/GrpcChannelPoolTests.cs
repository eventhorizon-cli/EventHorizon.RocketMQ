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

using System.Reflection;
using EventHorizon.RocketMQ.Grpc.Protocol;
using Microsoft.Extensions.Options;
using Xunit;

namespace EventHorizon.RocketMQ.Grpc.Tests.Protocol;

public sealed class GrpcChannelPoolTests
{
    [Fact]
    public async Task GetClient_SameEndpoint_CreatesOneChannelConcurrently()
    {
        await using var pool = new GrpcChannelPool();
        var endpoint = new Uri("http://127.0.0.1:8081");

        await Task.WhenAll(Enumerable.Range(0, 64).Select(_ => Task.Run(() => pool.GetClient(endpoint))));

        Assert.Equal(1, GetChannelCount(pool));
    }

    [Fact]
    public async Task GetClient_AfterDispose_Throws()
    {
        var pool = new GrpcChannelPool();
        await pool.DisposeAsync();

        Assert.Throws<ObjectDisposedException>(() => pool.GetClient(new Uri("http://127.0.0.1:8081")));
    }

    [Fact]
    public async Task DisposingSharedClients_SharedChannelPool_RemainsUsable()
    {
        var options = Options.Create(new GrpcClientOptions { Endpoint = "127.0.0.1:8081" });
        await using var pool = new GrpcChannelPool();
        await using var first = new RocketMQGrpcClient(
            options,
            new GrpcMetadataFactory(options, "first"),
            pool);
        await using var second = new RocketMQGrpcClient(
            options,
            new GrpcMetadataFactory(options, "second"),
            pool);

        await first.DisposeAsync();
        pool.GetClient(first.AccessPoint);
        await second.DisposeAsync();
        pool.GetClient(second.AccessPoint);

        await pool.DisposeAsync();
        Assert.Throws<ObjectDisposedException>(() => pool.GetClient(first.AccessPoint));
    }

    private static int GetChannelCount(GrpcChannelPool pool)
    {
        var field = typeof(GrpcChannelPool).GetField("_channels", BindingFlags.Instance | BindingFlags.NonPublic);
        Assert.NotNull(field);
        var channels = field.GetValue(pool);
        Assert.NotNull(channels);
        var count = channels.GetType().GetProperty("Count")?.GetValue(channels);
        return Assert.IsType<int>(count);
    }
}
