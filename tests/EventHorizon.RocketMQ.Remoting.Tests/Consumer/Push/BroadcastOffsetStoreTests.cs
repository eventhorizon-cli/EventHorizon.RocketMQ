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

using EventHorizon.RocketMQ.Consumer;
using EventHorizon.RocketMQ.Remoting.Consumer.Pull;
using EventHorizon.RocketMQ.Remoting.Consumer.Push;
using Xunit;

namespace EventHorizon.RocketMQ.Remoting.Tests.Consumer.Push;

public sealed class BroadcastOffsetStoreTests
{
    [Fact]
    public void DefaultPath_IsStableAndScopedByClientAndGroup()
    {
        var first = BroadcastOffsetStore.ResolvePath(null, "client-a", "group-a");
        var same = BroadcastOffsetStore.ResolvePath(null, "client-a", "group-a");
        var otherGroup = BroadcastOffsetStore.ResolvePath(null, "client-a", "group-b");

        Assert.Equal(first, same);
        Assert.NotEqual(first, otherGroup);
        Assert.Equal(".json", System.IO.Path.GetExtension(first));
        Assert.Contains("EventHorizon.RocketMQ", first, StringComparison.Ordinal);
    }

    [Fact]
    public async Task UpdateAsync_AtomicallyPersistsAndReloadsQueueOffset()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var directory = System.IO.Path.Combine(
            System.IO.Path.GetTempPath(),
            $"rocketmq-offset-store-{Guid.NewGuid():N}");
        var path = System.IO.Path.Combine(directory, "offsets.json");
        var queue = new RemotingPullMessageQueue("orders", 3, "broker-a", "127.0.0.1:10911");
        try
        {
            var store = new BroadcastOffsetStore(path, "client-a", "group-a");
            await store.UpdateAsync(queue, 42, cancellationToken);

            var reopened = new BroadcastOffsetStore(path, "client-a", "group-a");
            var offset = await reopened.ReadAsync(queue, cancellationToken);

            Assert.Equal(42, offset);
            Assert.Empty(Directory.EnumerateFiles(directory, "*.tmp"));
        }
        finally
        {
            if (Directory.Exists(directory))
            {
                Directory.Delete(directory, true);
            }
        }
    }
}
