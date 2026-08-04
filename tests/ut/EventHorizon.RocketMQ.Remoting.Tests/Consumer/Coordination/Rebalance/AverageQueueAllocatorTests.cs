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

using EventHorizon.RocketMQ.Remoting.Consumer;
using EventHorizon.RocketMQ.Remoting.Consumer.Coordination.Rebalance;
using Xunit;

namespace EventHorizon.RocketMQ.Remoting.Tests.Consumer.Coordination.Rebalance;

public sealed class AverageQueueAllocatorTests
{
    [Fact]
    public void Allocate_SortedApacheAverageStrategy_UsesQueues()
    {
        var queues = Enumerable.Range(0, 8)
            .Select(queueId => new RemotingConsumerQueue("orders", "broker-a", queueId))
            .Reverse()
            .ToArray();
        string[] consumers = ["client-c", "client-a", "client-b"];

        var first = AverageQueueAllocator.Allocate(queues, consumers, "client-a");
        var second = AverageQueueAllocator.Allocate(queues, consumers, "client-b");
        var third = AverageQueueAllocator.Allocate(queues, consumers, "client-c");

        Assert.Equal([0, 1, 2], first.Select(static queue => queue.QueueId));
        Assert.Equal([3, 4, 5], second.Select(static queue => queue.QueueId));
        Assert.Equal([6, 7], third.Select(static queue => queue.QueueId));
    }

    [Theory]
    [InlineData(3)]
    [InlineData(10)]
    public void Allocate_ThreeBrokerNineQueueRouteWithoutOverlap_Distributes(int consumerCount)
    {
        var queues = CreateMultiBrokerQueues();
        var consumers = Enumerable.Range(0, consumerCount)
            .Select(static index => $"client-{index:D2}")
            .ToArray();

        var allocations = consumers
            .Select(consumer => AverageQueueAllocator.Allocate(queues, consumers, consumer))
            .ToArray();
        var assigned = allocations
            .SelectMany(static allocation => allocation)
            .Select(static queue => (queue.BrokerName, queue.QueueId))
            .ToArray();

        Assert.Equal(9, assigned.Length);
        Assert.Equal(9, assigned.Distinct().Count());
        Assert.Equal(
            queues.Select(static queue => (queue.BrokerName, queue.QueueId)).Order(),
            assigned.Order());
        Assert.Equal(Math.Min(consumerCount, queues.Count), allocations.Count(static allocation => allocation.Count > 0));
        Assert.Equal(Math.Max(0, consumerCount - queues.Count), allocations.Count(static allocation => allocation.Count == 0));
        Assert.True(allocations.Max(static allocation => allocation.Count) -
                    allocations.Min(static allocation => allocation.Count) <= 1);
    }

    private static IReadOnlyList<RemotingConsumerQueue> CreateMultiBrokerQueues() =>
        Enumerable.Range(0, 3)
            .SelectMany(brokerIndex => Enumerable.Range(0, 3)
                .Select(queueId => new RemotingConsumerQueue(
                    "orders",
                    $"broker-{(char)('a' + brokerIndex)}",
                    queueId)))
            .ToArray();
}
