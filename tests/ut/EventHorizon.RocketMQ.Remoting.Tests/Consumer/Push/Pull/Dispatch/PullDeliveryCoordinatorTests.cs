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

using EventHorizon.RocketMQ.Remoting.Consumer.Push.Pull.Dispatch;
using Xunit;

namespace EventHorizon.RocketMQ.Remoting.Tests.Consumer.Push.Pull.Dispatch;

public sealed class PullDeliveryCoordinatorTests
{
    [Fact]
    public async Task ReserveDeliverySlotsAsync_ByteCapacityExhausted_WaitsUntilBytesAreReleased()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        using var scheduler = new PullDeliveryCoordinator(maxCachedMessages: 10, maxCachedMessageBytes: 5);

        await scheduler.ReserveDeliverySlotsAsync(count: 1, bodyBytes: 5, cancellationToken);
        var pending = scheduler.ReserveDeliverySlotsAsync(count: 1, bodyBytes: 1, cancellationToken).AsTask();

        Assert.False(pending.IsCompleted);

        scheduler.ReleaseDeliverySlots(count: 1, bodyBytes: 5);
        await pending.WaitAsync(TimeSpan.FromSeconds(3), cancellationToken);
    }

    [Fact]
    public async Task ReserveDeliverySlotsAsync_OldestBatchDoesNotFit_AllowsOneFittingWaiterToProceed()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        using var scheduler = new PullDeliveryCoordinator(maxCachedMessages: 3, maxCachedMessageBytes: 3);
        await scheduler.ReserveDeliverySlotsAsync(count: 2, bodyBytes: 2, cancellationToken);
        var large = scheduler.ReserveDeliverySlotsAsync(count: 2, bodyBytes: 2, cancellationToken).AsTask();
        var fitting = scheduler.ReserveDeliverySlotsAsync(count: 1, bodyBytes: 1, cancellationToken).AsTask();

        var fittingProceeded = false;
        try
        {
            await fitting.WaitAsync(TimeSpan.FromMilliseconds(500), cancellationToken);
            fittingProceeded = true;
            Assert.False(large.IsCompleted);
        }
        finally
        {
            if (fittingProceeded)
            {
                scheduler.ReleaseDeliverySlots(count: 1, bodyBytes: 1);
            }

            scheduler.ReleaseDeliverySlots(count: 2, bodyBytes: 2);
            await large.WaitAsync(TimeSpan.FromSeconds(1), cancellationToken);
            scheduler.ReleaseDeliverySlots(count: 2, bodyBytes: 2);
        }
    }

    [Fact]
    public async Task ReserveDeliverySlotsAsync_OldestBatchWasBypassedOnce_BlocksFurtherBypassUntilAdmitted()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        using var scheduler = new PullDeliveryCoordinator(maxCachedMessages: 3, maxCachedMessageBytes: 3);
        await scheduler.ReserveDeliverySlotsAsync(count: 2, bodyBytes: 2, cancellationToken);
        var large = scheduler.ReserveDeliverySlotsAsync(count: 2, bodyBytes: 2, cancellationToken).AsTask();
        var firstSmall = scheduler.ReserveDeliverySlotsAsync(count: 1, bodyBytes: 1, cancellationToken).AsTask();
        await firstSmall.WaitAsync(TimeSpan.FromMilliseconds(500), cancellationToken);
        var secondSmall = scheduler.ReserveDeliverySlotsAsync(count: 1, bodyBytes: 1, cancellationToken).AsTask();

        scheduler.ReleaseDeliverySlots(count: 1, bodyBytes: 1);
        await Task.Delay(TimeSpan.FromMilliseconds(100), cancellationToken);
        Assert.False(secondSmall.IsCompleted);

        scheduler.ReleaseDeliverySlots(count: 2, bodyBytes: 2);
        await large.WaitAsync(TimeSpan.FromSeconds(1), cancellationToken);
        await secondSmall.WaitAsync(TimeSpan.FromSeconds(1), cancellationToken);
        scheduler.ReleaseDeliverySlots(count: 2, bodyBytes: 2);
        scheduler.ReleaseDeliverySlots(count: 1, bodyBytes: 1);
    }
}
