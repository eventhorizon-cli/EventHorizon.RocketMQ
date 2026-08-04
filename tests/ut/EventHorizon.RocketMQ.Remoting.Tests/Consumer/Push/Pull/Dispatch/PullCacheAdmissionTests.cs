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

public sealed class PullCacheAdmissionTests
{
    [Theory]
    [InlineData(0, 1)]
    [InlineData(-1, 1)]
    [InlineData(1, 0)]
    [InlineData(1, -1)]
    public void Constructor_InvalidCapacity_ThrowsArgumentOutOfRangeException(
        int maxCachedMessages,
        int maxCachedMessageBytes)
    {
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            new PullCacheAdmission(maxCachedMessages, maxCachedMessageBytes));
    }

    [Theory]
    [InlineData(0, 1)]
    [InlineData(-1, 1)]
    [InlineData(1, -1)]
    public void ReserveAsync_InvalidArguments_ThrowsArgumentOutOfRangeException(int count, int bodyBytes)
    {
        var admission = new PullCacheAdmission(maxCachedMessages: 1, maxCachedMessageBytes: 1);

        Assert.Throws<ArgumentOutOfRangeException>(() =>
            admission.ReserveAsync(count, bodyBytes, CancellationToken.None).GetAwaiter().GetResult());
    }

    [Fact]
    public void ReserveAsync_CanceledToken_ThrowsOperationCanceledException()
    {
        var admission = new PullCacheAdmission(maxCachedMessages: 1, maxCachedMessageBytes: 1);
        using var cancellation = new CancellationTokenSource();
        cancellation.Cancel();

        Assert.Throws<OperationCanceledException>(() =>
            admission.ReserveAsync(count: 1, bodyBytes: 1, cancellation.Token).GetAwaiter().GetResult());
    }

    [Fact]
    public void Release_InvalidReservation_ThrowsInvalidOperationException()
    {
        var admission = new PullCacheAdmission(maxCachedMessages: 1, maxCachedMessageBytes: 1);

        Assert.Throws<InvalidOperationException>(() =>
            admission.Release(new PullCacheReservation(MessageCount: 1, BodyBytes: 1)));
    }

    [Fact]
    public async Task ReserveAsync_CanceledWaitingReservation_DoesNotLeakCapacity()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var admission = new PullCacheAdmission(maxCachedMessages: 1, maxCachedMessageBytes: 1);
        var reservation = new PullCacheReservation(MessageCount: 1, BodyBytes: 1);
        await admission.ReserveAsync(reservation.MessageCount, reservation.BodyBytes, cancellationToken);

        using var waitingCancellation = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        var waiting = admission.ReserveAsync(
            reservation.MessageCount,
            reservation.BodyBytes,
            waitingCancellation.Token).AsTask();
        Assert.False(waiting.IsCompleted);

        waitingCancellation.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => waiting);

        var later = admission.ReserveAsync(
            reservation.MessageCount,
            reservation.BodyBytes,
            cancellationToken).AsTask();
        Assert.False(later.IsCompleted);

        admission.Release(reservation);
        await later.WaitAsync(TimeSpan.FromSeconds(3), cancellationToken);
        admission.Release(reservation);
    }

    [Fact]
    public async Task ReserveAsync_GrantRacesWithCancellation_DoesNotLeakCapacity()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var admission = new PullCacheAdmission(maxCachedMessages: 1, maxCachedMessageBytes: 1);
        var reservation = new PullCacheReservation(MessageCount: 1, BodyBytes: 1);
        await admission.ReserveAsync(reservation.MessageCount, reservation.BodyBytes, cancellationToken);

        using var cancellation = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        var waiting = admission.ReserveAsync(
            reservation.MessageCount,
            reservation.BodyBytes,
            cancellation.Token).AsTask();
        Assert.False(waiting.IsCompleted);

        using var releaseOnCancellation = cancellation.Token.Register(() => admission.Release(reservation));
        cancellation.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => waiting);

        var later = admission.ReserveAsync(
            reservation.MessageCount,
            reservation.BodyBytes,
            cancellationToken).AsTask();
        await later.WaitAsync(TimeSpan.FromSeconds(3), cancellationToken);
        admission.Release(reservation);
    }

    [Fact]
    public async Task ReserveAsync_OversizedBatch_AdmitsOnlyWhenCacheIsEmpty()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var admission = new PullCacheAdmission(maxCachedMessages: 2, maxCachedMessageBytes: 2);
        var regularReservation = new PullCacheReservation(MessageCount: 1, BodyBytes: 1);
        var oversizedReservation = new PullCacheReservation(MessageCount: 3, BodyBytes: 3);

        await admission.ReserveAsync(
            regularReservation.MessageCount,
            regularReservation.BodyBytes,
            cancellationToken);
        var oversized = admission.ReserveAsync(
            oversizedReservation.MessageCount,
            oversizedReservation.BodyBytes,
            cancellationToken).AsTask();
        Assert.False(oversized.IsCompleted);

        admission.Release(regularReservation);
        await oversized.WaitAsync(TimeSpan.FromSeconds(3), cancellationToken);

        var secondOversized = admission.ReserveAsync(
            oversizedReservation.MessageCount,
            oversizedReservation.BodyBytes,
            cancellationToken).AsTask();
        Assert.False(secondOversized.IsCompleted);

        admission.Release(oversizedReservation);
        await secondOversized.WaitAsync(TimeSpan.FromSeconds(3), cancellationToken);
        admission.Release(oversizedReservation);
    }
}
