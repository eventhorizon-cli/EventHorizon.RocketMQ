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

using EventHorizon.RocketMQ.Grpc.Consumer;
using Xunit;

namespace EventHorizon.RocketMQ.Grpc.Tests.Consumer;

public sealed class ConsumeResultTests
{
    [Fact]
    public void SuccessAndFailure_RepeatedAccess_ReturnsSingletons()
    {
        Assert.Same(ConsumeResult.Success, ConsumeResult.Success);
        Assert.Same(ConsumeResult.Failure, ConsumeResult.Failure);
        Assert.Equal("Success", ConsumeResult.Success.ToString());
        Assert.Equal("Failure", ConsumeResult.Failure.ToString());
    }

    [Fact]
    public void Suspend_MinimumDuration_PreservesValueSemantics()
    {
        var duration = TimeSpan.FromMilliseconds(50);

        var first = ConsumeResult.Suspend(duration);
        var second = ConsumeResult.Suspend(duration);

        Assert.NotSame(first, second);
        Assert.Equal(first, second);
        Assert.Equal(duration, first.SuspendDuration);
        Assert.Equal("Suspend", first.ToString());
    }

    [Theory]
    [InlineData(-1)]
    [InlineData(0)]
    [InlineData(49)]
    public void Suspend_DurationBelowMinimum_Rejects(int milliseconds)
    {
        var exception = Assert.Throws<ArgumentOutOfRangeException>(() =>
            ConsumeResult.Suspend(TimeSpan.FromMilliseconds(milliseconds)));

        Assert.Equal("duration", exception.ParamName);
    }
}
