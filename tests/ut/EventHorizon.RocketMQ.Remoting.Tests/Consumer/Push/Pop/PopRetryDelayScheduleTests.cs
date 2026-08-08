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

using EventHorizon.RocketMQ.Remoting.Consumer.Push.Pop;
using Xunit;

namespace EventHorizon.RocketMQ.Remoting.Tests.Consumer.Push.Pop;

public sealed class PopRetryDelayScheduleTests
{
    [Theory]
    [InlineData(0, 1, 10)]
    [InlineData(0, 2, 30)]
    [InlineData(2, 1, 60)]
    [InlineData(15, 1, 7_200)]
    [InlineData(int.MaxValue, 1, 7_200)]
    public void Resolve_DelayLevelAndDeliveryAttempt_ReturnsJavaPopDelay(
        int delayLevelWhenNextConsume,
        int deliveryAttempt,
        int expectedSeconds)
    {
        var result = PopRetryDelaySchedule.Resolve(delayLevelWhenNextConsume, deliveryAttempt);

        Assert.Equal(TimeSpan.FromSeconds(expectedSeconds), result);
    }

    [Theory]
    [InlineData(0, 10)]
    [InlineData(10, 30)]
    [InlineData(30, 60)]
    [InlineData(3_600, 7_200)]
    [InlineData(7_200, 7_200)]
    [InlineData(14_400, 7_200)]
    public void ResolveAfterMaximumAttempts_AgeAtOrBelowCutoff_ReturnsNextJavaPopDelay(
        int messageAgeSeconds,
        int expectedDelaySeconds)
    {
        var result = PopRetryDelaySchedule.ResolveAfterMaximumAttempts(
            TimeSpan.FromSeconds(messageAgeSeconds));

        Assert.Equal(TimeSpan.FromSeconds(expectedDelaySeconds), result);
    }

    [Fact]
    public void ResolveAfterMaximumAttempts_AgeExceedsCutoff_ReturnsTerminalAcknowledgement()
    {
        var result = PopRetryDelaySchedule.ResolveAfterMaximumAttempts(
            TimeSpan.FromHours(4) + TimeSpan.FromMilliseconds(1));

        Assert.Null(result);
    }
}
