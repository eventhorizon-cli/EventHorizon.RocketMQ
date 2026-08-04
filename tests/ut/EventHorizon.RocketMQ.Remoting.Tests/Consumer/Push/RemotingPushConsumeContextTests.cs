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

using EventHorizon.RocketMQ.Remoting.Consumer.Push;
using Xunit;

namespace EventHorizon.RocketMQ.Remoting.Tests.Consumer.Push;

public sealed class RemotingPushConsumeContextTests
{
    [Fact]
    public void DelayLevelWhenNextConsume_NewContext_DelegatesRetryTimingToBroker()
    {
        var context = new RemotingPushConsumeContext();

        Assert.Equal(0, context.DelayLevelWhenNextConsume);
    }

    [Fact]
    public void AckIndex_CompleteBatchAcknowledgement_Defaults()
    {
        var context = new RemotingPushConsumeContext();

        Assert.Equal(int.MaxValue, context.AckIndex);
    }

    [Fact]
    public void AckIndex_NoMessages_AllowsAndAcknowledged()
    {
        var context = new RemotingPushConsumeContext
        {
            AckIndex = -1
        };

        Assert.Equal(-1, context.AckIndex);
    }

    [Theory]
    [InlineData(-2)]
    [InlineData(int.MinValue)]
    public void AckIndex_ValuesBelowMinusOne_Rejects(int value)
    {
        var context = new RemotingPushConsumeContext();

        var exception = Assert.Throws<ArgumentOutOfRangeException>(() => context.AckIndex = value);

        Assert.Equal("value", exception.ParamName);
    }
}
