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

using Xunit;
using GrpcConsumerOptions = EventHorizon.RocketMQ.Grpc.Consumer.ConsumerOptions;
using GrpcFilterExpression = EventHorizon.RocketMQ.Grpc.Consumer.FilterExpression;
using GrpcFilterExpressionType = EventHorizon.RocketMQ.Grpc.Consumer.FilterExpressionType;
using RemotingConsumerOptions = EventHorizon.RocketMQ.Remoting.Consumer.ConsumerOptions;
using RemotingFilterExpression = EventHorizon.RocketMQ.Remoting.Consumer.FilterExpression;
using RemotingFilterExpressionType = EventHorizon.RocketMQ.Remoting.Consumer.FilterExpressionType;

namespace EventHorizon.RocketMQ.Compatibility.Tests;

public sealed class ConsumerOptionsTests
{
    [Fact]
    public void FilterExpression_RejectsNullExpression()
    {
        Assert.Throws<ArgumentNullException>(() => new GrpcFilterExpression(null!));
        Assert.Throws<ArgumentNullException>(() => new RemotingFilterExpression(null!));
    }

    [Theory]
    [InlineData("")]
    [InlineData(" ")]
    [InlineData("\t")]
    public void FilterExpression_RejectsBlankExpression(string expression)
    {
        Assert.Throws<ArgumentException>(() => new GrpcFilterExpression(expression));
        Assert.Throws<ArgumentException>(() => new RemotingFilterExpression(expression));
    }

    [Fact]
    public void FilterExpression_RejectsUnknownType()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            new GrpcFilterExpression("*", (GrpcFilterExpressionType)int.MaxValue));
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            new RemotingFilterExpression("*", (RemotingFilterExpressionType)int.MaxValue));
    }

    [Fact]
    public void FilterExpression_PreservesPositionalDeconstruction()
    {
        var grpcExpression = new GrpcFilterExpression("region = 'west'", GrpcFilterExpressionType.Sql);
        var remotingExpression = new RemotingFilterExpression("region = 'west'", RemotingFilterExpressionType.Sql);

        var (grpcValue, grpcType) = grpcExpression;
        var (remotingValue, remotingType) = remotingExpression;

        Assert.Equal("region = 'west'", grpcValue);
        Assert.Equal(GrpcFilterExpressionType.Sql, grpcType);
        Assert.Equal("region = 'west'", remotingValue);
        Assert.Equal(RemotingFilterExpressionType.Sql, remotingType);
    }

    [Fact]
    public void Subscribe_RejectsBlankTopic()
    {
        var grpcOptions = new GrpcTestConsumerOptions();
        var remotingOptions = new RemotingTestConsumerOptions();

        Assert.Throws<ArgumentException>(() => grpcOptions.Subscribe(" "));
        Assert.Throws<ArgumentException>(() => remotingOptions.Subscribe(" "));
    }

    [Fact]
    public void Subscribe_UsesAllFilterAndExposesReadOnlyView()
    {
        var grpcOptions = new GrpcTestConsumerOptions();
        grpcOptions.Subscribe("orders");
        var remotingOptions = new RemotingTestConsumerOptions();
        remotingOptions.Subscribe("orders");

        Assert.Same(GrpcFilterExpression.All, grpcOptions.Subscriptions["orders"]);
        var grpcDictionary = Assert.IsAssignableFrom<IDictionary<string, GrpcFilterExpression>>(
            grpcOptions.Subscriptions);
        Assert.Throws<NotSupportedException>(() =>
            grpcDictionary.Add("payments", new GrpcFilterExpression("created")));

        Assert.Same(RemotingFilterExpression.All, remotingOptions.Subscriptions["orders"]);
        var remotingDictionary = Assert.IsAssignableFrom<IDictionary<string, RemotingFilterExpression>>(
            remotingOptions.Subscriptions);
        Assert.Throws<NotSupportedException>(() =>
            remotingDictionary.Add("payments", new RemotingFilterExpression("created")));
    }

    [Fact]
    public void Subscribe_ReplacesExistingTopicFilter()
    {
        var grpcOptions = new GrpcTestConsumerOptions();
        grpcOptions.Subscribe("orders", new GrpcFilterExpression("created"));
        grpcOptions.Subscribe("orders", new GrpcFilterExpression("paid"));
        var remotingOptions = new RemotingTestConsumerOptions();
        remotingOptions.Subscribe("orders", new RemotingFilterExpression("created"));
        remotingOptions.Subscribe("orders", new RemotingFilterExpression("paid"));

        Assert.Single(grpcOptions.Subscriptions);
        Assert.Equal("paid", grpcOptions.Subscriptions["orders"].Expression);
        Assert.Single(remotingOptions.Subscriptions);
        Assert.Equal("paid", remotingOptions.Subscriptions["orders"].Expression);
    }

    private sealed class GrpcTestConsumerOptions : GrpcConsumerOptions
    {
    }

    private sealed class RemotingTestConsumerOptions : RemotingConsumerOptions
    {
    }
}
