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
using Xunit;

namespace EventHorizon.RocketMQ.Shared.Tests.Consumer;

public sealed class ConsumerOptionsTests
{
    [Fact]
    public void FilterExpression_RejectsNullExpression()
    {
        Assert.Throws<ArgumentNullException>(() => new FilterExpression(null!));
    }

    [Theory]
    [InlineData("")]
    [InlineData(" ")]
    [InlineData("\t")]
    public void FilterExpression_RejectsBlankExpression(string expression)
    {
        Assert.Throws<ArgumentException>(() => new FilterExpression(expression));
    }

    [Fact]
    public void FilterExpression_RejectsUnknownType()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            new FilterExpression("*", (FilterExpressionType)int.MaxValue));
    }

    [Fact]
    public void FilterExpression_PreservesPositionalDeconstruction()
    {
        var expression = new FilterExpression("region = 'west'", FilterExpressionType.Sql);

        var (value, type) = expression;

        Assert.Equal("region = 'west'", value);
        Assert.Equal(FilterExpressionType.Sql, type);
    }

    [Fact]
    public void Subscribe_RejectsBlankTopic()
    {
        var options = new TestConsumerOptions();

        Assert.Throws<ArgumentException>(() => options.Subscribe(" "));
    }

    [Fact]
    public void Subscribe_UsesAllFilterAndExposesReadOnlyView()
    {
        var options = new TestConsumerOptions();
        options.Subscribe("orders");

        Assert.Same(FilterExpression.All, options.Subscriptions["orders"]);
        var dictionary = Assert.IsAssignableFrom<IDictionary<string, FilterExpression>>(options.Subscriptions);
        Assert.Throws<NotSupportedException>(() =>
            dictionary.Add("payments", new FilterExpression("created")));
    }

    [Fact]
    public void Subscribe_ReplacesExistingTopicFilter()
    {
        var options = new TestConsumerOptions();
        options.Subscribe("orders", new FilterExpression("created"));
        options.Subscribe("orders", new FilterExpression("paid"));

        Assert.Single(options.Subscriptions);
        Assert.Equal("paid", options.Subscriptions["orders"].Expression);
    }

    private sealed class TestConsumerOptions : ConsumerOptions;
}
