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

namespace EventHorizon.RocketMQ.Grpc.Consumer;

/// <summary>
/// Represents an expression used to filter messages in a topic subscription.
/// </summary>
public sealed record FilterExpression
{
    /// <summary>
    /// Gets the tag expression that matches every message.
    /// </summary>
    public static FilterExpression All { get; } = new("*");

    /// <summary>
    /// Initializes a new instance of the <see cref="FilterExpression"/> record.
    /// </summary>
    /// <param name="expression">The filter expression text.</param>
    /// <param name="type">The syntax used by <paramref name="expression"/>.</param>
    /// <exception cref="ArgumentNullException"><paramref name="expression"/> is <see langword="null"/>.</exception>
    /// <exception cref="ArgumentException">
    /// <paramref name="expression"/> is empty or consists only of white-space characters.
    /// </exception>
    /// <exception cref="ArgumentOutOfRangeException">
    /// <paramref name="type"/> is not a defined filter expression type.
    /// </exception>
    public FilterExpression(string expression, FilterExpressionType type = FilterExpressionType.Tag)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(expression);
        if (!Enum.IsDefined(type))
        {
            throw new ArgumentOutOfRangeException(nameof(type), type, "Unknown RocketMQ filter expression type.");
        }

        Expression = expression;
        Type = type;
    }

    /// <summary>
    /// Gets the filter expression text.
    /// </summary>
    public string Expression { get; }

    /// <summary>
    /// Gets the syntax used by the filter expression.
    /// </summary>
    public FilterExpressionType Type { get; }

    /// <summary>
    /// Deconstructs this filter into its expression text and syntax type.
    /// </summary>
    /// <param name="expression">The filter expression text.</param>
    /// <param name="type">The filter expression syntax type.</param>
    public void Deconstruct(out string expression, out FilterExpressionType type)
    {
        expression = Expression;
        type = Type;
    }
}
