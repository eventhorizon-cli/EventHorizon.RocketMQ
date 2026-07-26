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

namespace EventHorizon.RocketMQ.Remoting.Consumer;

/// <summary>
/// Provides common configuration for RocketMQ consumers.
/// </summary>
public abstract class ConsumerOptions
{
    private readonly Dictionary<string, FilterExpression> _subscriptions =
        new(StringComparer.Ordinal);
    private readonly System.Collections.ObjectModel.ReadOnlyDictionary<string, FilterExpression>
        _readOnlySubscriptions;

    /// <summary>
    /// Initializes a new instance of the <see cref="ConsumerOptions"/> class.
    /// </summary>
    protected ConsumerOptions()
    {
        _readOnlySubscriptions = new(_subscriptions);
    }

    /// <summary>
    /// Gets or sets the name of the consumer group.
    /// </summary>
    public string GroupName { get; set; } = string.Empty;

    /// <summary>
    /// Gets a live, read-only view of the topic subscriptions configured for this consumer.
    /// </summary>
    public IReadOnlyDictionary<string, FilterExpression> Subscriptions => _readOnlySubscriptions;

    /// <summary>
    /// Adds or replaces the filter expression for a topic subscription.
    /// </summary>
    /// <param name="topic">The topic to subscribe to.</param>
    /// <param name="expression">
    /// The filter expression, or <see langword="null"/> to match every message in the topic.
    /// </param>
    /// <exception cref="ArgumentNullException"><paramref name="topic"/> is <see langword="null"/>.</exception>
    /// <exception cref="ArgumentException">
    /// <paramref name="topic"/> is empty or consists only of white-space characters.
    /// </exception>
    public void Subscribe(string topic, FilterExpression? expression = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(topic);
        _subscriptions[topic] = expression ?? FilterExpression.All;
    }
}
