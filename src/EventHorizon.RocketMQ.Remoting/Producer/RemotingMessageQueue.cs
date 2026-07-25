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

namespace EventHorizon.RocketMQ.Remoting.Producer;

/// <summary>
/// Identifies the classic remoting message queue selected for a send operation.
/// </summary>
public sealed class RemotingMessageQueue
{
    /// <summary>
    /// Initializes a new instance of the <see cref="RemotingMessageQueue"/> class.
    /// </summary>
    /// <param name="topic">The topic to which the queue belongs.</param>
    /// <param name="brokerName">The logical name of the broker that owns the queue.</param>
    /// <param name="queueId">The queue identifier within the broker.</param>
    public RemotingMessageQueue(string topic, string brokerName, int queueId)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(topic);
        ArgumentException.ThrowIfNullOrWhiteSpace(brokerName);
        ArgumentOutOfRangeException.ThrowIfNegative(queueId);

        Topic = topic;
        BrokerName = brokerName;
        QueueId = queueId;
    }

    /// <summary>
    /// Gets the topic to which the queue belongs.
    /// </summary>
    public string Topic { get; }

    /// <summary>
    /// Gets the logical name of the broker that owns the queue.
    /// </summary>
    public string BrokerName { get; }

    /// <summary>
    /// Gets the queue identifier within the topic and broker.
    /// </summary>
    public int QueueId { get; }
}
