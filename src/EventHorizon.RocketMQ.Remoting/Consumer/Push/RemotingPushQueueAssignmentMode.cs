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

namespace EventHorizon.RocketMQ.Remoting.Consumer.Push;

/// <summary>
/// Specifies whether a classic Remoting Push consumer calculates queue assignments locally or queries them from the
/// Broker.
/// </summary>
/// <remarks>
/// <para>
/// Assignment ownership and message reception are separate decisions. This value does not change the public Push
/// consumer model and is not a PULL/POP selector.
/// </para>
/// <para>
/// <see cref="Client"/> is the default, matching the official Java Remoting client's default
/// <c>clientRebalance=true</c>. Client assignment always creates internal PULL receivers. Broker assignment is
/// effective only for concurrent clustered consumption; each assignment returned by the Broker contains its own PULL
/// or POP receive mode. Broadcasting and orderly consumption always use effective client assignment and PULL.
/// </para>
/// <para>
/// Applications use the same Push message handler in every case. The consumer queries but never changes the Broker's
/// topic-and-consumer-group request-mode configuration.
/// </para>
/// </remarks>
/// <seealso href="https://rocketmq.apache.org/docs/4.x/consumer/02push/">
/// Apache RocketMQ classic PushConsumer guide.
/// </seealso>
/// <seealso href="https://github.com/apache/rocketmq/blob/rocketmq-all-5.5.0/client/src/main/java/org/apache/rocketmq/client/consumer/DefaultMQPushConsumer.java#L266-L267">
/// Apache RocketMQ 5.5.0 default client-rebalance configuration.
/// </seealso>
/// <seealso href="https://github.com/apache/rocketmq/blob/rocketmq-all-5.5.0/client/src/main/java/org/apache/rocketmq/client/impl/consumer/RebalancePushImpl.java#L125-L128">
/// Apache RocketMQ 5.5.0 effective client-rebalance selection.
/// </seealso>
/// <seealso href="https://github.com/apache/rocketmq/blob/rocketmq-all-5.5.0/broker/src/main/java/org/apache/rocketmq/broker/processor/QueryAssignmentProcessor.java#L92-L132">
/// Apache RocketMQ 5.5.0 Broker assignment implementation.
/// </seealso>
public enum RemotingPushQueueAssignmentMode
{
    /// <summary>
    /// Uses client-side queue assignment and internal PULL reception.
    /// </summary>
    /// <remarks>
    /// <para>
    /// This is the default and applies to clustered, broadcasting, and orderly consumption. For clustered
    /// consumption, the client combines topic routes, the consumer-group membership, and its allocation strategy to
    /// select the queues owned by this instance. Broadcasting assigns every readable queue to each instance.
    /// </para>
    /// <para>
    /// The SDK long-polls those queues through internal PULL receivers and invokes the Push handler; applications do
    /// not manage receive offsets or invoke the wire operation themselves.
    /// </para>
    /// </remarks>
    Client,

    /// <summary>
    /// Uses Broker-side queue assignment when the consumer is concurrent and clustered.
    /// </summary>
    /// <remarks>
    /// <para>
    /// The consumer sends <c>QUERY_ASSIGNMENT</c>. Each returned assignment identifies the owned queue or wildcard
    /// queue and independently selects an internal PULL or POP receiver. Broker assignment therefore does not imply
    /// POP, and this value does not configure the Broker's request mode.
    /// </para>
    /// <para>
    /// When broadcasting or orderly consumption is configured, the runtime follows the classic Java client and
    /// applies client-side assignment with PULL instead.
    /// </para>
    /// </remarks>
    Broker
}
