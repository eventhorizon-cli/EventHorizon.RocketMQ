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
/// Provides acknowledgement settings for one classic remoting Push consumer batch delivery.
/// </summary>
/// <remarks>
/// For a concurrent non-FIFO batch, <see cref="AckIndex"/> selects the acknowledged prefix when the handler returns
/// <see cref="ConsumeResult.Success"/>, while <see cref="DelayLevelWhenNextConsume"/> selects retry timing or direct
/// dead-lettering when it returns <see cref="ConsumeResult.Retry"/>. This mirrors RocketMQ's concurrent-consumer
/// acknowledgement model. FIFO <c>MessageGroup</c> and orderly deliveries are singleton paths and ignore these
/// settings.
/// </remarks>
public sealed class RemotingPushConsumeContext
{
    private int _ackIndex = int.MaxValue;

    /// <summary>
    /// Gets or sets the zero-based index of the final message acknowledged from the current batch.
    /// </summary>
    /// <remarks>
    /// The default value acknowledges the complete batch. A value of <c>-1</c> acknowledges no messages.
    /// Values greater than the last message index are treated as the last message index. This setting is
    /// ignored when the handler returns <see cref="ConsumeResult.Retry"/>, because that outcome applies to the
    /// complete batch.
    /// </remarks>
    public int AckIndex
    {
        get => _ackIndex;
        set
        {
            if (value < -1)
            {
                throw new ArgumentOutOfRangeException(nameof(value), value, "Acknowledgement index cannot be less than -1.");
            }

            _ackIndex = value;
        }
    }

    /// <summary>
    /// Gets or sets the RocketMQ delay level used when the consumer sends unacknowledged messages back.
    /// </summary>
    /// <remarks>
    /// The default value is <c>0</c>, which lets the Broker choose the retry interval. Set it to a positive RocketMQ
    /// delay level to select a Broker-defined interval, or to a negative value to explicitly request direct
    /// dead-letter delivery. For POP retries at <see cref="RemotingPushConsumerOptions.MaxDeliveryAttempts"/>, the
    /// official age-based terminal policy takes precedence over this value. If
    /// <see cref="RemotingPushConsumerOptions.ConsumeTimeout"/> elapses first, the consumer ignores this value and
    /// retries the complete batch using the Broker-selected interval.
    /// </remarks>
    /// <seealso href="https://github.com/apache/rocketmq/blob/rocketmq-all-5.5.0/client/src/main/java/org/apache/rocketmq/client/consumer/listener/ConsumeConcurrentlyContext.java">
    /// Apache RocketMQ Java concurrent consume context.
    /// </seealso>
    /// <seealso href="https://github.com/apache/rocketmq-client-go/blob/99c433634e09f72fa2778ca04411de29d4fc9cff/primitive/ctx.go#L133-L152">
    /// Apache RocketMQ Go classic concurrent consume context.
    /// </seealso>
    public int DelayLevelWhenNextConsume { get; set; }
}
