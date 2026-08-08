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

namespace EventHorizon.RocketMQ.Remoting.Consumer.Push.Pop;

// This is Java classic POP's zero-based retry schedule, not the normal delayed-message level table. An application
// delay level of 0 selects by reconsumeTimes; RemotingMessageView exposes the corresponding count as the one-based
// DeliveryAttempt, hence the subtraction in Resolve. A positive level indexes this table directly, and an oversized
// level is capped at the final delay just as the Java client does. Once the delivery limit is reached, Java selects the
// next delay from message age and ACKs only after the age is strictly greater than twice the final delay.
// Design: docs/en-US/remoting/consumer-model.md#handler-outcomes-and-pop-fixed-deadlines.
// Apache Java reference:
// https://github.com/apache/rocketmq/blob/rocketmq-all-5.5.0/client/src/main/java/org/apache/rocketmq/client/impl/consumer/ConsumeMessagePopConcurrentlyService.java#L287-L298
internal static class PopRetryDelaySchedule
{
    private static ReadOnlySpan<int> DelaySeconds =>
    [
        10,
        30,
        60,
        120,
        180,
        240,
        300,
        360,
        420,
        480,
        540,
        600,
        1_200,
        1_800,
        3_600,
        7_200
    ];

    public static TimeSpan Resolve(int delayLevelWhenNextConsume, int deliveryAttempt)
    {
        if (delayLevelWhenNextConsume < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(delayLevelWhenNextConsume));
        }

        if (deliveryAttempt <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(deliveryAttempt));
        }

        var index = delayLevelWhenNextConsume == 0
            ? deliveryAttempt - 1
            : delayLevelWhenNextConsume;
        index = Math.Min(index, DelaySeconds.Length - 1);
        return TimeSpan.FromSeconds(DelaySeconds[index]);
    }

    public static TimeSpan? ResolveAfterMaximumAttempts(TimeSpan messageAge)
    {
        var maximumDelay = TimeSpan.FromSeconds(DelaySeconds[^1]);
        if (messageAge > maximumDelay + maximumDelay)
        {
            return null;
        }

        var index = DelaySeconds.Length - 1;
        for (; index >= 0; index--)
        {
            if (messageAge >= TimeSpan.FromSeconds(DelaySeconds[index]))
            {
                index++;
                break;
            }
        }

        // Reaching the maximum attempt before the first delay is not expected with this schedule, but clamping keeps
        // malformed or future producer timestamps from turning a retry decision into an indexing failure.
        index = Math.Clamp(index, 0, DelaySeconds.Length - 1);
        return TimeSpan.FromSeconds(DelaySeconds[index]);
    }
}
