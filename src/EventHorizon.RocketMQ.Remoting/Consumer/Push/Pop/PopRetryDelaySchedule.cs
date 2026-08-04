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

// This is Java classic POP's zero-based retry schedule, not the normal delayed-message level table. Level 0 selects
// by reconsume count; RemotingMessageView exposes that count as the one-based DeliveryAttempt.
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
}
