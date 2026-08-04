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

namespace EventHorizon.RocketMQ.Remoting.Consumer.Push.Processing;

internal readonly record struct PushHandlerBatchResult(
    ConsumeResult Result,
    int AcknowledgedCount,
    int DelayLevelWhenNextConsume,
    int MessageCount)
{
    public bool IsFullySuccessful => Result == ConsumeResult.Success && AcknowledgedCount == MessageCount;

    public string TelemetryOutcome =>
        Result == ConsumeResult.Success && !IsFullySuccessful ? "partial_success" : Result.ToString();

    public static PushHandlerBatchResult Create(
        ConsumeResult result,
        int acknowledgementIndex,
        int delayLevelWhenNextConsume,
        int messageCount)
    {
        if (messageCount <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(messageCount), messageCount, "Message count must be positive.");
        }

        var acknowledgedCount = result == ConsumeResult.Success
            ? Math.Min(acknowledgementIndex, messageCount - 1) + 1
            : 0;
        return new PushHandlerBatchResult(result, acknowledgedCount, delayLevelWhenNextConsume, messageCount);
    }

    public static PushHandlerBatchResult All(
        ConsumeResult result,
        int messageCount,
        int delayLevelWhenNextConsume) =>
        new(result, result == ConsumeResult.Success ? messageCount : 0, delayLevelWhenNextConsume, messageCount);

    public static PushHandlerBatchResult Retry(int messageCount, int delayLevelWhenNextConsume) =>
        new(ConsumeResult.Retry, 0, delayLevelWhenNextConsume, messageCount);

    public PushHandlerMessageOutcome GetOutcome(int index)
    {
        if ((uint)index >= (uint)MessageCount)
        {
            throw new ArgumentOutOfRangeException(nameof(index), index, "Message index is outside the batch.");
        }

        return new PushHandlerMessageOutcome(
            Result == ConsumeResult.Success && index >= AcknowledgedCount ? ConsumeResult.Retry : Result,
            DelayLevelWhenNextConsume);
    }
}
