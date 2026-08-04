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

using System.Collections.Concurrent;
using System.Diagnostics;

namespace EventHorizon.RocketMQ.Remoting.IntegrationTests.Consumer.Push.Pop.Support;

internal sealed class RemotingSettlementObserver : IDisposable
{
    private readonly ConcurrentQueue<SettlementObservation> _observations = new();
    private readonly SemaphoreSlim _changed = new(0);
    private readonly ActivityListener _listener;

    public RemotingSettlementObserver()
    {
        _listener = new ActivityListener
        {
            ShouldListenTo = static source => source.Name == RemotingRocketMQInstrumentation.ActivitySourceName,
            Sample = static (ref ActivityCreationOptions<ActivityContext> _) =>
                ActivitySamplingResult.AllDataAndRecorded,
            ActivityStopped = ObserveStoppedActivity
        };
        ActivitySource.AddActivityListener(_listener);
    }

    public async Task WaitForAsync(
        string operationName,
        string topic,
        string consumerGroup,
        int count,
        TimeSpan timeout,
        CancellationToken cancellationToken,
        string? messageId = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(operationName);
        ArgumentException.ThrowIfNullOrWhiteSpace(topic);
        ArgumentException.ThrowIfNullOrWhiteSpace(consumerGroup);
        ArgumentOutOfRangeException.ThrowIfLessThan(count, 1);

        var elapsed = Stopwatch.StartNew();
        while (true)
        {
            var matching = _observations
                .Where(observation =>
                    string.Equals(observation.OperationName, operationName, StringComparison.Ordinal) &&
                    string.Equals(observation.Topic, topic, StringComparison.Ordinal) &&
                    string.Equals(observation.ConsumerGroup, consumerGroup, StringComparison.Ordinal) &&
                    (messageId is null ||
                     string.Equals(observation.MessageId, messageId, StringComparison.Ordinal)))
                .ToArray();
            var failed = matching.FirstOrDefault(static observation => observation.Failed);
            if (failed is not null)
            {
                throw new InvalidOperationException(
                    $"Remoting settlement '{operationName}' failed for {topic}/{consumerGroup}/{messageId}: " +
                    $"{failed.ErrorType ?? "unknown error"}.");
            }

            if (matching.Length >= count)
            {
                return;
            }

            var remaining = timeout - elapsed.Elapsed;
            if (remaining <= TimeSpan.Zero ||
                !await _changed.WaitAsync(remaining, cancellationToken).ConfigureAwait(false))
            {
                throw new TimeoutException(
                    $"Observed {matching.Length} of {count} successful Remoting '{operationName}' settlements for " +
                    $"{topic}/{consumerGroup}/{messageId} within {timeout}.");
            }
        }
    }

    public void Dispose()
    {
        _listener.Dispose();
        _changed.Dispose();
    }

    private void ObserveStoppedActivity(Activity activity)
    {
        if (!string.Equals(
                activity.GetTagItem("messaging.operation.type") as string,
                "settle",
                StringComparison.Ordinal))
        {
            return;
        }

        _observations.Enqueue(new SettlementObservation(
            activity.GetTagItem("messaging.operation.name") as string ?? string.Empty,
            activity.GetTagItem("messaging.destination.name") as string ?? string.Empty,
            activity.GetTagItem("messaging.consumer.group.name") as string ?? string.Empty,
            activity.GetTagItem("messaging.message.id") as string,
            activity.Status == ActivityStatusCode.Error,
            activity.GetTagItem("error.type") as string));
        _changed.Release();
    }

    private sealed record SettlementObservation(
        string OperationName,
        string Topic,
        string ConsumerGroup,
        string? MessageId,
        bool Failed,
        string? ErrorType);
}
