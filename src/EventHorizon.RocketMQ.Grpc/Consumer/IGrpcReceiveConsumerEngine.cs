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

using Proto = Apache.Rocketmq.V2;

namespace EventHorizon.RocketMQ.Grpc.Consumer;

internal interface IGrpcReceiveConsumerEngine : IAsyncDisposable
{
    ValueTask StartAsync(CancellationToken cancellationToken);

    ValueTask StopAsync(CancellationToken cancellationToken = default);

    Task<IReadOnlyList<Proto.Assignment>> GetAssignmentsAsync(
        string topic,
        CancellationToken cancellationToken);

    KeyValuePair<string, FilterExpression>[] GetSubscriptions();

    Task SubscribeAsync(
        string topic,
        FilterExpression? expression,
        CancellationToken cancellationToken);

    Task UnsubscribeAsync(string topic, CancellationToken cancellationToken);

    Task<IReadOnlyList<GrpcMessageView>> ReceiveAsync(
        Proto.MessageQueue queue,
        FilterExpression filter,
        int maxMessages,
        TimeSpan invisibleDuration,
        TimeSpan longPollingTimeout,
        bool autoRenew,
        CancellationToken cancellationToken);

    Task AckAsync(GrpcMessageView message, CancellationToken cancellationToken);

    Task ChangeInvisibleDurationAsync(
        GrpcMessageView message,
        TimeSpan invisibleDuration,
        CancellationToken cancellationToken);

    Task ForwardToDeadLetterQueueAsync(
        GrpcMessageView message,
        int maxDeliveryAttempts,
        CancellationToken cancellationToken);

    int GetMaxDeliveryAttempts(GrpcMessageView message, int fallback);

    TimeSpan GetRetryDelay(GrpcMessageView message, TimeSpan fallback);

    TimeSpan GetRetryDelay(GrpcMessageView message, int deliveryAttempt, TimeSpan fallback);
}
