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

using EventHorizon.RocketMQ.Remoting.Consumer.Pull;

namespace EventHorizon.RocketMQ.Remoting.Consumer;

internal interface IRemotingConsumerEngine : IAsyncDisposable
{
    ValueTask StartAsync(CancellationToken cancellationToken = default);

    ValueTask StopAsync(CancellationToken cancellationToken = default);

    Task<IReadOnlyList<RemotingConsumerQueue>> GetMessageQueuesAsync(
        string topic,
        CancellationToken cancellationToken = default);

    Task<RemotingPullResult> PullAsync(
        RemotingConsumerQueue queue,
        long offset,
        int? maxMessages = null,
        CancellationToken cancellationToken = default);

    Task<long> GetOffsetAsync(
        RemotingConsumerQueue queue,
        CancellationToken cancellationToken = default);

    Task UpdateOffsetAsync(
        RemotingConsumerQueue queue,
        long offset,
        CancellationToken cancellationToken = default);

    Task<long> QueryOffsetAsync(
        RemotingConsumerQueue queue,
        ConsumeFromPosition position,
        DateTimeOffset? timestamp = null,
        CancellationToken cancellationToken = default);

    Task SendBackAsync(
        RemotingConsumerQueue queue,
        RemotingMessageView message,
        int delayLevel,
        int maxReconsumeTimes,
        CancellationToken cancellationToken);

    void SetSubscription(string topic, FilterExpression expression);

    void RemoveSubscription(string topic);
}
