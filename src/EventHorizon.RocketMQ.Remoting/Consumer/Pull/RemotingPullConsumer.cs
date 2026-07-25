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

using EventHorizon.RocketMQ.Consumer;
using EventHorizon.RocketMQ.Remoting.Consumer;

namespace EventHorizon.RocketMQ.Remoting.Consumer.Pull;

internal sealed class RemotingPullConsumer(IRemotingConsumerEngine engine) : IRemotingPullConsumer
{
    public ValueTask StartAsync(CancellationToken cancellationToken = default) =>
        engine.StartAsync(cancellationToken);

    public ValueTask StopAsync(CancellationToken cancellationToken = default) =>
        engine.StopAsync(cancellationToken);

    public Task<IReadOnlyList<RemotingPullMessageQueue>> GetMessageQueuesAsync(
        string topic,
        CancellationToken cancellationToken = default) =>
        engine.GetMessageQueuesAsync(topic, cancellationToken);

    public Task<RemotingPullResult> PullAsync(
        RemotingPullMessageQueue queue,
        long offset,
        int? maxMessages = null,
        CancellationToken cancellationToken = default) =>
        engine.PullAsync(queue, offset, maxMessages, cancellationToken);

    public Task<long> GetOffsetAsync(
        RemotingPullMessageQueue queue,
        CancellationToken cancellationToken = default) =>
        engine.GetOffsetAsync(queue, cancellationToken);

    public Task UpdateOffsetAsync(
        RemotingPullMessageQueue queue,
        long offset,
        CancellationToken cancellationToken = default) =>
        engine.UpdateOffsetAsync(queue, offset, cancellationToken);

    public Task<long> QueryOffsetAsync(
        RemotingPullMessageQueue queue,
        QueryOffsetPolicy policy,
        DateTimeOffset? timestamp = null,
        CancellationToken cancellationToken = default) =>
        engine.QueryOffsetAsync(queue, policy, timestamp, cancellationToken);

    public ValueTask DisposeAsync() => engine.DisposeAsync();
}
