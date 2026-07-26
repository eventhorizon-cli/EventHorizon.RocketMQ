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

using EventHorizon.RocketMQ.Remoting.Consumer;

namespace EventHorizon.RocketMQ.Remoting.Consumer.Pull;

internal sealed class RemotingPullConsumer : IRemotingPullConsumer
{
    private readonly IRemotingConsumerEngine _engine;

    public RemotingPullConsumer(IRemotingConsumerEngine engine)
    {
        _engine = engine;
    }

    public ValueTask StartAsync(CancellationToken cancellationToken = default) =>
        _engine.StartAsync(cancellationToken);

    public ValueTask StopAsync(CancellationToken cancellationToken = default) =>
        _engine.StopAsync(cancellationToken);

    public Task<IReadOnlyList<RemotingPullMessageQueue>> GetMessageQueuesAsync(
        string topic,
        CancellationToken cancellationToken = default) =>
        _engine.GetMessageQueuesAsync(topic, cancellationToken);

    public async Task<RemotingPullResult> PullAsync(
        RemotingPullMessageQueue queue,
        long offset,
        int? maxMessages = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(queue);
        return await _engine.PullAsync(queue, offset, maxMessages, cancellationToken).ConfigureAwait(false);
    }

    public Task<long> GetOffsetAsync(
        RemotingPullMessageQueue queue,
        CancellationToken cancellationToken = default) =>
        _engine.GetOffsetAsync(queue, cancellationToken);

    public async Task UpdateOffsetAsync(
        RemotingPullMessageQueue queue,
        long offset,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(queue);
        await _engine.UpdateOffsetAsync(queue, offset, cancellationToken).ConfigureAwait(false);
    }

    public Task<long> QueryOffsetAsync(
        RemotingPullMessageQueue queue,
        QueryOffsetPolicy policy,
        DateTimeOffset? timestamp = null,
        CancellationToken cancellationToken = default) =>
        _engine.QueryOffsetAsync(queue, policy, timestamp, cancellationToken);

    public ValueTask DisposeAsync() => _engine.DisposeAsync();
}
