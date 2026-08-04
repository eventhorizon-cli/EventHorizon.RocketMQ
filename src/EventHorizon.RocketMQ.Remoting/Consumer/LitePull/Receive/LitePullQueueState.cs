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

using EventHorizon.RocketMQ.Remoting.Consumer.LitePull.Assignment;

namespace EventHorizon.RocketMQ.Remoting.Consumer.LitePull.Receive;

internal sealed class LitePullQueueState(LitePullReceiveTarget target, long offset) : IDisposable
{
    private int _canceled;
    private int _disposed;

    public RemotingConsumerQueue Queue { get; set; } = target.Queue;

    public FilterExpression Filter { get; set; } = target.Filter;

    public long FetchOffset { get; set; } = offset;

    public long DeliveredOffset { get; set; } = offset;

    public bool Paused { get; set; }

    public bool HasReceivedResponse { get; set; }

    public CancellationTokenSource ReceiveCancellation { get; } = new();

    public Task ReceiverTask { get; set; } = Task.CompletedTask;

    public void CancelReceive()
    {
        if (Interlocked.Exchange(ref _canceled, 1) == 0)
        {
            ReceiveCancellation.Cancel();
        }
    }

    public void Dispose()
    {
        if (Interlocked.Exchange(ref _disposed, 1) == 0)
        {
            ReceiveCancellation.Dispose();
        }
    }
}
