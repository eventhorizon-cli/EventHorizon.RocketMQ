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

using EventHorizon.RocketMQ.Remoting.Consumer.Coordination;
using EventHorizon.RocketMQ.Remoting.Consumer.Push.Assignment;
using EventHorizon.RocketMQ.Remoting.Consumer.Push.Processing;
using EventHorizon.RocketMQ.Remoting.Consumer.Push.Pull.Dispatch;
using EventHorizon.RocketMQ.Remoting.Consumer.Push.Pull.Receive;

namespace EventHorizon.RocketMQ.Remoting.Consumer.Push;

internal sealed class RemotingPushConsumerRun : IDisposable
{
    private int _retryTopicRequired;

    public RemotingPushConsumerRun(
        RemotingConsumerGroupSession groupSession,
        PushMessageHandlerInvoker messageHandlerInvoker)
    {
        ArgumentNullException.ThrowIfNull(groupSession);
        ArgumentNullException.ThrowIfNull(messageHandlerInvoker);
        GroupSession = groupSession;
        MessageHandlerInvoker = messageHandlerInvoker;
    }

    public bool RetryTopicRequired => Volatile.Read(ref _retryTopicRequired) != 0;

    public CancellationTokenSource Stopping { get; } = new();

    public SemaphoreSlim CoordinationGate { get; } = new(1, 1);

    public RemotingConsumerGroupSession GroupSession { get; }

    public PushMessageHandlerInvoker MessageHandlerInvoker { get; }

    public ConcurrentPullReceiveLoop? ConcurrentPullReceiveLoop { get; set; }

    public PullConsumeRequestProcessor? PullConsumeRequestProcessor { get; set; }

    public OrderlyPullReceiveLoop? OrderlyPullReceiveLoop { get; set; }

    public PushReceiverRegistry Receivers { get; } = new();

    public PushReceiverSupervisor? ReceiverSupervisor { get; set; }

    public PullDeliveryCoordinator? PullDeliveryCoordinator { get; set; }

    public void RequireRetryTopic() => Interlocked.Exchange(ref _retryTopicRequired, 1);

    public Task RunPullReceiverAsync(PullAssignmentReceiver receiver)
    {
        ArgumentNullException.ThrowIfNull(receiver);
        if (ConcurrentPullReceiveLoop is { } concurrent)
        {
            return concurrent.RunAsync(receiver);
        }

        if (OrderlyPullReceiveLoop is { } orderly)
        {
            return orderly.RunAsync(receiver);
        }

        throw new InvalidOperationException("The PULL receive loop has not been composed for this run.");
    }

    public void Dispose()
    {
        CoordinationGate.Dispose();
        PullDeliveryCoordinator?.Dispose();
        MessageHandlerInvoker.Dispose();
        Stopping.Dispose();
    }
}
