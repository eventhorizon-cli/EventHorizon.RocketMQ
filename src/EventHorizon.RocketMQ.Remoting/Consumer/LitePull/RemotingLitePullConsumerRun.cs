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
using EventHorizon.RocketMQ.Remoting.Consumer.LitePull.Assignment;
using EventHorizon.RocketMQ.Remoting.Consumer.LitePull.Offset;
using EventHorizon.RocketMQ.Remoting.Consumer.LitePull.Receive;

namespace EventHorizon.RocketMQ.Remoting.Consumer.LitePull;

internal sealed class RemotingLitePullConsumerRun : IDisposable
{
    public RemotingLitePullConsumerRun(
        RemotingConsumerGroupSession groupSession,
        LitePullDeliveryState deliveryState,
        LitePullAssignmentCoordinator assignmentCoordinator,
        LitePullAssignmentReconciler assignmentReconciler,
        LitePullCommitLoop commitLoop)
    {
        ArgumentNullException.ThrowIfNull(groupSession);
        ArgumentNullException.ThrowIfNull(deliveryState);
        ArgumentNullException.ThrowIfNull(assignmentCoordinator);
        ArgumentNullException.ThrowIfNull(assignmentReconciler);
        ArgumentNullException.ThrowIfNull(commitLoop);

        GroupSession = groupSession;
        DeliveryState = deliveryState;
        AssignmentCoordinator = assignmentCoordinator;
        AssignmentReconciler = assignmentReconciler;
        CommitLoop = commitLoop;
    }

    public CancellationTokenSource Stopping { get; } = new();

    public SemaphoreSlim CoordinationGate { get; } = new(1, 1);

    public RemotingLitePullOperationGate OperationGate { get; } = new();

    public RemotingLitePullRunOperationTracker PublicOperations { get; } = new();

    public RemotingConsumerGroupSession GroupSession { get; }

    public LitePullDeliveryState DeliveryState { get; }

    public LitePullAssignmentCoordinator AssignmentCoordinator { get; }

    public LitePullAssignmentReconciler AssignmentReconciler { get; }

    public LitePullCommitLoop CommitLoop { get; }

    public LitePullReceiverRegistry Receivers { get; } = new();

    public Task AutoCommitTask { get; set; } = Task.CompletedTask;

    public LitePullManualAssignment? ManualAssignment { get; set; }

    public bool IsManualAssignment => ManualAssignment is not null;

    public IReadOnlyList<LitePullQueueState> CancelAllQueues()
    {
        var receivers = Receivers.CancelAll();
        DeliveryState.CancelAllAssignments();
        return receivers;
    }

    public void Dispose()
    {
        DeliveryState.Clear();
        Receivers.Clear();
        OperationGate.Dispose();
        CoordinationGate.Dispose();
        Stopping.Dispose();
    }
}
