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

using EventHorizon.RocketMQ.Grpc.Protocol.Telemetry;
using Proto = Apache.Rocketmq.V2;

namespace EventHorizon.RocketMQ.Grpc.Protocol;

internal interface IRocketMQGrpcClient : IAsyncDisposable
{
    Uri AccessPoint { get; }
    Proto.Endpoints AccessPointProtobuf { get; }
    Task<Proto.QueryRouteResponse> QueryRouteAsync(Proto.QueryRouteRequest request, CancellationToken cancellationToken);
    Task<Proto.SendMessageResponse> SendMessageAsync(Uri endpoint, Proto.SendMessageRequest request, TimeSpan timeout, CancellationToken cancellationToken);
    Task<Proto.QueryAssignmentResponse> QueryAssignmentAsync(Uri endpoint, Proto.QueryAssignmentRequest request, CancellationToken cancellationToken);
    Task<IReadOnlyList<Proto.ReceiveMessageResponse>> ReceiveMessageAsync(Uri endpoint, Proto.ReceiveMessageRequest request, TimeSpan timeout, CancellationToken cancellationToken);
    Task<Proto.AckMessageResponse> AckMessageAsync(Uri endpoint, Proto.AckMessageRequest request, CancellationToken cancellationToken);
    Task<Proto.ChangeInvisibleDurationResponse> ChangeInvisibleDurationAsync(Uri endpoint, Proto.ChangeInvisibleDurationRequest request, CancellationToken cancellationToken);
    Task<Proto.ForwardMessageToDeadLetterQueueResponse> ForwardToDeadLetterQueueAsync(Uri endpoint, Proto.ForwardMessageToDeadLetterQueueRequest request, CancellationToken cancellationToken);
    Task<Proto.EndTransactionResponse> EndTransactionAsync(Uri endpoint, Proto.EndTransactionRequest request, CancellationToken cancellationToken);
    Task<Proto.HeartbeatResponse> HeartbeatAsync(Uri endpoint, Proto.HeartbeatRequest request, CancellationToken cancellationToken);
    Task<Proto.NotifyClientTerminationResponse> NotifyTerminationAsync(Uri endpoint, Proto.NotifyClientTerminationRequest request, CancellationToken cancellationToken);
    Task<Proto.RecallMessageResponse> RecallMessageAsync(Uri endpoint, Proto.RecallMessageRequest request, CancellationToken cancellationToken);
    Task<Proto.SyncLiteSubscriptionResponse> SyncLiteSubscriptionAsync(
        Uri endpoint,
        Proto.SyncLiteSubscriptionRequest request,
        CancellationToken cancellationToken);
    Task<IGrpcTelemetrySession> OpenTelemetryAsync(
        Uri endpoint,
        Proto.Settings settings,
        Func<Proto.TelemetryCommand, CancellationToken, Task>? commandHandler,
        CancellationToken cancellationToken);
}
