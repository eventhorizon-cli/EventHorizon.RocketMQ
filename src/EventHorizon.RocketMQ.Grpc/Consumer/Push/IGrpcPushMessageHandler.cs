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

using EventHorizon.RocketMQ.Grpc.Consumer;
using Microsoft.Extensions.DependencyInjection;

namespace EventHorizon.RocketMQ.Grpc.Consumer.Push;

/// <summary>
/// Processes a message delivered by a gRPC Push or Lite Push consumer.
/// </summary>
/// <remarks>
/// A handler registered with <see cref="ServiceLifetime.Singleton"/> can receive concurrent calls and must be
/// thread-safe. Scoped and transient handlers are resolved for each message handling attempt.
/// </remarks>
public interface IGrpcPushMessageHandler
{
    /// <summary>
    /// Processes a received message and returns the requested delivery outcome.
    /// </summary>
    /// <param name="message">The message to process.</param>
    /// <param name="cancellationToken">The token that cancels processing when the consumer stops.</param>
    /// <returns>The requested delivery outcome.</returns>
    ValueTask<ConsumeResult> HandleAsync(GrpcMessageView message, CancellationToken cancellationToken);
}
