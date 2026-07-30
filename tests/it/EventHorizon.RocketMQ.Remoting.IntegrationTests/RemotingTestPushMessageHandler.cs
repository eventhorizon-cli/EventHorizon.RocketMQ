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
using EventHorizon.RocketMQ.Remoting.Consumer.Push;
using Microsoft.Extensions.DependencyInjection;

namespace EventHorizon.RocketMQ.Remoting.IntegrationTests;

internal static class RemotingTestPushMessageHandlerExtensions
{
    internal static RemotingRocketMQBuilder AddTestRemotingPushConsumer<TMarker>(
        this RemotingRocketMQBuilder builder,
        Action<RemotingPushConsumerOptions> configure,
        Func<
            IReadOnlyList<RemotingMessageView>,
            RemotingPushConsumeContext,
            CancellationToken,
            ValueTask<ConsumeResult>> handleAsync)
    {
        builder.Services.AddSingleton(new RemotingTestPushMessageHandlerState<TMarker>(handleAsync));
        return builder.AddRemotingPushConsumer<RemotingTestPushMessageHandler<TMarker>>(
            ServiceLifetime.Singleton,
            configure);
    }
}

internal sealed class RemotingTestPushMessageHandler<TMarker>(
    RemotingTestPushMessageHandlerState<TMarker> state) : IRemotingPushMessageHandler
{
    public ValueTask<ConsumeResult> HandleAsync(
        IReadOnlyList<RemotingMessageView> messages,
        RemotingPushConsumeContext context,
        CancellationToken cancellationToken) => state.HandleAsync(messages, context, cancellationToken);
}

internal sealed class RemotingTestPushMessageHandlerState<TMarker>(
    Func<
        IReadOnlyList<RemotingMessageView>,
        RemotingPushConsumeContext,
        CancellationToken,
        ValueTask<ConsumeResult>> handleAsync)
{
    internal ValueTask<ConsumeResult> HandleAsync(
        IReadOnlyList<RemotingMessageView> messages,
        RemotingPushConsumeContext context,
        CancellationToken cancellationToken) => handleAsync(messages, context, cancellationToken);
}
