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

using EventHorizon.RocketMQ.Grpc.Consumer.Lite;
using EventHorizon.RocketMQ.Grpc.Consumer.Push;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;

namespace EventHorizon.RocketMQ.Grpc.Consumer;

internal static class GrpcConsumerGroupCompatibilityValidator
{
    internal static bool ValidatePushConsumerSubscriptions(
        IServiceProvider provider,
        GrpcRocketMQRoleRegistration currentRegistration,
        GrpcPushConsumerOptions options)
    {
        var roleKey = currentRegistration.RoleKey;
        var hasLocalGroupPeer = false;
        foreach (var registration in provider.GetServices<GrpcRocketMQRoleRegistration>())
        {
            var otherRoleKey = registration.RoleKey;
            if (otherRoleKey == roleKey ||
                otherRoleKey.OptionsName != roleKey.OptionsName ||
                otherRoleKey.Role != GrpcRocketMQRole.PushConsumer)
            {
                continue;
            }

            var otherOptions = GrpcRocketMQRegistration.GetNamedOptions<GrpcPushConsumerOptions>(
                provider,
                registration.RoleOptionsName).Value;
            if (!string.Equals(options.GroupName, otherOptions.GroupName, StringComparison.Ordinal))
            {
                continue;
            }

            hasLocalGroupPeer = true;
            if (!HaveEquivalentSubscriptions(options.Subscriptions, otherOptions.Subscriptions))
            {
                throw new OptionsValidationException(
                    currentRegistration.RoleOptionsName,
                    typeof(GrpcPushConsumerOptions),
                    [
                        "Push consumers in the same consumer group must use identical topic subscriptions " +
                        "and filter expressions."
                    ]);
            }
        }

        return hasLocalGroupPeer;
    }

    internal static void ValidateLitePushConsumerBindTopic(
        IServiceProvider provider,
        GrpcRocketMQRoleRegistration currentRegistration,
        GrpcLitePushConsumerOptions options)
    {
        var roleKey = currentRegistration.RoleKey;
        foreach (var registration in provider.GetServices<GrpcRocketMQRoleRegistration>())
        {
            var otherRoleKey = registration.RoleKey;
            if (otherRoleKey == roleKey ||
                otherRoleKey.OptionsName != roleKey.OptionsName ||
                otherRoleKey.Role != GrpcRocketMQRole.LitePushConsumer)
            {
                continue;
            }

            var otherOptions = GrpcRocketMQRegistration.GetNamedOptions<GrpcLitePushConsumerOptions>(
                provider,
                registration.RoleOptionsName).Value;
            if (string.Equals(options.GroupName, otherOptions.GroupName, StringComparison.Ordinal) &&
                !string.Equals(options.BindTopic, otherOptions.BindTopic, StringComparison.Ordinal))
            {
                throw new OptionsValidationException(
                    currentRegistration.RoleOptionsName,
                    typeof(GrpcLitePushConsumerOptions),
                    ["Lite Push consumers in the same consumer group must use the same bind topic."]);
            }
        }
    }

    private static bool HaveEquivalentSubscriptions(
        IReadOnlyDictionary<string, FilterExpression> left,
        IReadOnlyDictionary<string, FilterExpression> right) =>
        left.Count == right.Count &&
        left.All(subscription =>
            right.TryGetValue(subscription.Key, out var expression) && subscription.Value == expression);
}
