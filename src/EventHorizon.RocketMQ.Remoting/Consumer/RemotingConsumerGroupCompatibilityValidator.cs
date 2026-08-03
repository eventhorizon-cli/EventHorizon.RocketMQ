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

using EventHorizon.RocketMQ.Remoting.Consumer.Pull.Lite;
using EventHorizon.RocketMQ.Remoting.Consumer.Push;
using EventHorizon.RocketMQ.Remoting.Protocol;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;

namespace EventHorizon.RocketMQ.Remoting.Consumer;

internal static class RemotingConsumerGroupCompatibilityValidator
{
    internal static bool ValidateGroupMemberSubscriptions(
        IServiceProvider provider,
        RemotingRocketMQRoleRegistration roleRegistration,
        ConsumerOptions options)
    {
        var roleKey = roleRegistration.RoleKey;
        var clientOptions = provider.GetRequiredService<IOptionsMonitor<RemotingClientOptions>>()
            .Get(roleKey.OptionsName);
        var groupName = LegacyNamespace.Wrap(clientOptions.Namespace, options.GroupName);
        var hasLocalGroupPeer = false;
        foreach (var registration in provider.GetServices<RemotingRocketMQRoleRegistration>())
        {
            var otherRoleKey = registration.RoleKey;
            if (otherRoleKey == roleKey ||
                otherRoleKey.OptionsName != roleKey.OptionsName ||
                otherRoleKey.Role is not (RemotingRocketMQRole.PushConsumer or
                    RemotingRocketMQRole.LitePullConsumer))
            {
                continue;
            }

            var other = GetGroupMemberOptions(provider, registration);
            var otherGroupName = LegacyNamespace.Wrap(clientOptions.Namespace, other.GroupName);
            if (!string.Equals(groupName, otherGroupName, StringComparison.Ordinal))
            {
                continue;
            }

            if (otherRoleKey.Role != roleKey.Role)
            {
                ThrowGroupOptionsValidation(
                    roleKey,
                    options,
                    "Push and Lite Pull consumers cannot use the same consumer group because their " +
                    "classic Broker consumption types differ.");
            }

            if (!SubscriptionsEqual(options.Subscriptions, other.Subscriptions))
            {
                ThrowGroupOptionsValidation(
                    roleKey,
                    options,
                    "Consumers in the same consumer group must use identical topic subscriptions " +
                    "and filter expressions.");
            }

            ValidateGroupMemberProtocolSettings(roleKey, options, other);
            hasLocalGroupPeer = true;
        }

        return hasLocalGroupPeer;
    }

    private static void ValidateGroupMemberProtocolSettings(
        RemotingRocketMQRoleKey roleKey,
        ConsumerOptions options,
        ConsumerOptions other)
    {
        if (options is RemotingPushConsumerOptions push && other is RemotingPushConsumerOptions otherPush)
        {
            if (push.ConsumerMode != otherPush.ConsumerMode)
            {
                ThrowGroupOptionsValidation(
                    roleKey,
                    options,
                    "Push consumers in the same consumer group must use the same consumer mode.");
            }

            if (push.ConsumeOrderly != otherPush.ConsumeOrderly)
            {
                ThrowGroupOptionsValidation(
                    roleKey,
                    options,
                    "Push consumers in the same consumer group must use the same orderly mode.");
            }

            if (push.InitialPosition != otherPush.InitialPosition ||
                push.InitialPosition == ConsumeFromPosition.Timestamp &&
                push.ConsumeTimestamp != otherPush.ConsumeTimestamp)
            {
                ThrowGroupOptionsValidation(
                    roleKey,
                    options,
                    "Push consumers in the same consumer group must use the same initial consume position.");
            }

            return;
        }

        if (options is RemotingLitePullConsumerOptions litePull &&
            other is RemotingLitePullConsumerOptions otherLitePull &&
            (litePull.InitialPosition != otherLitePull.InitialPosition ||
             litePull.InitialPosition == ConsumeFromPosition.Timestamp &&
             litePull.ConsumeTimestamp != otherLitePull.ConsumeTimestamp))
        {
            ThrowGroupOptionsValidation(
                roleKey,
                options,
                "Lite Pull consumers in the same consumer group must use the same initial consume position.");
        }
    }

    private static void ThrowGroupOptionsValidation(
        RemotingRocketMQRoleKey roleKey,
        ConsumerOptions options,
        string failure) =>
        throw new OptionsValidationException(roleKey.RoleOptionsName, options.GetType(), [failure]);

    private static ConsumerOptions GetGroupMemberOptions(
        IServiceProvider provider,
        RemotingRocketMQRoleRegistration registration) =>
        registration.RoleKey.Role switch
        {
            RemotingRocketMQRole.PushConsumer =>
                provider.GetRequiredService<IOptionsMonitor<RemotingPushConsumerOptions>>()
                    .Get(registration.RoleOptionsName),
            RemotingRocketMQRole.LitePullConsumer =>
                provider.GetRequiredService<IOptionsMonitor<RemotingLitePullConsumerOptions>>()
                    .Get(registration.RoleOptionsName),
            _ => throw new InvalidOperationException(
                $"{registration.RoleKey.DisplayRole} is not an active consumer group member.")
        };

    private static bool SubscriptionsEqual(
        IReadOnlyDictionary<string, FilterExpression> left,
        IReadOnlyDictionary<string, FilterExpression> right) =>
        left.Count == right.Count && left.All(subscription =>
            right.TryGetValue(subscription.Key, out var expression) && subscription.Value == expression);
}
