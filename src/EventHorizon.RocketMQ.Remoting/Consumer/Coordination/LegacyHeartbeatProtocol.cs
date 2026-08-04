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

using System.Text;
using EventHorizon.RocketMQ.Remoting.Protocol;
using Newtonsoft.Json;

namespace EventHorizon.RocketMQ.Remoting.Consumer.Coordination;

internal static class LegacyHeartbeatProtocol
{
    public static byte[] Encode(
        string clientId,
        string groupName,
        IReadOnlyDictionary<string, FilterExpression> subscriptions,
        long subscriptionVersion,
        string consumeType,
        string messageModel,
        string consumeFromWhere,
        bool unitMode)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(clientId);
        ArgumentException.ThrowIfNullOrWhiteSpace(groupName);
        ArgumentNullException.ThrowIfNull(subscriptions);
        ArgumentException.ThrowIfNullOrWhiteSpace(consumeType);
        ArgumentException.ThrowIfNullOrWhiteSpace(messageModel);
        ArgumentException.ThrowIfNullOrWhiteSpace(consumeFromWhere);
        var subscriptionData = subscriptions
            .OrderBy(static entry => entry.Key, StringComparer.Ordinal)
            .Select(entry => CreateSubscription(entry.Key, entry.Value, subscriptionVersion))
            .ToArray();
        var body = new HeartbeatData
        {
            ClientId = clientId,
            ConsumerDataSet =
            [
                new ConsumerData
                {
                    GroupName = groupName,
                    ConsumeType = consumeType,
                    MessageModel = messageModel,
                    ConsumeFromWhere = consumeFromWhere,
                    SubscriptionDataSet = subscriptionData,
                    UnitMode = unitMode
                }
            ]
        };
        return Encoding.UTF8.GetBytes(body.ToJson());
    }

    private static SubscriptionData CreateSubscription(
        string topic,
        FilterExpression filter,
        long subscriptionVersion)
    {
        var tags = filter.Type == FilterExpressionType.Tag && filter.Expression != "*"
            ? filter.Expression.Split("||", StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries)
                .Distinct(StringComparer.Ordinal)
                .OrderBy(static tag => tag, StringComparer.Ordinal)
                .ToArray()
            : Array.Empty<string>();
        return new SubscriptionData
        {
            Topic = topic,
            SubString = filter.Expression,
            TagsSet = tags,
            CodeSet = tags.Select(JavaStringHashCode).ToArray(),
            SubVersion = subscriptionVersion,
            ExpressionType = filter.Type == FilterExpressionType.Sql ? "SQL92" : "TAG"
        };
    }

    private static int JavaStringHashCode(string value)
    {
        var hash = 0;
        foreach (var character in value)
        {
            hash = unchecked(hash * 31 + character);
        }

        return hash;
    }

    private sealed class HeartbeatData
    {
        [JsonProperty("clientID")]
        public string ClientId { get; init; } = string.Empty;

        public object[] ProducerDataSet { get; init; } = [];
        public ConsumerData[] ConsumerDataSet { get; init; } = [];
    }

    private sealed class ConsumerData
    {
        public string GroupName { get; init; } = string.Empty;
        public string ConsumeType { get; init; } = string.Empty;
        public string MessageModel { get; init; } = string.Empty;
        public string ConsumeFromWhere { get; init; } = string.Empty;
        public SubscriptionData[] SubscriptionDataSet { get; init; } = [];
        public bool UnitMode { get; init; }
    }

    private sealed class SubscriptionData
    {
        public bool ClassFilterMode { get; init; }
        public string Topic { get; init; } = string.Empty;
        public string SubString { get; init; } = string.Empty;
        public string[] TagsSet { get; init; } = [];
        public int[] CodeSet { get; init; } = [];
        public long SubVersion { get; init; }
        public string ExpressionType { get; init; } = "TAG";
    }
}
