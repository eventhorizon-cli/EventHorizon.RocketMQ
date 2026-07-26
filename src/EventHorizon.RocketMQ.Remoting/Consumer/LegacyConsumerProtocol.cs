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
using EventHorizon.RocketMQ.Remoting.Consumer;
using EventHorizon.RocketMQ.Remoting.Consumer.Pull;
using EventHorizon.RocketMQ.Remoting.Consumer.Push;
using EventHorizon.RocketMQ.Remoting.Protocol;
using Newtonsoft.Json;

namespace EventHorizon.RocketMQ.Remoting.Consumer;

internal static class LegacyConsumerProtocol
{
    public static byte[] EncodeHeartbeat(
        string clientId,
        string groupName,
        IReadOnlyDictionary<string, FilterExpression> subscriptions,
        long subscriptionVersion,
        ConsumerMode consumerMode,
        ConsumeFromPosition initialPosition,
        bool unitMode) =>
        EncodeHeartbeat(
            clientId,
            groupName,
            subscriptions,
            subscriptionVersion,
            "CONSUME_PASSIVELY",
            consumerMode switch
            {
                ConsumerMode.Clustering => "CLUSTERING",
                ConsumerMode.Broadcasting => "BROADCASTING",
                _ => throw new ArgumentOutOfRangeException(nameof(consumerMode), consumerMode, null)
            },
            initialPosition switch
            {
                ConsumeFromPosition.Beginning => "CONSUME_FROM_FIRST_OFFSET",
                ConsumeFromPosition.End => "CONSUME_FROM_LAST_OFFSET",
                ConsumeFromPosition.Timestamp => "CONSUME_FROM_TIMESTAMP",
                _ => throw new ArgumentOutOfRangeException(nameof(initialPosition), initialPosition, null)
            },
            unitMode);

    public static byte[] EncodeHeartbeat(
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

    public static IReadOnlyList<string> DecodeConsumerIds(byte[]? body)
    {
        if (body is not { Length: > 0 })
        {
            return Array.Empty<string>();
        }

        var response = Encoding.UTF8.GetString(body).FromJson<GetConsumerListByGroupResponseBody>();
        return response.ConsumerIdList?
            .Where(static value => !string.IsNullOrWhiteSpace(value))
            .Distinct(StringComparer.Ordinal)
            .OrderBy(static value => value, StringComparer.Ordinal)
            .ToArray() ?? Array.Empty<string>();
    }

    public static IReadOnlyList<RemotingPullMessageQueue> Allocate(
        IReadOnlyList<RemotingPullMessageQueue> queues,
        IReadOnlyList<string> consumerIds,
        string currentConsumerId)
    {
        if (queues.Count == 0 || consumerIds.Count == 0)
        {
            return Array.Empty<RemotingPullMessageQueue>();
        }

        var orderedConsumers = consumerIds
            .Distinct(StringComparer.Ordinal)
            .OrderBy(static value => value, StringComparer.Ordinal)
            .ToArray();
        var index = Array.IndexOf(orderedConsumers, currentConsumerId);
        if (index < 0)
        {
            return Array.Empty<RemotingPullMessageQueue>();
        }

        var orderedQueues = queues
            .OrderBy(static queue => queue.Topic, StringComparer.Ordinal)
            .ThenBy(static queue => queue.BrokerName, StringComparer.Ordinal)
            .ThenBy(static queue => queue.QueueId)
            .ToArray();
        var remainder = orderedQueues.Length % orderedConsumers.Length;
        var averageSize = orderedQueues.Length <= orderedConsumers.Length
            ? 1
            : remainder > 0 && index < remainder
                ? orderedQueues.Length / orderedConsumers.Length + 1
                : orderedQueues.Length / orderedConsumers.Length;
        var startIndex = remainder > 0 && index < remainder
            ? index * averageSize
            : index * averageSize + remainder;
        var range = Math.Min(averageSize, orderedQueues.Length - startIndex);
        return range <= 0
            ? Array.Empty<RemotingPullMessageQueue>()
            : orderedQueues.AsSpan(startIndex, range).ToArray();
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

    private sealed class GetConsumerListByGroupResponseBody
    {
        public string[]? ConsumerIdList { get; init; }
    }
}
