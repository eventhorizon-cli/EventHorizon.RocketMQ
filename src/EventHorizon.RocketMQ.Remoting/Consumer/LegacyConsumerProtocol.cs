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
using EventHorizon.RocketMQ.Remoting.Consumer.Push;
using EventHorizon.RocketMQ.Remoting.Consumer.Push.Assignment;
using EventHorizon.RocketMQ.Remoting.Protocol;
using Newtonsoft.Json;

namespace EventHorizon.RocketMQ.Remoting.Consumer;

internal static class LegacyConsumerProtocol
{
    public static byte[] EncodeQueryAssignment(
        string topic,
        string consumerGroup,
        string clientId,
        string strategyName = "AVG")
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(topic);
        ArgumentException.ThrowIfNullOrWhiteSpace(consumerGroup);
        ArgumentException.ThrowIfNullOrWhiteSpace(clientId);
        ArgumentException.ThrowIfNullOrWhiteSpace(strategyName);
        return Encoding.UTF8.GetBytes(new QueryAssignmentRequestBody
        {
            Topic = topic,
            ConsumerGroup = consumerGroup,
            ClientId = clientId,
            StrategyName = strategyName,
            MessageModel = "CLUSTERING"
        }.ToJson());
    }

    public static RemotingPushAssignmentUpdate DecodeQueryAssignment(byte[]? body)
    {
        if (body is not { Length: > 0 })
        {
            return new RemotingPushAssignmentUpdate(false, []);
        }

        QueryAssignmentResponseBody response;
        try
        {
            response = Encoding.UTF8.GetString(body).FromJson<QueryAssignmentResponseBody>() ??
                       throw new InvalidDataException("Broker assignment response is null.");
        }
        catch (InvalidDataException)
        {
            throw;
        }
        catch (Exception exception) when (exception is JsonException or FormatException)
        {
            throw new InvalidDataException("Broker assignment response is malformed.", exception);
        }

        if (response.MessageQueueAssignments is null)
        {
            return new RemotingPushAssignmentUpdate(false, []);
        }

        var assignments = new List<RemotingPushAssignment>(response.MessageQueueAssignments.Length);
        var identities = new HashSet<(string Topic, string BrokerName, int QueueId)>();
        foreach (var value in response.MessageQueueAssignments)
        {
            var queue = value?.MessageQueue ?? throw new InvalidDataException(
                "Broker assignment response contains an assignment without a message queue.");
            if (string.IsNullOrWhiteSpace(queue.Topic) || string.IsNullOrWhiteSpace(queue.BrokerName))
            {
                throw new InvalidDataException("Broker assignment response contains an incomplete message queue.");
            }

            var mode = value!.Mode switch
            {
                null or "" or "PULL" => RemotingPushReceiveMode.Pull,
                "POP" => RemotingPushReceiveMode.Pop,
                _ => throw new InvalidDataException(
                    $"Broker assignment response contains unsupported receive mode '{value.Mode}'.")
            };
            if (queue.QueueId < 0 && (queue.QueueId != -1 || mode != RemotingPushReceiveMode.Pop))
            {
                throw new InvalidDataException(
                    $"Broker assignment response contains invalid queue ID {queue.QueueId} for {mode} mode.");
            }

            if (!identities.Add((queue.Topic, queue.BrokerName, queue.QueueId)))
            {
                throw new InvalidDataException(
                    $"Broker assignment response contains duplicate queue '{queue.Topic}/{queue.BrokerName}/{queue.QueueId}'.");
            }

            var attachments = value.Attachments is null
                ? new Dictionary<string, string>(StringComparer.Ordinal)
                : new Dictionary<string, string>(value.Attachments, StringComparer.Ordinal);
            assignments.Add(new RemotingPushAssignment(
                queue.Topic,
                queue.BrokerName,
                queue.QueueId,
                mode,
                attachments));
        }

        return new RemotingPushAssignmentUpdate(
            true,
            assignments
                .OrderBy(static value => value.Topic, StringComparer.Ordinal)
                .ThenBy(static value => value.BrokerName, StringComparer.Ordinal)
                .ThenBy(static value => value.QueueId)
                .ThenBy(static value => value.Mode)
                .ToArray());
    }

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

    public static IReadOnlyList<RemotingConsumerQueue> Allocate(
        IReadOnlyList<RemotingConsumerQueue> queues,
        IReadOnlyList<string> consumerIds,
        string currentConsumerId)
    {
        if (queues.Count == 0 || consumerIds.Count == 0)
        {
            return Array.Empty<RemotingConsumerQueue>();
        }

        var orderedConsumers = consumerIds
            .Distinct(StringComparer.Ordinal)
            .OrderBy(static value => value, StringComparer.Ordinal)
            .ToArray();
        var index = Array.IndexOf(orderedConsumers, currentConsumerId);
        if (index < 0)
        {
            return Array.Empty<RemotingConsumerQueue>();
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
            ? Array.Empty<RemotingConsumerQueue>()
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

    private sealed class QueryAssignmentRequestBody
    {
        public string Topic { get; init; } = string.Empty;
        public string ConsumerGroup { get; init; } = string.Empty;
        public string ClientId { get; init; } = string.Empty;
        public string StrategyName { get; init; } = string.Empty;
        public string MessageModel { get; init; } = string.Empty;
    }

    private sealed class QueryAssignmentResponseBody
    {
        public MessageQueueAssignmentBody?[]? MessageQueueAssignments { get; init; }
    }

    private sealed class MessageQueueAssignmentBody
    {
        public MessageQueueBody? MessageQueue { get; init; }
        public string? Mode { get; init; }
        public Dictionary<string, string>? Attachments { get; init; }
    }

    private sealed class MessageQueueBody
    {
        public string Topic { get; init; } = string.Empty;
        public string BrokerName { get; init; } = string.Empty;
        public int QueueId { get; init; }
    }
}
