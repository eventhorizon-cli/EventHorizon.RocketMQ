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

namespace EventHorizon.RocketMQ.Remoting.Consumer.Push.Assignment;

internal static class LegacyAssignmentProtocol
{
    public static byte[] EncodeQuery(
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

    public static RemotingPushAssignmentUpdate DecodeResponse(byte[]? body)
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
                    $"Broker assignment response contains duplicate queue " +
                    $"'{queue.Topic}/{queue.BrokerName}/{queue.QueueId}'.");
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
