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

using System.Globalization;
using EventHorizon.RocketMQ.Remoting.Consumer.Push.Assignment;
using EventHorizon.RocketMQ.Remoting.Exceptions;
using EventHorizon.RocketMQ.Remoting.Protocol;

namespace EventHorizon.RocketMQ.Remoting.Consumer.Push.Pop;

internal static class LegacyPopMessageDecoder
{
    private const string PopCheckpointProperty = "POP_CK";
    private const string RetryTopicProperty = "RETRY_TOPIC";
    private const string StartOffsetInfoField = "startOffsetInfo";
    private const string MessageOffsetInfoField = "msgOffsetInfo";
    private const string PopTimeField = "popTime";
    private const string InvisibleTimeField = "invisibleTime";
    private const string ReviveQueueIdField = "reviveQid";
    private const string LogicalQueueBrokerPrefix = "LOGICAL_QUEUE_MOCK_BROKER_";

    public static RemotingPopResult Decode(
        RemotingCommand response,
        RemotingPushAssignment assignment,
        string? @namespace)
    {
        ArgumentNullException.ThrowIfNull(assignment);
        if (assignment.Mode != RemotingPushReceiveMode.Pop)
        {
            throw new ArgumentException("The assignment must use POP receive mode.", nameof(assignment));
        }

        return Decode(
            response,
            assignment.Topic,
            assignment.BrokerName,
            assignment.QueueId,
            @namespace);
    }

    public static RemotingPopResult Decode(
        RemotingCommand response,
        RemotingConsumerQueue queue,
        string? @namespace)
    {
        ArgumentNullException.ThrowIfNull(queue);
        return Decode(
            response,
            queue.Topic,
            queue.BrokerName,
            queue.QueueId,
            @namespace);
    }

    private static RemotingPopResult Decode(
        RemotingCommand response,
        string topic,
        string brokerName,
        int requestedQueueId,
        string? @namespace)
    {
        ArgumentNullException.ThrowIfNull(response);
        ArgumentException.ThrowIfNullOrWhiteSpace(topic);
        ArgumentException.ThrowIfNullOrWhiteSpace(brokerName);

        var restNum = GetRestNum(response);
        return response.Code switch
        {
            ResponseCodes.ResSuccess => new RemotingPopResult(
                response.Body is { Length: > 0 }
                    ? DecodeMessages(
                        LegacyMessageDecoder.Decode(response.Body, brokerName, @namespace),
                        topic,
                        brokerName,
                        requestedQueueId,
                        response.ExtFields)
                    : Array.Empty<RemotingPopMessage>(),
                RemotingPopStatus.Found,
                restNum),
            ResponseCodes.ResPullNotFound or ResponseCodes.ResPollingTimeout => new RemotingPopResult(
                Array.Empty<RemotingPopMessage>(),
                RemotingPopStatus.NoNewMessage,
                restNum),
            ResponseCodes.ResPollingFull => new RemotingPopResult(
                Array.Empty<RemotingPopMessage>(),
                RemotingPopStatus.PollingFull,
                restNum),
            _ => throw new RemotingCommandException(
                response.Code,
                response.Remark ?? "Broker rejected the POP request.")
        };
    }

    public static IReadOnlyList<RemotingPopMessage> DecodeMessages(
        IReadOnlyList<RemotingMessageView> messages,
        RemotingConsumerQueue queue)
    {
        ArgumentNullException.ThrowIfNull(queue);
        return DecodeMessages(
            messages,
            queue.Topic,
            queue.BrokerName,
            queue.QueueId,
            responseFields: null);
    }

    private static IReadOnlyList<RemotingPopMessage> DecodeMessages(
        IReadOnlyList<RemotingMessageView> messages,
        string topic,
        string brokerName,
        int requestedQueueId,
        IReadOnlyDictionary<string, object>? responseFields)
    {
        ArgumentNullException.ThrowIfNull(messages);
        if (messages.Count == 0)
        {
            return Array.Empty<RemotingPopMessage>();
        }

        foreach (var message in messages)
        {
            EnsureMatchingPhysicalQueue(message, brokerName, requestedQueueId);
            EnsureSupportedMessage(message);
        }

        var receiptOffsets = GetReceiptOffsets(messages, brokerName, requestedQueueId, responseFields);
        var result = new RemotingPopMessage[messages.Count];
        for (var index = 0; index < messages.Count; index++)
        {
            var message = messages[index].WithTopic(topic);
            if (message.Properties.TryGetValue(PopCheckpointProperty, out var extraInfo))
            {
                if (string.IsNullOrWhiteSpace(extraInfo))
                {
                    throw InvalidCheckpoint(message, "must contain exactly eight non-empty fields");
                }

                result[index] = new RemotingPopMessage(
                    message,
                    DecodeReceipt(message, extraInfo));
                continue;
            }

            if (responseFields is null || receiptOffsets is null)
            {
                throw new InvalidDataException(
                    $"POP message '{message.MessageId}' does not contain a valid {PopCheckpointProperty} receipt.");
            }

            var receiptOffset = receiptOffsets[index];
            result[index] = new RemotingPopMessage(
                message,
                SynthesizeReceipt(message, responseFields, receiptOffset));
        }

        return Array.AsReadOnly(result);
    }

    public static RemotingPopMessage DecodeMessage(
        RemotingMessageView message,
        RemotingConsumerQueue queue)
    {
        ArgumentNullException.ThrowIfNull(message);
        ArgumentNullException.ThrowIfNull(queue);
        var messages = DecodeMessages(
            [message],
            queue.Topic,
            queue.BrokerName,
            queue.QueueId,
            responseFields: null);
        return messages[0];
    }

    private static RemotingPopReceipt SynthesizeReceipt(
        RemotingMessageView message,
        IReadOnlyDictionary<string, object> responseFields,
        ReceiptOffset receiptOffset)
    {
        if (receiptOffset.CheckpointOffset < 0 || message.QueueId < 0 || message.QueueOffset < 0)
        {
            throw new InvalidDataException(
                $"POP message '{message.MessageId}' does not have a valid physical queue position.");
        }

        var brokerName = message.BrokerName ?? throw new InvalidDataException(
            $"POP message '{message.MessageId}' does not identify its logical broker.");
        var popTimeMilliseconds = GetRequiredPositiveResponseInt64(responseFields, PopTimeField, message);
        var invisibleTimeMilliseconds = GetRequiredPositiveResponseInt64(responseFields, InvisibleTimeField, message);
        var reviveQueueId = GetRequiredNonNegativeResponseInt32(responseFields, ReviveQueueIdField, message);
        var extraInfo = string.Join(
            ' ',
            receiptOffset.CheckpointOffset.ToString(CultureInfo.InvariantCulture),
            popTimeMilliseconds.ToString(CultureInfo.InvariantCulture),
            invisibleTimeMilliseconds.ToString(CultureInfo.InvariantCulture),
            reviveQueueId.ToString(CultureInfo.InvariantCulture),
            GetRetryTopicMarker(receiptOffset.RetryTopicKind),
            brokerName,
            message.QueueId.ToString(CultureInfo.InvariantCulture),
            message.QueueOffset.ToString(CultureInfo.InvariantCulture));

        try
        {
            return new RemotingPopReceipt(
                message.Topic,
                brokerName,
                message.QueueId,
                receiptOffset.CheckpointOffset,
                message.QueueOffset,
                DateTimeOffset.FromUnixTimeMilliseconds(popTimeMilliseconds),
                TimeSpan.FromMilliseconds(invisibleTimeMilliseconds),
                reviveQueueId,
                extraInfo,
                receiptOffset.RetryTopicKind);
        }
        catch (ArgumentOutOfRangeException exception)
        {
            throw new InvalidDataException(
                $"POP response for message '{message.MessageId}' contains an out-of-range receipt value.",
                exception);
        }
        catch (OverflowException exception)
        {
            throw new InvalidDataException(
                $"POP response for message '{message.MessageId}' contains an out-of-range receipt value.",
                exception);
        }
    }

    private static IReadOnlyList<ReceiptOffset>? GetReceiptOffsets(
        IReadOnlyList<RemotingMessageView> messages,
        string brokerName,
        int requestedQueueId,
        IReadOnlyDictionary<string, object>? responseFields)
    {
        if (responseFields is null)
        {
            return null;
        }

        if (!TryGetResponseField(responseFields, StartOffsetInfoField, out var startOffsetInfo) ||
            startOffsetInfo.Length == 0)
        {
            return messages.Select(static message =>
                new ReceiptOffset(message.QueueOffset, GetRetryTopicKind(message))).ToArray();
        }

        if (string.IsNullOrWhiteSpace(startOffsetInfo))
        {
            throw InvalidOffsetMapping(StartOffsetInfoField, brokerName, requestedQueueId, "it is empty");
        }

        if (!TryGetResponseField(responseFields, MessageOffsetInfoField, out var messageOffsetInfo) ||
            string.IsNullOrWhiteSpace(messageOffsetInfo))
        {
            throw InvalidOffsetMapping(
                MessageOffsetInfoField,
                brokerName,
                requestedQueueId,
                "it is missing or empty");
        }

        var checkpoints = ParseCheckpointOffsets(startOffsetInfo, brokerName, requestedQueueId);
        var mappedOffsets = ParseMessageOffsets(messageOffsetInfo, brokerName, requestedQueueId);
        var messageGroups = messages
            .Select((message, index) => (Message: message, Index: index))
            .Where(static item => !item.Message.Properties.ContainsKey(PopCheckpointProperty))
            .GroupBy(item => new ReceiptKey(GetRetryTopicKind(item.Message, mappedOffsets), item.Message.QueueId));
        var result = new ReceiptOffset[messages.Count];

        foreach (var group in messageGroups)
        {
            if (!checkpoints.TryGetValue(group.Key, out var checkpointOffset))
            {
                throw InvalidOffsetMapping(
                    StartOffsetInfoField,
                    brokerName,
                    requestedQueueId,
                    "it does not contain a required physical queue segment");
            }

            if (!mappedOffsets.TryGetValue(group.Key, out var offsets))
            {
                throw InvalidOffsetMapping(
                    MessageOffsetInfoField,
                    brokerName,
                    requestedQueueId,
                    "it does not contain a required physical queue segment");
            }

            var groupMessages = group.ToArray();
            if (offsets.Count != groupMessages.Length)
            {
                throw InvalidOffsetMapping(
                    MessageOffsetInfoField,
                    brokerName,
                    requestedQueueId,
                    $"it contains {offsets.Count} offsets for {groupMessages.Length} messages");
            }

            for (var index = 0; index < groupMessages.Length; index++)
            {
                var (message, messageIndex) = groupMessages[index];
                if (offsets[index] != message.QueueOffset)
                {
                    throw InvalidOffsetMapping(
                        MessageOffsetInfoField,
                        brokerName,
                        requestedQueueId,
                        $"offset {offsets[index]} does not match message offset {message.QueueOffset}");
                }

                if (checkpointOffset > message.QueueOffset)
                {
                    throw InvalidOffsetMapping(
                        StartOffsetInfoField,
                        brokerName,
                        requestedQueueId,
                        $"checkpoint offset {checkpointOffset} is after message offset {message.QueueOffset}");
                }

                result[messageIndex] = new ReceiptOffset(checkpointOffset, group.Key.RetryTopicKind);
            }
        }

        return result;
    }

    private static Dictionary<ReceiptKey, long> ParseCheckpointOffsets(
        string value,
        string brokerName,
        int requestedQueueId)
    {
        var result = new Dictionary<ReceiptKey, long>();
        foreach (var segment in ParseSegments(value, StartOffsetInfoField, brokerName, requestedQueueId))
        {
            if (!long.TryParse(
                    segment.Value,
                    NumberStyles.Integer,
                    CultureInfo.InvariantCulture,
                    out var checkpointOffset) ||
                checkpointOffset < 0)
            {
                throw InvalidOffsetMapping(
                    StartOffsetInfoField,
                    brokerName,
                    requestedQueueId,
                    "it contains an invalid checkpoint offset");
            }

            if (!result.TryAdd(segment.Key, checkpointOffset))
            {
                throw InvalidOffsetMapping(
                    StartOffsetInfoField,
                    brokerName,
                    requestedQueueId,
                    "it contains duplicate physical queue segments");
            }
        }

        return result;
    }

    private static Dictionary<ReceiptKey, IReadOnlyList<long>> ParseMessageOffsets(
        string value,
        string brokerName,
        int requestedQueueId)
    {
        var result = new Dictionary<ReceiptKey, IReadOnlyList<long>>();
        foreach (var segment in ParseSegments(value, MessageOffsetInfoField, brokerName, requestedQueueId))
        {
            var rawOffsets = segment.Value.Split(',', StringSplitOptions.None);
            var offsets = new long[rawOffsets.Length];
            for (var index = 0; index < rawOffsets.Length; index++)
            {
                if (!long.TryParse(
                        rawOffsets[index],
                        NumberStyles.Integer,
                        CultureInfo.InvariantCulture,
                        out var offset) ||
                    offset < 0)
                {
                    throw InvalidOffsetMapping(
                        MessageOffsetInfoField,
                        brokerName,
                        requestedQueueId,
                        "it contains an invalid message offset");
                }

                offsets[index] = offset;
            }

            if (!result.TryAdd(segment.Key, offsets))
            {
                throw InvalidOffsetMapping(
                    MessageOffsetInfoField,
                    brokerName,
                    requestedQueueId,
                    "it contains duplicate physical queue segments");
            }
        }

        return result;
    }

    private static IEnumerable<(ReceiptKey Key, string Value)> ParseSegments(
        string value,
        string fieldName,
        string brokerName,
        int requestedQueueId)
    {
        foreach (var rawSegment in value.Split(';', StringSplitOptions.None))
        {
            var fields = rawSegment.Split(
                ' ',
                StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
            if (fields.Length != 3 ||
                !TryParseRetryTopicKind(fields[0], out var retryTopicKind) ||
                !int.TryParse(fields[1], NumberStyles.Integer, CultureInfo.InvariantCulture, out var queueId) ||
                queueId < 0 ||
                string.IsNullOrWhiteSpace(fields[2]))
            {
                throw InvalidOffsetMapping(
                    fieldName,
                    brokerName,
                    requestedQueueId,
                    "it contains a malformed physical queue segment");
            }

            yield return (new ReceiptKey(retryTopicKind, queueId), fields[2]);
        }
    }

    private static bool TryGetResponseField(
        IReadOnlyDictionary<string, object> fields,
        string name,
        out string value)
    {
        value = string.Empty;
        if (!fields.TryGetValue(name, out var raw) || raw is null)
        {
            return false;
        }

        value = Convert.ToString(raw, CultureInfo.InvariantCulture) ?? string.Empty;
        return true;
    }

    private static void EnsureMatchingPhysicalQueue(
        RemotingMessageView message,
        string brokerName,
        int requestedQueueId)
    {
        if (!string.Equals(message.BrokerName, brokerName, StringComparison.Ordinal) ||
            requestedQueueId >= 0 && message.QueueId != requestedQueueId)
        {
            throw new InvalidDataException(
                $"POP message '{message.MessageId}' does not match the requested physical queue.");
        }
    }

    private static void EnsureSupportedMessage(RemotingMessageView message)
    {
        if (message.BrokerName?.StartsWith(LogicalQueueBrokerPrefix, StringComparison.Ordinal) == true)
        {
            throw new InvalidDataException(
                $"POP message '{message.MessageId}' targets an unsupported logical queue and cannot be represented by a {PopCheckpointProperty} receipt.");
        }
    }

    private static InvalidDataException InvalidOffsetMapping(
        string fieldName,
        string brokerName,
        int requestedQueueId,
        string reason) =>
        new(
            $"POP response field '{fieldName}' contains an invalid mapping for physical queue " +
            $"'{brokerName}:{requestedQueueId}': {reason}.");

    private static RemotingPopReceipt DecodeReceipt(
        RemotingMessageView message,
        string extraInfo)
    {
        var fields = extraInfo.Split(' ', StringSplitOptions.None);
        if (fields.Length != 8 || fields.Any(string.IsNullOrEmpty))
        {
            throw InvalidCheckpoint(message, "must contain exactly eight non-empty fields");
        }

        var checkpointOffset = ParseNonNegativeInt64(fields[0], message, "checkpoint offset");
        var popTimeMilliseconds = ParsePositiveInt64(fields[1], message, "pop time");
        var invisibleTimeMilliseconds = ParsePositiveInt64(fields[2], message, "invisible time");
        var reviveQueueId = ParseNonNegativeInt32(fields[3], message, "revive queue ID");
        if (!TryParseRetryTopicKind(fields[4], out var retryTopicKind))
        {
            throw InvalidCheckpoint(message, "contains an unknown retry topic marker");
        }

        var brokerName = fields[5];
        if (!string.Equals(brokerName, message.BrokerName, StringComparison.Ordinal))
        {
            throw InvalidCheckpoint(message, "does not match the message broker");
        }

        var queueId = ParseNonNegativeInt32(fields[6], message, "queue ID");
        if (queueId != message.QueueId)
        {
            throw InvalidCheckpoint(message, "does not match the message queue");
        }

        var queueOffset = ParseNonNegativeInt64(fields[7], message, "message queue offset");
        if (queueOffset != message.QueueOffset)
        {
            throw InvalidCheckpoint(message, "does not match the message offset");
        }

        if (checkpointOffset > queueOffset)
        {
            throw InvalidCheckpoint(message, "has a checkpoint offset after the message offset");
        }

        try
        {
            return new RemotingPopReceipt(
                message.Topic,
                brokerName,
                queueId,
                checkpointOffset,
                queueOffset,
                DateTimeOffset.FromUnixTimeMilliseconds(popTimeMilliseconds),
                TimeSpan.FromMilliseconds(invisibleTimeMilliseconds),
                reviveQueueId,
                extraInfo,
                retryTopicKind);
        }
        catch (ArgumentOutOfRangeException exception)
        {
            throw new InvalidDataException(
                $"POP message '{message.MessageId}' contains an out-of-range {PopCheckpointProperty} receipt.",
                exception);
        }
        catch (OverflowException exception)
        {
            throw new InvalidDataException(
                $"POP message '{message.MessageId}' contains an out-of-range {PopCheckpointProperty} receipt.",
                exception);
        }
    }

    private static long GetRestNum(RemotingCommand response)
    {
        if (!response.ExtFields.TryGetValue("restNum", out var raw) || raw is null)
        {
            return 0;
        }

        var value = Convert.ToString(raw, CultureInfo.InvariantCulture);
        if (!long.TryParse(value, NumberStyles.Integer, CultureInfo.InvariantCulture, out var restNum) ||
            restNum < 0)
        {
            throw new InvalidDataException("POP response did not contain a valid non-negative 'restNum' field.");
        }

        return restNum;
    }

    private static RemotingPopRetryTopicKind GetRetryTopicKind(
        RemotingMessageView message,
        IReadOnlyDictionary<ReceiptKey, IReadOnlyList<long>>? mappedOffsets = null)
    {
        if (!message.Properties.TryGetValue(RetryTopicProperty, out var retryTopic) ||
            string.IsNullOrWhiteSpace(retryTopic))
        {
            return RemotingPopRetryTopicKind.Normal;
        }

        if (mappedOffsets is not null)
        {
            var retryKinds = mappedOffsets.Keys
                .Where(key => key.QueueId == message.QueueId && key.RetryTopicKind != RemotingPopRetryTopicKind.Normal)
                .Select(static key => key.RetryTopicKind)
                .Distinct()
                .ToArray();
            if (retryKinds.Length == 1)
            {
                return retryKinds[0];
            }
        }

        return RemotingPopRetryTopicKind.RetryV1;
    }

    private static string GetRetryTopicMarker(RemotingPopRetryTopicKind retryTopicKind) => retryTopicKind switch
    {
        RemotingPopRetryTopicKind.Normal => "0",
        RemotingPopRetryTopicKind.RetryV1 => "1",
        RemotingPopRetryTopicKind.RetryV2 => "2",
        _ => throw new InvalidOperationException($"Unsupported POP retry topic kind '{retryTopicKind}'.")
    };

    private static bool TryParseRetryTopicKind(string value, out RemotingPopRetryTopicKind retryTopicKind)
    {
        retryTopicKind = value switch
        {
            "0" => RemotingPopRetryTopicKind.Normal,
            "1" => RemotingPopRetryTopicKind.RetryV1,
            "2" => RemotingPopRetryTopicKind.RetryV2,
            _ => (RemotingPopRetryTopicKind)(-1)
        };
        return retryTopicKind >= RemotingPopRetryTopicKind.Normal;
    }

    private static long GetRequiredPositiveResponseInt64(
        IReadOnlyDictionary<string, object> fields,
        string name,
        RemotingMessageView message)
    {
        if (!fields.TryGetValue(name, out var raw) ||
            !long.TryParse(
                Convert.ToString(raw, CultureInfo.InvariantCulture),
                NumberStyles.Integer,
                CultureInfo.InvariantCulture,
                out var value) ||
            value <= 0)
        {
            throw new InvalidDataException(
                $"POP response for message '{message.MessageId}' did not contain a valid positive '{name}' field.");
        }

        return value;
    }

    private static int GetRequiredNonNegativeResponseInt32(
        IReadOnlyDictionary<string, object> fields,
        string name,
        RemotingMessageView message)
    {
        if (!fields.TryGetValue(name, out var raw) ||
            !int.TryParse(
                Convert.ToString(raw, CultureInfo.InvariantCulture),
                NumberStyles.Integer,
                CultureInfo.InvariantCulture,
                out var value) ||
            value < 0)
        {
            throw new InvalidDataException(
                $"POP response for message '{message.MessageId}' did not contain a valid non-negative '{name}' field.");
        }

        return value;
    }

    private static long ParseNonNegativeInt64(string value, RemotingMessageView message, string field)
    {
        if (!long.TryParse(value, NumberStyles.Integer, CultureInfo.InvariantCulture, out var parsed) || parsed < 0)
        {
            throw InvalidCheckpoint(message, $"contains an invalid {field}");
        }

        return parsed;
    }

    private static long ParsePositiveInt64(string value, RemotingMessageView message, string field)
    {
        var parsed = ParseNonNegativeInt64(value, message, field);
        return parsed > 0 ? parsed : throw InvalidCheckpoint(message, $"contains an invalid {field}");
    }

    private static int ParseNonNegativeInt32(string value, RemotingMessageView message, string field)
    {
        if (!int.TryParse(value, NumberStyles.Integer, CultureInfo.InvariantCulture, out var parsed) || parsed < 0)
        {
            throw InvalidCheckpoint(message, $"contains an invalid {field}");
        }

        return parsed;
    }

    private static InvalidDataException InvalidCheckpoint(RemotingMessageView message, string reason) =>
        new($"POP message '{message.MessageId}' contains an invalid {PopCheckpointProperty} receipt: {reason}.");

    private readonly record struct ReceiptKey(RemotingPopRetryTopicKind RetryTopicKind, int QueueId);

    private readonly record struct ReceiptOffset(long CheckpointOffset, RemotingPopRetryTopicKind RetryTopicKind);
}
