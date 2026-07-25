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
using EventHorizon.RocketMQ.Remoting.Consumer.Pull;
using EventHorizon.RocketMQ.Remoting.Exceptions;
using EventHorizon.RocketMQ.Remoting.Protocol;

namespace EventHorizon.RocketMQ.Remoting.Consumer.Push.Pop;

internal static class LegacyPopMessageDecoder
{
    private const string PopCheckpointProperty = "POP_CK";
    private const string RetryTopicProperty = "RETRY_TOPIC";
    private const string NormalTopicMarker = "0";
    private const string StartOffsetInfoField = "startOffsetInfo";
    private const string MessageOffsetInfoField = "msgOffsetInfo";
    private const string PopTimeField = "popTime";
    private const string InvisibleTimeField = "invisibleTime";
    private const string ReviveQueueIdField = "reviveQid";
    private const string RetryTopicPrefix = "%RETRY%";
    private const string DeadLetterTopicPrefix = "%DLQ%";
    private const string LogicalQueueBrokerPrefix = "LOGICAL_QUEUE_MOCK_BROKER_";

    public static RemotingPopResult Decode(
        RemotingCommand response,
        RemotingPullMessageQueue queue,
        string? @namespace = null)
    {
        ArgumentNullException.ThrowIfNull(queue);
        return Decode(response, queue, queue.GetPullBrokerAddress(), @namespace);
    }

    public static RemotingPopResult Decode(
        RemotingCommand response,
        RemotingPullMessageQueue queue,
        string brokerAddress,
        string? @namespace)
    {
        ArgumentNullException.ThrowIfNull(response);
        ArgumentNullException.ThrowIfNull(queue);
        ArgumentException.ThrowIfNullOrWhiteSpace(brokerAddress);

        var restNum = GetRestNum(response);
        return response.Code switch
        {
            ResponseCodes.ResSuccess => new RemotingPopResult(
                response.Body is { Length: > 0 }
                    ? DecodeMessages(
                        LegacyMessageDecoder.Decode(response.Body, queue.BrokerName, @namespace),
                        queue,
                        brokerAddress,
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
        RemotingPullMessageQueue queue)
    {
        ArgumentNullException.ThrowIfNull(queue);
        return DecodeMessages(messages, queue, queue.GetPullBrokerAddress());
    }

    public static IReadOnlyList<RemotingPopMessage> DecodeMessages(
        IReadOnlyList<RemotingMessageView> messages,
        RemotingPullMessageQueue queue,
        string brokerAddress)
    {
        return DecodeMessages(messages, queue, brokerAddress, responseFields: null);
    }

    private static IReadOnlyList<RemotingPopMessage> DecodeMessages(
        IReadOnlyList<RemotingMessageView> messages,
        RemotingPullMessageQueue queue,
        string brokerAddress,
        IReadOnlyDictionary<string, object>? responseFields)
    {
        ArgumentNullException.ThrowIfNull(messages);
        ArgumentNullException.ThrowIfNull(queue);
        ArgumentException.ThrowIfNullOrWhiteSpace(brokerAddress);
        if (messages.Count == 0)
        {
            return Array.Empty<RemotingPopMessage>();
        }

        var checkpointOffsets = GetCheckpointOffsets(messages, queue, responseFields);
        var result = new RemotingPopMessage[messages.Count];
        for (var index = 0; index < messages.Count; index++)
        {
            result[index] = DecodeMessage(
                messages[index],
                queue,
                brokerAddress,
                responseFields,
                checkpointOffsets?[index]);
        }

        return Array.AsReadOnly(result);
    }

    public static RemotingPopMessage DecodeMessage(
        RemotingMessageView message,
        RemotingPullMessageQueue queue)
    {
        ArgumentNullException.ThrowIfNull(queue);
        return DecodeMessage(message, queue, queue.GetPullBrokerAddress());
    }

    public static RemotingPopMessage DecodeMessage(
        RemotingMessageView message,
        RemotingPullMessageQueue queue,
        string brokerAddress)
    {
        return DecodeMessage(message, queue, brokerAddress, responseFields: null);
    }

    private static RemotingPopMessage DecodeMessage(
        RemotingMessageView message,
        RemotingPullMessageQueue queue,
        string brokerAddress,
        IReadOnlyDictionary<string, object>? responseFields,
        long? checkpointOffset = null)
    {
        ArgumentNullException.ThrowIfNull(message);
        ArgumentNullException.ThrowIfNull(queue);
        ArgumentException.ThrowIfNullOrWhiteSpace(brokerAddress);
        EnsureMatchingPhysicalQueue(message, queue);
        EnsureSupportedMessage(message);

        if (message.Properties.TryGetValue(PopCheckpointProperty, out var extraInfo))
        {
            if (string.IsNullOrWhiteSpace(extraInfo))
            {
                throw new InvalidDataException(
                    $"POP message '{message.MessageId}' does not contain a valid {PopCheckpointProperty} receipt.");
            }

            return new RemotingPopMessage(message, DecodeReceipt(message, brokerAddress, extraInfo));
        }

        if (responseFields is null || checkpointOffset is null)
        {
            throw new InvalidDataException(
                $"POP message '{message.MessageId}' does not contain a valid {PopCheckpointProperty} receipt.");
        }

        return new RemotingPopMessage(
            message,
            SynthesizeReceipt(message, brokerAddress, responseFields, checkpointOffset.Value));
    }

    private static RemotingPopReceipt SynthesizeReceipt(
        RemotingMessageView message,
        string brokerAddress,
        IReadOnlyDictionary<string, object> responseFields,
        long checkpointOffset)
    {
        if (checkpointOffset < 0 || message.QueueId < 0 || message.QueueOffset < 0)
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
            checkpointOffset.ToString(CultureInfo.InvariantCulture),
            popTimeMilliseconds.ToString(CultureInfo.InvariantCulture),
            invisibleTimeMilliseconds.ToString(CultureInfo.InvariantCulture),
            reviveQueueId.ToString(CultureInfo.InvariantCulture),
            NormalTopicMarker,
            brokerName,
            message.QueueId.ToString(CultureInfo.InvariantCulture),
            message.QueueOffset.ToString(CultureInfo.InvariantCulture));
        try
        {
            return new RemotingPopReceipt(
                message.Topic,
                brokerName,
                brokerAddress,
                message.QueueId,
                checkpointOffset,
                message.QueueOffset,
                DateTimeOffset.FromUnixTimeMilliseconds(popTimeMilliseconds),
                TimeSpan.FromMilliseconds(invisibleTimeMilliseconds),
                reviveQueueId,
                extraInfo);
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

    private static IReadOnlyList<long>? GetCheckpointOffsets(
        IReadOnlyList<RemotingMessageView> messages,
        RemotingPullMessageQueue queue,
        IReadOnlyDictionary<string, object>? responseFields)
    {
        if (responseFields is null)
        {
            return null;
        }

        for (var index = 0; index < messages.Count; index++)
        {
            EnsureMatchingPhysicalQueue(messages[index], queue);
            EnsureSupportedMessage(messages[index]);
        }

        if (!TryGetResponseField(responseFields, StartOffsetInfoField, out var startOffsetInfo) ||
            startOffsetInfo.Length == 0)
        {
            return GetFallbackCheckpointOffsets(messages);
        }

        if (string.IsNullOrWhiteSpace(startOffsetInfo))
        {
            throw InvalidOffsetMapping(StartOffsetInfoField, queue, "it is empty");
        }

        if (!TryGetResponseField(responseFields, MessageOffsetInfoField, out var messageOffsetInfo) ||
            string.IsNullOrWhiteSpace(messageOffsetInfo))
        {
            throw InvalidOffsetMapping(MessageOffsetInfoField, queue, "it is missing or empty");
        }

        var checkpointOffset = ParseCheckpointOffset(startOffsetInfo, queue);
        var messageOffsets = ParseMessageOffsets(messageOffsetInfo, queue);
        if (messageOffsets.Count != messages.Count)
        {
            throw InvalidOffsetMapping(
                MessageOffsetInfoField,
                queue,
                $"it contains {messageOffsets.Count} offsets for {messages.Count} messages");
        }

        for (var index = 0; index < messages.Count; index++)
        {
            if (messageOffsets[index] != messages[index].QueueOffset)
            {
                throw InvalidOffsetMapping(
                    MessageOffsetInfoField,
                    queue,
                    $"offset {messageOffsets[index]} does not match message offset {messages[index].QueueOffset}");
            }

            if (checkpointOffset > messages[index].QueueOffset)
            {
                throw InvalidOffsetMapping(
                    StartOffsetInfoField,
                    queue,
                    $"checkpoint offset {checkpointOffset} is after message offset {messages[index].QueueOffset}");
            }
        }

        var checkpointOffsets = new long[messages.Count];
        Array.Fill(checkpointOffsets, checkpointOffset);
        return checkpointOffsets;
    }

    private static IReadOnlyList<long> GetFallbackCheckpointOffsets(IReadOnlyList<RemotingMessageView> messages)
    {
        var fallbackOffsets = new long[messages.Count];
        for (var index = 0; index < messages.Count; index++)
        {
            fallbackOffsets[index] = messages[index].QueueOffset;
        }

        return fallbackOffsets;
    }

    private static long ParseCheckpointOffset(string startOffsetInfo, RemotingPullMessageQueue queue)
    {
        var fields = GetPhysicalQueueSegment(startOffsetInfo, StartOffsetInfoField, queue);
        if (fields.Length != 3)
        {
            throw InvalidOffsetMapping(StartOffsetInfoField, queue, "its physical queue segment is malformed");
        }

        if (!long.TryParse(
                fields[2],
                NumberStyles.Integer,
                CultureInfo.InvariantCulture,
                out var checkpointOffset) ||
            checkpointOffset < 0)
        {
            throw InvalidOffsetMapping(StartOffsetInfoField, queue, "its checkpoint offset is invalid");
        }

        return checkpointOffset;
    }

    private static IReadOnlyList<long> ParseMessageOffsets(string messageOffsetInfo, RemotingPullMessageQueue queue)
    {
        var fields = GetPhysicalQueueSegment(messageOffsetInfo, MessageOffsetInfoField, queue);
        if (fields.Length != 3 || string.IsNullOrWhiteSpace(fields[2]))
        {
            throw InvalidOffsetMapping(MessageOffsetInfoField, queue, "its physical queue segment is malformed");
        }

        var rawOffsets = fields[2].Split(',', StringSplitOptions.None);
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
                throw InvalidOffsetMapping(MessageOffsetInfoField, queue, "it contains an invalid message offset");
            }

            offsets[index] = offset;
        }

        return offsets;
    }

    private static string[] GetPhysicalQueueSegment(
        string value,
        string fieldName,
        RemotingPullMessageQueue queue)
    {
        string[]? physicalQueueSegment = null;
        foreach (var rawSegment in value.Split(';', StringSplitOptions.None))
        {
            var segment = rawSegment.Split(
                ' ',
                StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
            if (segment.Length < 2 || !string.Equals(segment[0], NormalTopicMarker, StringComparison.Ordinal) ||
                !int.TryParse(segment[1], NumberStyles.Integer, CultureInfo.InvariantCulture, out var queueId) ||
                queueId != queue.QueueId)
            {
                continue;
            }

            if (physicalQueueSegment is not null)
            {
                throw InvalidOffsetMapping(fieldName, queue, "it contains duplicate physical queue segments");
            }

            physicalQueueSegment = segment;
        }

        return physicalQueueSegment ?? throw InvalidOffsetMapping(
            fieldName,
            queue,
            "it does not contain the requested physical queue");
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
        RemotingPullMessageQueue queue)
    {
        if (!string.Equals(message.BrokerName, queue.BrokerName, StringComparison.Ordinal) ||
            message.QueueId != queue.QueueId)
        {
            throw new InvalidDataException(
                $"POP message '{message.MessageId}' does not match the requested physical queue.");
        }
    }

    private static void EnsureSupportedMessage(RemotingMessageView message)
    {
        if (IsRetryOrLogicalMessage(message))
        {
            throw new InvalidDataException(
                $"POP message '{message.MessageId}' targets an unsupported retry or logical queue and cannot be represented by a {PopCheckpointProperty} receipt.");
        }
    }

    private static InvalidDataException InvalidOffsetMapping(
        string fieldName,
        RemotingPullMessageQueue queue,
        string reason) =>
        new(
            $"POP response field '{fieldName}' contains an invalid mapping for physical queue " +
            $"'{queue.BrokerName}:{queue.QueueId}': {reason}.");

    private static RemotingPopReceipt DecodeReceipt(
        RemotingMessageView message,
        string brokerAddress,
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
        if (!string.Equals(fields[4], NormalTopicMarker, StringComparison.Ordinal))
        {
            throw InvalidCheckpoint(message, "targets a retry or logical topic");
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
                brokerAddress,
                queueId,
                checkpointOffset,
                queueOffset,
                DateTimeOffset.FromUnixTimeMilliseconds(popTimeMilliseconds),
                TimeSpan.FromMilliseconds(invisibleTimeMilliseconds),
                reviveQueueId,
                extraInfo);
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

    private static bool IsRetryOrLogicalMessage(RemotingMessageView message) =>
        message.Properties.TryGetValue(RetryTopicProperty, out var retryTopic) &&
        !string.IsNullOrWhiteSpace(retryTopic) ||
        message.Topic.StartsWith(RetryTopicPrefix, StringComparison.Ordinal) ||
        message.Topic.StartsWith(DeadLetterTopicPrefix, StringComparison.Ordinal) ||
        message.BrokerName?.StartsWith(LogicalQueueBrokerPrefix, StringComparison.Ordinal) == true;

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
}
