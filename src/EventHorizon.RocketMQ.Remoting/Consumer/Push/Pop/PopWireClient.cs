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

using System.Diagnostics;
using System.Globalization;
using EventHorizon.RocketMQ.Remoting.Consumer.Push.Assignment;
using EventHorizon.RocketMQ.Remoting.Consumer.Route;
using EventHorizon.RocketMQ.Remoting.Exceptions;
using EventHorizon.RocketMQ.Remoting.Instrumentation;
using EventHorizon.RocketMQ.Remoting.Protocol;

namespace EventHorizon.RocketMQ.Remoting.Consumer.Push.Pop;

internal sealed class PopWireClient
{
    private readonly RemotingPushConsumerOptions _options;
    private readonly RemotingClientOptions _clientOptions;
    private readonly RemotingConsumerRouteResolver _routes;
    private readonly IRemotingClient _remotingClient;
    private readonly TimeProvider _timeProvider;
    private readonly IRemotingRocketMQTelemetry _telemetry;

    public PopWireClient(
        RemotingPushConsumerOptions options,
        RemotingClientOptions clientOptions,
        RemotingConsumerRouteResolver routes,
        IRemotingClient remotingClient,
        TimeProvider timeProvider,
        IRemotingRocketMQTelemetry? telemetry = null)
    {
        _options = options;
        _clientOptions = clientOptions;
        _routes = routes;
        _remotingClient = remotingClient;
        _timeProvider = timeProvider;
        _telemetry = telemetry ?? RemotingRocketMQTelemetry.Disabled;
    }

    public async Task<RemotingPopResult> PopAsync(
        RemotingPushAssignment assignment,
        FilterExpression filter,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(assignment);
        ArgumentNullException.ThrowIfNull(filter);

        var parentContext = Activity.Current?.Context;
        var startTime = _timeProvider.GetUtcNow();
        var startTimestamp = Stopwatch.GetTimestamp();
        try
        {
            var broker = await _routes.ResolveBrokerAsync(
                assignment.Topic,
                assignment.BrokerName,
                cancellationToken).ConfigureAwait(false);
            var response = await _remotingClient.InvokeAsync(
                EndpointParser.Parse(broker.Address),
                new RemotingCommand(RequestCode.PopMessage, new PopMessageRequestHeader
                {
                    ConsumerGroup = GetConsumerGroup(),
                    Topic = GetWireTopic(assignment.Topic),
                    QueueId = assignment.QueueId,
                    MaxMsgNums = Math.Min(
                        _options.PopBatchSize,
                        _options.PopMaxInflightMessagesPerAssignment),
                    InvisibleTime = ToPositiveMilliseconds(_options.PopInvisibleDuration),
                    PollTime = ToPositiveMilliseconds(_options.LongPollingTimeout),
                    BornTime = _timeProvider.GetUtcNow().ToUnixTimeMilliseconds(),
                    InitMode = 0,
                    ExpType = filter.Type == FilterExpressionType.Sql ? "SQL92" : "TAG",
                    Exp = filter.Expression,
                    Order = false,
                    Bname = assignment.BrokerName
                }),
                _clientOptions.RequestTimeout + _options.LongPollingTimeout,
                cancellationToken).ConfigureAwait(false);
            EnsureResponseBodyWithinLimit(response);
            var result = LegacyPopMessageDecoder.Decode(
                response,
                assignment,
                _clientOptions.Namespace);
            using var telemetry = _telemetry.StartReceive(
                assignment.Topic,
                _options.GroupName,
                result.Messages.Count,
                parentContext,
                startTime,
                startTimestamp,
                assignment.QueueId >= 0 ? assignment.QueueId : null,
                createActivity: result.Messages.Count > 0,
                messageProperties: result.Messages.Select(static message => message.Message.Properties));
            if (telemetry.Activity is { } activity)
            {
                foreach (var message in result.Messages)
                {
                    message.Message.ReceiveActivityContext = activity.Context;
                }
            }

            telemetry.Complete();
            return result;
        }
        catch (Exception exception)
        {
            using var telemetry = _telemetry.StartReceive(
                assignment.Topic,
                _options.GroupName,
                0,
                parentContext,
                startTime,
                startTimestamp,
                assignment.QueueId >= 0 ? assignment.QueueId : null,
                createActivity: true);
            telemetry.Complete(exception);
            throw;
        }
    }

    public async Task AcknowledgeAsync(
        RemotingPopReceipt receipt,
        RemotingMessageView message,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(receipt);
        ArgumentNullException.ThrowIfNull(message);
        using var telemetry = _telemetry.StartSettle(
            "ack",
            receipt.Topic,
            _options.GroupName,
            message.MessageId,
            receipt.QueueId,
            message.Properties);
        try
        {
            var broker = await _routes.ResolveBrokerAsync(
                receipt.Topic,
                receipt.BrokerName,
                cancellationToken).ConfigureAwait(false);
            var response = await _remotingClient.InvokeAsync(
                EndpointParser.Parse(broker.Address),
                new RemotingCommand(RequestCode.AckMessage, new AckMessageRequestHeader
                {
                    ConsumerGroup = GetConsumerGroup(),
                    Topic = GetReceiptWireTopic(receipt),
                    QueueId = receipt.QueueId,
                    ExtraInfo = receipt.ExtraInfo,
                    Offset = receipt.QueueOffset,
                    Bname = receipt.BrokerName
                }),
                _clientOptions.RequestTimeout,
                cancellationToken).ConfigureAwait(false);
            EnsureSuccess(response, "acknowledge the POP message");
            telemetry.Complete();
        }
        catch (Exception exception)
        {
            telemetry.Complete(exception);
            throw;
        }
    }

    // This is the POP Retry/NACK wire operation, not handler-active lease renewal. The replacement invisible window
    // keeps the message unavailable to this consumer group for the selected delay. In the Broker's classic revive-log
    // path, it appends a replacement checkpoint and acknowledges the original checkpoint so the old deadline cannot
    // also revive the message. If the replacement expires without an ACK, PopReviveService copies the business message
    // to its POP retry topic (or keeps an existing retry topic) and increments reconsumeTimes because Suspend is false.
    // Brokers using the POP KV service store the state differently but preserve the delayed-redelivery contract.
    // Design: docs/en-US/remoting/consumer-model.md#handler-outcomes-and-pop-fixed-deadlines.
    // Apache Broker references:
    // https://github.com/apache/rocketmq/blob/rocketmq-all-5.5.0/broker/src/main/java/org/apache/rocketmq/broker/processor/ChangeInvisibleTimeProcessor.java#L172-L180
    // https://github.com/apache/rocketmq/blob/rocketmq-all-5.5.0/broker/src/main/java/org/apache/rocketmq/broker/processor/ChangeInvisibleTimeProcessor.java#L310-L360
    // https://github.com/apache/rocketmq/blob/rocketmq-all-5.5.0/broker/src/main/java/org/apache/rocketmq/broker/processor/PopReviveService.java#L107-L154
    public async Task<RemotingPopReceipt> ChangeInvisibleTimeAsync(
        RemotingPopReceipt receipt,
        TimeSpan invisibleDuration,
        RemotingMessageView message,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(receipt);
        ArgumentNullException.ThrowIfNull(message);
        using var telemetry = _telemetry.StartSettle(
            "nack",
            receipt.Topic,
            _options.GroupName,
            message.MessageId,
            receipt.QueueId,
            message.Properties);
        try
        {
            var broker = await _routes.ResolveBrokerAsync(
                receipt.Topic,
                receipt.BrokerName,
                cancellationToken).ConfigureAwait(false);
            var response = await _remotingClient.InvokeAsync(
                EndpointParser.Parse(broker.Address),
                new RemotingCommand(RequestCode.ChangeMessageInvisibleTime, new ChangeInvisibleTimeRequestHeader
                {
                    ConsumerGroup = GetConsumerGroup(),
                    Topic = GetReceiptWireTopic(receipt),
                    QueueId = receipt.QueueId,
                    ExtraInfo = receipt.ExtraInfo,
                    Offset = receipt.QueueOffset,
                    InvisibleTime = ToPositiveMilliseconds(invisibleDuration),
                    Bname = receipt.BrokerName,
                    // Retry semantics: the Broker increments reconsumeTimes when the replacement checkpoint revives.
                    Suspend = false
                }),
                _clientOptions.RequestTimeout,
                cancellationToken).ConfigureAwait(false);
            EnsureSuccess(response, "change the POP message invisibility");
            // The response identifies the replacement checkpoint. Rebuild ExtraInfo from these values so any later
            // operation uses the new receipt instead of the acknowledged original checkpoint. The current one-shot
            // Retry path intentionally discards it because scheduling redelivery completes that delivery attempt.
            var changed = DecodeChangedReceipt(receipt, response);
            telemetry.Complete();
            return changed;
        }
        catch (Exception exception)
        {
            telemetry.Complete(exception);
            throw;
        }
    }

    private string GetReceiptWireTopic(RemotingPopReceipt receipt) => receipt.GetWireTopic(
        GetWireTopic(receipt.Topic),
        GetConsumerGroup());

    private string GetConsumerGroup() => LegacyNamespace.Wrap(_clientOptions.Namespace, _options.GroupName);

    private string GetWireTopic(string topic) => LegacyNamespace.Wrap(_clientOptions.Namespace, topic);

    private static long ToPositiveMilliseconds(TimeSpan value)
    {
        var milliseconds = checked((long)value.TotalMilliseconds);
        return milliseconds > 0
            ? milliseconds
            : throw new ArgumentOutOfRangeException(nameof(value));
    }

    private static void EnsureSuccess(RemotingCommand response, string operation)
    {
        if (response.Code != ResponseCodes.ResSuccess)
        {
            throw new RemotingCommandException(
                response.Code,
                response.Remark ?? $"Broker rejected the request to {operation}.");
        }
    }

    private static RemotingPopReceipt DecodeChangedReceipt(RemotingPopReceipt receipt, RemotingCommand response)
    {
        var popTimeMilliseconds = GetRequiredPositiveInt64(response.ExtFields, "popTime");
        var invisibleTimeMilliseconds = GetRequiredPositiveInt64(response.ExtFields, "invisibleTime");
        var reviveQueueId = GetRequiredNonNegativeInt32(response.ExtFields, "reviveQid");
        try
        {
            return receipt.WithChangedInvisibleTime(popTimeMilliseconds, invisibleTimeMilliseconds, reviveQueueId);
        }
        catch (Exception exception) when (exception is ArgumentOutOfRangeException or OverflowException)
        {
            throw new InvalidDataException(
                "POP invisibility response contained an out-of-range receipt value.",
                exception);
        }
    }

    private static long GetRequiredPositiveInt64(IReadOnlyDictionary<string, object> fields, string name)
    {
        if (!fields.TryGetValue(name, out var raw) ||
            !long.TryParse(
                Convert.ToString(raw, CultureInfo.InvariantCulture),
                NumberStyles.Integer,
                CultureInfo.InvariantCulture,
                out var value) ||
            value <= 0)
        {
            throw new InvalidDataException($"POP response did not contain a valid positive '{name}' field.");
        }

        return value;
    }

    private static int GetRequiredNonNegativeInt32(IReadOnlyDictionary<string, object> fields, string name)
    {
        if (!fields.TryGetValue(name, out var raw) ||
            !int.TryParse(
                Convert.ToString(raw, CultureInfo.InvariantCulture),
                NumberStyles.Integer,
                CultureInfo.InvariantCulture,
                out var value) ||
            value < 0)
        {
            throw new InvalidDataException($"POP response did not contain a valid non-negative '{name}' field.");
        }

        return value;
    }

    private void EnsureResponseBodyWithinLimit(RemotingCommand response)
    {
        if (response.Body is { Length: > 0 } body && body.Length > _options.MaxMessageBytes)
        {
            throw new InvalidDataException(
                $"POP response body of {body.Length} bytes exceeds the configured limit of " +
                $"{_options.MaxMessageBytes} bytes.");
        }
    }
}
