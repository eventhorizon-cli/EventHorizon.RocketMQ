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
using EventHorizon.RocketMQ.Remoting.Exceptions;
using EventHorizon.RocketMQ.Remoting.Instrumentation;
using EventHorizon.RocketMQ.Remoting.Protocol;

namespace EventHorizon.RocketMQ.Remoting.Consumer.Push.Pop;

internal sealed class PopWireClient
{
    private readonly RemotingPushConsumerOptions _options;
    private readonly RemotingClientOptions _clientOptions;
    private readonly IRemotingClient _remotingClient;
    private readonly TimeProvider _timeProvider;
    private readonly IRemotingRocketMQTelemetry _telemetry;

    public PopWireClient(
        RemotingPushConsumerOptions options,
        RemotingClientOptions clientOptions,
        IRemotingClient remotingClient,
        TimeProvider timeProvider,
        IRemotingRocketMQTelemetry? telemetry = null)
    {
        _options = options;
        _clientOptions = clientOptions;
        _remotingClient = remotingClient;
        _timeProvider = timeProvider;
        _telemetry = telemetry ?? RemotingRocketMQTelemetry.Disabled;
    }

    public async Task<RemotingPopResult> PopAsync(
        RemotingPushAssignment assignment,
        string brokerAddress,
        FilterExpression filter,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(assignment);
        ArgumentException.ThrowIfNullOrWhiteSpace(brokerAddress);
        ArgumentNullException.ThrowIfNull(filter);

        var parentContext = Activity.Current?.Context;
        var startTime = _timeProvider.GetUtcNow();
        var startTimestamp = Stopwatch.GetTimestamp();
        try
        {
            var response = await _remotingClient.InvokeAsync(
                EndpointParser.Parse(brokerAddress),
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
                brokerAddress,
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
            var response = await _remotingClient.InvokeAsync(
                EndpointParser.Parse(receipt.BrokerAddress),
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

    public async Task<RemotingPopReceipt> ChangeInvisibleTimeAsync(
        RemotingPopReceipt receipt,
        TimeSpan invisibleDuration,
        bool suspend,
        RemotingMessageView message,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(receipt);
        ArgumentNullException.ThrowIfNull(message);
        using var telemetry = _telemetry.StartSettle(
            suspend ? "renew" : "nack",
            receipt.Topic,
            _options.GroupName,
            message.MessageId,
            receipt.QueueId,
            message.Properties);
        try
        {
            var response = await _remotingClient.InvokeAsync(
                EndpointParser.Parse(receipt.BrokerAddress),
                new RemotingCommand(RequestCode.ChangeMessageInvisibleTime, new ChangeInvisibleTimeRequestHeader
                {
                    ConsumerGroup = GetConsumerGroup(),
                    Topic = GetReceiptWireTopic(receipt),
                    QueueId = receipt.QueueId,
                    ExtraInfo = receipt.ExtraInfo,
                    Offset = receipt.QueueOffset,
                    InvisibleTime = ToPositiveMilliseconds(invisibleDuration),
                    Bname = receipt.BrokerName,
                    Suspend = suspend
                }),
                _clientOptions.RequestTimeout,
                cancellationToken).ConfigureAwait(false);
            EnsureSuccess(response, "change the POP message invisibility");
            var renewed = RenewReceipt(receipt, response);
            telemetry.Complete();
            return renewed;
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

    private static RemotingPopReceipt RenewReceipt(RemotingPopReceipt receipt, RemotingCommand response)
    {
        var popTimeMilliseconds = GetRequiredPositiveInt64(response.ExtFields, "popTime");
        var invisibleTimeMilliseconds = GetRequiredPositiveInt64(response.ExtFields, "invisibleTime");
        var reviveQueueId = GetRequiredNonNegativeInt32(response.ExtFields, "reviveQid");
        try
        {
            return receipt.Renew(popTimeMilliseconds, invisibleTimeMilliseconds, reviveQueueId);
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
