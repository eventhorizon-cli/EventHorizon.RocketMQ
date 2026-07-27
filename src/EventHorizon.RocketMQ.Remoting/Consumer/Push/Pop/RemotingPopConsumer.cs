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
using EventHorizon.RocketMQ.Remoting.Consumer.Pull;
using EventHorizon.RocketMQ.Remoting.Exceptions;
using EventHorizon.RocketMQ.Remoting.Instrumentation;
using EventHorizon.RocketMQ.Remoting.Protocol;
using Microsoft.Extensions.Options;

namespace EventHorizon.RocketMQ.Remoting.Consumer.Push.Pop;

internal sealed class RemotingPopConsumer : IRemotingPopConsumer
{
    private readonly RemotingPopConsumerOptions _options;
    private readonly RemotingClientOptions _clientOptions;
    private readonly IRemotingConsumerEngine _consumerEngine;
    private readonly IRemotingClient _remotingClient;
    private readonly TimeProvider _timeProvider;
    private readonly IRemotingRocketMQTelemetry _telemetry;
    private readonly SemaphoreSlim _lifecycleGate = new(1, 1);
    private int _started;
    private int _disposed;

    public RemotingPopConsumer(
        IOptions<RemotingPopConsumerOptions> options,
        IOptions<RemotingClientOptions> clientOptions,
        IRemotingConsumerEngine consumerEngine,
        IRemotingClient remotingClient,
        TimeProvider timeProvider,
        IRemotingRocketMQTelemetry? telemetry = null)
    {
        _options = options.Value;
        _clientOptions = clientOptions.Value;
        _consumerEngine = consumerEngine;
        _remotingClient = remotingClient;
        _timeProvider = timeProvider;
        _telemetry = telemetry ?? RemotingRocketMQTelemetry.Disabled;
    }

    public async ValueTask StartAsync(CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);
        await _lifecycleGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);
            if (Volatile.Read(ref _started) != 0)
            {
                return;
            }

            await _consumerEngine.StartAsync(cancellationToken).ConfigureAwait(false);
            Volatile.Write(ref _started, 1);
        }
        finally
        {
            _lifecycleGate.Release();
        }
    }

    public async ValueTask StopAsync(CancellationToken cancellationToken = default)
    {
        await _lifecycleGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (Volatile.Read(ref _started) == 0)
            {
                return;
            }

            Volatile.Write(ref _started, 0);
            await _consumerEngine.StopAsync(cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            _lifecycleGate.Release();
        }
    }

    public Task<IReadOnlyList<RemotingPullMessageQueue>> GetMessageQueuesAsync(
        string topic,
        CancellationToken cancellationToken = default)
    {
        EnsureStarted();
        return _consumerEngine.GetMessageQueuesAsync(topic, cancellationToken);
    }

    public async Task<RemotingPopResult> PopAsync(
        RemotingPullMessageQueue queue,
        int? maxMessages = null,
        TimeSpan? invisibleDuration = null,
        FilterExpression? filter = null,
        CancellationToken cancellationToken = default)
    {
        EnsureStarted();
        ArgumentNullException.ThrowIfNull(queue);
        var messageCount = maxMessages ?? _options.BatchSize;
        if (messageCount is <= 0 or > RemotingPopConsumerOptions.MaximumBatchSize)
        {
            throw new ArgumentOutOfRangeException(nameof(maxMessages));
        }

        var visibility = invisibleDuration ?? _options.InvisibleDuration;
        var invisibleTimeMilliseconds = ToPositiveMilliseconds(visibility, nameof(invisibleDuration));
        var filterExpression = GetFilter(queue, filter);
        var brokerAddress = queue.GetPullBrokerAddress();
        var request = new RemotingCommand(RequestCode.PopMessage, new PopMessageRequestHeader
        {
            ConsumerGroup = GetConsumerGroup(),
            Topic = GetWireTopic(queue.Topic),
            QueueId = queue.QueueId,
            MaxMsgNums = messageCount,
            InvisibleTime = invisibleTimeMilliseconds,
            PollTime = ToPositiveMilliseconds(_options.LongPollingTimeout, nameof(_options.LongPollingTimeout)),
            BornTime = _timeProvider.GetUtcNow().ToUnixTimeMilliseconds(),
            InitMode = 0,
            ExpType = filterExpression.Type == FilterExpressionType.Sql ? "SQL92" : "TAG",
            Exp = filterExpression.Expression,
            Order = false,
            Bname = queue.BrokerName
        });
        var parentContext = Activity.Current?.Context;
        var startTime = DateTimeOffset.UtcNow;
        var startTimestamp = Stopwatch.GetTimestamp();
        try
        {
            var response = await _remotingClient.InvokeAsync(
                EndpointParser.Parse(brokerAddress),
                request,
                _clientOptions.RequestTimeout + _options.LongPollingTimeout,
                cancellationToken).ConfigureAwait(false);
            EnsureResponseBodyWithinLimit(response);
            var result = LegacyPopMessageDecoder.Decode(response, queue, brokerAddress, _clientOptions.Namespace);
            using var telemetry = _telemetry.StartReceive(
                queue.Topic,
                _options.GroupName,
                result.Messages.Count,
                parentContext,
                startTime,
                startTimestamp,
                queue.QueueId,
                createActivity: result.Messages.Count > 0,
                messageProperties: result.Messages.Select(static message => message.Message.Properties));
            telemetry.Complete();
            return result;
        }
        catch (Exception exception)
        {
            using var telemetry = _telemetry.StartReceive(
                queue.Topic,
                _options.GroupName,
                0,
                parentContext,
                startTime,
                startTimestamp,
                queue.QueueId);
            telemetry.Complete(exception);
            throw;
        }
    }

    public async Task AcknowledgeAsync(RemotingPopReceipt receipt, CancellationToken cancellationToken = default)
    {
        EnsureStarted();
        ArgumentNullException.ThrowIfNull(receipt);
        using var telemetry = _telemetry.StartSettle(
            "ack",
            receipt.Topic,
            _options.GroupName,
            null,
            receipt.QueueId,
            null);
        try
        {
            var response = await _remotingClient.InvokeAsync(
                EndpointParser.Parse(receipt.BrokerAddress),
                new RemotingCommand(RequestCode.AckMessage, new AckMessageRequestHeader
                {
                    ConsumerGroup = GetConsumerGroup(),
                    Topic = GetWireTopic(receipt.Topic),
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
        CancellationToken cancellationToken = default)
    {
        EnsureStarted();
        ArgumentNullException.ThrowIfNull(receipt);
        var invisibleTimeMilliseconds = ToPositiveMilliseconds(invisibleDuration, nameof(invisibleDuration));
        using var telemetry = _telemetry.StartSettle(
            "nack",
            receipt.Topic,
            _options.GroupName,
            null,
            receipt.QueueId,
            null);
        try
        {
            var response = await _remotingClient.InvokeAsync(
                EndpointParser.Parse(receipt.BrokerAddress),
                new RemotingCommand(RequestCode.ChangeMessageInvisibleTime, new ChangeInvisibleTimeRequestHeader
                {
                    ConsumerGroup = GetConsumerGroup(),
                    Topic = GetWireTopic(receipt.Topic),
                    QueueId = receipt.QueueId,
                    ExtraInfo = receipt.ExtraInfo,
                    Offset = receipt.QueueOffset,
                    InvisibleTime = invisibleTimeMilliseconds,
                    Bname = receipt.BrokerName
                }),
                _clientOptions.RequestTimeout,
                cancellationToken).ConfigureAwait(false);
            EnsureSuccess(response, "change the POP message invisibility");
            var renewedReceipt = RenewReceipt(receipt, response);
            telemetry.Complete();
            return renewedReceipt;
        }
        catch (Exception exception)
        {
            telemetry.Complete(exception);
            throw;
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
        {
            return;
        }

        try
        {
            await _lifecycleGate.WaitAsync().ConfigureAwait(false);
            try
            {
                if (Volatile.Read(ref _started) != 0)
                {
                    Volatile.Write(ref _started, 0);
                    await _consumerEngine.StopAsync(CancellationToken.None).ConfigureAwait(false);
                }
            }
            finally
            {
                _lifecycleGate.Release();
            }
        }
        finally
        {
            await _consumerEngine.DisposeAsync().ConfigureAwait(false);
        }
    }

    private static long ToPositiveMilliseconds(TimeSpan value, string parameterName)
    {
        if (value <= TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(parameterName);
        }

        var milliseconds = checked((long)value.TotalMilliseconds);
        return milliseconds > 0 ? milliseconds : throw new ArgumentOutOfRangeException(parameterName);
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

    private static RemotingPopReceipt RenewReceipt(RemotingPopReceipt receipt, RemotingCommand response)
    {
        var popTimeMilliseconds = GetRequiredPositiveInt64(response.ExtFields, "popTime");
        var invisibleTimeMilliseconds = GetRequiredPositiveInt64(response.ExtFields, "invisibleTime");
        var reviveQueueId = GetRequiredNonNegativeInt32(response.ExtFields, "reviveQid");
        try
        {
            return receipt.Renew(popTimeMilliseconds, invisibleTimeMilliseconds, reviveQueueId);
        }
        catch (ArgumentOutOfRangeException exception)
        {
            throw new InvalidDataException(
                "POP invisibility response contained an out-of-range receipt value.",
                exception);
        }
        catch (OverflowException exception)
        {
            throw new InvalidDataException(
                "POP invisibility response contained an out-of-range receipt value.",
                exception);
        }
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

    private FilterExpression GetFilter(RemotingPullMessageQueue queue, FilterExpression? filter)
    {
        if (filter is not null)
        {
            return filter;
        }

        return _options.Subscriptions.TryGetValue(queue.Topic, out var configured)
            ? configured
            : FilterExpression.All;
    }

    private string GetConsumerGroup() => LegacyNamespace.Wrap(_clientOptions.Namespace, _options.GroupName);

    private string GetWireTopic(string topic) => LegacyNamespace.Wrap(_clientOptions.Namespace, topic);

    private void EnsureStarted()
    {
        ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);
        if (Volatile.Read(ref _started) == 0)
        {
            throw new InvalidOperationException("POP consumer has not been started.");
        }
    }
}
