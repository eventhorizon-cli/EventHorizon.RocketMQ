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
using EventHorizon.RocketMQ.Remoting.Consumer.Route;
using EventHorizon.RocketMQ.Remoting.Exceptions;
using EventHorizon.RocketMQ.Remoting.Instrumentation;
using EventHorizon.RocketMQ.Remoting.Producer;
using EventHorizon.RocketMQ.Remoting.Protocol;
using EventHorizon.RocketMQ.Remoting.Protocol.Route;

namespace EventHorizon.RocketMQ.Remoting.Consumer.Settlement;

internal sealed class RemotingSettlementClient : IRemotingSettlementClient
{
    private const string InnerProducerGroup = "CLIENT_INNER_PRODUCER";
    private const int DefaultTopicQueueCount = 4;
    private const int OrderlyRetrySendMaxAttempts = 3;
    private readonly RemotingConsumerSettings _settings;
    private readonly RemotingClientOptions _clientOptions;
    private readonly RemotingConsumerRouteResolver _routes;
    private readonly IRemotingClient _remotingClient;
    private readonly IRemotingRocketMQTelemetry _telemetry;
    private readonly TimeProvider _timeProvider;
    private int _orderlyRetryQueueIndex;

    public RemotingSettlementClient(
        RemotingConsumerSettings settings,
        RemotingClientOptions clientOptions,
        RemotingConsumerRouteResolver routes,
        IRemotingClient remotingClient,
        TimeProvider timeProvider,
        IRemotingRocketMQTelemetry telemetry)
    {
        ArgumentNullException.ThrowIfNull(settings);
        ArgumentNullException.ThrowIfNull(clientOptions);
        ArgumentNullException.ThrowIfNull(routes);
        ArgumentNullException.ThrowIfNull(remotingClient);
        ArgumentNullException.ThrowIfNull(timeProvider);
        ArgumentNullException.ThrowIfNull(telemetry);

        _settings = settings;
        _clientOptions = clientOptions;
        _routes = routes;
        _remotingClient = remotingClient;
        _timeProvider = timeProvider;
        _telemetry = telemetry;
    }

    public async Task SendOrderlyRetryAsync(
        RemotingMessageView message,
        int maxReconsumeTimes,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(message);
        var effectiveMaxReconsumeTimes = maxReconsumeTimes < 0 ? int.MaxValue : maxReconsumeTimes;
        var nextReconsumeTimes = message.ReconsumeTimes == int.MaxValue
            ? int.MaxValue
            : message.ReconsumeTimes + 1;
        var delayLevel = message.ReconsumeTimes > int.MaxValue - 3
            ? int.MaxValue
            : message.ReconsumeTimes + 3;
        using var telemetry = _telemetry.StartSettle(
            "reject",
            message.Topic,
            _settings.GroupName,
            message.MessageId,
            message.QueueId,
            message.Properties);
        try
        {
            // Released Java republishes an exhausted orderly message through the client's internal producer. The
            // Broker then compares the retry counters and redirects the new message to DLQ; this is intentionally not
            // CONSUMER_SEND_MSG_BACK with a negative delay level.
            // https://github.com/apache/rocketmq/blob/rocketmq-all-5.5.0/client/src/main/java/org/apache/rocketmq/client/impl/consumer/ConsumeMessageOrderlyService.java#L351-L399
            var retryTopic = $"%RETRY%{_settings.GroupName}";
            var wireRetryTopic = LegacyNamespace.Wrap(_clientOptions.Namespace, retryTopic);
            var originMessageId = message.Properties.TryGetValue("ORIGIN_MESSAGE_ID", out var origin) &&
                                  !string.IsNullOrWhiteSpace(origin)
                ? origin
                : message.MessageId;
            var properties = new Dictionary<string, string>(message.Properties, StringComparer.Ordinal)
            {
                ["ORIGIN_MESSAGE_ID"] = originMessageId,
                ["RETRY_TOPIC"] = message.Topic,
                ["RECONSUME_TIME"] = nextReconsumeTimes.ToString(CultureInfo.InvariantCulture),
                ["MAX_RECONSUME_TIMES"] = effectiveMaxReconsumeTimes.ToString(CultureInfo.InvariantCulture),
                ["DELAY"] = delayLevel.ToString(CultureInfo.InvariantCulture)
            };
            properties.Remove("TRAN_MSG");

            // Java's internal DefaultMQProducer uses two retries after the initial send. Each attempt resolves a fresh
            // writable queue so a failed Broker is not pinned for the whole logical settlement operation.
            // https://github.com/apache/rocketmq/blob/rocketmq-all-5.5.0/client/src/main/java/org/apache/rocketmq/client/producer/DefaultMQProducer.java
            Exception? lastException = null;
            string? lastBrokerName = null;
            for (var attempt = 0; attempt < OrderlyRetrySendMaxAttempts; attempt++)
            {
                cancellationToken.ThrowIfCancellationRequested();
                try
                {
                    var queue = await SelectWritableQueueAsync(
                        retryTopic,
                        forceRefresh: attempt > 0,
                        lastBrokerName,
                        cancellationToken).ConfigureAwait(false);
                    lastBrokerName = queue.BrokerName;
                    var response = await _remotingClient.InvokeAsync(
                        EndpointParser.Parse(queue.Address),
                        new RemotingCommand(RequestCode.SendMessage, new SendMessageRequestHeader
                        {
                            ProducerGroup = InnerProducerGroup,
                            Topic = wireRetryTopic,
                            QueueId = queue.QueueId,
                            SysFlag = 0,
                            BornTimestamp = _timeProvider.GetUtcNow().ToUnixTimeMilliseconds(),
                            Flag = message.Flag,
                            Properties = MessagePropertyCodec.Serialize(properties),
                            ReconsumeTimes = nextReconsumeTimes,
                            UnitMode = _clientOptions.UnitMode,
                            MaxReconsumeTimes = effectiveMaxReconsumeTimes,
                            Batch = false,
                            DefaultTopicQueueNums = DefaultTopicQueueCount,
                            Bname = queue.BrokerName
                        })
                        {
                            Body = message.Body
                        },
                        _clientOptions.RequestTimeout,
                        cancellationToken).ConfigureAwait(false);
                    EnsureSendSuccess(response);
                    telemetry.Complete();
                    return;
                }
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                {
                    throw;
                }
                catch (RemotingCommandException exception) when (
                    IsRetryableSendResponse(exception.ResponseCode) &&
                    attempt + 1 < OrderlyRetrySendMaxAttempts)
                {
                    lastException = exception;
                }
                catch (RemotingCommandException)
                {
                    throw;
                }
                catch (Exception exception) when (exception is InvalidDataException or ArgumentException)
                {
                    throw;
                }
                catch (Exception exception) when (attempt + 1 < OrderlyRetrySendMaxAttempts)
                {
                    lastException = exception;
                }
            }

            throw lastException ?? new InvalidOperationException("The orderly retry message could not be sent.");
        }
        catch (Exception exception)
        {
            telemetry.Complete(exception);
            throw;
        }
    }

    private async Task<WritableQueue> SelectWritableQueueAsync(
        string retryTopic,
        bool forceRefresh,
        string? lastBrokerName,
        CancellationToken cancellationToken)
    {
        var queues = Array.Empty<WritableQueue>();
        try
        {
            var route = await _routes.GetRawRouteAsync(
                retryTopic,
                forceRefresh,
                cancellationToken).ConfigureAwait(false);
            queues = GetWritableQueues(route, useDefaultRoute: false);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch
        {
            // A missing retry-topic route is handled by the same default-topic fallback below.
        }

        if (queues.Length == 0)
        {
            var defaultRoute = await _routes.GetRawRouteAsync(
                "TBW102",
                forceRefresh,
                cancellationToken).ConfigureAwait(false);
            queues = GetWritableQueues(defaultRoute, useDefaultRoute: true);
        }

        if (queues.Length == 0)
        {
            throw new InvalidOperationException($"No writable message queue is available for retry topic '{retryTopic}'.");
        }

        var start = (Interlocked.Increment(ref _orderlyRetryQueueIndex) & int.MaxValue) % queues.Length;
        for (var offset = 0; offset < queues.Length; offset++)
        {
            var candidate = queues[(start + offset) % queues.Length];
            if (lastBrokerName is null ||
                candidate.BrokerName != lastBrokerName ||
                queues.All(queue => queue.BrokerName == lastBrokerName))
            {
                return candidate;
            }
        }

        return queues[start];
    }

    private static WritableQueue[] GetWritableQueues(TopicRouteData route, bool useDefaultRoute)
    {
        const int writePermission = 1 << 1;
        return route.QueueDatas
            .Where(queue => (queue.Perm & writePermission) == writePermission)
            .SelectMany(
                queue => Enumerable.Range(
                    0,
                    useDefaultRoute
                        ? Math.Min(queue.WriteQueueNums, DefaultTopicQueueCount)
                        : queue.WriteQueueNums),
                (queue, queueId) => new { queue.BrokerName, QueueId = queueId })
            .Join(
                route.BrokerDatas,
                static queue => queue.BrokerName,
                static broker => broker.BrokerName,
                static (queue, broker) => new { queue.BrokerName, queue.QueueId, broker.BrokerAddrs })
            .Where(static value => value.BrokerAddrs.TryGetValue(0, out _))
            .Select(static value => new WritableQueue(
                value.BrokerName,
                value.BrokerAddrs[0],
                value.QueueId))
            .OrderBy(static queue => queue.BrokerName, StringComparer.Ordinal)
            .ThenBy(static queue => queue.QueueId)
            .ToArray();
    }

    private static void EnsureSendSuccess(RemotingCommand response)
    {
        if (response.Code is not (
            ResponseCodes.ResSuccess or
            ResponseCodes.ResFlushDiskTimeout or
            ResponseCodes.ResFlushSlaveTimeout or
            ResponseCodes.ResSlaveNotAvailable))
        {
            throw new RemotingCommandException(
                response.Code,
                response.Remark ?? "Broker rejected the orderly retry message.");
        }
    }

    private static bool IsRetryableSendResponse(int responseCode) =>
        responseCode is ResponseCodes.ResTopicNotExist or
            ResponseCodes.ResServiceNotAvailable or
            ResponseCodes.ResError or
            ResponseCodes.ResSystemBusy or
            ResponseCodes.ResNoPermission or
            ResponseCodes.ResNoBuyerId or
            ResponseCodes.ResNotInCurrentUnit or
            ResponseCodes.ResGoAway;

    public async Task SendBackAsync(
        RemotingConsumerQueue queue,
        RemotingMessageView message,
        int delayLevel,
        int maxReconsumeTimes,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(queue);
        ArgumentNullException.ThrowIfNull(message);
        using var telemetry = _telemetry.StartSettle(
            delayLevel < 0 ? "reject" : "nack",
            message.Topic,
            _settings.GroupName,
            message.MessageId,
            queue.QueueId,
            message.Properties);
        try
        {
            var broker = await _routes.ResolveBrokerAsync(
                queue,
                useSuggestedBroker: false,
                cancellationToken).ConfigureAwait(false);
            var response = await _remotingClient.InvokeAsync(
                EndpointParser.Parse(broker.Address),
                new RemotingCommand(RequestCode.ConsumerSendMsgBack, new ConsumerSendMessageBackRequestHeader
                {
                    Offset = message.CommitLogOffset,
                    Group = GetConsumerGroup(),
                    DelayLevel = delayLevel,
                    OriginMsgId = message.Properties.TryGetValue("ORIGIN_MESSAGE_ID", out var originMessageId)
                        ? originMessageId
                        : message.MessageId,
                    OriginTopic = GetWireTopic(message.Topic),
                    UnitMode = _clientOptions.UnitMode,
                    MaxReconsumeTimes = maxReconsumeTimes,
                    Bname = queue.BrokerName
                }),
                _clientOptions.RequestTimeout,
                cancellationToken).ConfigureAwait(false);
            EnsureSuccess(response, "send message back");
            telemetry.Complete();
        }
        catch (Exception exception)
        {
            telemetry.Complete(exception);
            throw;
        }
    }

    private static void EnsureSuccess(RemotingCommand response, string operation)
    {
        if (response.Code != ResponseCodes.ResSuccess)
        {
            throw new RemotingCommandException(
                response.Code,
                response.Remark ?? $"Broker rejected the {operation} request.");
        }
    }

    private string GetConsumerGroup() => LegacyNamespace.Wrap(_clientOptions.Namespace, _settings.GroupName);

    private string GetWireTopic(string topic) => LegacyNamespace.Wrap(_clientOptions.Namespace, topic);

    private sealed record WritableQueue(string BrokerName, string Address, int QueueId);
}
