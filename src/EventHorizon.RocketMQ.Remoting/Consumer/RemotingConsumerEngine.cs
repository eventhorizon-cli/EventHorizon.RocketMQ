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

using System.Collections.Concurrent;
using System.Globalization;
using EventHorizon.RocketMQ.Consumer;
using EventHorizon.RocketMQ.Remoting.Consumer.Pull;
using EventHorizon.RocketMQ.Remoting.Exceptions;
using EventHorizon.RocketMQ.Remoting.Protocol;
using EventHorizon.RocketMQ.Remoting.Protocol.Route;
using Microsoft.Extensions.Options;

namespace EventHorizon.RocketMQ.Remoting.Consumer;

internal sealed class RemotingConsumerEngine : IRemotingConsumerEngine
{
    private const int ReadPermission = 1 << 2;
    private const int SuspendFlag = 1 << 1;
    private const int SubscriptionFlag = 1 << 2;

    // Broker checks expired long-poll requests on a five-second cadence.
    private static readonly TimeSpan BrokerLongPollingCheckInterval = TimeSpan.FromSeconds(5);

    private readonly RemotingConsumerSettings _settings;
    private readonly RemotingClientOptions _clientOptions;
    private readonly ITopicRouteService _routes;
    private readonly IRemotingClient _remotingClient;
    private readonly TimeProvider _timeProvider;
    private readonly ConcurrentDictionary<string, FilterExpression> _subscriptions;
    private readonly SemaphoreSlim _lifecycleGate = new(1, 1);
    private int _started;
    private int _disposed;

    public RemotingConsumerEngine(
        RemotingConsumerSettings settings,
        IOptions<RemotingClientOptions> clientOptions,
        ITopicRouteService routes,
        IRemotingClient remotingClient,
        TimeProvider timeProvider)
    {
        _settings = settings;
        _clientOptions = clientOptions.Value;
        _routes = routes;
        _remotingClient = remotingClient;
        _timeProvider = timeProvider;
        _subscriptions = new ConcurrentDictionary<string, FilterExpression>(
            _settings.Subscriptions,
            StringComparer.Ordinal);
    }

    public async ValueTask StartAsync(CancellationToken cancellationToken = default)
    {
        await _lifecycleGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);
            if (Volatile.Read(ref _started) != 0)
            {
                return;
            }

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
        }
        finally
        {
            _lifecycleGate.Release();
        }
    }

    public async Task<IReadOnlyList<RemotingPullMessageQueue>> GetMessageQueuesAsync(
        string topic,
        CancellationToken cancellationToken = default)
    {
        EnsureStarted();
        ArgumentException.ThrowIfNullOrWhiteSpace(topic);
        var logicalTopic = LegacyNamespace.WithoutNamespace(_clientOptions.Namespace, topic);
        var route = await _routes.GetAsync(
            GetWireTopic(logicalTopic),
            cancellationToken: cancellationToken).ConfigureAwait(false);
        var queues = new List<RemotingPullMessageQueue>();
        foreach (var queueData in route.QueueDatas
                     .Where(static queue => (queue.Perm & ReadPermission) == ReadPermission)
                     .OrderBy(static queue => queue.BrokerName, StringComparer.Ordinal))
        {
            var broker = route.BrokerDatas.FirstOrDefault(value => value.BrokerName == queueData.BrokerName);
            if (broker is null || broker.BrokerAddrs.Count == 0)
            {
                continue;
            }

            for (var queueId = 0; queueId < queueData.ReadQueueNums; queueId++)
            {
                queues.Add(new RemotingPullMessageQueue(
                    logicalTopic,
                    queueId,
                    queueData.BrokerName,
                    broker.BrokerAddrs));
            }
        }

        return queues;
    }

    public async Task<RemotingPullResult> PullAsync(
        RemotingPullMessageQueue queue,
        long offset,
        int? maxMessages = null,
        CancellationToken cancellationToken = default)
    {
        EnsureStarted();
        ArgumentNullException.ThrowIfNull(queue);
        if (offset < 0) throw new ArgumentOutOfRangeException(nameof(offset));
        var batchSize = maxMessages ?? _settings.BatchSize;
        if (batchSize <= 0) throw new ArgumentOutOfRangeException(nameof(maxMessages));
        var filter = _subscriptions.TryGetValue(queue.Topic, out var configured)
            ? configured
            : FilterExpression.All;
        var request = new RemotingCommand(RequestCode.PullMessage, new PullMessageRequestHeader
        {
            ConsumerGroup = GetConsumerGroup(),
            Topic = GetWireTopic(queue.Topic),
            QueueId = queue.QueueId,
            QueueOffset = offset,
            MaxMsgNums = batchSize,
            SysFlag = SuspendFlag | SubscriptionFlag,
            CommitOffset = 0,
            SuspendTimeoutMillis = checked((long)_settings.LongPollingTimeout.TotalMilliseconds),
            Subscription = filter.Expression,
            SubVersion = _timeProvider.GetUtcNow().ToUnixTimeMilliseconds(),
            ExpressionType = filter.Type == FilterExpressionType.Sql ? "SQL92" : "TAG",
            MaxMsgBytes = Math.Min(
                _settings.MaxMessageBytes,
                _clientOptions.MaxRemotingFrameSize - RemotingClientOptions.RemotingMessageFrameReserve),
            Bname = queue.BrokerName
        });
        var response = await _remotingClient.InvokeAsync(
            EndpointParser.Parse(queue.GetPullBrokerAddress()),
            request,
            _clientOptions.RequestTimeout + _settings.LongPollingTimeout + BrokerLongPollingCheckInterval,
            cancellationToken).ConfigureAwait(false);

        var suggestedBrokerId = GetInt64(response.ExtFields, "suggestWhichBrokerId", long.MinValue);
        if (suggestedBrokerId != long.MinValue)
        {
            queue.SetPreferredBrokerId(suggestedBrokerId);
        }

        var status = response.Code switch
        {
            ResponseCodes.ResSuccess => RemotingPullStatus.Found,
            ResponseCodes.ResPullNotFound => RemotingPullStatus.NoNewMessage,
            ResponseCodes.ResPullRetryImmediately => RemotingPullStatus.NoMatchedMessage,
            ResponseCodes.ResPullOffsetMoved => RemotingPullStatus.OffsetIllegal,
            _ => throw new RemotingCommandException(
                response.Code,
                response.Remark ?? "Broker rejected the pull request.")
        };
        var messages = status == RemotingPullStatus.Found && response.Body is { Length: > 0 }
            ? LegacyMessageDecoder.Decode(response.Body, queue.BrokerName, _clientOptions.Namespace)
            : Array.Empty<RemotingMessageView>();
        return new RemotingPullResult(
            messages,
            GetInt64(response.ExtFields, "nextBeginOffset", offset),
            status,
            GetInt64(response.ExtFields, "minOffset", 0),
            GetInt64(response.ExtFields, "maxOffset", 0));
    }

    public async Task<long> GetOffsetAsync(
        RemotingPullMessageQueue queue,
        CancellationToken cancellationToken = default)
    {
        EnsureStarted();
        var response = await InvokeOffsetAsync(
            RequestCode.QueryConsumerOffset,
            queue,
            new ConsumerOffsetRequestHeader
            {
                ConsumerGroup = GetConsumerGroup(),
                Topic = GetWireTopic(queue.Topic),
                QueueId = queue.QueueId,
                SetZeroIfNotFound = false,
                Bname = queue.BrokerName
            },
            cancellationToken).ConfigureAwait(false);
        if (response.Code == ResponseCodes.ResQueryNotFound)
        {
            return -1;
        }

        EnsureSuccess(response, "query consumer offset");
        return GetRequiredInt64(response.ExtFields, "offset");
    }

    public async Task UpdateOffsetAsync(
        RemotingPullMessageQueue queue,
        long offset,
        CancellationToken cancellationToken = default)
    {
        EnsureStarted();
        if (offset < 0) throw new ArgumentOutOfRangeException(nameof(offset));
        var response = await InvokeOffsetAsync(
            RequestCode.UpdateConsumerOffset,
            queue,
            new ConsumerOffsetRequestHeader
            {
                ConsumerGroup = GetConsumerGroup(),
                Topic = GetWireTopic(queue.Topic),
                QueueId = queue.QueueId,
                CommitOffset = offset,
                Bname = queue.BrokerName
            },
            cancellationToken).ConfigureAwait(false);
        EnsureSuccess(response, "update consumer offset");
    }

    public async Task<long> QueryOffsetAsync(
        RemotingPullMessageQueue queue,
        QueryOffsetPolicy policy,
        DateTimeOffset? timestamp = null,
        CancellationToken cancellationToken = default)
    {
        EnsureStarted();
        if (policy == QueryOffsetPolicy.Timestamp && timestamp is null)
        {
            throw new ArgumentException("A timestamp is required for timestamp-based offset queries.", nameof(timestamp));
        }

        var requestCode = policy switch
        {
            QueryOffsetPolicy.End => RequestCode.GetMaxOffset,
            QueryOffsetPolicy.Timestamp => RequestCode.SearchOffsetByTimestamp,
            _ => RequestCode.GetMinOffset
        };
        var response = await InvokeOffsetAsync(
            requestCode,
            queue,
            new QueryQueueOffsetRequestHeader
            {
                Topic = GetWireTopic(queue.Topic),
                QueueId = queue.QueueId,
                Timestamp = timestamp?.ToUnixTimeMilliseconds() ?? 0,
                Bname = queue.BrokerName
            },
            cancellationToken).ConfigureAwait(false);
        EnsureSuccess(response, "query queue offset");
        return GetRequiredInt64(response.ExtFields, "offset");
    }

    public async Task SendBackAsync(
        RemotingPullMessageQueue queue,
        RemotingMessageView message,
        int delayLevel,
        int maxReconsumeTimes,
        CancellationToken cancellationToken)
    {
        EnsureStarted();
        var response = await _remotingClient.InvokeAsync(
            GetBrokerEndpoint(queue),
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
    }

    public void SetSubscription(string topic, FilterExpression expression)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(topic);
        ArgumentNullException.ThrowIfNull(expression);
        _subscriptions[topic] = expression;
    }

    public void RemoveSubscription(string topic)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(topic);
        _subscriptions.TryRemove(topic, out _);
    }

    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
        {
            return;
        }

        await _lifecycleGate.WaitAsync().ConfigureAwait(false);
        try
        {
            Volatile.Write(ref _started, 0);
        }
        finally
        {
            _lifecycleGate.Release();
        }
    }

    private Task<RemotingCommand> InvokeOffsetAsync(
        int requestCode,
        RemotingPullMessageQueue queue,
        CommandCustomHeader header,
        CancellationToken cancellationToken) =>
        _remotingClient.InvokeAsync(
            GetBrokerEndpoint(queue),
            new RemotingCommand(requestCode, header),
            _clientOptions.RequestTimeout,
            cancellationToken);

    private static System.Net.EndPoint GetBrokerEndpoint(RemotingPullMessageQueue queue) =>
        EndpointParser.Parse(queue.BrokerAddress);

    private static void EnsureSuccess(RemotingCommand response, string operation)
    {
        if (response.Code != ResponseCodes.ResSuccess)
        {
            throw new RemotingCommandException(
                response.Code,
                response.Remark ?? $"Broker rejected the {operation} request.");
        }
    }

    private static long GetRequiredInt64(IReadOnlyDictionary<string, object> fields, string name)
    {
        var value = GetInt64(fields, name, long.MinValue);
        return value != long.MinValue
            ? value
            : throw new InvalidDataException($"Broker response did not contain a valid '{name}' field.");
    }

    private static long GetInt64(IReadOnlyDictionary<string, object> fields, string name, long fallback) =>
        fields.TryGetValue(name, out var raw) &&
        long.TryParse(Convert.ToString(raw, CultureInfo.InvariantCulture), NumberStyles.Integer,
            CultureInfo.InvariantCulture, out var value)
            ? value
            : fallback;

    private string GetConsumerGroup() => LegacyNamespace.Wrap(_clientOptions.Namespace, _settings.GroupName);

    private string GetWireTopic(string topic) => LegacyNamespace.Wrap(_clientOptions.Namespace, topic);

    private void EnsureStarted()
    {
        ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);
        if (Volatile.Read(ref _started) == 0)
        {
            throw new InvalidOperationException("Pull consumer has not been started.");
        }
    }
}
