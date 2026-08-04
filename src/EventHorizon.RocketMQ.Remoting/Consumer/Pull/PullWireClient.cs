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
using EventHorizon.RocketMQ.Remoting.Consumer.Route;
using EventHorizon.RocketMQ.Remoting.Exceptions;
using EventHorizon.RocketMQ.Remoting.Instrumentation;
using EventHorizon.RocketMQ.Remoting.Protocol;

namespace EventHorizon.RocketMQ.Remoting.Consumer.Pull;

internal sealed class PullWireClient : IRemotingPullWireClient
{
    private const int SuspendFlag = 1 << 1;
    private const int SubscriptionFlag = 1 << 2;

    // Broker checks expired long-poll requests on a five-second cadence.
    private static readonly TimeSpan BrokerLongPollingCheckInterval = TimeSpan.FromSeconds(5);

    private readonly RemotingConsumerSettings _settings;
    private readonly RemotingClientOptions _clientOptions;
    private readonly RemotingConsumerRouteResolver _routes;
    private readonly IRemotingClient _remotingClient;
    private readonly TimeProvider _timeProvider;
    private readonly IRemotingRocketMQTelemetry _telemetry;

    public PullWireClient(
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

    public async Task<RemotingPullResult> PullAsync(
        RemotingConsumerQueue queue,
        long offset,
        FilterExpression filter,
        int? maxMessages = null,
        int? maxBytes = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(queue);
        ArgumentNullException.ThrowIfNull(filter);
        ArgumentOutOfRangeException.ThrowIfNegative(offset);
        var batchSize = maxMessages ?? _settings.BatchSize;
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(batchSize, nameof(maxMessages));
        var responseBytes = maxBytes ?? _settings.MaxMessageBytes;
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(responseBytes, nameof(maxBytes));

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
                responseBytes,
                _clientOptions.MaxRemotingFrameSize - RemotingClientOptions.RemotingMessageFrameReserve),
            Bname = queue.BrokerName
        });
        var parentContext = Activity.Current?.Context;
        var startTime = DateTimeOffset.UtcNow;
        var startTimestamp = Stopwatch.GetTimestamp();
        try
        {
            var broker = await _routes.ResolveBrokerAsync(
                queue,
                useSuggestedBroker: true,
                cancellationToken).ConfigureAwait(false);
            var response = await _remotingClient.InvokeAsync(
                EndpointParser.Parse(broker.Address),
                request,
                _clientOptions.RequestTimeout + _settings.LongPollingTimeout + BrokerLongPollingCheckInterval,
                cancellationToken).ConfigureAwait(false);

            var suggestedBrokerId = GetInt64(response.ExtFields, "suggestWhichBrokerId", long.MinValue);
            if (suggestedBrokerId != long.MinValue)
            {
                _routes.RecordSuggestedBrokerId(queue, suggestedBrokerId);
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
            var result = new RemotingPullResult(
                messages,
                GetInt64(response.ExtFields, "nextBeginOffset", offset),
                status,
                GetInt64(response.ExtFields, "minOffset", 0),
                GetInt64(response.ExtFields, "maxOffset", 0));
            using var telemetry = _telemetry.StartReceive(
                queue.Topic,
                _settings.GroupName,
                result.Messages.Count,
                parentContext,
                startTime,
                startTimestamp,
                queue.QueueId,
                createActivity: result.Messages.Count > 0,
                messageProperties: result.Messages.Select(static message => message.Properties));
            if (telemetry.Activity is { } activity)
            {
                foreach (var message in result.Messages)
                {
                    message.ReceiveActivityContext = activity.Context;
                }
            }

            telemetry.Complete();
            return result;
        }
        catch (Exception exception)
        {
            using var telemetry = _telemetry.StartReceive(
                queue.Topic,
                _settings.GroupName,
                0,
                parentContext,
                startTime,
                startTimestamp,
                queue.QueueId);
            telemetry.Complete(exception);
            throw;
        }
    }

    private static long GetInt64(IReadOnlyDictionary<string, object> fields, string name, long fallback) =>
        fields.TryGetValue(name, out var raw) &&
        long.TryParse(
            Convert.ToString(raw, CultureInfo.InvariantCulture),
            NumberStyles.Integer,
            CultureInfo.InvariantCulture,
            out var value)
            ? value
            : fallback;

    private string GetConsumerGroup() => LegacyNamespace.Wrap(_clientOptions.Namespace, _settings.GroupName);

    private string GetWireTopic(string topic) => LegacyNamespace.Wrap(_clientOptions.Namespace, topic);
}
