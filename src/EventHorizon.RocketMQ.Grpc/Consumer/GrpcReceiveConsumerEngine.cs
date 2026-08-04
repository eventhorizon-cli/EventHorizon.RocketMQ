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
using System.Diagnostics;
using EventHorizon.RocketMQ.Grpc.Exceptions;
using EventHorizon.RocketMQ.Grpc.Instrumentation;
using EventHorizon.RocketMQ.Grpc.Protocol;
using EventHorizon.RocketMQ.Grpc.Protocol.Route;
using EventHorizon.RocketMQ.Grpc.Protocol.Telemetry;
using Google.Protobuf.WellKnownTypes;
using Grpc.Core;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Proto = Apache.Rocketmq.V2;

namespace EventHorizon.RocketMQ.Grpc.Consumer;

internal sealed class GrpcReceiveConsumerEngine : IGrpcReceiveConsumerEngine
{
    private const int CompletionMaxAttempts = 3;
    private static readonly TimeSpan CompletionInitialRetryDelay = TimeSpan.FromMilliseconds(200);
    private static readonly TimeSpan CompletionMaximumRetryDelay = TimeSpan.FromSeconds(1);
    private readonly IRocketMQGrpcClient _client;
    private readonly IGrpcRouteService _routes;
    private readonly GrpcClientOptions _clientOptions;
    private readonly string _groupName;
    private readonly TimeSpan _longPollingTimeout;
    private readonly Proto.ClientType _clientType;
    private readonly IGrpcRocketMQTelemetry _telemetry;
    private readonly ILogger<GrpcReceiveConsumerEngine> _logger;
    private readonly ILogger<GrpcSessionManager> _sessionLogger;
    private readonly Func<Uri, Proto.TelemetryCommand, CancellationToken, Task>? _telemetryHandler;
    private readonly ConcurrentDictionary<string, FilterExpression> _subscriptions;
    private readonly SemaphoreSlim _subscriptionGate = new(1, 1);
    private readonly object _messageOwner = new();
    private readonly SemaphoreSlim _lifecycleGate = new(1, 1);
    private GrpcSessionManager? _sessions;
    private int _started;

    public GrpcReceiveConsumerEngine(
        IRocketMQGrpcClient client,
        IGrpcRouteService routes,
        IOptions<GrpcClientOptions> clientOptions,
        string groupName,
        IReadOnlyDictionary<string, FilterExpression> subscriptions,
        Proto.ClientType clientType,
        TimeSpan longPollingTimeout,
        ILogger<GrpcReceiveConsumerEngine> logger,
        ILogger<GrpcSessionManager> sessionLogger,
        Func<Uri, Proto.TelemetryCommand, CancellationToken, Task>? telemetryHandler = null,
        IGrpcRocketMQTelemetry? telemetry = null)
    {
        _client = client;
        _routes = routes;
        _clientOptions = clientOptions.Value;
        _groupName = groupName;
        _longPollingTimeout = longPollingTimeout;
        _clientType = clientType;
        _telemetry = telemetry ?? GrpcRocketMQTelemetry.Disabled;
        _logger = logger;
        _sessionLogger = sessionLogger;
        _telemetryHandler = telemetryHandler;
        _subscriptions = new ConcurrentDictionary<string, FilterExpression>(
            subscriptions,
            StringComparer.Ordinal);
    }

    public async ValueTask StartAsync(CancellationToken cancellationToken)
    {
        await _lifecycleGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (Volatile.Read(ref _started) != 0)
            {
                return;
            }

            var sessions = new GrpcSessionManager(
                _client,
                Options.Create(_clientOptions),
                _clientType,
                _groupName,
                _sessionLogger,
                _telemetryHandler);
            sessions.Start();
            Volatile.Write(ref _sessions, sessions);
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
            var sessions = Interlocked.Exchange(ref _sessions, null);
            if (sessions is not null)
            {
                await sessions.DisposeAsync().ConfigureAwait(false);
            }
        }
        finally
        {
            _lifecycleGate.Release();
        }
    }

    public async Task<IReadOnlyList<Proto.Assignment>> GetAssignmentsAsync(string topic, CancellationToken cancellationToken)
    {
        var sessions = GetSessions();
        var route = await _routes.GetAsync(topic, false, cancellationToken).ConfigureAwait(false);
        var endpoints = route.Select(queue => GrpcEndpoint.FromProtobuf(queue.Broker.Endpoints, _clientOptions.UseTLS)).ToArray();
        await sessions.EnsureAsync(endpoints, BuildSettings(), cancellationToken).ConfigureAwait(false);
        var response = await _client.QueryAssignmentAsync(_client.AccessPoint, new Proto.QueryAssignmentRequest
        {
            Topic = Resource(topic),
            Group = Resource(_groupName),
            Endpoints = _client.AccessPointProtobuf
        }, cancellationToken).ConfigureAwait(false);
        GrpcStatus.EnsureSuccess(response.Status);
        return response.Assignments.Select(static assignment => assignment.Clone()).ToArray();
    }

    public KeyValuePair<string, FilterExpression>[] GetSubscriptions() =>
        _subscriptions
            .OrderBy(static subscription => subscription.Key, StringComparer.Ordinal)
            .ToArray();

    public async Task SubscribeAsync(
        string topic,
        FilterExpression? expression,
        CancellationToken cancellationToken)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(topic);
        await _subscriptionGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            _subscriptions[topic] = expression ?? FilterExpression.All;
            await SyncSubscriptionSettingsAsync(cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            _subscriptionGate.Release();
        }
    }

    public async Task UnsubscribeAsync(string topic, CancellationToken cancellationToken)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(topic);
        await _subscriptionGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (_subscriptions.TryRemove(topic, out _))
            {
                await SyncSubscriptionSettingsAsync(cancellationToken).ConfigureAwait(false);
            }
        }
        finally
        {
            _subscriptionGate.Release();
        }
    }

    public async Task<IReadOnlyList<GrpcMessageView>> ReceiveAsync(
        Proto.MessageQueue queue,
        FilterExpression filter,
        int maxMessages,
        TimeSpan invisibleDuration,
        TimeSpan longPollingTimeout,
        bool autoRenew,
        CancellationToken cancellationToken)
    {
        EnsureStarted();
        var parentContext = Activity.Current?.Context;
        var startTime = DateTimeOffset.UtcNow;
        var startTimestamp = Stopwatch.GetTimestamp();
        try
        {
            var endpoint = GrpcEndpoint.FromProtobuf(queue.Broker.Endpoints, _clientOptions.UseTLS);
            await GetSessions().EnsureAsync([endpoint], BuildSettings(), cancellationToken).ConfigureAwait(false);
            var serverSettings = GetServerSettings(endpoint);
            if (IsPushConsumer())
            {
                var subscription = serverSettings?.Subscription;
                if (subscription is { HasReceiveBatchSize: true, ReceiveBatchSize: > 0 })
                {
                    maxMessages = Math.Min(maxMessages, subscription.ReceiveBatchSize);
                }

                if (TryGetPositiveDuration(subscription?.LongPollingTimeout, out var brokerLongPollingTimeout))
                {
                    longPollingTimeout = brokerLongPollingTimeout;
                }
            }

            // The server may advertise its own internal request budget. The RPC deadline remains
            // the client-configured request timeout plus the long-poll duration, matching Apache clients.
            var timeout = _clientOptions.RequestTimeout + longPollingTimeout;
            var request = new Proto.ReceiveMessageRequest
            {
                Group = Resource(_groupName),
                MessageQueue = queue,
                FilterExpression = ToProtobuf(filter),
                BatchSize = maxMessages,
                InvisibleDuration = Duration.FromTimeSpan(invisibleDuration),
                AutoRenew = autoRenew
            };
            if (IsPushConsumer())
            {
                request.LongPollingTimeout = Duration.FromTimeSpan(longPollingTimeout);
                request.AttemptId = Guid.NewGuid().ToString();
            }

            var responses = await _client.ReceiveMessageAsync(
                endpoint,
                request,
                timeout,
                cancellationToken).ConfigureAwait(false);

            var messages = new List<GrpcMessageView>();
            Proto.Status? status = null;
            foreach (var response in responses)
            {
                switch (response.ContentCase)
                {
                    case Proto.ReceiveMessageResponse.ContentOneofCase.Status:
                        status = response.Status;
                        break;
                    case Proto.ReceiveMessageResponse.ContentOneofCase.Message:
                        var message = GrpcMessageViewFactory.Create(response.Message, queue, endpoint);
                        BindMessage(message);
                        messages.Add(message);
                        break;
                }
            }

            GrpcStatus.EnsureReceiveSuccess(status);
            using var telemetry = _telemetry.StartReceive(
                queue.Topic.Name,
                _groupName,
                messages.Count,
                parentContext,
                startTime,
                startTimestamp,
                queue.Id,
                createActivity: messages.Count > 0,
                messageProperties: messages.Select(static message => message.Properties));
            if (telemetry.Activity is { } activity)
            {
                foreach (var message in messages)
                {
                    message.ReceiveActivityContext = activity.Context;
                }
            }

            telemetry.Complete();
            return messages;
        }
        catch (Exception exception)
        {
            using var telemetry = _telemetry.StartReceive(
                queue.Topic.Name,
                _groupName,
                0,
                parentContext,
                startTime,
                startTimestamp,
                queue.Id);
            telemetry.Complete(exception);
            throw;
        }
    }

    public Task AckAsync(GrpcMessageView message, CancellationToken cancellationToken)
    {
        var endpoint = ValidateCompletionMessage(message);
        return ExecuteCompletionAsync("acknowledge", "ack", message, async token =>
        {
            var entry = new Proto.AckMessageEntry
            {
                MessageId = message.MessageId,
                ReceiptHandle = message.ReceiptHandle
            };
            if (!string.IsNullOrEmpty(message.LiteTopic))
            {
                entry.LiteTopic = message.LiteTopic;
            }

            var response = await _client.AckMessageAsync(endpoint, new Proto.AckMessageRequest
            {
                Group = Resource(_groupName),
                Topic = Resource(message.Topic),
                Entries = { entry }
            }, token).ConfigureAwait(false);
            GrpcStatus.EnsureSuccess(response.Status);
            foreach (var result in response.Entries)
            {
                GrpcStatus.EnsureSuccess(result.Status);
            }
        }, cancellationToken);
    }

    public Task ChangeInvisibleDurationAsync(
        GrpcMessageView message,
        TimeSpan invisibleDuration,
        CancellationToken cancellationToken) =>
        SetInvisibleDurationAsync(message, invisibleDuration, "renew", cancellationToken);

    public Task ScheduleRetryAsync(
        GrpcMessageView message,
        TimeSpan invisibleDuration,
        CancellationToken cancellationToken) =>
        SetInvisibleDurationAsync(message, invisibleDuration, "nack", cancellationToken);

    private Task SetInvisibleDurationAsync(
        GrpcMessageView message,
        TimeSpan invisibleDuration,
        string telemetryOperation,
        CancellationToken cancellationToken)
    {
        var endpoint = ValidateCompletionMessage(message);
        if (invisibleDuration <= TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(nameof(invisibleDuration), "Invisible duration must be positive.");
        }

        return ExecuteCompletionAsync("change invisible duration for", telemetryOperation, message, async token =>
        {
            var request = new Proto.ChangeInvisibleDurationRequest
            {
                Group = Resource(_groupName),
                Topic = Resource(message.Topic),
                ReceiptHandle = message.ReceiptHandle,
                MessageId = message.MessageId,
                InvisibleDuration = Duration.FromTimeSpan(invisibleDuration)
            };
            if (!string.IsNullOrEmpty(message.LiteTopic))
            {
                request.LiteTopic = message.LiteTopic;
            }

            var response = await _client.ChangeInvisibleDurationAsync(
                endpoint,
                request,
                token).ConfigureAwait(false);
            GrpcStatus.EnsureSuccess(response.Status);
            if (!string.IsNullOrEmpty(response.ReceiptHandle))
            {
                message.ReceiptHandle = response.ReceiptHandle;
            }

            message.InvisibleDuration = invisibleDuration;
        }, cancellationToken);
    }

    public Task ForwardToDeadLetterQueueAsync(GrpcMessageView message, int maxDeliveryAttempts, CancellationToken cancellationToken)
    {
        var endpoint = ValidateCompletionMessage(message);
        if (maxDeliveryAttempts <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(maxDeliveryAttempts), "Maximum delivery attempts must be positive.");
        }

        return ExecuteCompletionAsync("forward to the dead-letter queue", "reject", message, async token =>
        {
            var request = new Proto.ForwardMessageToDeadLetterQueueRequest
            {
                Group = Resource(_groupName),
                Topic = Resource(message.Topic),
                ReceiptHandle = message.ReceiptHandle,
                MessageId = message.MessageId,
                DeliveryAttempt = message.DeliveryAttempt,
                MaxDeliveryAttempts = maxDeliveryAttempts
            };
            if (!string.IsNullOrEmpty(message.LiteTopic))
            {
                request.LiteTopic = message.LiteTopic;
            }

            var response = await _client.ForwardToDeadLetterQueueAsync(
                endpoint,
                request,
                token).ConfigureAwait(false);
            GrpcStatus.EnsureSuccess(response.Status);
        }, cancellationToken);
    }

    public int GetMaxDeliveryAttempts(GrpcMessageView message, int fallback)
    {
        var policy = GetServerSettings(message.Endpoint)?.BackoffPolicy;
        return policy is { MaxAttempts: > 0 } ? policy.MaxAttempts : fallback;
    }

    public TimeSpan GetRetryDelay(GrpcMessageView message, TimeSpan fallback) =>
        GetRetryDelay(message, message.DeliveryAttempt, fallback);

    public TimeSpan GetRetryDelay(GrpcMessageView message, int deliveryAttempt, TimeSpan fallback)
    {
        var policy = GetServerSettings(message.Endpoint)?.BackoffPolicy;
        if (policy is null)
        {
            return fallback;
        }

        var attempt = Math.Max(1, deliveryAttempt);
        switch (policy.StrategyCase)
        {
            case Proto.RetryPolicy.StrategyOneofCase.CustomizedBackoff:
                var next = policy.CustomizedBackoff.Next;
                if (next.Count > 0 &&
                    TryGetPositiveDuration(next[Math.Min(attempt, next.Count) - 1], out var customizedDelay))
                {
                    return customizedDelay;
                }

                break;
            case Proto.RetryPolicy.StrategyOneofCase.ExponentialBackoff:
                var backoff = policy.ExponentialBackoff;
                if (TryGetPositiveDuration(backoff.Initial, out var initial) &&
                    TryGetPositiveDuration(backoff.Max, out var maximum) &&
                    backoff.Multiplier > 0)
                {
                    var scaledTicks = initial.Ticks * Math.Pow(backoff.Multiplier, attempt - 1);
                    if (!double.IsNaN(scaledTicks) && scaledTicks > 0)
                    {
                        return TimeSpan.FromTicks((long)Math.Min(scaledTicks, maximum.Ticks));
                    }
                }

                break;
        }

        return fallback;
    }

    public async ValueTask DisposeAsync() => await StopAsync(CancellationToken.None).ConfigureAwait(false);

    internal void BindMessage(GrpcMessageView message)
    {
        ArgumentNullException.ThrowIfNull(message);
        message.Owner = _messageOwner;
    }

    private Proto.Settings BuildSettings()
    {
        var settings = new Proto.Settings
        {
            ClientType = _clientType,
            AccessPoint = _client.AccessPointProtobuf,
            RequestTimeout = Duration.FromTimeSpan(_clientOptions.RequestTimeout),
            UserAgent = new Proto.UA
            {
                Language = Proto.Language.DotNet,
                Version = typeof(GrpcReceiveConsumerEngine).Assembly.GetName().Version?.ToString() ?? "unknown",
                Platform = System.Runtime.InteropServices.RuntimeInformation.OSDescription,
                Hostname = Environment.MachineName
            },
            Subscription = new Proto.Subscription { Group = Resource(_groupName) }
        };
        settings.Subscription.Subscriptions.AddRange(GetSubscriptions().Select(subscription => new Proto.SubscriptionEntry
        {
            Topic = Resource(subscription.Key),
            Expression = ToProtobuf(subscription.Value)
        }));
        if (_longPollingTimeout > TimeSpan.Zero)
        {
            settings.Subscription.LongPollingTimeout = Duration.FromTimeSpan(_longPollingTimeout);
        }

        return settings;
    }

    private Task SyncSubscriptionSettingsAsync(CancellationToken cancellationToken)
    {
        var sessions = Volatile.Read(ref _sessions);
        return sessions is null || Volatile.Read(ref _started) == 0
            ? Task.CompletedTask
            : sessions.SyncSettingsAsync(BuildSettings(), cancellationToken);
    }

    private async Task ExecuteCompletionAsync(
        string operation,
        string telemetryOperation,
        GrpcMessageView message,
        Func<CancellationToken, Task> action,
        CancellationToken cancellationToken)
    {
        using var telemetry = _telemetry.StartSettle(
            telemetryOperation,
            message.Topic,
            _groupName,
            message.MessageId,
            message.QueueId,
            message.Properties);
        try
        {
            for (var attempt = 1; ; attempt++)
            {
                try
                {
                    await action(cancellationToken).ConfigureAwait(false);
                    telemetry.Complete();
                    return;
                }
                catch (Exception exception) when (
                    attempt < CompletionMaxAttempts &&
                    IsTransientCompletionFailure(exception, cancellationToken))
                {
                    var delay = TimeSpan.FromMilliseconds(Math.Min(
                        CompletionInitialRetryDelay.TotalMilliseconds * Math.Pow(2, attempt - 1),
                        CompletionMaximumRetryDelay.TotalMilliseconds));
                    _logger.LogWarning(
                        exception,
                        "Transient failure while attempting to {Operation} message {MessageId}; retrying attempt {NextAttempt} of {MaxAttempts} in {Delay}",
                        operation,
                        message.MessageId,
                        attempt + 1,
                        CompletionMaxAttempts,
                        delay);
                    await Task.Delay(delay, cancellationToken).ConfigureAwait(false);
                }
            }
        }
        catch (Exception exception)
        {
            telemetry.Complete(exception);
            throw;
        }
    }

    private Proto.Settings? GetServerSettings(Uri endpoint)
    {
        var sessions = Volatile.Read(ref _sessions);
        if (sessions is null)
        {
            return null;
        }

        try
        {
            return sessions.GetServerSettings(endpoint);
        }
        catch (ObjectDisposedException)
        {
            return null;
        }
    }

    private GrpcSessionManager GetSessions()
    {
        EnsureStarted();
        return Volatile.Read(ref _sessions)
            ?? throw new InvalidOperationException("Consumer session is not available.");
    }

    private Uri ValidateCompletionMessage(GrpcMessageView message)
    {
        ArgumentNullException.ThrowIfNull(message);
        if (!ReferenceEquals(message.Owner, _messageOwner) || string.IsNullOrWhiteSpace(message.ReceiptHandle))
        {
            throw new ArgumentException(
                "The message receipt was not issued to this consumer.",
                nameof(message));
        }

        return message.Endpoint;
    }

    private static bool TryGetPositiveDuration(Duration? value, out TimeSpan duration)
    {
        duration = default;
        if (value is null)
        {
            return false;
        }

        try
        {
            duration = value.ToTimeSpan();
            return duration > TimeSpan.Zero;
        }
        catch (InvalidOperationException)
        {
            return false;
        }
        catch (OverflowException)
        {
            return false;
        }
    }

    private static bool IsTransientCompletionFailure(Exception exception, CancellationToken cancellationToken) =>
        exception switch
        {
            OperationCanceledException => !cancellationToken.IsCancellationRequested,
            RpcException rpc => rpc.StatusCode is
                StatusCode.Unavailable or
                StatusCode.DeadlineExceeded or
                StatusCode.ResourceExhausted or
                StatusCode.Aborted or
                StatusCode.Internal,
            GrpcServiceException service => service.ResponseCode is
                (int)Proto.Code.RequestTimeout or
                (int)Proto.Code.TooManyRequests or
                (int)Proto.Code.InternalError or
                (int)Proto.Code.InternalServerError or
                (int)Proto.Code.HaNotAvailable or
                (int)Proto.Code.ProxyTimeout or
                (int)Proto.Code.MasterPersistenceTimeout or
                (int)Proto.Code.SlavePersistenceTimeout,
            IOException or HttpRequestException or TimeoutException => true,
            _ => false
        };

    private bool IsPushConsumer() => _clientType is Proto.ClientType.PushConsumer or Proto.ClientType.LitePushConsumer;

    private Proto.Resource Resource(string name) => new()
    {
        ResourceNamespace = _clientOptions.Namespace ?? string.Empty,
        Name = name
    };

    private static Proto.FilterExpression ToProtobuf(FilterExpression filter) => new()
    {
        Expression = filter.Expression,
        Type = filter.Type == FilterExpressionType.Sql ? Proto.FilterType.Sql : Proto.FilterType.Tag
    };

    private void EnsureStarted()
    {
        if (Volatile.Read(ref _started) == 0)
        {
            throw new InvalidOperationException("Consumer has not been started.");
        }
    }
}
