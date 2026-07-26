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

using System.Buffers.Binary;
using System.Numerics;
using System.Text;
using System.Threading.Channels;
using EventHorizon.RocketMQ.Grpc.Exceptions;
using EventHorizon.RocketMQ.Grpc.Producer;
using EventHorizon.RocketMQ.Grpc.Producer.Transactions;
using EventHorizon.RocketMQ.Grpc.Protocol;
using EventHorizon.RocketMQ.Grpc.Protocol.Route;
using EventHorizon.RocketMQ.Grpc.Protocol.Telemetry;
using Google.Protobuf;
using Google.Protobuf.WellKnownTypes;
using Grpc.Core;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Proto = Apache.Rocketmq.V2;

namespace EventHorizon.RocketMQ.Grpc.Producer;

internal sealed class GrpcProducer : IGrpcProducer
{
    private readonly GrpcProducerOptions _options;
    private readonly GrpcClientOptions _clientOptions;
    private readonly IRocketMQGrpcClient _client;
    private readonly IGrpcRouteService _routes;
    private readonly ILoggerFactory _loggerFactory;
    private GrpcSessionManager? _sessions;
    private TransactionCheckDispatcher? _transactionChecks;
    private readonly ILogger _logger;
    private readonly HashSet<string> _declaredTopics;
    private readonly HashSet<string> _topics;
    private readonly object _topicsGate = new();
    private readonly SemaphoreSlim _lifecycleGate = new(1, 1);
    private int _started;
    private int _disposed;
    private int _queueIndex;

    public GrpcProducer(
        IOptions<GrpcProducerOptions> options,
        IOptions<GrpcClientOptions> clientOptions,
        IRocketMQGrpcClient client,
        IGrpcRouteService routes,
        ILoggerFactory? loggerFactory = null)
    {
        _options = options.Value;
        _clientOptions = clientOptions.Value;
        _client = client;
        _routes = routes;
        _loggerFactory = loggerFactory ?? NullLoggerFactory.Instance;
        _logger = _loggerFactory.CreateLogger<GrpcProducer>();
        if (_options.MaxConcurrentTransactionChecks <= 0)
        {
            throw new ArgumentOutOfRangeException(
                nameof(options),
                _options.MaxConcurrentTransactionChecks,
                "Maximum concurrent transaction checks must be positive.");
        }

        _declaredTopics = new HashSet<string>(_options.Topics, StringComparer.Ordinal);
        _topics = new HashSet<string>(_declaredTopics, StringComparer.Ordinal);
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

            var sessions = CreateSessionManager();
            var transactionChecks = new TransactionCheckDispatcher(
                _options.MaxConcurrentTransactionChecks,
                ProcessTransactionCheckAsync);
            _sessions = sessions;
            _transactionChecks = transactionChecks;
            sessions.Start();
            try
            {
                if (_declaredTopics.Count > 0)
                {
                    var endpoints = new HashSet<Uri>();
                    foreach (var topic in _declaredTopics)
                    {
                        var queues = await _routes.GetAsync(topic, false, cancellationToken).ConfigureAwait(false);
                        foreach (var queue in queues.Where(static queue =>
                                     queue.Permission is Proto.Permission.Write or Proto.Permission.ReadWrite))
                        {
                            endpoints.Add(GrpcEndpoint.FromProtobuf(
                                queue.Broker.Endpoints,
                                _clientOptions.UseTLS));
                        }
                    }

                    if (endpoints.Count > 0)
                    {
                        await sessions.EnsureAsync(endpoints, BuildSettings(), cancellationToken)
                            .ConfigureAwait(false);
                    }
                }

                Volatile.Write(ref _started, 1);
            }
            catch
            {
                _sessions = null;
                _transactionChecks = null;

                try
                {
                    await sessions.DisposeAsync().ConfigureAwait(false);
                }
                catch (Exception exception)
                {
                    _logger.LogDebug(exception, "Failed to clean up RocketMQ sessions after producer startup failed");
                }

                try
                {
                    await transactionChecks.DisposeAsync().ConfigureAwait(false);
                }
                catch (Exception exception)
                {
                    _logger.LogDebug(exception, "Failed to stop transaction check workers after producer startup failed");
                }

                throw;
            }
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
            var sessions = _sessions;
            var transactionChecks = _transactionChecks;
            _sessions = null;
            _transactionChecks = null;
            try
            {
                if (sessions is not null)
                {
                    await sessions.DisposeAsync().ConfigureAwait(false);
                }
            }
            finally
            {
                if (transactionChecks is not null)
                {
                    await transactionChecks.DisposeAsync().ConfigureAwait(false);
                }
            }
        }
        finally
        {
            _lifecycleGate.Release();
        }
    }

    public async Task<GrpcSendReceipt> SendAsync(Message message, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(message);
        EnsureStarted();
        return (await SendCoreAsync(message, false, cancellationToken).ConfigureAwait(false)).Receipt;
    }

    public async Task<IRocketMQTransaction> SendTransactionAsync(
        Message message,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(message);
        EnsureStarted();
        if (_options.TransactionChecker is null)
        {
            throw new InvalidOperationException(
                "A transaction checker must be configured before sending transactional messages.");
        }

        if (!_declaredTopics.Contains(message.Topic))
        {
            throw new InvalidOperationException(
                $"Transactional topic '{message.Topic}' must be declared in " +
                $"{nameof(GrpcProducerOptions)}.{nameof(GrpcProducerOptions.Topics)} before startup.");
        }

        var outcome = await SendCoreAsync(message, true, cancellationToken).ConfigureAwait(false);
        var transactionId = outcome.Receipt.TransactionId;
        if (string.IsNullOrWhiteSpace(transactionId))
        {
            throw new InvalidDataException("RocketMQ returned no transaction identifier for a transactional message.");
        }

        var systemProperties = outcome.Message.SystemProperties;
        return new RocketMQTransaction(
            this,
            new TransactionReceipt(
                outcome.Endpoint,
                outcome.Message.Topic.Clone(),
                outcome.Receipt.MessageId,
                transactionId,
                systemProperties.HasTraceContext ? systemProperties.TraceContext : null),
            outcome.Receipt);
    }

    private async Task<SendOutcome> SendCoreAsync(
        Message message,
        bool transactional,
        CancellationToken cancellationToken)
    {
        Validate(message, transactional);
        var sessions = GetSessions();
        var isNewTopic = AddTopic(message.Topic);
        var outboundMessage = ToProtobuf(message, Guid.NewGuid().ToString("N"), transactional);
        Exception? lastException = null;
        var attempts = Math.Max(0, _options.RetryTimesWhenSendFailed) + 1;
        Proto.RetryPolicy? retryPolicy = null;
        var failedBrokerNames = new HashSet<string>(StringComparer.Ordinal);

        for (var attempt = 0; attempt < attempts; attempt++)
        {
            cancellationToken.ThrowIfCancellationRequested();
            string? attemptedBrokerName = null;
            try
            {
                var queues = await _routes.GetAsync(message.Topic, attempt > 0, cancellationToken).ConfigureAwait(false);
                var candidates = queues.Where(static queue =>
                        queue.Permission is Proto.Permission.Write or Proto.Permission.ReadWrite)
                    .ToArray();
                if (candidates.Length == 0)
                {
                    throw new RocketMQClientException(
                        $"No writable message queue is available for topic '{message.Topic}'.");
                }

                var messageType = outboundMessage.SystemProperties.MessageType;
                var queue = SelectQueue(message, candidates, failedBrokerNames);
                attemptedBrokerName = queue.Broker.Name;
                var endpoint = GrpcEndpoint.FromProtobuf(queue.Broker.Endpoints, _clientOptions.UseTLS);
                var settings = BuildSettings();
                await sessions.EnsureAsync(new[] { endpoint }, settings, cancellationToken).ConfigureAwait(false);
                var serverSettings = sessions.GetServerSettings(endpoint);
                ApplyPublishingLimits(message, serverSettings);
                if (serverSettings?.BackoffPolicy is { } serverRetryPolicy)
                {
                    retryPolicy = serverRetryPolicy;
                    if (serverRetryPolicy.MaxAttempts > 0)
                    {
                        attempts = serverRetryPolicy.MaxAttempts;
                    }
                }

                if (ShouldValidateMessageType(serverSettings) && !AcceptsMessageType(queue, messageType))
                {
                    var compatibleCandidates = candidates
                        .Where(candidate => AcceptsMessageType(candidate, messageType))
                        .ToArray();
                    if (compatibleCandidates.Length == 0)
                    {
                        throw new RocketMQClientException(
                            $"Topic '{message.Topic}' does not accept {messageType} messages.");
                    }

                    queue = SelectQueue(message, compatibleCandidates, failedBrokerNames);
                    attemptedBrokerName = queue.Broker.Name;
                    endpoint = GrpcEndpoint.FromProtobuf(queue.Broker.Endpoints, _clientOptions.UseTLS);
                    await sessions.EnsureAsync(new[] { endpoint }, settings, cancellationToken).ConfigureAwait(false);
                    serverSettings = sessions.GetServerSettings(endpoint);
                    ApplyPublishingLimits(message, serverSettings);
                    if (serverSettings?.BackoffPolicy is { } compatibleRetryPolicy)
                    {
                        retryPolicy = compatibleRetryPolicy;
                        if (compatibleRetryPolicy.MaxAttempts > 0)
                        {
                            attempts = compatibleRetryPolicy.MaxAttempts;
                        }
                    }
                }

                if (isNewTopic)
                {
                    await sessions.SyncSettingsAsync(settings, cancellationToken).ConfigureAwait(false);
                    isNewTopic = false;
                }

                var attemptMessage = outboundMessage.Clone();
                attemptMessage.SystemProperties.QueueId = queue.Id;
                var response = await _client.SendMessageAsync(endpoint, new Proto.SendMessageRequest
                {
                    Messages = { attemptMessage }
                }, _options.SendMsgTimeout, cancellationToken).ConfigureAwait(false);
                GrpcStatus.EnsureSuccess(response.Status);
                if (response.Entries.Count != 1)
                {
                    throw new InvalidDataException(
                        $"RocketMQ returned {response.Entries.Count} send result entries for a single message.");
                }

                var entry = response.Entries[0];
                GrpcStatus.EnsureSuccess(entry.Status);
                if (string.IsNullOrWhiteSpace(entry.MessageId))
                {
                    throw new InvalidDataException("RocketMQ returned no message identifier for a sent message.");
                }

                var receipt = new GrpcSendReceipt(
                    entry.MessageId,
                    entry.Offset,
                    string.IsNullOrEmpty(entry.TransactionId) ? null : entry.TransactionId,
                    string.IsNullOrEmpty(entry.RecallHandle) ? null : entry.RecallHandle,
                    GrpcEndpoint.FromProtobufAll(queue.Broker.Endpoints, _clientOptions.UseTLS));
                return new SendOutcome(receipt, endpoint, attemptMessage);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception exception) when (!IsTransientSendFailure(exception))
            {
                throw;
            }
            catch (Exception exception)
            {
                lastException = exception;
                if (!string.IsNullOrWhiteSpace(attemptedBrokerName))
                {
                    failedBrokerNames.Add(attemptedBrokerName);
                }

                if (attempt + 1 >= attempts)
                {
                    break;
                }

                var delay = GetRetryDelay(retryPolicy, attempt + 1);
                _logger.LogWarning(
                    exception,
                    "RocketMQ gRPC send failed; retrying attempt {Attempt} of {Attempts} in {Delay}",
                    attempt + 2,
                    attempts,
                    delay);
                if (delay > TimeSpan.Zero)
                {
                    await Task.Delay(delay, cancellationToken).ConfigureAwait(false);
                }
            }
        }

        throw new RocketMQClientException(
            $"Failed to send message for topic '{message.Topic}' after {attempts} attempts.",
            lastException);
    }

    internal async Task EndTransactionAsync(
        TransactionReceipt receipt,
        TransactionResolution resolution,
        Proto.TransactionSource source,
        CancellationToken cancellationToken)
    {
        EnsureStarted();
        var protobufResolution = resolution switch
        {
            TransactionResolution.Commit => Proto.TransactionResolution.Commit,
            TransactionResolution.Rollback => Proto.TransactionResolution.Rollback,
            _ => throw new ArgumentOutOfRangeException(nameof(resolution), resolution, "A transaction must be committed or rolled back.")
        };
        var response = await _client.EndTransactionAsync(receipt.Endpoint, new Proto.EndTransactionRequest
        {
            Topic = receipt.Topic.Clone(),
            MessageId = receipt.MessageId,
            TransactionId = receipt.TransactionId,
            Resolution = protobufResolution,
            Source = source,
            TraceContext = receipt.TraceContext ?? string.Empty
        }, cancellationToken).ConfigureAwait(false);
        GrpcStatus.EnsureSuccess(response.Status);
    }

    public async Task<string> RecallAsync(string topic, string recallHandle, CancellationToken cancellationToken = default)
    {
        EnsureStarted();
        ArgumentException.ThrowIfNullOrWhiteSpace(topic);
        ArgumentException.ThrowIfNullOrWhiteSpace(recallHandle);
        var sessions = GetSessions();
        var isNewTopic = AddTopic(topic);
        var queues = await _routes.GetAsync(topic, false, cancellationToken).ConfigureAwait(false);
        var endpoint = GrpcEndpoint.FromProtobuf(queues.First().Broker.Endpoints, _clientOptions.UseTLS);
        var settings = BuildSettings();
        await sessions.EnsureAsync(new[] { endpoint }, settings, cancellationToken).ConfigureAwait(false);
        if (isNewTopic)
        {
            await sessions.SyncSettingsAsync(settings, cancellationToken).ConfigureAwait(false);
        }

        var response = await _client.RecallMessageAsync(endpoint, new Proto.RecallMessageRequest
        {
            Topic = Resource(topic),
            RecallHandle = recallHandle
        }, cancellationToken).ConfigureAwait(false);
        GrpcStatus.EnsureSuccess(response.Status);
        return response.MessageId;
    }

    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
        {
            return;
        }

        await StopAsync().ConfigureAwait(false);
    }

    private Proto.MessageQueue SelectQueue(
        Message message,
        Proto.MessageQueue[] queues,
        IReadOnlySet<string> excludedBrokerNames)
    {
        if (!string.IsNullOrEmpty(message.MessageGroup))
        {
            return queues[GetFifoQueueIndex(message.MessageGroup, queues.Length)];
        }

        var candidates = queues
            .Where(queue => !excludedBrokerNames.Contains(queue.Broker.Name))
            .ToArray();
        if (candidates.Length == 0)
        {
            candidates = queues;
        }

        var index = Interlocked.Increment(ref _queueIndex) & int.MaxValue;
        return candidates[index % candidates.Length];
    }

    internal static int GetFifoQueueIndex(string messageGroup, int queueCount)
    {
        ArgumentNullException.ThrowIfNull(messageGroup);
        if (queueCount <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(queueCount), queueCount, "Queue count must be positive.");
        }

        var hash = unchecked((long)ComputeFifoHash(messageGroup));
        var index = hash % queueCount;
        return (int)(index < 0 ? index + queueCount : index);
    }

    internal static ulong ComputeFifoHash(string messageGroup)
    {
        ArgumentNullException.ThrowIfNull(messageGroup);
        return SipHash24(Encoding.UTF8.GetBytes(messageGroup));
    }

    private static ulong SipHash24(ReadOnlySpan<byte> data)
    {
        const ulong key0 = 0x0706050403020100UL;
        const ulong key1 = 0x0f0e0d0c0b0a0908UL;
        var v0 = 0x736f6d6570736575UL ^ key0;
        var v1 = 0x646f72616e646f6dUL ^ key1;
        var v2 = 0x6c7967656e657261UL ^ key0;
        var v3 = 0x7465646279746573UL ^ key1;
        var offset = 0;

        while (data.Length - offset >= sizeof(ulong))
        {
            var block = BinaryPrimitives.ReadUInt64LittleEndian(data[offset..]);
            v3 ^= block;
            SipRound(ref v0, ref v1, ref v2, ref v3);
            SipRound(ref v0, ref v1, ref v2, ref v3);
            v0 ^= block;
            offset += sizeof(ulong);
        }

        var finalBlock = (ulong)data.Length << 56;
        for (var index = 0; index < data.Length - offset; index++)
        {
            finalBlock |= (ulong)data[offset + index] << (index * 8);
        }

        v3 ^= finalBlock;
        SipRound(ref v0, ref v1, ref v2, ref v3);
        SipRound(ref v0, ref v1, ref v2, ref v3);
        v0 ^= finalBlock;
        v2 ^= 0xff;
        SipRound(ref v0, ref v1, ref v2, ref v3);
        SipRound(ref v0, ref v1, ref v2, ref v3);
        SipRound(ref v0, ref v1, ref v2, ref v3);
        SipRound(ref v0, ref v1, ref v2, ref v3);
        return v0 ^ v1 ^ v2 ^ v3;
    }

    internal static bool AcceptsMessageType(Proto.MessageQueue queue, Proto.MessageType messageType)
    {
        ArgumentNullException.ThrowIfNull(queue);
        return queue.AcceptMessageTypes.Count == 0 || queue.AcceptMessageTypes.Contains(messageType);
    }

    internal static TimeSpan GetRetryDelay(Proto.RetryPolicy? policy, int retryAttempt)
    {
        if (policy is null || retryAttempt <= 0)
        {
            return TimeSpan.Zero;
        }

        switch (policy.StrategyCase)
        {
            case Proto.RetryPolicy.StrategyOneofCase.CustomizedBackoff:
                var delays = policy.CustomizedBackoff.Next;
                return delays.Count > 0 && TryGetDuration(delays[Math.Min(retryAttempt, delays.Count) - 1], out var customized)
                    ? customized
                    : TimeSpan.Zero;
            case Proto.RetryPolicy.StrategyOneofCase.ExponentialBackoff:
                var backoff = policy.ExponentialBackoff;
                if (TryGetDuration(backoff.Initial, out var initial) &&
                    TryGetDuration(backoff.Max, out var maximum) &&
                    backoff.Multiplier > 0)
                {
                    var ticks = initial.Ticks * Math.Pow(backoff.Multiplier, retryAttempt - 1);
                    if (!double.IsNaN(ticks) && ticks > 0)
                    {
                        return TimeSpan.FromTicks((long)Math.Min(ticks, maximum.Ticks));
                    }
                }

                return TimeSpan.Zero;
            default:
                return TimeSpan.Zero;
        }
    }

    private static void SipRound(ref ulong v0, ref ulong v1, ref ulong v2, ref ulong v3)
    {
        v0 += v1;
        v1 = BitOperations.RotateLeft(v1, 13);
        v1 ^= v0;
        v0 = BitOperations.RotateLeft(v0, 32);
        v2 += v3;
        v3 = BitOperations.RotateLeft(v3, 16);
        v3 ^= v2;
        v0 += v3;
        v3 = BitOperations.RotateLeft(v3, 21);
        v3 ^= v0;
        v2 += v1;
        v1 = BitOperations.RotateLeft(v1, 17);
        v1 ^= v2;
        v2 = BitOperations.RotateLeft(v2, 32);
    }

    private Proto.Message ToProtobuf(Message message, string messageId, bool transactional)
    {
        var properties = new Proto.SystemProperties
        {
            MessageId = messageId,
            BornTimestamp = Timestamp.FromDateTime(DateTime.UtcNow),
            BornHost = Environment.MachineName,
            BodyEncoding = Proto.Encoding.Identity,
            MessageType = GetMessageType(message, transactional),
            Keys = { message.Keys }
        };
        if (message.Tag is not null) properties.Tag = message.Tag;
        if (message.MessageGroup is not null) properties.MessageGroup = message.MessageGroup;
        if (message.DeliveryTimestamp is not null) properties.DeliveryTimestamp = Timestamp.FromDateTime(message.DeliveryTimestamp.Value.UtcDateTime);
        if (message.Priority is not null) properties.Priority = message.Priority.Value;
        if (message.LiteTopic is not null) properties.LiteTopic = message.LiteTopic;

        return new Proto.Message
        {
            Topic = Resource(message.Topic),
            Body = ByteString.CopyFrom(message.Body),
            SystemProperties = properties,
            UserProperties = { message.Properties }
        };
    }

    private Proto.Settings BuildSettings()
    {
        string[] topics;
        lock (_topicsGate)
        {
            topics = _topics.ToArray();
        }

        var settings = BaseSettings(Proto.ClientType.Producer);
        settings.BackoffPolicy = new Proto.RetryPolicy
        {
            MaxAttempts = Math.Max(0, _options.RetryTimesWhenSendFailed) + 1,
            ExponentialBackoff = new Proto.ExponentialBackoff
            {
                Initial = Duration.FromTimeSpan(TimeSpan.Zero),
                Max = Duration.FromTimeSpan(TimeSpan.Zero),
                Multiplier = 1
            }
        };
        settings.Publishing = new Proto.Publishing
        {
            MaxBodySize = _options.MaxMessageSize,
            Topics = { topics.Select(Resource) }
        };
        return settings;
    }

    private Proto.Settings BaseSettings(Proto.ClientType clientType) => new()
    {
        ClientType = clientType,
        AccessPoint = _client.AccessPointProtobuf,
        RequestTimeout = Duration.FromTimeSpan(_clientOptions.RequestTimeout),
        UserAgent = new Proto.UA
        {
            Language = Proto.Language.DotNet,
            Version = typeof(GrpcProducer).Assembly.GetName().Version?.ToString() ?? "unknown",
            Platform = System.Runtime.InteropServices.RuntimeInformation.OSDescription,
            Hostname = Environment.MachineName
        }
    };

    private Proto.Resource Resource(string name) => new()
    {
        ResourceNamespace = _clientOptions.Namespace ?? string.Empty,
        Name = name
    };

    private bool AddTopic(string topic)
    {
        lock (_topicsGate)
        {
            return _topics.Add(topic);
        }
    }

    private static Proto.MessageType GetMessageType(Message message, bool transactional)
    {
        var specialized = 0;
        if (message.MessageGroup is not null) specialized++;
        if (message.DeliveryTimestamp is not null) specialized++;
        if (message.Priority is not null) specialized++;
        if (message.LiteTopic is not null) specialized++;
        if (specialized > 1)
        {
            throw new ArgumentException("FIFO, delay, priority, and lite message properties are mutually exclusive.", nameof(message));
        }

        if (transactional)
        {
            if (specialized > 0)
            {
                throw new ArgumentException(
                    "Transactional messages cannot also be FIFO, delay, priority, or lite messages.",
                    nameof(message));
            }

            return Proto.MessageType.Transaction;
        }

        if (message.MessageGroup is not null) return Proto.MessageType.Fifo;
        if (message.DeliveryTimestamp is not null) return Proto.MessageType.Delay;
        if (message.Priority is not null) return Proto.MessageType.Priority;
        if (message.LiteTopic is not null) return Proto.MessageType.Lite;
        return Proto.MessageType.Normal;
    }

    private void Validate(Message message, bool transactional)
    {
        if (message.Body.Length == 0)
        {
            throw new ArgumentException("Message body cannot be empty.", nameof(message));
        }

        if (message.Body.Length > _options.MaxMessageSize)
        {
            throw new ArgumentException($"Message body exceeds the {_options.MaxMessageSize}-byte limit.", nameof(message));
        }

        if (message.Priority < 0)
        {
            throw new ArgumentException("Message priority must be greater than or equal to zero.", nameof(message));
        }

        _ = GetMessageType(message, transactional);
    }

    private static bool ShouldValidateMessageType(Proto.Settings? settings) =>
        settings?.Publishing?.ValidateMessageType ?? true;

    private static void ApplyPublishingLimits(Message message, Proto.Settings? settings)
    {
        var maxBodySize = settings?.Publishing?.MaxBodySize ?? 0;
        if (maxBodySize > 0 && message.Body.Length > maxBodySize)
        {
            throw new ArgumentException(
                $"Message body exceeds the broker's {maxBodySize}-byte limit.",
                nameof(message));
        }
    }

    private static bool IsTransientSendFailure(Exception exception) =>
        exception switch
        {
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

    private static bool TryGetDuration(Duration? value, out TimeSpan duration)
    {
        duration = default;
        if (value is null)
        {
            return false;
        }

        try
        {
            duration = value.ToTimeSpan();
            return duration >= TimeSpan.Zero;
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

    private async Task HandleTelemetryCommandAsync(
        Uri endpoint,
        Proto.TelemetryCommand command,
        CancellationToken cancellationToken)
    {
        if (command.CommandCase != Proto.TelemetryCommand.CommandOneofCase.RecoverOrphanedTransactionCommand)
        {
            return;
        }

        var recovery = command.RecoverOrphanedTransactionCommand;
        var message = recovery.Message;
        var systemProperties = message?.SystemProperties;
        if (message?.Topic is null || systemProperties is null ||
            string.IsNullOrWhiteSpace(message.Topic.Name) ||
            string.IsNullOrWhiteSpace(systemProperties.MessageId) ||
            string.IsNullOrWhiteSpace(recovery.TransactionId))
        {
            _logger.LogWarning("Ignoring an invalid RocketMQ orphan transaction recovery command from {Endpoint}", endpoint);
            return;
        }

        if (_options.TransactionChecker is null)
        {
            _logger.LogWarning(
                "Ignoring orphan transaction {TransactionId} because no transaction checker is configured",
                recovery.TransactionId);
            return;
        }

        var transactionChecks = Volatile.Read(ref _transactionChecks);

        if (transactionChecks is null)
        {
            return;
        }

        try
        {
            await transactionChecks.EnqueueAsync(
                new TransactionCheckWork(endpoint, message.Clone(), recovery.TransactionId),
                cancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested || Volatile.Read(ref _started) == 0)
        {
        }
        catch (ChannelClosedException) when (Volatile.Read(ref _started) == 0)
        {
        }
    }

    private async Task ProcessTransactionCheckAsync(
        TransactionCheckWork work,
        CancellationToken cancellationToken)
    {
        var checker = _options.TransactionChecker;
        if (checker is null)
        {
            return;
        }

        TransactionResolution resolution;
        try
        {
            resolution = await checker(
                new TransactionMessage(work.Message, work.TransactionId),
                cancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            return;
        }
        catch (Exception exception)
        {
            _logger.LogError(
                exception,
                "Transaction checker failed for orphan transaction {TransactionId}",
                work.TransactionId);
            return;
        }

        if (resolution == TransactionResolution.Unknown)
        {
            return;
        }

        try
        {
            await EndTransactionAsync(
                new TransactionReceipt(
                    work.Endpoint,
                    work.Message.Topic.Clone(),
                    work.Message.SystemProperties.MessageId,
                    work.TransactionId,
                    work.Message.SystemProperties.HasTraceContext
                        ? work.Message.SystemProperties.TraceContext
                        : null),
                resolution,
                Proto.TransactionSource.SourceServerCheck,
                cancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
        }
        catch (Exception exception)
        {
            _logger.LogError(
                exception,
                "Failed to resolve orphan transaction {TransactionId} as {Resolution}",
                work.TransactionId,
                resolution);
        }
    }

    private GrpcSessionManager CreateSessionManager() => new(
        _client,
        Options.Create(_clientOptions),
        Proto.ClientType.Producer,
        null,
        _loggerFactory,
        telemetryHandler: HandleTelemetryCommandAsync);

    private GrpcSessionManager GetSessions()
    {
        EnsureStarted();
        return Volatile.Read(ref _sessions)
            ?? throw new InvalidOperationException("Producer session is not available.");
    }

    private void EnsureStarted()
    {
        if (Volatile.Read(ref _started) == 0)
        {
            throw new InvalidOperationException("Producer has not been started.");
        }
    }

    private sealed record SendOutcome(GrpcSendReceipt Receipt, Uri Endpoint, Proto.Message Message);

    private sealed record TransactionCheckWork(Uri Endpoint, Proto.Message Message, string TransactionId);

    private sealed class TransactionCheckDispatcher : IAsyncDisposable
    {
        private readonly Channel<TransactionCheckWork> _channel;
        private readonly CancellationTokenSource _cts = new();
        private readonly Task[] _workers;

        public TransactionCheckDispatcher(
            int concurrency,
            Func<TransactionCheckWork, CancellationToken, Task> handler)
        {
            _channel = Channel.CreateBounded<TransactionCheckWork>(new BoundedChannelOptions(Math.Max(32, concurrency))
            {
                FullMode = BoundedChannelFullMode.Wait,
                SingleReader = concurrency == 1,
                SingleWriter = false
            });
            _workers = Enumerable.Range(0, concurrency)
                .Select(_ => RunAsync(handler, _cts.Token))
                .ToArray();
        }

        public ValueTask EnqueueAsync(TransactionCheckWork work, CancellationToken cancellationToken) =>
            _channel.Writer.WriteAsync(work, cancellationToken);

        public async ValueTask DisposeAsync()
        {
            _channel.Writer.TryComplete();
            _cts.Cancel();
            try
            {
                await Task.WhenAll(_workers).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
            }
            finally
            {
                _cts.Dispose();
            }
        }

        private async Task RunAsync(
            Func<TransactionCheckWork, CancellationToken, Task> handler,
            CancellationToken cancellationToken)
        {
            try
            {
                await foreach (var work in _channel.Reader.ReadAllAsync(cancellationToken).ConfigureAwait(false))
                {
                    await handler(work, cancellationToken).ConfigureAwait(false);
                }
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
            }
        }
    }
}
