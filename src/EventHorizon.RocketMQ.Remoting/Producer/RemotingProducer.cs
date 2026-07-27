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
using System.IO.Compression;
using System.Net;
using System.Text;
using System.Threading.Channels;
using EventHorizon.RocketMQ.Remoting.Exceptions;
using EventHorizon.RocketMQ.Remoting.Instrumentation;
using EventHorizon.RocketMQ.Remoting.Producer.Transactions;
using EventHorizon.RocketMQ.Remoting.Protocol;
using EventHorizon.RocketMQ.Remoting.Protocol.Route;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace EventHorizon.RocketMQ.Remoting.Producer;

internal sealed class RemotingProducer : IRemotingProducer
{
    private const string DefaultRegionId = "DefaultRegion";
    private readonly RemotingProducerOptions _options;
    private readonly RemotingClientOptions _clientOptions;
    private readonly ITopicRouteService _routeService;
    private readonly IRemotingClient _remotingClient;
    private readonly TimeProvider _timeProvider;
    private readonly IRemotingRocketMQTelemetry _telemetry;
    private readonly ILogger<RemotingProducer> _logger;
    private readonly SemaphoreSlim _lifecycleGate = new(1, 1);
    private readonly ConcurrentDictionary<string, PendingReply> _pendingReplies = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<string, ProducerBrokerEndpoint> _requestReplyBrokers = new(StringComparer.Ordinal);
    private readonly string _clientId;
    private IDisposable? _replyMessageRegistration;
    private IDisposable? _transactionCheckRegistration;
    private TransactionCheckDispatcher? _transactionChecks;
    private CancellationTokenSource? _producerHeartbeatCancellationTokenSource;
    private Task? _producerHeartbeatTask;
    private int _started;
    private int _disposed;
    private int _queueIndex;

    public RemotingProducer(
        IOptions<RemotingProducerOptions> options,
        IOptions<RemotingClientOptions> clientOptions,
        ITopicRouteService routeService,
        IRemotingClient remotingClient,
        TimeProvider timeProvider,
        ILogger<RemotingProducer> logger,
        IRemotingRocketMQTelemetry? telemetry = null)
    {
        _options = options.Value;
        _clientOptions = clientOptions.Value;
        _routeService = routeService;
        _remotingClient = remotingClient;
        _timeProvider = timeProvider;
        _telemetry = telemetry ?? RemotingRocketMQTelemetry.Disabled;
        _clientId = _clientOptions.BuildRemotingClientId();
        _logger = logger;
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

            if (_clientOptions.HeartbeatBrokerInterval <= TimeSpan.Zero)
            {
                throw new ArgumentOutOfRangeException(
                    nameof(_clientOptions.HeartbeatBrokerInterval),
                    "Producer heartbeat interval must be positive.");
            }
            IDisposable? replyMessageRegistration = null;
            IDisposable? transactionCheckRegistration = null;
            TransactionCheckDispatcher? transactionChecks = null;
            CancellationTokenSource? producerHeartbeatCancellationTokenSource = null;
            try
            {
                replyMessageRegistration = _remotingClient.RegisterRequestHandler(
                    RequestCode.PushReplyMessageToClient,
                    HandleReplyMessageAsync);
                if (_options.TransactionChecker is not null)
                {
                    ArgumentOutOfRangeException.ThrowIfNegativeOrZero(_options.MaxConcurrentTransactionChecks);
                    transactionChecks = new TransactionCheckDispatcher(
                        _options.MaxConcurrentTransactionChecks,
                        ProcessTransactionCheckAsync);
                    transactionCheckRegistration = _remotingClient.RegisterRequestHandler(
                        RequestCode.CheckTransactionState,
                        HandleTransactionCheckAsync);
                }

                producerHeartbeatCancellationTokenSource = new CancellationTokenSource();
                _replyMessageRegistration = replyMessageRegistration;
                _transactionCheckRegistration = transactionCheckRegistration;
                Volatile.Write(ref _transactionChecks, transactionChecks);
                _producerHeartbeatCancellationTokenSource = producerHeartbeatCancellationTokenSource;
                _producerHeartbeatTask = RunProducerHeartbeatLoopAsync(producerHeartbeatCancellationTokenSource.Token);
                Volatile.Write(ref _started, 1);
            }
            catch
            {
                producerHeartbeatCancellationTokenSource?.Cancel();
                producerHeartbeatCancellationTokenSource?.Dispose();
                transactionCheckRegistration?.Dispose();
                if (transactionChecks is not null)
                {
                    await transactionChecks.DisposeAsync().ConfigureAwait(false);
                }

                replyMessageRegistration?.Dispose();
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
        IDisposable? replyMessageRegistration = null;
        IDisposable? transactionCheckRegistration = null;
        TransactionCheckDispatcher? transactionChecks = null;
        CancellationTokenSource? producerHeartbeatCancellationTokenSource = null;
        Task? producerHeartbeatTask = null;
        ProducerBrokerEndpoint[] producerBrokers = [];
        await _lifecycleGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (Volatile.Read(ref _started) == 0)
            {
                return;
            }

            Volatile.Write(ref _started, 0);
            replyMessageRegistration = _replyMessageRegistration;
            _replyMessageRegistration = null;
            transactionCheckRegistration = _transactionCheckRegistration;
            _transactionCheckRegistration = null;
            transactionChecks = Interlocked.Exchange(ref _transactionChecks, null);
            producerHeartbeatCancellationTokenSource = _producerHeartbeatCancellationTokenSource;
            _producerHeartbeatCancellationTokenSource = null;
            producerHeartbeatTask = _producerHeartbeatTask;
            _producerHeartbeatTask = null;
            producerBrokers = _requestReplyBrokers.Values.ToArray();
            _requestReplyBrokers.Clear();
        }
        finally
        {
            _lifecycleGate.Release();
        }

        replyMessageRegistration?.Dispose();
        transactionCheckRegistration?.Dispose();
        if (transactionChecks is not null)
        {
            await transactionChecks.DisposeAsync().ConfigureAwait(false);
        }

        if (producerHeartbeatCancellationTokenSource is not null)
        {
            producerHeartbeatCancellationTokenSource.Cancel();
            try
            {
                if (producerHeartbeatTask is not null)
                {
                    await producerHeartbeatTask.ConfigureAwait(false);
                }
            }
            finally
            {
                producerHeartbeatCancellationTokenSource.Dispose();
            }
        }

        foreach (var pending in _pendingReplies)
        {
            if (_pendingReplies.TryRemove(pending.Key, out var reply))
            {
                reply.Completion.TrySetException(new InvalidOperationException("The producer stopped before the reply arrived."));
            }
        }

        foreach (var broker in producerBrokers)
        {
            await UnregisterProducerAsync(broker).ConfigureAwait(false);
        }
    }

    /// <inheritdoc />
    public async Task<IReadOnlyList<RemotingMessageQueue>> GetPublishMessageQueuesAsync(
        string topic,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(topic);
        EnsureStarted();
        var wireTopic = LegacyNamespace.Wrap(_clientOptions.Namespace, topic);
        var route = await _routeService.GetAsync(wireTopic, cancellationToken: cancellationToken).ConfigureAwait(false);
        return GetPublishQueues(wireTopic, route)
            .Select(queue => new RemotingMessageQueue(topic, queue.MessageQueue.BrokerName, queue.MessageQueue.QueueId))
            .ToArray();
    }

    /// <inheritdoc />
    public async Task<RemotingSendResult> SendAsync(Message message, CancellationToken cancellationToken = default)
    {
        var outcome = await SendCoreAsync(
            message,
            transactionPrepared: false,
            cancellationToken: cancellationToken).ConfigureAwait(false);
        return outcome.SendResult;
    }

    /// <inheritdoc />
    public async Task<RemotingSendResult> SendAsync(
        Message message,
        RemotingMessageQueue messageQueue,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(message);
        ArgumentNullException.ThrowIfNull(messageQueue);
        EnsureMessageQueueMatchesTopic(message, messageQueue);
        var outcome = await SendCoreAsync(
            message,
            transactionPrepared: false,
            queueSelector: (wireTopic, route, _) => ResolvePublishQueue(wireTopic, route, messageQueue),
            cancellationToken: cancellationToken).ConfigureAwait(false);
        return outcome.SendResult;
    }

    /// <inheritdoc />
    public async Task<RemotingSendResult> SendAsync(
        Message message,
        RemotingMessageQueueSelector selector,
        object? argument = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(message);
        ArgumentNullException.ThrowIfNull(selector);
        var outcome = await SendCoreAsync(
            message,
            transactionPrepared: false,
            queueSelector: (wireTopic, route, _) => SelectPublishQueue(
                message,
                selector,
                argument,
                wireTopic,
                route),
            cancellationToken: cancellationToken).ConfigureAwait(false);
        return outcome.SendResult;
    }

    /// <inheritdoc />
    public Task<RemotingSendResult> SendAsync(
        IReadOnlyCollection<Message> messages,
        CancellationToken cancellationToken = default) =>
        SendBatchAsync(PrepareBatch(messages), cancellationToken);

    /// <inheritdoc />
    public Task<RemotingSendResult> SendAsync(
        IReadOnlyCollection<Message> messages,
        RemotingMessageQueue messageQueue,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(messageQueue);
        var batch = PrepareBatch(messages);
        EnsureMessageQueueMatchesTopic(batch.RoutingMessage, messageQueue);
        return SendBatchAsync(
            batch,
            cancellationToken,
            (wireTopic, route, _) => ResolvePublishQueue(wireTopic, route, messageQueue));
    }

    /// <inheritdoc />
    public Task<RemotingSendResult> SendAsync(
        IReadOnlyCollection<Message> messages,
        RemotingMessageQueueSelector selector,
        object? argument = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(selector);
        var batch = PrepareBatch(messages);
        return SendBatchAsync(
            batch,
            cancellationToken,
            (wireTopic, route, _) => SelectPublishQueue(
                batch.RoutingMessage,
                selector,
                argument,
                wireTopic,
                route));
    }

    /// <inheritdoc />
    public async Task<RemotingTransactionSendResult> SendTransactionAsync(
        Message message,
        object? argument = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(message);
        EnsureStarted();
        if (_options.LocalTransactionExecutor is null || _options.TransactionChecker is null)
        {
            throw new InvalidOperationException(
                "Both a local transaction executor and a transaction checker must be configured before sending transactional messages.");
        }

        if (Volatile.Read(ref _transactionChecks) is null)
        {
            throw new InvalidOperationException("The transaction checker is not available while the producer is stopped.");
        }

        var outcome = await SendCoreAsync(
            message,
            transactionPrepared: true,
            cancellationToken: cancellationToken).ConfigureAwait(false);
        var localOutcome = outcome.SendResult.Status == RemotingSendStatus.SendOk
            ? await ExecuteLocalTransactionAsync(message, argument, cancellationToken).ConfigureAwait(false)
            : new LocalTransactionOutcome(RemotingTransactionResolution.Rollback, null);
        var commitLogOffset = ParseCommitLogOffset(outcome.SendResult.OffsetMessageId);

        try
        {
            await EndTransactionAsync(
                outcome.BrokerEndPoint,
                outcome.BrokerName,
                outcome.WireTopic,
                outcome.SendResult.MessageId,
                outcome.SendResult.TransactionId,
                outcome.SendResult.QueueOffset,
                commitLogOffset,
                localOutcome.Resolution,
                fromTransactionCheck: false,
                remark: localOutcome.Remark,
                cancellationToken: CancellationToken.None).ConfigureAwait(false);
        }
        catch (Exception exception)
        {
            _logger.LogWarning(
                exception,
                "Failed to resolve transaction {TransactionId} as {Resolution} after sending its half message {MessageId}",
                outcome.SendResult.TransactionId,
                localOutcome.Resolution,
                outcome.SendResult.MessageId);
        }

        return new RemotingTransactionSendResult(outcome.SendResult, localOutcome.Resolution);
    }

    /// <inheritdoc />
    public async Task<string> RecallAsync(
        string topic,
        string recallHandle,
        CancellationToken cancellationToken = default)
    {
        EnsureStarted();
        ArgumentException.ThrowIfNullOrWhiteSpace(topic);
        ArgumentException.ThrowIfNullOrWhiteSpace(recallHandle);
        if (topic.StartsWith("%RETRY%", StringComparison.Ordinal) ||
            topic.StartsWith("%DLQ%", StringComparison.Ordinal))
        {
            throw new ArgumentException("Retry and dead-letter topics do not support delayed-message recall.", nameof(topic));
        }

        var handle = ParseRecallHandle(recallHandle);
        var wireTopic = LegacyNamespace.Wrap(_clientOptions.Namespace, topic);
        if (!string.Equals(handle.Topic, wireTopic, StringComparison.Ordinal))
        {
            throw new ArgumentException("The recall handle does not belong to the supplied topic.", nameof(recallHandle));
        }

        var route = await _routeService.GetAsync(wireTopic, cancellationToken: cancellationToken).ConfigureAwait(false);
        var brokerAddress = GetRecallBrokerAddress(route, handle.BrokerName);
        var response = await _remotingClient.InvokeAsync(
            EndpointParser.Parse(brokerAddress),
            new RemotingCommand(RequestCode.RecallMessage, new RecallMessageRequestHeader
            {
                ProducerGroup = GetWireProducerGroup(),
                Topic = wireTopic,
                RecallHandle = recallHandle,
                Bname = handle.BrokerName
            }),
            _options.SendMsgTimeout,
            cancellationToken).ConfigureAwait(false);
        if (response.Code != ResponseCodes.ResSuccess)
        {
            throw new RemotingCommandException(response.Code, response.Remark ?? "Broker rejected the recall request.");
        }

        var messageId = GetString(response.ExtFields, "msgId");
        return !string.IsNullOrWhiteSpace(messageId)
            ? messageId
            : throw new InvalidDataException("Broker response did not contain a valid recalled-message ID.");
    }

    /// <inheritdoc />
    public Task<RemotingReplyMessage> RequestAsync(
        Message message,
        TimeSpan timeout,
        CancellationToken cancellationToken = default) =>
        RequestCoreAsync(message, timeout, cancellationToken);

    /// <inheritdoc />
    public Task<RemotingReplyMessage> RequestAsync(
        Message message,
        RemotingMessageQueue messageQueue,
        TimeSpan timeout,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(message);
        ArgumentNullException.ThrowIfNull(messageQueue);
        EnsureMessageQueueMatchesTopic(message, messageQueue);
        return RequestCoreAsync(
            message,
            timeout,
            cancellationToken,
            (wireTopic, route, _) => ResolvePublishQueue(wireTopic, route, messageQueue));
    }

    /// <inheritdoc />
    public Task<RemotingReplyMessage> RequestAsync(
        Message message,
        RemotingMessageQueueSelector selector,
        object? argument,
        TimeSpan timeout,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(message);
        ArgumentNullException.ThrowIfNull(selector);
        return RequestCoreAsync(
            message,
            timeout,
            cancellationToken,
            (wireTopic, route, _) => SelectPublishQueue(
                message,
                selector,
                argument,
                wireTopic,
                route));
    }

    /// <inheritdoc />
    public async Task<RemotingSendResult> SendReplyAsync(
        RemotingReply reply,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(reply);
        ValidateMessage(reply);
        EnsureStarted();
        EnsureUniqueMessageId(reply);
        var telemetry = _telemetry.StartSend(reply.Topic, 1, reply.Body.LongLength);
        try
        {
            _telemetry.InjectContext(telemetry.Activity, reply.Properties);
            var outcome = await SendCoreAsync(
                reply,
                queue => BuildRequest(reply, queue, requestCode: RequestCode.SendReplyMessage),
                (response, topic, brokerName) => CreateSendResult(reply, topic, brokerName, response),
                cancellationToken,
                queueSelector: null).ConfigureAwait(false);
            telemetry.Complete();
            return outcome.SendResult;
        }
        catch (Exception exception)
        {
            telemetry.Complete(exception);
            throw;
        }
        finally
        {
            telemetry.Dispose();
        }
    }

    private async Task<RemotingReplyMessage> RequestCoreAsync(
        Message message,
        TimeSpan timeout,
        CancellationToken cancellationToken,
        PublishQueueSelector? queueSelector = null)
    {
        ArgumentNullException.ThrowIfNull(message);
        if (timeout <= TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(nameof(timeout), "Request timeout must be positive.");
        }

        EnsureStarted();
        var correlationId = Guid.NewGuid().ToString("N").ToUpperInvariant();
        var pending = new PendingReply(new TaskCompletionSource<RemotingReplyMessage>(
            TaskCreationOptions.RunContinuationsAsynchronously));
        if (!_pendingReplies.TryAdd(correlationId, pending))
        {
            throw new InvalidOperationException("Unable to register the request correlation ID.");
        }

        message.Properties["CORRELATION_ID"] = correlationId;
        message.Properties["REPLY_TO_CLIENT"] = _clientId;
        message.Properties["TTL"] = Math.Ceiling(timeout.TotalMilliseconds).ToString(CultureInfo.InvariantCulture);
        using var timeoutCancellationTokenSource = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        timeoutCancellationTokenSource.CancelAfter(timeout);
        try
        {
            await SendCoreAsync(
                message,
                transactionPrepared: false,
                cancellationToken: timeoutCancellationTokenSource.Token,
                queueSelector: queueSelector,
                beforeInvoke: EnsureProducerHeartbeatAsync).ConfigureAwait(false);
            return await pending.Completion.Task.WaitAsync(timeoutCancellationTokenSource.Token).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested &&
                                               timeoutCancellationTokenSource.IsCancellationRequested)
        {
            throw new TimeoutException($"No reply arrived for topic '{message.Topic}' within {timeout}.");
        }
        finally
        {
            _pendingReplies.TryRemove(correlationId, out _);
        }
    }

    private async Task<SendOutcome> SendCoreAsync(
        Message message,
        bool transactionPrepared,
        CancellationToken cancellationToken,
        PublishQueueSelector? queueSelector = null,
        Func<EndPoint, string, CancellationToken, Task>? beforeInvoke = null)
    {
        ThrowIfReplyMessage(message);
        ValidateMessage(message, transactionPrepared);
        EnsureStarted();
        EnsureUniqueMessageId(message);
        var telemetry = _telemetry.StartSend(message.Topic, 1, message.Body.LongLength);
        try
        {
            _telemetry.InjectContext(telemetry.Activity, message.Properties);
            var outcome = await SendCoreAsync(
                message,
                queue => BuildRequest(message, queue, transactionPrepared),
                (response, topic, brokerName) => CreateSendResult(message, topic, brokerName, response),
                cancellationToken,
                queueSelector,
                beforeInvoke).ConfigureAwait(false);
            telemetry.Complete();
            return outcome;
        }
        catch (Exception exception)
        {
            telemetry.Complete(exception);
            throw;
        }
        finally
        {
            telemetry.Dispose();
        }
    }

    private async Task<SendOutcome> SendCoreAsync(
        Message message,
        Func<RemotingMessageQueue, RemotingCommand> requestFactory,
        Func<RemotingCommand, string, string, RemotingSendResult> resultFactory,
        CancellationToken cancellationToken,
        PublishQueueSelector? queueSelector,
        Func<EndPoint, string, CancellationToken, Task>? beforeInvoke = null)
    {
        ArgumentNullException.ThrowIfNull(message);
        ArgumentNullException.ThrowIfNull(requestFactory);
        ArgumentNullException.ThrowIfNull(resultFactory);

        Exception? lastException = null;
        string? lastBroker = null;
        var attempts = Math.Max(0, _options.RetryTimesWhenSendFailed) + 1;
        var topic = LegacyNamespace.Wrap(_clientOptions.Namespace, message.Topic);
        for (var attempt = 0; attempt < attempts; attempt++)
        {
            cancellationToken.ThrowIfCancellationRequested();
            try
            {
                var route = await _routeService.GetAsync(
                    topic,
                    forceRefresh: attempt > 0,
                    cancellationToken).ConfigureAwait(false);
                var queue = queueSelector is null
                    ? SelectQueue(message, topic, route, lastBroker)
                    : queueSelector(topic, route, lastBroker);
                lastBroker = queue.MessageQueue.BrokerName;
                var brokerEndPoint = EndpointParser.Parse(queue.BrokerAddress);
                if (beforeInvoke is not null)
                {
                    await beforeInvoke(brokerEndPoint, queue.MessageQueue.BrokerName, cancellationToken)
                        .ConfigureAwait(false);
                }

                var request = requestFactory(queue.MessageQueue);
                var response = await _remotingClient.InvokeAsync(
                    brokerEndPoint,
                    request,
                    _options.SendMsgTimeout,
                    cancellationToken).ConfigureAwait(false);

                if (IsRetryableResponse(response.Code) && attempt + 1 < attempts)
                {
                    lastException = new RemotingCommandException(
                        response.Code,
                        response.Remark ?? "Broker rejected the message.");
                    continue;
                }

                var sendResult = resultFactory(
                    response,
                    LegacyNamespace.WithoutNamespace(_clientOptions.Namespace, message.Topic),
                    queue.MessageQueue.BrokerName);
                return new SendOutcome(sendResult, brokerEndPoint, queue.MessageQueue.BrokerName, topic);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                throw;
            }
            catch (RemotingCommandException exception) when (
                IsRetryableResponse(exception.ResponseCode) &&
                attempt + 1 < attempts)
            {
                lastException = exception;
                _logger.LogWarning(
                    exception,
                    "Failed to send message to broker {BrokerName}; retrying attempt {Attempt} of {Attempts}",
                    lastBroker,
                    attempt + 2,
                    attempts);
            }
            catch (RemotingCommandException)
            {
                throw;
            }
            catch (InvalidDataException)
            {
                throw;
            }
            catch (ArgumentException)
            {
                throw;
            }
            catch (Exception exception) when (attempt + 1 < attempts)
            {
                lastException = exception;
                _logger.LogWarning(
                    exception,
                    "Failed to send message to broker {BrokerName}; retrying attempt {Attempt} of {Attempts}",
                    lastBroker,
                    attempt + 2,
                    attempts);
            }
            catch (Exception exception)
            {
                lastException = exception;
                break;
            }
        }

        throw new RocketMQClientException(
            $"Failed to send message for topic '{message.Topic}' after {attempts} attempts.",
            lastException);
    }

    private ValueTask<RemotingRequestResult> HandleReplyMessageAsync(RemotingRequestContext context)
    {
        try
        {
            var reply = RemotingReplyMessageDecoder.Decode(
                context.Request,
                _clientOptions.Namespace,
                _timeProvider.GetUtcNow());
            if (_pendingReplies.TryRemove(reply.CorrelationId, out var pending))
            {
                pending.Completion.TrySetResult(reply);
            }
            else
            {
                _logger.LogWarning(
                    "Received an unmatched reply for correlation ID {CorrelationId} from {EndPoint}",
                    reply.CorrelationId,
                    context.RemoteEndPoint);
            }

            return ValueTask.FromResult(RemotingRequestResult.Respond(new RemotingCommand
            {
                Code = ResponseCodes.ResSuccess
            }));
        }
        catch (Exception exception)
        {
            _logger.LogWarning(exception, "Unable to decode a reply callback from {EndPoint}", context.RemoteEndPoint);
            return ValueTask.FromResult(RemotingRequestResult.Respond(new RemotingCommand
            {
                Code = ResponseCodes.ResError,
                Remark = "The reply callback could not be processed."
            }));
        }
    }

    private async Task EnsureProducerHeartbeatAsync(
        EndPoint endPoint,
        string brokerName,
        CancellationToken cancellationToken)
    {
        var broker = new ProducerBrokerEndpoint(brokerName, endPoint);
        _requestReplyBrokers[broker.Key] = broker;
        await SendProducerHeartbeatAsync(broker, cancellationToken).ConfigureAwait(false);
    }

    private async Task RunProducerHeartbeatLoopAsync(CancellationToken cancellationToken)
    {
        try
        {
            while (true)
            {
                await Task.Delay(_clientOptions.HeartbeatBrokerInterval, _timeProvider, cancellationToken)
                    .ConfigureAwait(false);
                foreach (var broker in _requestReplyBrokers.Values)
                {
                    try
                    {
                        await SendProducerHeartbeatAsync(broker, cancellationToken).ConfigureAwait(false);
                    }
                    catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                    {
                        return;
                    }
                    catch (Exception exception)
                    {
                        _logger.LogWarning(
                            exception,
                            "Unable to renew producer heartbeat with broker {BrokerName} at {EndPoint}",
                            broker.BrokerName,
                            broker.EndPoint);
                    }
                }
            }
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
        }
    }

    private async Task SendProducerHeartbeatAsync(
        ProducerBrokerEndpoint broker,
        CancellationToken cancellationToken)
    {
        var response = await _remotingClient.InvokeAsync(
            broker.EndPoint,
            new RemotingCommand(RequestCode.HeartBeat, new HeartbeatRequestHeader())
            {
                Body = ProducerHeartbeatCodec.Encode(_clientId, GetWireProducerGroup())
            },
            _clientOptions.RequestTimeout,
            cancellationToken).ConfigureAwait(false);
        if (response.Code != ResponseCodes.ResSuccess)
        {
            throw new RemotingCommandException(response.Code, response.Remark ?? "Broker rejected the producer heartbeat.");
        }
    }

    private async Task UnregisterProducerAsync(ProducerBrokerEndpoint broker)
    {
        try
        {
            var response = await _remotingClient.InvokeAsync(
                broker.EndPoint,
                new RemotingCommand(RequestCode.UnregisterClient, new UnregisterClientRequestHeader
                {
                    ClientID = _clientId,
                    ProducerGroup = GetWireProducerGroup(),
                    Bname = broker.BrokerName
                }),
                _clientOptions.RequestTimeout,
                CancellationToken.None).ConfigureAwait(false);
            if (response.Code != ResponseCodes.ResSuccess)
            {
                _logger.LogDebug(
                    "Broker {BrokerName} rejected producer unregister with code {ResponseCode}: {Remark}",
                    broker.BrokerName,
                    response.Code,
                    response.Remark);
            }
        }
        catch (Exception exception)
        {
            _logger.LogDebug(
                exception,
                "Unable to unregister producer from broker {BrokerName} at {EndPoint}",
                broker.BrokerName,
                broker.EndPoint);
        }
    }

    private async Task<RemotingSendResult> SendBatchAsync(
        PreparedBatch batch,
        CancellationToken cancellationToken,
        PublishQueueSelector? queueSelector = null)
    {
        EnsureStarted();
        var telemetry = _telemetry.StartSend(
            batch.RoutingMessage.Topic,
            batch.Messages.Count,
            batch.Body.LongLength);
        try
        {
            foreach (var message in batch.Messages)
            {
                _telemetry.InjectContext(telemetry.Activity, message.Properties);
            }

            var body = RemotingMessageBatchCodec.Encode(batch.Messages, message => BuildProperties(message, false));
            if (body.Length > _options.MaxMessageSize)
            {
                throw new ArgumentException(
                    $"Encoded batch body exceeds the configured maximum of {_options.MaxMessageSize} bytes.",
                    nameof(batch));
            }

            var instrumentedBatch = batch with { Body = body };
            var outcome = await SendCoreAsync(
                instrumentedBatch.RoutingMessage,
                queue => BuildBatchRequest(instrumentedBatch, queue),
                (response, topic, brokerName) => CreateBatchSendResult(instrumentedBatch, topic, brokerName, response),
                cancellationToken,
                queueSelector).ConfigureAwait(false);
            telemetry.Complete();
            return outcome.SendResult;
        }
        catch (Exception exception)
        {
            telemetry.Complete(exception);
            throw;
        }
        finally
        {
            telemetry.Dispose();
        }
    }

    private PreparedBatch PrepareBatch(IReadOnlyCollection<Message> messages)
    {
        ArgumentNullException.ThrowIfNull(messages);
        if (messages.Count == 0)
        {
            throw new ArgumentException("A message batch cannot be empty.", nameof(messages));
        }

        var batchMessages = messages.ToArray();
        var routingMessage = batchMessages[0] ?? throw new ArgumentException(
            "A message batch cannot contain null messages.",
            nameof(messages));
        foreach (var message in batchMessages)
        {
            ArgumentNullException.ThrowIfNull(message);
            ThrowIfReplyMessage(message);
            ValidateMessage(message);
            if (!string.Equals(routingMessage.Topic, message.Topic, StringComparison.Ordinal))
            {
                throw new ArgumentException("All messages in a batch must use the same topic.", nameof(messages));
            }

            ValidateBatchMessage(message);
            EnsureUniqueMessageId(message);
        }

        var body = RemotingMessageBatchCodec.Encode(batchMessages, message => BuildProperties(message, false));
        if (body.Length > _options.MaxMessageSize)
        {
            throw new ArgumentException(
                $"Encoded batch body exceeds the configured maximum of {_options.MaxMessageSize} bytes.",
                nameof(messages));
        }

        return new PreparedBatch(
            routingMessage,
            batchMessages,
            body,
            Guid.NewGuid().ToString("N").ToUpperInvariant());
    }

    /// <inheritdoc />
    public Task SendOnewayAsync(Message message, CancellationToken cancellationToken = default) =>
        SendOnewayCoreAsync(message, cancellationToken);

    /// <inheritdoc />
    public Task SendOnewayAsync(
        Message message,
        RemotingMessageQueue messageQueue,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(message);
        ArgumentNullException.ThrowIfNull(messageQueue);
        EnsureMessageQueueMatchesTopic(message, messageQueue);
        return SendOnewayCoreAsync(
            message,
            cancellationToken,
            (wireTopic, route, _) => ResolvePublishQueue(wireTopic, route, messageQueue));
    }

    /// <inheritdoc />
    public Task SendOnewayAsync(
        Message message,
        RemotingMessageQueueSelector selector,
        object? argument = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(message);
        ArgumentNullException.ThrowIfNull(selector);
        return SendOnewayCoreAsync(
            message,
            cancellationToken,
            (wireTopic, route, _) => SelectPublishQueue(
                message,
                selector,
                argument,
                wireTopic,
                route));
    }

    private async Task SendOnewayCoreAsync(
        Message message,
        CancellationToken cancellationToken,
        PublishQueueSelector? queueSelector = null)
    {
        ThrowIfReplyMessage(message);
        ValidateMessage(message);
        EnsureStarted();
        EnsureUniqueMessageId(message);
        var telemetry = _telemetry.StartSend(message.Topic, 1, message.Body.LongLength);
        try
        {
            _telemetry.InjectContext(telemetry.Activity, message.Properties);
            Exception? lastException = null;
            string? lastBroker = null;
            var attempts = Math.Max(0, _options.RetryTimesWhenSendFailed) + 1;
            var topic = LegacyNamespace.Wrap(_clientOptions.Namespace, message.Topic);
            for (var attempt = 0; attempt < attempts; attempt++)
            {
                cancellationToken.ThrowIfCancellationRequested();
                try
                {
                    var route = await _routeService.GetAsync(
                        topic,
                        forceRefresh: attempt > 0,
                        cancellationToken).ConfigureAwait(false);
                    var queue = queueSelector is null
                        ? SelectQueue(message, topic, route, lastBroker)
                        : queueSelector(topic, route, lastBroker);
                    lastBroker = queue.MessageQueue.BrokerName;
                    await _remotingClient.InvokeOnewayAsync(
                        EndpointParser.Parse(queue.BrokerAddress),
                        BuildRequest(message, queue.MessageQueue),
                        _options.SendMsgTimeout,
                        cancellationToken).ConfigureAwait(false);
                    telemetry.Complete();
                    return;
                }
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                {
                    throw;
                }
                catch (ArgumentException)
                {
                    throw;
                }
                catch (Exception exception) when (attempt + 1 < attempts)
                {
                    lastException = exception;
                }
                catch (Exception exception)
                {
                    lastException = exception;
                    break;
                }
            }

            throw new RocketMQClientException(
                $"Failed to send one-way message for topic '{message.Topic}' after {attempts} attempts.",
                lastException);
        }
        catch (Exception exception)
        {
            telemetry.Complete(exception);
            throw;
        }
        finally
        {
            telemetry.Dispose();
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
        {
            return;
        }

        await StopAsync().ConfigureAwait(false);
    }

    private RemotingCommand BuildRequest(
        Message message,
        RemotingMessageQueue queue,
        bool transactionPrepared = false,
        int requestCode = RequestCode.SendMessage)
    {
        var body = message.Body;
        var sysFlag = 0;
        if (body.Length >= _options.CompressMsgBodyOverHowmuch)
        {
            using var output = new MemoryStream();
            using (var compressor = new ZLibStream(output, CompressionLevel.Optimal, leaveOpen: true))
            {
                compressor.Write(body);
            }

            body = output.ToArray();
            sysFlag |= MessageSysFlag.CompressedFlag;
        }

        if (transactionPrepared)
        {
            sysFlag |= MessageSysFlag.TransactionPreparedType;
        }

        var header = CreateSendMessageRequestHeader(
            queue,
            sysFlag,
            MessagePropertyCodec.Serialize(BuildProperties(message, transactionPrepared)),
            batch: false);
        return new RemotingCommand(requestCode, header) { Body = body };
    }

    private RemotingCommand BuildBatchRequest(PreparedBatch batch, RemotingMessageQueue queue)
    {
        var properties = new Dictionary<string, string>(StringComparer.Ordinal)
        {
            ["UNIQ_KEY"] = batch.BatchMessageId
        };
        var header = CreateSendMessageRequestHeader(
            queue,
            sysFlag: 0,
            MessagePropertyCodec.Serialize(properties),
            batch: true);
        return new RemotingCommand(RequestCode.SendMessage, header) { Body = batch.Body };
    }

    private SendMessageRequestHeader CreateSendMessageRequestHeader(
        RemotingMessageQueue queue,
        int sysFlag,
        string properties,
        bool batch) =>
        new()
        {
            ProducerGroup = GetWireProducerGroup(),
            Topic = queue.Topic,
            QueueId = queue.QueueId,
            SysFlag = sysFlag,
            BornTimestamp = _timeProvider.GetUtcNow().ToUnixTimeMilliseconds(),
            Flag = 0,
            Properties = properties,
            ReconsumeTimes = 0,
            UnitMode = _clientOptions.UnitMode,
            MaxReconsumeTimes = 0,
            Batch = batch,
            DefaultTopicQueueNums = _options.DefaultTopicQueueNums,
            Bname = queue.BrokerName
        };

    private IDictionary<string, string> BuildProperties(Message message, bool transactionPrepared)
    {
        var properties = new Dictionary<string, string>(message.Properties, StringComparer.Ordinal);
        if (!string.IsNullOrWhiteSpace(message.Tag))
        {
            properties["TAGS"] = message.Tag;
        }

        if (message.Keys.Count > 0)
        {
            properties["KEYS"] = string.Join(' ', message.Keys);
        }

        if (!string.IsNullOrWhiteSpace(message.MessageGroup))
        {
            properties["__SHARDINGKEY"] = message.MessageGroup;
        }

        if (message.DeliveryTimestamp is { } deliveryTimestamp)
        {
            properties["TIMER_DELIVER_MS"] = deliveryTimestamp.ToUnixTimeMilliseconds()
                .ToString(CultureInfo.InvariantCulture);
        }

        if (message.Priority is { } priority)
        {
            properties["_SYS_MSG_PRIORITY_"] = priority.ToString(CultureInfo.InvariantCulture);
        }

        if (!string.IsNullOrWhiteSpace(message.LiteTopic))
        {
            properties["__LITE_TOPIC"] = message.LiteTopic;
        }

        if (transactionPrepared)
        {
            properties["TRAN_MSG"] = "true";
            properties["PGROUP"] = GetWireProducerGroup();
        }

        if (message is RemotingReply reply)
        {
            properties["MSG_TYPE"] = "reply";
            properties["CORRELATION_ID"] = reply.CorrelationId;
            properties["REPLY_TO_CLIENT"] = reply.ReplyToClient;
            properties["TTL"] = Math.Ceiling(reply.TimeToLive.TotalMilliseconds)
                .ToString(CultureInfo.InvariantCulture);
        }

        return properties;
    }

    private PublishQueue SelectQueue(
        Message message,
        string topic,
        TopicRouteData route,
        string? lastBroker)
    {
        var queues = GetPublishQueues(topic, route);

        var affinityKey = !string.IsNullOrWhiteSpace(message.MessageGroup)
            ? message.MessageGroup
            : !string.IsNullOrWhiteSpace(message.LiteTopic)
                ? message.LiteTopic
                : null;
        if (affinityKey is not null)
        {
            return queues[GetAffinityQueueIndex(affinityKey, queues.Length)];
        }

        var start = (Interlocked.Increment(ref _queueIndex) & int.MaxValue) % queues.Length;
        for (var offset = 0; offset < queues.Length; offset++)
        {
            var candidate = queues[(start + offset) % queues.Length];
            if (lastBroker is null || candidate.MessageQueue.BrokerName != lastBroker ||
                queues.All(item => item.MessageQueue.BrokerName == lastBroker))
            {
                return candidate;
            }
        }

        return queues[start];
    }

    private PublishQueue SelectPublishQueue(
        Message message,
        RemotingMessageQueueSelector selector,
        object? argument,
        string wireTopic,
        TopicRouteData route)
    {
        var queues = GetPublishQueues(wireTopic, route);
        var logicalTopic = LegacyNamespace.WithoutNamespace(_clientOptions.Namespace, wireTopic);
        var availableQueues = queues
            .Select(queue => new RemotingMessageQueue(
                logicalTopic,
                queue.MessageQueue.BrokerName,
                queue.MessageQueue.QueueId))
            .ToArray();
        var selectedQueue = selector(availableQueues, message, argument);
        ArgumentNullException.ThrowIfNull(selectedQueue);
        EnsureMessageQueueMatchesTopic(message, selectedQueue);
        return ResolvePublishQueue(wireTopic, route, selectedQueue);
    }

    private static PublishQueue ResolvePublishQueue(
        string wireTopic,
        TopicRouteData route,
        RemotingMessageQueue messageQueue)
    {
        var queue = GetPublishQueues(wireTopic, route).FirstOrDefault(candidate =>
            candidate.MessageQueue.BrokerName == messageQueue.BrokerName &&
            candidate.MessageQueue.QueueId == messageQueue.QueueId);
        return queue ?? throw new ArgumentException(
            $"The message queue '{messageQueue.BrokerName}:{messageQueue.QueueId}' is not writable for topic " +
            $"'{messageQueue.Topic}'.",
            nameof(messageQueue));
    }

    private static PublishQueue[] GetPublishQueues(string topic, TopicRouteData route)
    {
        const int writePermission = 1 << 1;
        var queues = route.QueueDatas
            .Where(queue => (queue.Perm & writePermission) == writePermission)
            .SelectMany(queue => Enumerable.Range(0, queue.WriteQueueNums),
                (queue, queueId) => new RemotingMessageQueue(topic, queue.BrokerName, queueId))
            .Select(queue => new
            {
                Queue = queue,
                Broker = route.BrokerDatas.FirstOrDefault(broker => broker.BrokerName == queue.BrokerName)
            })
            .Where(item => item.Broker is not null && item.Broker.BrokerAddrs.TryGetValue(0, out _))
            .Select(item => new PublishQueue(item.Queue, item.Broker!.BrokerAddrs[0]))
            .OrderBy(item => item.MessageQueue.BrokerName, StringComparer.Ordinal)
            .ThenBy(item => item.MessageQueue.QueueId)
            .ToArray();
        if (queues.Length == 0)
        {
            throw new RocketMQClientException($"No writable message queue is available for topic '{topic}'.");
        }

        return queues;
    }

    private static string GetRecallBrokerAddress(TopicRouteData route, string brokerName)
    {
        var broker = route.BrokerDatas.FirstOrDefault(candidate => candidate.BrokerName == brokerName);
        if (broker is not null && broker.BrokerAddrs.Count > 0)
        {
            return broker.BrokerAddrs.TryGetValue(0, out var master)
                ? master
                : broker.BrokerAddrs.OrderBy(static address => address.Key).First().Value;
        }

        var fallback = route.BrokerDatas
            .SelectMany(candidate => candidate.BrokerAddrs.OrderBy(static address => address.Key))
            .Select(static address => address.Value)
            .FirstOrDefault();
        return !string.IsNullOrWhiteSpace(fallback)
            ? fallback
            : throw new RocketMQClientException(
                $"No broker endpoint is available to recall a message for broker '{brokerName}'.");
    }

    private static void EnsureMessageQueueMatchesTopic(Message message, RemotingMessageQueue messageQueue)
    {
        if (!string.Equals(message.Topic, messageQueue.Topic, StringComparison.Ordinal))
        {
            throw new ArgumentException(
                $"The message topic '{message.Topic}' does not match queue topic '{messageQueue.Topic}'.",
                nameof(messageQueue));
        }
    }

    private static int GetAffinityQueueIndex(string key, int queueCount)
    {
        var hash = 0;
        foreach (var character in key)
        {
            hash = unchecked((hash * 31) + character);
        }

        var index = hash % queueCount;
        return index < 0 ? -index : index;
    }

    private static RemotingSendResult CreateSendResult(
        Message message,
        string topic,
        string brokerName,
        RemotingCommand response) =>
        CreateSendResult(message.Properties["UNIQ_KEY"], topic, brokerName, response);

    private static RemotingSendResult CreateBatchSendResult(
        PreparedBatch batch,
        string topic,
        string brokerName,
        RemotingCommand response)
    {
        var messageId = GetString(response.ExtFields, "batchUniqId");
        if (string.IsNullOrWhiteSpace(messageId))
        {
            messageId = string.Join(',', batch.Messages.Select(message => message.Properties["UNIQ_KEY"]));
        }

        return CreateSendResult(messageId, topic, brokerName, response);
    }

    private static RemotingSendResult CreateSendResult(
        string messageId,
        string topic,
        string brokerName,
        RemotingCommand response)
    {
        var status = response.Code switch
        {
            ResponseCodes.ResSuccess => RemotingSendStatus.SendOk,
            ResponseCodes.ResFlushDiskTimeout => RemotingSendStatus.FlushDiskTimeout,
            ResponseCodes.ResFlushSlaveTimeout => RemotingSendStatus.FlushSlaveTimeout,
            ResponseCodes.ResSlaveNotAvailable => RemotingSendStatus.SlaveNotAvailable,
            _ => throw new RemotingCommandException(
                response.Code,
                response.Remark ?? "Broker rejected the message.")
        };

        var queueId = ParseInt32(response.ExtFields, "queueId");
        var queueOffset = ParseInt64(response.ExtFields, "queueOffset");
        var offsetMessageId = GetString(response.ExtFields, "msgId");
        if (string.IsNullOrWhiteSpace(offsetMessageId))
        {
            throw new InvalidDataException("Broker response did not contain a valid 'msgId' field.");
        }

        var regionId = GetString(response.ExtFields, "MSG_REGION");
        if (string.IsNullOrWhiteSpace(regionId))
        {
            regionId = DefaultRegionId;
        }

        var trace = GetString(response.ExtFields, "TRACE_ON");
        return new RemotingSendResult(
            status,
            messageId,
            offsetMessageId,
            new RemotingMessageQueue(topic, brokerName, queueId),
            queueOffset,
            GetString(response.ExtFields, "transactionId"),
            GetString(response.ExtFields, "recallHandle"),
            regionId,
            !string.Equals(trace, "false", StringComparison.OrdinalIgnoreCase));
    }

    private static int ParseInt32(IReadOnlyDictionary<string, object> fields, string name) =>
        int.TryParse(GetString(fields, name), NumberStyles.Integer, CultureInfo.InvariantCulture, out var value)
            ? value
            : throw new InvalidDataException($"Broker response did not contain a valid '{name}' field.");

    private static long ParseInt64(IReadOnlyDictionary<string, object> fields, string name) =>
        long.TryParse(GetString(fields, name), NumberStyles.Integer, CultureInfo.InvariantCulture, out var value)
            ? value
            : throw new InvalidDataException($"Broker response did not contain a valid '{name}' field.");

    private static string? GetString(IReadOnlyDictionary<string, object> fields, string name) =>
        fields.TryGetValue(name, out var value) ? Convert.ToString(value, CultureInfo.InvariantCulture) : null;

    private static RecallHandle ParseRecallHandle(string recallHandle)
    {
        try
        {
            var base64 = recallHandle.Replace('-', '+').Replace('_', '/');
            var remainder = base64.Length % 4;
            if (remainder == 1)
            {
                throw new FormatException("The Base64 URL value has an invalid length.");
            }

            if (remainder != 0)
            {
                base64 = base64.PadRight(base64.Length + (4 - remainder), '=');
            }

            var values = Encoding.UTF8.GetString(Convert.FromBase64String(base64)).Split(' ');
            if (values.Length < 5 ||
                !string.Equals(values[0], "v1", StringComparison.Ordinal) ||
                values.Take(5).Skip(1).Any(string.IsNullOrWhiteSpace))
            {
                throw new FormatException("The recall-handle fields are invalid.");
            }

            return new RecallHandle(values[1], values[2]);
        }
        catch (FormatException exception)
        {
            throw new ArgumentException("The recall handle is invalid.", nameof(recallHandle), exception);
        }
    }

    private async Task<LocalTransactionOutcome> ExecuteLocalTransactionAsync(
        Message message,
        object? argument,
        CancellationToken cancellationToken)
    {
        var executor = _options.LocalTransactionExecutor
            ?? throw new InvalidOperationException("A local transaction executor is not configured.");
        try
        {
            var resolution = await executor(message, argument, cancellationToken).ConfigureAwait(false);
            return new LocalTransactionOutcome(resolution, null);
        }
        catch (Exception exception)
        {
            _logger.LogError(
                exception,
                "The local transaction executor failed for message {MessageId}; reporting an unknown transaction state",
                message.Properties["UNIQ_KEY"]);
            return new LocalTransactionOutcome(
                RemotingTransactionResolution.Unknown,
                $"The local transaction executor failed: {exception.GetType().Name}: {exception.Message}");
        }
    }

    private async Task EndTransactionAsync(
        EndPoint brokerEndPoint,
        string brokerName,
        string wireTopic,
        string messageId,
        string? transactionId,
        long tranStateTableOffset,
        long commitLogOffset,
        RemotingTransactionResolution resolution,
        bool fromTransactionCheck,
        string? remark,
        CancellationToken cancellationToken)
    {
        var header = new EndTransactionRequestHeader
        {
            Topic = wireTopic,
            ProducerGroup = GetWireProducerGroup(),
            TranStateTableOffset = tranStateTableOffset,
            CommitLogOffset = commitLogOffset,
            CommitOrRollback = ToTransactionStateFlag(resolution),
            FromTransactionCheck = fromTransactionCheck,
            MsgId = messageId,
            TransactionId = transactionId,
            Bname = brokerName
        };
        var request = new RemotingCommand(RequestCode.EndTransaction, header)
        {
            Remark = remark
        };
        await _remotingClient.InvokeOnewayAsync(
            brokerEndPoint,
            request,
            _options.SendMsgTimeout,
            cancellationToken).ConfigureAwait(false);
    }

    private ValueTask<RemotingRequestResult> HandleTransactionCheckAsync(RemotingRequestContext context)
    {
        if (Volatile.Read(ref _started) == 0 || !TryParseTransactionCheckHeader(context.Request, out var header))
        {
            return ValueTask.FromResult(RemotingRequestResult.NotHandled);
        }

        var fallbackMessageId = !string.IsNullOrWhiteSpace(header.MessageId)
            ? header.MessageId
            : header.OffsetMessageId;
        if (string.IsNullOrWhiteSpace(fallbackMessageId))
        {
            _logger.LogWarning("Ignoring a transaction check without a message ID from {EndPoint}", context.RemoteEndPoint);
            return ValueTask.FromResult(RemotingRequestResult.NoResponse);
        }

        RemotingTransactionMessage message;
        try
        {
            message = TransactionMessageDecoder.Decode(
                context.Request.Body,
                header.TransactionId,
                fallbackMessageId,
                _clientOptions.Namespace);
        }
        catch (Exception exception)
        {
            _logger.LogWarning(exception, "Ignoring an invalid transaction check from {EndPoint}", context.RemoteEndPoint);
            return ValueTask.FromResult(RemotingRequestResult.NoResponse);
        }

        if (!message.Properties.TryGetValue("PGROUP", out var producerGroup) ||
            !string.Equals(producerGroup, GetWireProducerGroup(), StringComparison.Ordinal))
        {
            return ValueTask.FromResult(RemotingRequestResult.NotHandled);
        }

        var transactionChecks = Volatile.Read(ref _transactionChecks);
        if (transactionChecks is null || !transactionChecks.TryEnqueue(new TransactionCheckWork(context.RemoteEndPoint, header, message)))
        {
            _logger.LogWarning(
                "Deferring transaction check {TransactionId} because the transaction-check queue is unavailable or full",
                header.TransactionId);
        }

        return ValueTask.FromResult(RemotingRequestResult.NoResponse);
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

        LocalTransactionOutcome outcome;
        try
        {
            outcome = new LocalTransactionOutcome(
                await checker(work.Message, cancellationToken).ConfigureAwait(false),
                null);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            return;
        }
        catch (Exception exception)
        {
            _logger.LogError(
                exception,
                "The transaction checker failed for transaction {TransactionId}; reporting an unknown transaction state",
                work.Header.TransactionId);
            outcome = new LocalTransactionOutcome(
                RemotingTransactionResolution.Unknown,
                $"The transaction checker failed: {exception.GetType().Name}: {exception.Message}");
        }

        try
        {
            await EndTransactionAsync(
                work.BrokerEndPoint,
                work.Header.BrokerName,
                work.Header.Topic,
                work.Message.MessageId,
                work.Header.TransactionId,
                work.Header.TranStateTableOffset,
                work.Header.CommitLogOffset,
                outcome.Resolution,
                fromTransactionCheck: true,
                remark: outcome.Remark,
                cancellationToken: cancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
        }
        catch (Exception exception)
        {
            _logger.LogWarning(
                exception,
                "Failed to resolve broker transaction check {TransactionId} as {Resolution}",
                work.Header.TransactionId,
                outcome.Resolution);
        }
    }

    private string GetWireProducerGroup() => LegacyNamespace.Wrap(_clientOptions.Namespace, _options.GroupName);

    private static long ParseCommitLogOffset(string offsetMessageId) =>
        LegacyOffsetMessageId.Parse(offsetMessageId).CommitLogOffset;

    private static int ToTransactionStateFlag(RemotingTransactionResolution resolution) => resolution switch
    {
        RemotingTransactionResolution.Unknown => MessageSysFlag.TransactionNotType,
        RemotingTransactionResolution.Commit => MessageSysFlag.TransactionCommitType,
        RemotingTransactionResolution.Rollback => MessageSysFlag.TransactionRollbackType,
        _ => throw new ArgumentOutOfRangeException(nameof(resolution), resolution, "Unknown transaction resolution.")
    };

    private static bool TryParseTransactionCheckHeader(
        RemotingCommand request,
        out TransactionCheckHeader header)
    {
        var topic = GetString(request.ExtFields, "topic");
        var tranStateTableOffset = GetString(request.ExtFields, "tranStateTableOffset");
        var commitLogOffset = GetString(request.ExtFields, "commitLogOffset");
        if (string.IsNullOrWhiteSpace(topic) ||
            !long.TryParse(tranStateTableOffset, NumberStyles.Integer, CultureInfo.InvariantCulture, out var tableOffset) ||
            !long.TryParse(commitLogOffset, NumberStyles.Integer, CultureInfo.InvariantCulture, out var logOffset))
        {
            header = null!;
            return false;
        }

        header = new TransactionCheckHeader(
            topic,
            tableOffset,
            logOffset,
            GetString(request.ExtFields, "msgId"),
            GetString(request.ExtFields, "transactionId"),
            GetString(request.ExtFields, "offsetMsgId"),
            GetString(request.ExtFields, "bname") ?? string.Empty);
        return true;
    }

    private static bool IsRetryableResponse(int responseCode) =>
        responseCode is ResponseCodes.ResError or
            ResponseCodes.ResSystemBusy or
            ResponseCodes.ResServiceNotAvailable;

    private void ValidateMessage(Message message, bool transactionPrepared = false)
    {
        ArgumentNullException.ThrowIfNull(message);
        if (message.Body.Length == 0)
        {
            throw new ArgumentException("Message body cannot be empty.", nameof(message));
        }

        if (message.Body.Length > _options.MaxMessageSize)
        {
            throw new ArgumentException($"Message body exceeds the configured maximum of {_options.MaxMessageSize} bytes.", nameof(message));
        }

        var specializedTypeCount = 0;
        if (message.MessageGroup is not null)
        {
            if (string.IsNullOrWhiteSpace(message.MessageGroup))
            {
                throw new ArgumentException("Message group cannot be empty or whitespace.", nameof(message));
            }

            specializedTypeCount++;
        }

        if (message.DeliveryTimestamp is not null)
        {
            specializedTypeCount++;
        }

        if (message.Priority is { } priority)
        {
            if (priority < 0)
            {
                throw new ArgumentException("Message priority must be greater than or equal to zero.", nameof(message));
            }

            specializedTypeCount++;
        }

        if (message.LiteTopic is not null)
        {
            if (string.IsNullOrWhiteSpace(message.LiteTopic))
            {
                throw new ArgumentException("Lite topic cannot be empty or whitespace.", nameof(message));
            }

            specializedTypeCount++;
        }

        if (specializedTypeCount > 1)
        {
            throw new ArgumentException(
                "MessageGroup, DeliveryTimestamp, Priority, and LiteTopic are mutually exclusive message types.",
                nameof(message));
        }

        if (!transactionPrepared)
        {
            return;
        }

        if (specializedTypeCount != 0)
        {
            throw new ArgumentException(
                "Transactional messages cannot use MessageGroup, DeliveryTimestamp, Priority, or LiteTopic.",
                nameof(message));
        }

        if (HasDelayedDeliveryProperties(message))
        {
            throw new ArgumentException("Transactional messages cannot use delayed delivery.", nameof(message));
        }
    }

    private static void ValidateBatchMessage(Message message)
    {
        if (message.DeliveryTimestamp is not null || HasDelayedDeliveryProperties(message))
        {
            throw new ArgumentException("Delayed messages are not supported in a batch.", nameof(message));
        }

        if (message.Topic.StartsWith("%RETRY%", StringComparison.Ordinal))
        {
            throw new ArgumentException("Retry-topic messages are not supported in a batch.", nameof(message));
        }

        if (message.Properties.ContainsKey("TRAN_MSG"))
        {
            throw new ArgumentException("Transactional messages are not supported in a batch.", nameof(message));
        }
    }

    private static bool HasDelayedDeliveryProperties(Message message) =>
        message.Properties.ContainsKey("DELAY") ||
        message.Properties.ContainsKey("TIMER_DELAY_MS") ||
        message.Properties.ContainsKey("TIMER_DELAY_SEC") ||
        message.Properties.ContainsKey("TIMER_DELIVER_MS") ||
        message.Properties.ContainsKey("__STARTDELIVERTIME");

    private static void ThrowIfReplyMessage(Message message)
    {
        if (message is RemotingReply)
        {
            throw new ArgumentException(
                "A RemotingReply must be sent with SendReplyAsync.",
                nameof(message));
        }
    }

    private static void EnsureUniqueMessageId(Message message)
    {
        if (!message.Properties.TryGetValue("UNIQ_KEY", out var value) || string.IsNullOrWhiteSpace(value))
        {
            message.Properties["UNIQ_KEY"] = Guid.NewGuid().ToString("N").ToUpperInvariant();
        }
    }

    private void EnsureStarted()
    {
        ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);
        if (Volatile.Read(ref _started) == 0)
        {
            throw new InvalidOperationException("The producer has not been started by the host or StartAsync.");
        }
    }

    private sealed record PublishQueue(RemotingMessageQueue MessageQueue, string BrokerAddress);

    private delegate PublishQueue PublishQueueSelector(
        string wireTopic,
        TopicRouteData route,
        string? lastBroker);

    private sealed record SendOutcome(
        RemotingSendResult SendResult,
        EndPoint BrokerEndPoint,
        string BrokerName,
        string WireTopic);

    private sealed record PreparedBatch(
        Message RoutingMessage,
        IReadOnlyList<Message> Messages,
        byte[] Body,
        string BatchMessageId);

    private sealed record RecallHandle(string Topic, string BrokerName);

    private sealed record PendingReply(TaskCompletionSource<RemotingReplyMessage> Completion);

    private sealed record ProducerBrokerEndpoint(string BrokerName, EndPoint EndPoint)
    {
        public string Key => $"{BrokerName}|{EndPoint}";
    }

    private sealed record LocalTransactionOutcome(RemotingTransactionResolution Resolution, string? Remark);

    private sealed record TransactionCheckHeader(
        string Topic,
        long TranStateTableOffset,
        long CommitLogOffset,
        string? MessageId,
        string? TransactionId,
        string? OffsetMessageId,
        string BrokerName);

    private sealed record TransactionCheckWork(
        EndPoint BrokerEndPoint,
        TransactionCheckHeader Header,
        RemotingTransactionMessage Message);

    private sealed class TransactionCheckDispatcher : IAsyncDisposable
    {
        private readonly Channel<TransactionCheckWork> _channel;
        private readonly CancellationTokenSource _cancellationTokenSource = new();
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
                .Select(_ => RunAsync(handler, _cancellationTokenSource.Token))
                .ToArray();
        }

        public bool TryEnqueue(TransactionCheckWork work) => _channel.Writer.TryWrite(work);

        public async ValueTask DisposeAsync()
        {
            _channel.Writer.TryComplete();
            _cancellationTokenSource.Cancel();
            try
            {
                await Task.WhenAll(_workers).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (_cancellationTokenSource.IsCancellationRequested)
            {
            }
            finally
            {
                _cancellationTokenSource.Dispose();
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
