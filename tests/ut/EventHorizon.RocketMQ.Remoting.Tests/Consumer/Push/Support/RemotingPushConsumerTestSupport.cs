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
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Net;
using System.Runtime.CompilerServices;
using System.Text;
using System.Text.Json;
using EventHorizon.RocketMQ.Remoting.Consumer;
using EventHorizon.RocketMQ.Remoting.Consumer.Coordination.Rebalance;
using EventHorizon.RocketMQ.Remoting.Consumer.Push;
using EventHorizon.RocketMQ.Remoting.Consumer.Route;
using EventHorizon.RocketMQ.Remoting.Instrumentation;
using EventHorizon.RocketMQ.Remoting.Protocol;
using EventHorizon.RocketMQ.Remoting.Protocol.Route;
using EventHorizon.RocketMQ.Remoting.Tests.Consumer.Coordination.Rebalance;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Moq;
using Xunit;

namespace EventHorizon.RocketMQ.Remoting.Tests.Consumer.Push.Support;

public abstract class RemotingPushConsumerTestSupport
{
    private static readonly ConditionalWeakTable<
        RemotingPushConsumerOptions,
        Func<IReadOnlyList<RemotingMessageView>, RemotingPushConsumeContext, CancellationToken, ValueTask<ConsumeResult>>>
        MessageHandlers = new();

    private protected static RemotingCommand CreateLockResponse(RemotingCommand request, bool grant)
    {
        using var body = JsonDocument.Parse(request.Body!);
        return new RemotingCommand
        {
            Code = ResponseCodes.ResSuccess,
            Body = JsonSerializer.SerializeToUtf8Bytes(new
            {
                lockOKMQSet = grant
                    ? (object)body.RootElement.GetProperty("mqSet").Clone()
                    : Array.Empty<object>()
            })
        };
    }

    private protected static bool RequestContainsQueueTopic(RemotingCommand request, string topic)
    {
        if (request.Body is not { Length: > 0 })
        {
            return false;
        }

        using var body = JsonDocument.Parse(request.Body);
        return body.RootElement.TryGetProperty("mqSet", out var queues) &&
               queues.EnumerateArray().Any(queue =>
                   string.Equals(queue.GetProperty("topic").GetString(), topic, StringComparison.Ordinal));
    }

    private protected static async Task WaitForOnewayRequestAsync(
        FakeRemotingClient remoting,
        Func<RemotingCommand, bool> predicate,
        CancellationToken cancellationToken)
    {
        while (!remoting.OnewayRequests.Any(predicate))
        {
            await Task.Delay(10, cancellationToken);
        }
    }

    private protected static RemotingCommand PullSuccess(byte[] body, long nextOffset) => new()
    {
        Code = ResponseCodes.ResSuccess,
        Body = body,
        ExtFields = new Dictionary<string, object>
        {
            ["nextBeginOffset"] = nextOffset.ToString(),
            ["minOffset"] = "0",
            ["maxOffset"] = nextOffset.ToString()
        }
    };

    private protected static RemotingCommand PullEmpty(long nextOffset) => new()
    {
        Code = ResponseCodes.ResPullNotFound,
        ExtFields = new Dictionary<string, object>
        {
            ["nextBeginOffset"] = nextOffset.ToString(),
            ["minOffset"] = "0",
            ["maxOffset"] = nextOffset.ToString()
        }
    };

    private protected static RemotingCommand PullOffsetIllegal(long nextOffset) => new()
    {
        Code = ResponseCodes.ResPullOffsetMoved,
        ExtFields = new Dictionary<string, object>
        {
            ["nextBeginOffset"] = nextOffset.ToString(),
            ["minOffset"] = "0",
            ["maxOffset"] = nextOffset.ToString()
        }
    };

    private protected static async Task<RemotingCommand> WaitForCanceledPullAsync(CancellationToken cancellationToken)
    {
        await Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken);
        throw new InvalidOperationException("An infinite pull completed unexpectedly.");
    }

    private protected static async Task<RemotingCommand> SignalThenWaitForCanceledPullAsync(
        TaskCompletionSource signal,
        CancellationToken cancellationToken)
    {
        signal.TrySetResult();
        return await WaitForCanceledPullAsync(cancellationToken);
    }

    private protected static void UpdateMaximum(ref int target, int candidate)
    {
        var observed = Volatile.Read(ref target);
        while (candidate > observed)
        {
            var replaced = Interlocked.CompareExchange(ref target, candidate, observed);
            if (replaced == observed)
            {
                return;
            }

            observed = replaced;
        }
    }

    private protected static byte[] CreateMessageRecord(
        string topic,
        string messageId,
        string? messageGroup,
        long queueOffset,
        long commitLogOffset,
        int queueId = 0)
    {
        var body = Encoding.UTF8.GetBytes(messageId);
        var propertyText = $"UNIQ_KEY\u0001{messageId}\u0002";
        if (messageGroup is not null)
        {
            propertyText += $"__SHARDINGKEY\u0001{messageGroup}\u0002";
        }

        var properties = Encoding.UTF8.GetBytes(propertyText);
        using var stream = new MemoryStream();
        WriteInt32(stream, 0);
        WriteInt32(stream, -626843481);
        WriteInt32(stream, 0);
        WriteInt32(stream, queueId);
        WriteInt32(stream, 0);
        WriteInt64(stream, queueOffset);
        WriteInt64(stream, commitLogOffset);
        WriteInt32(stream, 0);
        WriteInt64(stream, 1_700_000_000_000);
        stream.Write(new byte[] { 127, 0, 0, 1, 0x2A, 0x9F, 0, 0 });
        WriteInt64(stream, 1_700_000_000_100);
        stream.Write(new byte[] { 127, 0, 0, 1, 0x2A, 0x9F, 0, 0 });
        WriteInt32(stream, 0);
        WriteInt64(stream, 0);
        WriteInt32(stream, body.Length);
        stream.Write(body);
        stream.WriteByte(checked((byte)Encoding.UTF8.GetByteCount(topic)));
        stream.Write(Encoding.UTF8.GetBytes(topic));
        WriteUInt16(stream, checked((ushort)properties.Length));
        stream.Write(properties);

        var record = stream.ToArray();
        BinaryPrimitives.WriteInt32BigEndian(record, record.Length);
        return record;
    }

    private static void WriteInt32(Stream stream, int value)
    {
        Span<byte> bytes = stackalloc byte[sizeof(int)];
        BinaryPrimitives.WriteInt32BigEndian(bytes, value);
        stream.Write(bytes);
    }

    private static void WriteInt64(Stream stream, long value)
    {
        Span<byte> bytes = stackalloc byte[sizeof(long)];
        BinaryPrimitives.WriteInt64BigEndian(bytes, value);
        stream.Write(bytes);
    }

    private static void WriteUInt16(Stream stream, ushort value)
    {
        Span<byte> bytes = stackalloc byte[sizeof(ushort)];
        BinaryPrimitives.WriteUInt16BigEndian(bytes, value);
        stream.Write(bytes);
    }

    private protected static TopicRouteData Route(int queueCount) => new()
    {
        QueueDatas =
        [
            new QueueData
            {
                BrokerName = "broker-a",
                ReadQueueNums = queueCount,
                WriteQueueNums = queueCount,
                Perm = 6
            }
        ],
        BrokerDatas =
        [
            new BrokerData
            {
                BrokerName = "broker-a",
                BrokerAddrs = new ConcurrentDictionary<long, string>(
                    [new KeyValuePair<long, string>(0, "127.0.0.1:10911")])
            }
        ]
    };

    private protected static TopicRouteData MultiBrokerRoute() => new()
    {
        QueueDatas = Enumerable.Range(0, 3)
            .Select(static brokerIndex => new QueueData
            {
                BrokerName = $"broker-{(char)('a' + brokerIndex)}",
                ReadQueueNums = 3,
                WriteQueueNums = 3,
                Perm = 6
            })
            .ToArray(),
        BrokerDatas = Enumerable.Range(0, 3)
            .Select(static brokerIndex => new BrokerData
            {
                BrokerName = $"broker-{(char)('a' + brokerIndex)}",
                BrokerAddrs = new ConcurrentDictionary<long, string>(
                    [new KeyValuePair<long, string>(0, $"127.0.0.1:{10911 + (brokerIndex * 10)}")])
            })
            .ToArray()
    };

    private protected static IReadOnlyList<RemotingConsumerQueue> CreateMultiBrokerQueues() =>
        Enumerable.Range(0, 3)
            .SelectMany(brokerIndex => Enumerable.Range(0, 3)
                .Select(queueId => new RemotingConsumerQueue(
                    "orders",
                    $"broker-{(char)('a' + brokerIndex)}",
                    queueId)))
            .ToArray();

    private protected static RemotingPushConsumerOptions CreateBroadcastOptions(
        string offsetPath,
        Func<IReadOnlyList<RemotingMessageView>, RemotingPushConsumeContext, CancellationToken, ValueTask<ConsumeResult>> handler)
    {
        var options = WithMessageHandler(new RemotingPushConsumerOptions
        {
            GroupName = "broadcast-group",
            ConsumerMode = ConsumerMode.Broadcasting,
            LocalOffsetStorePath = offsetPath,
            InitialPosition = ConsumeFromPosition.Beginning,
            MaxConcurrency = 2,
            PullMaxCachedMessages = 8,
            LongPollingTimeout = TimeSpan.FromSeconds(1),
        }, handler);
        options.Subscribe("orders");
        return options;
    }

    private protected static RemotingPushConsumerOptions CreateClusteringOptions() => WithMessageHandler(
        new RemotingPushConsumerOptions
        {
            GroupName = "legacy-group",
            MaxConcurrency = 2,
            PullMaxCachedMessages = 8,
            LongPollingTimeout = TimeSpan.FromSeconds(1)
        },
        static (_, _, _) => ValueTask.FromResult(ConsumeResult.Success));

    private protected static RemotingPushConsumerOptions WithMessageHandler(
        RemotingPushConsumerOptions options,
        Func<
            IReadOnlyList<RemotingMessageView>,
            RemotingPushConsumeContext,
            CancellationToken,
            ValueTask<ConsumeResult>> messageHandler)
    {
        MessageHandlers.Add(options, messageHandler);
        return options;
    }

    private protected static Func<
        IReadOnlyList<RemotingMessageView>,
        RemotingPushConsumeContext,
        CancellationToken,
        ValueTask<ConsumeResult>> GetMessageHandler(RemotingPushConsumerOptions options) =>
        MessageHandlers.TryGetValue(options, out var messageHandler)
            ? messageHandler
            : throw new InvalidOperationException("No test message handler was registered for these options.");

    private protected static RemotingPushConsumer CreateRemotingPushConsumer(
        RemotingPushConsumerOptions options,
        ITopicRouteService routes,
        IRemotingClient remoting,
        string instanceName,
        IRemotingRocketMQTelemetry? telemetry = null,
        IRemotingRebalanceService? rebalanceService = null) => CreateRemotingPushConsumer(
            Options.Create(options),
            Options.Create(new RemotingClientOptions
            {
                ClientIP = "127.0.0.1",
                InstanceName = instanceName,
                PollNameServerInterval = TimeSpan.FromHours(1),
                HeartbeatBrokerInterval = TimeSpan.FromHours(1)
            }),
            routes,
            remoting,
            TimeProvider.System,
            NullLogger<RemotingPushConsumer>.Instance,
            telemetry,
            rebalanceService);

    private protected static RemotingPushConsumer CreateRemotingPushConsumer(
        IOptions<RemotingPushConsumerOptions> options,
        IOptions<RemotingClientOptions> clientOptions,
        ITopicRouteService routes,
        IRemotingClient remoting,
        TimeProvider timeProvider,
        ILogger<RemotingPushConsumer> logger,
        IRemotingRocketMQTelemetry? telemetry = null,
        IRemotingRebalanceService? rebalanceService = null)
    {
        var routeResolver = new RemotingConsumerRouteResolver(clientOptions, routes);
        var consumerEngine = CreateRemotingConsumerEngine(
            options.Value,
            options.Value.PullBatchSize,
            options.Value.MaxMessageBytes,
            options.Value.LongPollingTimeout,
            clientOptions,
            routeResolver,
            remoting,
            timeProvider,
            telemetry);
        return new RemotingPushConsumer(
            options,
            clientOptions,
            consumerEngine,
            routeResolver,
            remoting,
            rebalanceService ?? new TestRemotingRebalanceService(),
            timeProvider,
            logger,
            GetMessageHandler(options.Value),
            telemetry);
    }

    private protected static RemotingConsumerEngine CreateRemotingConsumerEngine(
        ConsumerOptions options,
        int batchSize,
        int maxMessageBytes,
        TimeSpan longPollingTimeout,
        IOptions<RemotingClientOptions> clientOptions,
        RemotingConsumerRouteResolver routes,
        IRemotingClient remoting,
        TimeProvider timeProvider,
        IRemotingRocketMQTelemetry? telemetry = null) =>
        new(
            new RemotingConsumerSettings(
                options.GroupName,
                batchSize,
                maxMessageBytes,
                longPollingTimeout),
            clientOptions,
            routes,
            remoting,
            timeProvider,
            telemetry);

    private protected static Mock<ITopicRouteService> CreateRouteServiceMock(
        int queueCount = 1,
        ConcurrentQueue<string>? topics = null)
    {
        var routes = new Mock<ITopicRouteService>(MockBehavior.Strict);
        routes
            .Setup(value => value.GetAsync(
                It.IsAny<string>(),
                It.IsAny<bool>(),
                It.IsAny<CancellationToken>()))
            .Returns<string, bool, CancellationToken>((topic, _, _) =>
            {
                topics?.Enqueue(topic);
                return Task.FromResult(Route(queueCount));
            });
        return routes;
    }

    private protected sealed class ProcessTelemetry(IRemotingRocketMQTelemetryOperation processOperation) : IRemotingRocketMQTelemetry
    {
        private int _processBatchCalls;

        public int ProcessBatchCalls => Volatile.Read(ref _processBatchCalls);

        public IRemotingRocketMQTelemetryOperation StartSend(string topic, int messageCount, long bodySize) =>
            RemotingRocketMQTelemetry.Operation.Disabled;

        public IRemotingRocketMQTelemetryOperation StartReceive(
            string topic,
            string consumerGroup,
            int messageCount,
            ActivityContext? parentContext,
            DateTimeOffset startTime,
            long startTimestamp,
            int? queueId = null,
            bool createActivity = true,
            IEnumerable<IReadOnlyDictionary<string, string>>? messageProperties = null) =>
            RemotingRocketMQTelemetry.Operation.Disabled;

        public IRemotingRocketMQTelemetryOperation StartProcess(
            string topic,
            string consumerGroup,
            string messageId,
            int queueId,
            long bodySize,
            IReadOnlyDictionary<string, string> properties,
            ActivityContext? receiveContext) =>
            RemotingRocketMQTelemetry.Operation.Disabled;

        public IRemotingRocketMQTelemetryOperation StartProcessBatch(
            string topic,
            string consumerGroup,
            IReadOnlyList<RemotingMessageView> messages)
        {
            Interlocked.Increment(ref _processBatchCalls);
            return processOperation;
        }

        public IRemotingRocketMQTelemetryOperation StartSettle(
            string operationName,
            string topic,
            string consumerGroup,
            string? messageId,
            int? queueId,
            IReadOnlyDictionary<string, string>? properties) =>
            RemotingRocketMQTelemetry.Operation.Disabled;

        public void InjectContext(Activity? activity, IDictionary<string, string> properties)
        {
        }
    }

    private protected sealed class FakeRemotingClient(string clientId) : IRemotingClient, IRemotingRequestHandlerRegistry
    {
        private readonly TaskCompletionSource _pullChanged =
            new(TaskCreationOptions.RunContinuationsAsynchronously);
        private readonly TaskCompletionSource _pullCanceled =
            new(TaskCreationOptions.RunContinuationsAsynchronously);
        private readonly ConcurrentDictionary<(string Topic, int QueueId), long> _committedOffsets = new();
        private readonly ConcurrentDictionary<int, RemotingRequestHandler> _requestHandlers = new();
        private string[] _consumerIds = [clientId];
        private int _failNextOffsetUpdate;

        public ConcurrentQueue<byte[]> HeartbeatBodies { get; } = new();
        public ConcurrentQueue<(string Topic, int QueueId, long Offset)> PullOffsets { get; } = new();
        public ConcurrentQueue<string> CanceledPullTopics { get; } = new();
        public ConcurrentQueue<(string Topic, long Offset)> UpdatedOffsets { get; } = new();
        public ConcurrentQueue<RemotingCommand> Requests { get; } = new();
        public ConcurrentQueue<RemotingCommand> OnewayRequests { get; } = new();
        public Func<RemotingCommand, CancellationToken, Task<RemotingCommand>>? HeartbeatHandler { get; init; }
        public Func<RemotingCommand, CancellationToken, Task<RemotingCommand>>? ConsumerIdsHandler { get; init; }
        public Func<RemotingCommand, CancellationToken, Task<RemotingCommand>>? ConsumerOffsetHandler { get; init; }
        public Func<RemotingCommand, CancellationToken, Task<RemotingCommand>>? LockHandler { get; set; }
        public Func<RemotingCommand, CancellationToken, Task<RemotingCommand>>? MaxOffsetHandler { get; init; }
        public Func<RemotingCommand, CancellationToken, Task<RemotingCommand>>? PullHandler { get; init; }
        public Func<RemotingCommand, CancellationToken, Task<RemotingCommand>>? QueryAssignmentHandler { get; init; }
        public Func<RemotingCommand, CancellationToken, Task<RemotingCommand>>? PopHandler { get; init; }
        public Func<RemotingCommand, CancellationToken, Task<RemotingCommand>>? AckHandler { get; init; }
        public Func<RemotingCommand, CancellationToken, Task<RemotingCommand>>? ChangeInvisibleHandler { get; init; }
        public Func<RemotingCommand, CancellationToken, Task<RemotingCommand>>? SendBackHandler { get; init; }
        public Func<RemotingCommand, CancellationToken, Task<RemotingCommand>>? UpdateOffsetHandler { get; init; }
        public Func<RemotingCommand, CancellationToken, Task<RemotingCommand>>? UnregisterHandler { get; init; }
        public bool TrackCommittedOffsets { get; init; }
        public int CanceledPulls;
        public int UnregisterCalls;

        public int ActiveRequestHandlerRegistrations =>
            _requestHandlers.Count;

        public void SetConsumerIds(params string[] consumerIds) =>
            Volatile.Write(ref _consumerIds, consumerIds);

        public void FailNextOffsetUpdate() => Interlocked.Exchange(ref _failNextOffsetUpdate, 1);

        public IDisposable RegisterRequestHandler(int requestCode, RemotingRequestHandler handler)
        {
            if (requestCode is not RequestCode.NotifyConsumerIdsChanged and not RequestCode.ResetConsumerOffset)
            {
                throw new ArgumentOutOfRangeException(nameof(requestCode));
            }

            if (!_requestHandlers.TryAdd(requestCode, handler))
            {
                throw new InvalidOperationException($"Request handler {requestCode} is already registered.");
            }

            return new CallbackRegistration(() =>
                ((ICollection<KeyValuePair<int, RemotingRequestHandler>>)_requestHandlers).Remove(
                    new KeyValuePair<int, RemotingRequestHandler>(requestCode, handler)));
        }

        public async ValueTask<RemotingCommand?> NotifyConsumerIdsChangedAsync(
            string consumerGroup,
            CancellationToken cancellationToken)
        {
            var result = await InvokeBrokerRequestAsync(
                RequestCode.NotifyConsumerIdsChanged,
                new Dictionary<string, object> { ["consumerGroup"] = consumerGroup },
                body: null,
                cancellationToken);
            return result.Response;
        }

        public ValueTask<RemotingRequestResult> InvokeBrokerRequestAsync(
            int requestCode,
            Dictionary<string, object> fields,
            byte[]? body,
            CancellationToken cancellationToken)
        {
            if (!_requestHandlers.TryGetValue(requestCode, out var handler))
            {
                throw new InvalidOperationException($"Request handler {requestCode} is not registered.");
            }

            return handler(new RemotingRequestContext(
                new IPEndPoint(IPAddress.Loopback, 10911),
                new RemotingCommand
                {
                    Code = requestCode,
                    ExtFields = fields,
                    Body = body
                },
                cancellationToken));
        }

        public async Task<RemotingCommand> InvokeAsync(
            EndPoint endPoint,
            RemotingCommand request,
            TimeSpan timeout,
            CancellationToken cancellationToken = default)
        {
            Requests.Enqueue(request);
            switch (request.Code)
            {
                case RequestCode.HeartBeat:
                    HeartbeatBodies.Enqueue(request.Body!);
                    return HeartbeatHandler is null
                        ? Success()
                        : await HeartbeatHandler(request, cancellationToken);
                case RequestCode.GetConsumerListByGroup:
                    return ConsumerIdsHandler is null
                        ? Success(JsonSerializer.SerializeToUtf8Bytes(new
                        {
                            consumerIdList = Volatile.Read(ref _consumerIds)
                        }))
                        : await ConsumerIdsHandler(request, cancellationToken);
                case RequestCode.LockBatchMq:
                    return LockHandler is null
                        ? LockSuccess(request)
                        : await LockHandler(request, cancellationToken);
                case RequestCode.QueryConsumerOffset:
                    if (ConsumerOffsetHandler is not null)
                    {
                        return await ConsumerOffsetHandler(request, cancellationToken);
                    }

                    if (TrackCommittedOffsets &&
                        _committedOffsets.TryGetValue((
                            Assert.IsType<string>(request.ExtFields["topic"]),
                            Convert.ToInt32(request.ExtFields["queueId"])), out var committedOffset))
                    {
                        return Success(offset: committedOffset);
                    }

                    return new RemotingCommand { Code = ResponseCodes.ResQueryNotFound };
                case RequestCode.GetMinOffset:
                    return Success(offset: 0);
                case RequestCode.GetMaxOffset:
                    return MaxOffsetHandler is null
                        ? Success(offset: 17)
                        : await MaxOffsetHandler(request, cancellationToken);
                case RequestCode.SearchOffsetByTimestamp:
                    return Success(offset: 9);
                case RequestCode.UpdateConsumerOffset:
                    var offsetKey = (
                        Assert.IsType<string>(request.ExtFields["topic"]),
                        Convert.ToInt32(request.ExtFields["queueId"]));
                    var updatedOffset = Convert.ToInt64(request.ExtFields["commitOffset"]);
                    if (Interlocked.Exchange(ref _failNextOffsetUpdate, 0) != 0)
                    {
                        throw new IOException("The configured offset update failed.");
                    }

                    if (UpdateOffsetHandler is not null)
                    {
                        var response = await UpdateOffsetHandler(request, cancellationToken);
                        UpdatedOffsets.Enqueue((offsetKey.Item1, updatedOffset));
                        return response;
                    }

                    UpdatedOffsets.Enqueue((offsetKey.Item1, updatedOffset));
                    if (TrackCommittedOffsets)
                    {
                        _committedOffsets[offsetKey] = updatedOffset;
                    }

                    return Success();
                case RequestCode.PullMessage:
                    PullOffsets.Enqueue((
                        Assert.IsType<string>(request.ExtFields["topic"]),
                        Convert.ToInt32(request.ExtFields["queueId"]),
                        Convert.ToInt64(request.ExtFields["queueOffset"])));
                    _pullChanged.TrySetResult();
                    try
                    {
                        if (PullHandler is not null)
                        {
                            return await PullHandler(request, cancellationToken);
                        }

                        await Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken);
                        throw new InvalidOperationException("An infinite pull completed unexpectedly.");
                    }
                    catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                    {
                        CanceledPullTopics.Enqueue(
                            Assert.IsType<string>(request.ExtFields["topic"]));
                        Interlocked.Increment(ref CanceledPulls);
                        _pullCanceled.TrySetResult();
                        throw;
                    }
                case RequestCode.QueryAssignment:
                    return QueryAssignmentHandler is null
                        ? throw new InvalidOperationException("No query-assignment response was configured.")
                        : await QueryAssignmentHandler(request, cancellationToken);
                case RequestCode.PopMessage:
                    return PopHandler is null
                        ? throw new InvalidOperationException("No POP response was configured.")
                        : await PopHandler(request, cancellationToken);
                case RequestCode.AckMessage:
                    return AckHandler is null
                        ? throw new InvalidOperationException("No POP acknowledgement response was configured.")
                        : await AckHandler(request, cancellationToken);
                case RequestCode.ChangeMessageInvisibleTime:
                    return ChangeInvisibleHandler is null
                        ? throw new InvalidOperationException("No POP invisibility response was configured.")
                        : await ChangeInvisibleHandler(request, cancellationToken);
                case RequestCode.ConsumerSendMsgBack:
                    return SendBackHandler is null
                        ? Success()
                        : await SendBackHandler(request, cancellationToken);
                case RequestCode.UnregisterClient:
                    Interlocked.Increment(ref UnregisterCalls);
                    return UnregisterHandler is null
                        ? Success()
                        : await UnregisterHandler(request, cancellationToken);
                default:
                    return Success();
            }
        }

        public async Task WaitForPullsAsync(int count, CancellationToken cancellationToken)
        {
            while (PullOffsets.Count < count)
            {
                await _pullChanged.Task.WaitAsync(TimeSpan.FromSeconds(5), cancellationToken);
                if (PullOffsets.Count < count)
                {
                    await Task.Delay(10, cancellationToken);
                }
            }
        }

        public async Task WaitForCanceledPullsAsync(int count, CancellationToken cancellationToken)
        {
            while (Volatile.Read(ref CanceledPulls) < count)
            {
                await _pullCanceled.Task.WaitAsync(TimeSpan.FromSeconds(5), cancellationToken);
                if (Volatile.Read(ref CanceledPulls) < count)
                {
                    await Task.Delay(10, cancellationToken);
                }
            }
        }

        public async Task WaitForTopicPullsAsync(
            string topic,
            int count,
            CancellationToken cancellationToken)
        {
            while (PullOffsets.Count(pull => pull.Topic == topic) < count)
            {
                await _pullChanged.Task.WaitAsync(TimeSpan.FromSeconds(5), cancellationToken);
                if (PullOffsets.Count(pull => pull.Topic == topic) < count)
                {
                    await Task.Delay(10, cancellationToken);
                }
            }
        }

        public async Task WaitForRequestCountAsync(
            int requestCode,
            int count,
            CancellationToken cancellationToken)
        {
            while (Requests.Count(request => request.Code == requestCode) < count)
            {
                await Task.Delay(10, cancellationToken);
            }
        }

        public Task InvokeOnewayAsync(
            EndPoint endPoint,
            RemotingCommand request,
            TimeSpan timeout,
            CancellationToken cancellationToken = default)
        {
            Requests.Enqueue(request);
            OnewayRequests.Enqueue(request);
            return Task.CompletedTask;
        }

        public ValueTask DisposeAsync() => ValueTask.CompletedTask;

        private static RemotingCommand Success(byte[]? body = null, long? offset = null) => new()
        {
            Code = ResponseCodes.ResSuccess,
            Body = body,
            ExtFields = offset.HasValue
                ? new Dictionary<string, object> { ["offset"] = offset.Value.ToString() }
                : new Dictionary<string, object>()
        };

        private static RemotingCommand LockSuccess(RemotingCommand request)
        {
            using var body = JsonDocument.Parse(request.Body!);
            return Success(JsonSerializer.SerializeToUtf8Bytes(new
            {
                lockOKMQSet = body.RootElement.GetProperty("mqSet").Clone()
            }));
        }

        private sealed class CallbackRegistration(Action callback) : IDisposable
        {
            private Action? _callback = callback;

            public void Dispose() => Interlocked.Exchange(ref _callback, null)?.Invoke();
        }
    }
}
