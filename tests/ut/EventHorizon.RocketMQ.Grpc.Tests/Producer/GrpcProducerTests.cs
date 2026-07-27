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
using EventHorizon.RocketMQ.Grpc.Exceptions;
using EventHorizon.RocketMQ.Grpc.Producer;
using EventHorizon.RocketMQ.Grpc.Producer.Transactions;
using EventHorizon.RocketMQ.Grpc.Protocol;
using EventHorizon.RocketMQ.Grpc.Protocol.Route;
using EventHorizon.RocketMQ.Grpc.Protocol.Telemetry;
using Google.Protobuf;
using Google.Protobuf.WellKnownTypes;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Moq;
using Xunit;
using Proto = Apache.Rocketmq.V2;

namespace EventHorizon.RocketMQ.Grpc.Tests.Producer;

public sealed class GrpcProducerTests
{
    [Fact]
    public void Constructor_RejectsNonPositiveTransactionCheckConcurrency()
    {
        var options = TransactionOptions();
        options.MaxConcurrentTransactionChecks = 0;

        Assert.Throws<ArgumentOutOfRangeException>(() => CreateProducer(new FakeGrpcClient(), options));
    }

    [Fact]
    public void QueueAndHashHelpers_RejectInvalidArguments()
    {
        Assert.Throws<ArgumentNullException>(() => GrpcProducer.ComputeFifoHash(null!));
        Assert.Throws<ArgumentNullException>(() => GrpcProducer.GetFifoQueueIndex(null!, 1));
        Assert.Throws<ArgumentOutOfRangeException>(() => GrpcProducer.GetFifoQueueIndex("orders", 0));
        Assert.Throws<ArgumentNullException>(() => GrpcProducer.AcceptsMessageType(null!, Proto.MessageType.Normal));
    }

    [Fact]
    public void GetRetryDelay_HandlesCustomizedExponentialAndInvalidPolicies()
    {
        var customized = new Proto.RetryPolicy
        {
            CustomizedBackoff = new Proto.CustomizedBackoff
            {
                Next =
                {
                    Duration.FromTimeSpan(TimeSpan.FromSeconds(1)),
                    Duration.FromTimeSpan(TimeSpan.FromSeconds(3))
                }
            }
        };
        var exponential = new Proto.RetryPolicy
        {
            ExponentialBackoff = new Proto.ExponentialBackoff
            {
                Initial = Duration.FromTimeSpan(TimeSpan.FromSeconds(2)),
                Max = Duration.FromTimeSpan(TimeSpan.FromSeconds(5)),
                Multiplier = 2
            }
        };
        var invalidExponential = new Proto.RetryPolicy
        {
            ExponentialBackoff = new Proto.ExponentialBackoff
            {
                Initial = new Duration { Nanos = -1 },
                Max = Duration.FromTimeSpan(TimeSpan.FromSeconds(5)),
                Multiplier = 0
            }
        };

        Assert.Equal(TimeSpan.Zero, GrpcProducer.GetRetryDelay(null, 1));
        Assert.Equal(TimeSpan.Zero, GrpcProducer.GetRetryDelay(customized, 0));
        Assert.Equal(TimeSpan.FromSeconds(1), GrpcProducer.GetRetryDelay(customized, 1));
        Assert.Equal(TimeSpan.FromSeconds(3), GrpcProducer.GetRetryDelay(customized, 3));
        Assert.Equal(TimeSpan.FromSeconds(5), GrpcProducer.GetRetryDelay(exponential, 3));
        Assert.Equal(TimeSpan.Zero, GrpcProducer.GetRetryDelay(invalidExponential, 1));
        Assert.Equal(TimeSpan.Zero, GrpcProducer.GetRetryDelay(new Proto.RetryPolicy(), 1));
    }

    [Fact]
    public void ComputeFifoHash_MatchesSipHash24ReferenceVectors()
    {
        Assert.Equal(0x726fdb47dd0e0e31UL, GrpcProducer.ComputeFifoHash(string.Empty));
        Assert.Equal(0x74f839c593dc67fdUL, GrpcProducer.ComputeFifoHash("\0"));
        Assert.Equal(0x0d6c8009d9a94f5aUL, GrpcProducer.ComputeFifoHash("\0\u0001"));
        Assert.Equal(0x85676696d7fb7e2dUL, GrpcProducer.ComputeFifoHash("\0\u0001\u0002"));
    }

    [Fact]
    public void GetFifoQueueIndex_UsesStablePositiveModulo()
    {
        Assert.Equal(1, GrpcProducer.GetFifoQueueIndex("\0\u0001\u0002", 7));
    }

    [Fact]
    public void AcceptsMessageType_HonorsRouteCapabilitiesAndOlderEmptyRoutes()
    {
        var unrestricted = new Proto.MessageQueue();
        var normalOnly = new Proto.MessageQueue
        {
            AcceptMessageTypes = { Proto.MessageType.Normal }
        };

        Assert.True(GrpcProducer.AcceptsMessageType(unrestricted, Proto.MessageType.Fifo));
        Assert.True(GrpcProducer.AcceptsMessageType(normalOnly, Proto.MessageType.Normal));
        Assert.False(GrpcProducer.AcceptsMessageType(normalOnly, Proto.MessageType.Fifo));
    }

    [Fact]
    public async Task SendTransactionAsync_RetriesWithStableMessageIdentity()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var options = TransactionOptions();
        options.RetryTimesWhenSendFailed = 1;
        var client = new FakeGrpcClient
        {
            SendHandler = static (request, attempt) => attempt == 1
                ? Task.FromException<Proto.SendMessageResponse>(new IOException("transient send failure"))
                : Task.FromResult(SendSuccess(request.Messages[0].SystemProperties.MessageId, "transaction-1"))
        };
        await using var producer = CreateProducer(client, options);
        await producer.StartAsync(cancellationToken);

        var transaction = await producer.SendTransactionAsync(new Message("orders", [1, 2, 3]), cancellationToken);

        Assert.Equal("transaction-1", transaction.TransactionId);
        Assert.Equal("transaction-1", transaction.SendReceipt.TransactionId);
        Assert.Equal(transaction.MessageId, transaction.SendReceipt.MessageId);
        Assert.Equal(2, client.SendRequests.Count);
        Assert.Equal(
            client.SendRequests[0].Messages[0].SystemProperties.MessageId,
            client.SendRequests[1].Messages[0].SystemProperties.MessageId);
        Assert.All(
            client.SendRequests,
            request => Assert.Equal(Proto.MessageType.Transaction, request.Messages[0].SystemProperties.MessageType));
        Assert.Contains(
            client.OpenSettings.Single().Publishing.Topics,
            topic => topic.Name == "orders" && topic.ResourceNamespace == "tenant-a");
    }

    [Fact]
    public async Task SendAsync_ConcurrentNewTopicsKeepEveryTopicInTelemetrySettings()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var firstSettingsRead = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseFirstSettingsRead = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var settingsUpdates = new ConcurrentQueue<Proto.Settings>();
        var serverSettingsReads = 0;
        var completion = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var session = new Mock<IGrpcTelemetrySession>(MockBehavior.Strict);
        session
            .SetupGet(value => value.ServerSettings)
            .Returns(() =>
            {
                if (Interlocked.Increment(ref serverSettingsReads) == 1)
                {
                    firstSettingsRead.TrySetResult();
                    releaseFirstSettingsRead.Task.GetAwaiter().GetResult();
                }

                return null;
            });
        session.SetupGet(value => value.Completion).Returns(completion.Task);
        session
            .Setup(value => value.WriteSettingsAsync(
                It.IsAny<Proto.Settings>(),
                It.IsAny<CancellationToken>()))
            .Callback<Proto.Settings, CancellationToken>((settings, _) => settingsUpdates.Enqueue(settings.Clone()))
            .Returns(Task.CompletedTask);
        session
            .Setup(value => value.WriteCommandAsync(
                It.IsAny<Proto.TelemetryCommand>(),
                It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask);
        session
            .Setup(value => value.DisposeAsync())
            .Callback(() => completion.TrySetResult())
            .Returns(ValueTask.CompletedTask);
        var client = new FakeGrpcClient
        {
            OpenTelemetryHandler = _ => Task.FromResult(session.Object)
        };
        var options = new GrpcProducerOptions
        {
            RetryTimesWhenSendFailed = 0,
            TransactionChecker = static (_, _) => ValueTask.FromResult(TransactionResolution.Unknown)
        };
        await using var producer = CreateProducer(client, options);
        await producer.StartAsync(cancellationToken);

        var firstSend = Task.Run(async () => await producer.SendAsync(
            new Message("alpha", [1]), cancellationToken).ConfigureAwait(false));
        await firstSettingsRead.Task.WaitAsync(TimeSpan.FromSeconds(3), cancellationToken);

        try
        {
            await producer.SendAsync(new Message("beta", [2]), cancellationToken);
        }
        finally
        {
            releaseFirstSettingsRead.TrySetResult();
        }

        await firstSend.WaitAsync(TimeSpan.FromSeconds(3), cancellationToken);

        var updates = settingsUpdates.ToArray();
        Assert.Equal(2, updates.Length);
        Assert.All(
            updates,
            settings => Assert.Equal(
                ["alpha", "beta"],
                settings.Publishing.Topics.Select(static topic => topic.Name).OrderBy(static topic => topic)));
    }

    [Fact]
    public async Task SendAsync_RetryExcludesEveryQueueOnFailedBroker()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var options = TransactionOptions();
        options.RetryTimesWhenSendFailed = 1;
        var client = new FakeGrpcClient
        {
            SendHandler = static (request, attempt) => attempt == 1
                ? Task.FromException<Proto.SendMessageResponse>(new IOException("broker unavailable"))
                : Task.FromResult(SendSuccess(request.Messages[0].SystemProperties.MessageId, string.Empty))
        };
        var secondBroker = new Uri("http://127.0.0.1:8082");
        var routes = CreateRouteService(
        [
            Queue("orders", 0, "broker-a", client.BrokerEndpoint),
            Queue("orders", 1, "broker-a", client.BrokerEndpoint),
            Queue("orders", 2, "broker-a", client.BrokerEndpoint),
            Queue("orders", 3, "broker-b", secondBroker)
        ]);
        await using var producer = CreateProducer(client, options, routes);
        await producer.StartAsync(cancellationToken);

        var result = await producer.SendAsync(new Message("orders", [1]), cancellationToken);

        Assert.NotEmpty(result.MessageId);
        Assert.Equal(42, result.Offset);
        Assert.Null(result.TransactionId);
        Assert.Null(result.RecallHandle);
        Assert.Equal([secondBroker], result.Endpoints);
        Assert.Equal([client.BrokerEndpoint, secondBroker], client.SendEndpoints);
    }

    [Fact]
    public async Task SendAsync_MapsReceipt()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var client = new FakeGrpcClient
        {
            SendHandler = static (_, _) =>
                Task.FromResult(SendSuccess("message-1", "transaction-1", "recall-1", 43))
        };
        await using var producer = CreateProducer(client, TransactionOptions());
        await producer.StartAsync(cancellationToken);

        var receipt = await producer.SendAsync(new Message("orders", [1]), cancellationToken);

        Assert.Equal("message-1", receipt.MessageId);
        Assert.Equal(43, receipt.Offset);
        Assert.Equal("transaction-1", receipt.TransactionId);
        Assert.Equal("recall-1", receipt.RecallHandle);
        Assert.Equal([client.BrokerEndpoint], receipt.Endpoints);
    }

    [Fact]
    public async Task SendAsync_AppliesBrokerPublishingLimitAndRetryPolicy()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var options = TransactionOptions();
        options.RetryTimesWhenSendFailed = 10;
        var settings = new Proto.Settings
        {
            Publishing = new Proto.Publishing
            {
                MaxBodySize = 2,
                ValidateMessageType = true
            },
            BackoffPolicy = new Proto.RetryPolicy
            {
                MaxAttempts = 2,
                CustomizedBackoff = new Proto.CustomizedBackoff
                {
                    Next = { Duration.FromTimeSpan(TimeSpan.FromMilliseconds(1)) }
                }
            }
        };
        var client = new FakeGrpcClient
        {
            OpenTelemetryHandler = _ => Task.FromResult(CreateTelemetrySession(settings).Session),
            SendHandler = static (_, _) =>
                Task.FromException<Proto.SendMessageResponse>(new IOException("transient send failure"))
        };
        await using var producer = CreateProducer(client, options);
        await producer.StartAsync(cancellationToken);

        await Assert.ThrowsAsync<ArgumentException>(() => producer.SendAsync(
            new Message("orders", [1, 2, 3]),
            cancellationToken));
        await Assert.ThrowsAsync<RocketMQClientException>(() => producer.SendAsync(
            new Message("orders", [1, 2]),
            cancellationToken));

        Assert.Equal(2, client.SendRequests.Count);
    }

    [Fact]
    public async Task SendAsync_RejectsSuccessfulResponseWithoutResultEntry()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var client = new FakeGrpcClient
        {
            SendHandler = static (_, _) => Task.FromResult(new Proto.SendMessageResponse
            {
                Status = new Proto.Status { Code = Proto.Code.Ok }
            })
        };
        await using var producer = CreateProducer(client, TransactionOptions());
        await producer.StartAsync(cancellationToken);

        await Assert.ThrowsAsync<InvalidDataException>(() => producer.SendAsync(
            new Message("orders", [1]),
            cancellationToken));
    }

    [Fact]
    public async Task SendAsync_RejectsSuccessfulResponseWithMultipleResultEntries()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var response = SendSuccess("message-1", string.Empty);
        response.Entries.Add(new Proto.SendResultEntry
        {
            Status = OkStatus(),
            MessageId = "message-2"
        });
        var client = new FakeGrpcClient
        {
            SendHandler = (_, _) => Task.FromResult(response)
        };
        await using var producer = CreateProducer(client, TransactionOptions());
        await producer.StartAsync(cancellationToken);

        await Assert.ThrowsAsync<InvalidDataException>(() => producer.SendAsync(
            new Message("orders", [1]),
            cancellationToken));
    }

    [Fact]
    public async Task SendAsync_RejectsSuccessfulResponseWithoutMessageId()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var client = new FakeGrpcClient
        {
            SendHandler = static (_, _) => Task.FromResult(SendSuccess(string.Empty, string.Empty))
        };
        await using var producer = CreateProducer(client, TransactionOptions());
        await producer.StartAsync(cancellationToken);

        await Assert.ThrowsAsync<InvalidDataException>(() => producer.SendAsync(
            new Message("orders", [1]),
            cancellationToken));
    }

    [Theory]
    [InlineData(true, (int)Proto.TransactionResolution.Commit)]
    [InlineData(false, (int)Proto.TransactionResolution.Rollback)]
    public async Task TransactionResolution_SendsOriginalReceiptToBroker(
        bool commit,
        int expectedResolution)
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var options = TransactionOptions();
        var client = new FakeGrpcClient();
        await using var producer = CreateProducer(client, options);
        await producer.StartAsync(cancellationToken);
        var message = new Message("orders", [4, 5, 6]);
        var transaction = await producer.SendTransactionAsync(message, cancellationToken);

        if (commit)
        {
            await transaction.CommitAsync(cancellationToken);
        }
        else
        {
            await transaction.RollbackAsync(cancellationToken);
        }

        var resolution = Assert.Single(client.EndTransactions);
        Assert.Equal(new Uri("http://127.0.0.1:8081"), resolution.Endpoint);
        Assert.Equal("orders", resolution.Request.Topic.Name);
        Assert.Equal("tenant-a", resolution.Request.Topic.ResourceNamespace);
        Assert.Equal(transaction.MessageId, resolution.Request.MessageId);
        Assert.Equal(transaction.TransactionId, resolution.Request.TransactionId);
        Assert.Equal((Proto.TransactionResolution)expectedResolution, resolution.Request.Resolution);
        Assert.Equal(Proto.TransactionSource.SourceClient, resolution.Request.Source);
    }

    [Fact]
    public async Task FailedCommit_CanOnlyBeRetriedAsCommit()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var options = TransactionOptions();
        var client = new FakeGrpcClient
        {
            EndTransactionHandler = static (_, attempt) => attempt == 1
                ? Task.FromException<Proto.EndTransactionResponse>(new IOException("outcome unknown"))
                : Task.FromResult(EndTransactionSuccess())
        };
        await using var producer = CreateProducer(client, options);
        await producer.StartAsync(cancellationToken);
        var transaction = await producer.SendTransactionAsync(new Message("orders", [1]), cancellationToken);

        await Assert.ThrowsAsync<IOException>(() => transaction.CommitAsync(cancellationToken));
        await Assert.ThrowsAsync<InvalidOperationException>(() => transaction.RollbackAsync(cancellationToken));
        await transaction.CommitAsync(cancellationToken);
        await transaction.CommitAsync(cancellationToken);

        Assert.Equal(2, client.EndTransactions.Count);
        Assert.All(
            client.EndTransactions,
            item => Assert.Equal(Proto.TransactionResolution.Commit, item.Request.Resolution));
    }

    [Fact]
    public async Task OrphanRecovery_UsesCheckerAndServerCheckSource()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        TransactionMessage? checkedMessage = null;
        var options = TransactionOptions();
        options.TransactionChecker = (message, _) =>
        {
            checkedMessage = message;
            return ValueTask.FromResult(TransactionResolution.Rollback);
        };
        var client = new FakeGrpcClient();
        await using var producer = CreateProducer(client, options);
        await producer.StartAsync(cancellationToken);

        await client.DispatchAsync(OrphanCommand(), cancellationToken);
        await WaitUntilAsync(() => client.EndTransactions.Count == 1, cancellationToken);

        Assert.NotNull(checkedMessage);
        Assert.Equal("transaction-orphan", checkedMessage.TransactionId);
        Assert.Equal("message-orphan", checkedMessage.MessageId);
        Assert.Equal("orders", checkedMessage.Topic);
        Assert.Equal("important", checkedMessage.Tag);
        Assert.Equal([7, 8, 9], checkedMessage.Body);
        Assert.Equal("value", checkedMessage.Properties["key"]);
        var resolution = Assert.Single(client.EndTransactions);
        Assert.Equal(Proto.TransactionResolution.Rollback, resolution.Request.Resolution);
        Assert.Equal(Proto.TransactionSource.SourceServerCheck, resolution.Request.Source);
        Assert.Equal("trace-parent", resolution.Request.TraceContext);
        Assert.Equal("tenant-from-server", resolution.Request.Topic.ResourceNamespace);
    }

    [Fact]
    public async Task OrphanRecovery_LeavesUnknownOrFailedChecksUnresolved()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var unknownChecked = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var failedChecked = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var options = TransactionOptions();
        options.TransactionChecker = (_, _) =>
        {
            unknownChecked.TrySetResult();
            return ValueTask.FromResult(TransactionResolution.Unknown);
        };
        var client = new FakeGrpcClient();
        await using var producer = CreateProducer(client, options);
        await producer.StartAsync(cancellationToken);

        await client.DispatchAsync(OrphanCommand(), cancellationToken);
        await unknownChecked.Task.WaitAsync(cancellationToken);
        options.TransactionChecker = (_, _) =>
        {
            failedChecked.TrySetResult();
            throw new InvalidOperationException("checker failed");
        };
        await client.DispatchAsync(OrphanCommand(), cancellationToken);
        await failedChecked.Task.WaitAsync(cancellationToken);

        Assert.Empty(client.EndTransactions);
    }

    [Fact]
    public async Task OrphanRecovery_UsesConfiguredMaximumConcurrency()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var releaseChecks = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var twoChecksStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var current = 0;
        var peak = 0;
        var options = TransactionOptions();
        options.MaxConcurrentTransactionChecks = 2;
        options.TransactionChecker = async (_, token) =>
        {
            var active = Interlocked.Increment(ref current);
            UpdateMaximum(ref peak, active);
            if (active == 2)
            {
                twoChecksStarted.TrySetResult();
            }

            try
            {
                await releaseChecks.Task.WaitAsync(token);
                return TransactionResolution.Commit;
            }
            finally
            {
                Interlocked.Decrement(ref current);
            }
        };
        var client = new FakeGrpcClient();
        await using var producer = CreateProducer(client, options);
        await producer.StartAsync(cancellationToken);

        await client.DispatchAsync(OrphanCommand("transaction-1", "message-1"), cancellationToken);
        await client.DispatchAsync(OrphanCommand("transaction-2", "message-2"), cancellationToken);
        await client.DispatchAsync(OrphanCommand("transaction-3", "message-3"), cancellationToken);
        await twoChecksStarted.Task.WaitAsync(cancellationToken);

        Assert.Equal(2, Volatile.Read(ref peak));
        Assert.Equal(2, Volatile.Read(ref current));
        releaseChecks.TrySetResult();
        await WaitUntilAsync(() => client.EndTransactions.Count == 3, cancellationToken);
        Assert.Equal(2, Volatile.Read(ref peak));
    }

    [Fact]
    public async Task Producer_CanRestartAfterInitializationFailureAndStop()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var options = TransactionOptions();
        var client = new FakeGrpcClient
        {
            OpenTelemetryHandler = static attempt => attempt == 1
                ? Task.FromException<IGrpcTelemetrySession>(new IOException("telemetry unavailable"))
                : Task.FromResult(CreateTelemetrySession().Session)
        };
        await using var producer = CreateProducer(client, options);

        await Assert.ThrowsAsync<IOException>(() => producer.StartAsync(cancellationToken).AsTask());
        await producer.StartAsync(cancellationToken);
        await producer.StopAsync(cancellationToken);
        await producer.StartAsync(cancellationToken);
        var transaction = await producer.SendTransactionAsync(new Message("orders", [1]), cancellationToken);

        Assert.Equal("transaction-1", transaction.TransactionId);
        Assert.Equal(3, client.OpenSettings.Count);
    }

    [Fact]
    public async Task TelemetrySessionFailure_ReconnectsAndContinuesOrphanRecovery()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var sessions = new ConcurrentQueue<(
            IGrpcTelemetrySession Session,
            TaskCompletionSource Completion)>();
        var options = TransactionOptions();
        options.TransactionChecker = static (_, _) => ValueTask.FromResult(TransactionResolution.Commit);
        var client = new FakeGrpcClient
        {
            OpenTelemetryHandler = _ =>
            {
                var session = CreateTelemetrySession();
                sessions.Enqueue(session);
                return Task.FromResult(session.Session);
            }
        };
        await using var producer = CreateProducer(client, options);
        await producer.StartAsync(cancellationToken);
        var firstSession = Assert.Single(sessions);

        firstSession.Completion.TrySetException(new IOException("telemetry connection lost"));
        await WaitUntilAsync(() => client.OpenSettings.Count == 2, cancellationToken);
        await client.DispatchAsync(OrphanCommand(), cancellationToken);
        await WaitUntilAsync(() => client.EndTransactions.Count == 1, cancellationToken);

        Assert.Equal(2, sessions.Count);
        Assert.All(
            client.OpenSettings,
            settings => Assert.Contains(settings.Publishing.Topics, topic => topic.Name == "orders"));
        Assert.Equal(
            Proto.TransactionSource.SourceServerCheck,
            client.EndTransactions.Single().Request.Source);
    }

    [Fact]
    public async Task SendTransactionAsync_RejectsSpecializedMessageTypes()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var options = TransactionOptions();
        var client = new FakeGrpcClient();
        await using var producer = CreateProducer(client, options);
        await producer.StartAsync(cancellationToken);

        var exception = await Assert.ThrowsAsync<ArgumentException>(() => producer.SendTransactionAsync(
            new Message("orders", [1]) { MessageGroup = "account-1" },
            cancellationToken));

        Assert.Contains("Transactional messages", exception.Message);
        Assert.Empty(client.SendRequests);
    }

    [Fact]
    public async Task SendAsync_RejectsEmptyBodyAndNegativePriority()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var options = TransactionOptions();
        var client = new FakeGrpcClient();
        await using var producer = CreateProducer(client, options);
        await producer.StartAsync(cancellationToken);

        await Assert.ThrowsAsync<ArgumentException>(() => producer.SendAsync(
            new Message("orders", []),
            cancellationToken));
        await Assert.ThrowsAsync<ArgumentException>(() => producer.SendAsync(
            new Message("orders", [1]) { Priority = -1 },
            cancellationToken));

        Assert.Empty(client.SendRequests);
    }

    [Fact]
    public async Task SendAsync_RejectsOversizedAndMutuallyExclusiveMessageProperties()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var options = TransactionOptions();
        options.MaxMessageSize = 1;
        var client = new FakeGrpcClient();
        await using var producer = CreateProducer(client, options);
        await producer.StartAsync(cancellationToken);

        await Assert.ThrowsAsync<ArgumentException>(() => producer.SendAsync(
            new Message("orders", [1, 2]),
            cancellationToken));
        await Assert.ThrowsAsync<ArgumentException>(() => producer.SendAsync(
            new Message("orders", [1])
            {
                MessageGroup = "account-1",
                Priority = 1
            },
            cancellationToken));

        Assert.Empty(client.SendRequests);
    }

    [Fact]
    public async Task SendAsync_RejectsRouteWithoutWritableQueue()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var client = new FakeGrpcClient();
        var queue = Queue("orders", 0, "broker-a", client.BrokerEndpoint);
        queue.Permission = Proto.Permission.Read;
        await using var producer = CreateProducer(
            client,
            TransactionOptions(),
            CreateRouteService([queue]));
        await producer.StartAsync(cancellationToken);

        var exception = await Assert.ThrowsAsync<RocketMQClientException>(() => producer.SendAsync(
            new Message("orders", [1]),
            cancellationToken));

        Assert.Contains("No writable message queue", exception.Message, StringComparison.Ordinal);
        Assert.Empty(client.SendRequests);
    }

    [Fact]
    public async Task SendAsync_SelectsMessageTypeCompatibleQueueAndServerRetryPolicy()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var client = new FakeGrpcClient();
        var serverSettings = new Proto.Settings
        {
            Publishing = new Proto.Publishing { ValidateMessageType = true },
            BackoffPolicy = new Proto.RetryPolicy
            {
                MaxAttempts = 2,
                CustomizedBackoff = new Proto.CustomizedBackoff
                {
                    Next = { Duration.FromTimeSpan(TimeSpan.Zero) }
                }
            }
        };
        client.OpenTelemetryHandler = _ =>
            Task.FromResult(CreateTelemetrySession(serverSettings).Session);
        var compatible = Queue("orders", 0, "broker-a", client.BrokerEndpoint);
        compatible.AcceptMessageTypes.Add(Proto.MessageType.Fifo);
        var incompatible = Queue("orders", 1, "broker-a", client.BrokerEndpoint);
        incompatible.AcceptMessageTypes.Add(Proto.MessageType.Normal);
        await using var producer = CreateProducer(
            client,
            TransactionOptions(),
            CreateRouteService([compatible, incompatible]));
        await producer.StartAsync(cancellationToken);

        await producer.SendAsync(new Message("orders", [1]) { MessageGroup = "account-1" }, cancellationToken);

        var request = Assert.Single(client.SendRequests);
        Assert.Equal(0, request.Messages[0].SystemProperties.QueueId);
        Assert.Equal(Proto.MessageType.Fifo, request.Messages[0].SystemProperties.MessageType);
    }

    [Fact]
    public async Task SendAsync_MapsEverySpecializedMessageType()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var client = new FakeGrpcClient();
        await using var producer = CreateProducer(client, TransactionOptions());
        await producer.StartAsync(cancellationToken);
        var deliveryTimestamp = DateTimeOffset.UtcNow.AddMinutes(1);
        Message[] messages =
        [
            new("orders", [1]) { MessageGroup = "account-1" },
            new("orders", [2]) { DeliveryTimestamp = deliveryTimestamp },
            new("orders", [3]) { Priority = 2 },
            new("orders", [4]) { LiteTopic = "lite-orders" }
        ];

        foreach (var message in messages)
        {
            await producer.SendAsync(message, cancellationToken);
        }

        Assert.Equal(
            [Proto.MessageType.Fifo, Proto.MessageType.Delay, Proto.MessageType.Priority, Proto.MessageType.Lite],
            client.SendRequests.Select(static request => request.Messages[0].SystemProperties.MessageType));
        Assert.Equal("account-1", client.SendRequests[0].Messages[0].SystemProperties.MessageGroup);
        Assert.Equal(deliveryTimestamp, client.SendRequests[1].Messages[0].SystemProperties.DeliveryTimestamp.ToDateTimeOffset());
        Assert.Equal(2, client.SendRequests[2].Messages[0].SystemProperties.Priority);
        Assert.Equal("lite-orders", client.SendRequests[3].Messages[0].SystemProperties.LiteTopic);
    }

    [Fact]
    public async Task SendAsync_PropagatesCancellationRaisedByTransport()
    {
        using var cancellation = new CancellationTokenSource();
        var client = new FakeGrpcClient
        {
            SendHandler = (_, _) =>
            {
                cancellation.Cancel();
                return Task.FromCanceled<Proto.SendMessageResponse>(cancellation.Token);
            }
        };
        await using var producer = CreateProducer(client, TransactionOptions());
        await producer.StartAsync(TestContext.Current.CancellationToken);

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => producer.SendAsync(
            new Message("orders", [1]),
            cancellation.Token));

        Assert.Single(client.SendRequests);
    }

    [Fact]
    public async Task Lifecycle_IsIdempotentAndStartRejectsDisposedProducer()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var producer = CreateProducer(new FakeGrpcClient(), TransactionOptions());

        await producer.StartAsync(cancellationToken);
        await producer.StartAsync(cancellationToken);
        await producer.DisposeAsync();
        await producer.DisposeAsync();

        await Assert.ThrowsAsync<ObjectDisposedException>(() => producer.StartAsync(cancellationToken).AsTask());
    }

    [Fact]
    public async Task RecallAsync_UsesResolvedBrokerEndpoint()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var client = new FakeGrpcClient();
        await using var producer = CreateProducer(client, TransactionOptions());
        await producer.StartAsync(cancellationToken);

        var messageId = await producer.RecallAsync("orders", "recall-handle", cancellationToken);

        var recall = Assert.Single(client.RecallRequests);
        Assert.Equal("recalled-message", messageId);
        Assert.Equal(client.BrokerEndpoint, recall.Endpoint);
        Assert.NotEqual(client.AccessPoint, recall.Endpoint);
        Assert.Equal("orders", recall.Request.Topic.Name);
        Assert.Equal("tenant-a", recall.Request.Topic.ResourceNamespace);
        Assert.Equal("recall-handle", recall.Request.RecallHandle);
    }

    [Fact]
    public async Task SendTransactionAsync_RequiresCheckerDeclaredTopicAndTransactionId()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var options = TransactionOptions();
        var client = new FakeGrpcClient();
        await using var producer = CreateProducer(client, options);
        await producer.StartAsync(cancellationToken);

        options.TransactionChecker = null;
        await Assert.ThrowsAsync<InvalidOperationException>(() => producer.SendTransactionAsync(
            new Message("orders", [1]),
            cancellationToken));
        options.TransactionChecker = static (_, _) => ValueTask.FromResult(TransactionResolution.Unknown);
        await Assert.ThrowsAsync<InvalidOperationException>(() => producer.SendTransactionAsync(
            new Message("undeclared", [1]),
            cancellationToken));
        client.SendHandler = static (request, _) => Task.FromResult(SendSuccess(
            request.Messages[0].SystemProperties.MessageId,
            string.Empty));
        await Assert.ThrowsAsync<InvalidDataException>(() => producer.SendTransactionAsync(
            new Message("orders", [1]),
            cancellationToken));
    }

    private static GrpcProducerOptions TransactionOptions()
    {
        var options = new GrpcProducerOptions
        {
            RetryTimesWhenSendFailed = 0,
            TransactionChecker = static (_, _) => ValueTask.FromResult(TransactionResolution.Unknown)
        };
        options.Topics.Add("orders");
        return options;
    }

    private static GrpcProducer CreateProducer(
        FakeGrpcClient client,
        GrpcProducerOptions options,
        IGrpcRouteService? routes = null) =>
        new(
            Options.Create(options),
            Options.Create(new GrpcClientOptions
            {
                Endpoint = "127.0.0.1:8080",
                Namespace = "tenant-a",
                HeartbeatInterval = TimeSpan.FromHours(1)
            }),
            client,
            routes ?? CreateRouteService(client.BrokerEndpoint),
            NullLogger<GrpcProducer>.Instance,
            NullLogger<GrpcSessionManager>.Instance);

    private static IGrpcRouteService CreateRouteService(Uri endpoint)
    {
        var routeService = new Mock<IGrpcRouteService>(MockBehavior.Strict);
        routeService
            .Setup(service => service.GetAsync(
                It.IsAny<string>(),
                It.IsAny<bool>(),
                It.IsAny<CancellationToken>()))
            .Returns((string topic, bool _, CancellationToken _) =>
                Task.FromResult<IReadOnlyList<Proto.MessageQueue>>(
                [
                    new Proto.MessageQueue
                    {
                        Topic = new Proto.Resource { Name = topic },
                        Id = 1,
                        Permission = Proto.Permission.ReadWrite,
                        Broker = new Proto.Broker
                        {
                            Name = "broker-a",
                            Endpoints = GrpcEndpoint.ToProtobuf([endpoint])
                        }
                    }
                ]));
        return routeService.Object;
    }

    private static IGrpcRouteService CreateRouteService(IReadOnlyList<Proto.MessageQueue> queues)
    {
        var routeService = new Mock<IGrpcRouteService>(MockBehavior.Strict);
        routeService
            .Setup(service => service.GetAsync(
                It.IsAny<string>(),
                It.IsAny<bool>(),
                It.IsAny<CancellationToken>()))
            .Returns(Task.FromResult(queues));
        return routeService.Object;
    }

    private static Proto.MessageQueue Queue(string topic, int id, string brokerName, Uri endpoint) => new()
    {
        Topic = new Proto.Resource { Name = topic },
        Id = id,
        Permission = Proto.Permission.ReadWrite,
        Broker = new Proto.Broker
        {
            Name = brokerName,
            Endpoints = GrpcEndpoint.ToProtobuf([endpoint])
        }
    };

    private static Proto.TelemetryCommand OrphanCommand(
        string transactionId = "transaction-orphan",
        string messageId = "message-orphan") => new()
        {
            RecoverOrphanedTransactionCommand = new Proto.RecoverOrphanedTransactionCommand
            {
                TransactionId = transactionId,
                Message = new Proto.Message
                {
                    Topic = new Proto.Resource { Name = "orders", ResourceNamespace = "tenant-from-server" },
                    Body = ByteString.CopyFrom([7, 8, 9]),
                    SystemProperties = new Proto.SystemProperties
                    {
                        MessageId = messageId,
                        Tag = "important",
                        TraceContext = "trace-parent",
                        BornTimestamp = Timestamp.FromDateTime(DateTime.UtcNow)
                    },
                    UserProperties = { ["key"] = "value" }
                }
            }
        };

    private static Proto.SendMessageResponse SendSuccess(
        string messageId,
        string transactionId,
        string recallHandle = "",
        long offset = 42) => new()
        {
            Status = OkStatus(),
            Entries =
        {
            new Proto.SendResultEntry
            {
                Status = OkStatus(),
                MessageId = messageId,
                TransactionId = transactionId,
                Offset = offset,
                RecallHandle = recallHandle
            }
        }
        };

    private static Proto.EndTransactionResponse EndTransactionSuccess() => new() { Status = OkStatus() };

    private static Proto.Status OkStatus() => new() { Code = Proto.Code.Ok };

    private static async Task WaitUntilAsync(Func<bool> condition, CancellationToken cancellationToken)
    {
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        timeout.CancelAfter(TimeSpan.FromSeconds(5));
        while (!condition())
        {
            await Task.Delay(TimeSpan.FromMilliseconds(10), timeout.Token);
        }
    }

    private static void UpdateMaximum(ref int maximum, int value)
    {
        var observed = Volatile.Read(ref maximum);
        while (value > observed)
        {
            var previous = Interlocked.CompareExchange(ref maximum, value, observed);
            if (previous == observed)
            {
                return;
            }

            observed = previous;
        }
    }

    private sealed class FakeGrpcClient : IRocketMQGrpcClient
    {
        private Func<Proto.TelemetryCommand, CancellationToken, Task>? _telemetryHandler;

        public Func<Proto.SendMessageRequest, int, Task<Proto.SendMessageResponse>> SendHandler { get; set; } =
            static (request, _) => Task.FromResult(SendSuccess(
                request.Messages[0].SystemProperties.MessageId,
                "transaction-1"));

        public Func<Proto.EndTransactionRequest, int, Task<Proto.EndTransactionResponse>> EndTransactionHandler { get; set; } =
            static (_, _) => Task.FromResult(EndTransactionSuccess());

        public Func<int, Task<IGrpcTelemetrySession>> OpenTelemetryHandler { get; set; } =
            static _ => Task.FromResult(CreateTelemetrySession().Session);

        public Uri AccessPoint { get; } = new("http://127.0.0.1:8080");
        public Uri BrokerEndpoint { get; } = new("http://127.0.0.1:8081");
        public Proto.Endpoints AccessPointProtobuf => GrpcEndpoint.ToProtobuf([AccessPoint]);
        public List<Proto.SendMessageRequest> SendRequests { get; } = [];
        public List<Uri> SendEndpoints { get; } = [];
        public ConcurrentQueue<(Uri Endpoint, Proto.EndTransactionRequest Request)> EndTransactions { get; } = [];
        public ConcurrentQueue<(Uri Endpoint, Proto.RecallMessageRequest Request)> RecallRequests { get; } = [];
        public ConcurrentQueue<Proto.Settings> OpenSettings { get; } = [];

        public Task<Proto.SendMessageResponse> SendMessageAsync(
            Uri endpoint,
            Proto.SendMessageRequest request,
            TimeSpan timeout,
            CancellationToken cancellationToken)
        {
            SendEndpoints.Add(endpoint);
            SendRequests.Add(request.Clone());
            return SendHandler(request, SendRequests.Count);
        }

        public Task<Proto.EndTransactionResponse> EndTransactionAsync(
            Uri endpoint,
            Proto.EndTransactionRequest request,
            CancellationToken cancellationToken)
        {
            EndTransactions.Enqueue((endpoint, request.Clone()));
            return EndTransactionHandler(request, EndTransactions.Count);
        }

        public Task<IGrpcTelemetrySession> OpenTelemetryAsync(
            Uri endpoint,
            Proto.Settings settings,
            Func<Proto.TelemetryCommand, CancellationToken, Task>? commandHandler,
            CancellationToken cancellationToken)
        {
            OpenSettings.Enqueue(settings.Clone());
            _telemetryHandler = commandHandler;
            return OpenTelemetryHandler(OpenSettings.Count);
        }

        public Task DispatchAsync(Proto.TelemetryCommand command, CancellationToken cancellationToken) =>
            (_telemetryHandler ?? throw new InvalidOperationException("Telemetry was not opened."))(
                command,
                cancellationToken);

        public Task<Proto.NotifyClientTerminationResponse> NotifyTerminationAsync(
            Uri endpoint,
            Proto.NotifyClientTerminationRequest request,
            CancellationToken cancellationToken) => Task.FromResult(new Proto.NotifyClientTerminationResponse { Status = OkStatus() });

        public Task<Proto.HeartbeatResponse> HeartbeatAsync(Uri endpoint, Proto.HeartbeatRequest request, CancellationToken cancellationToken) =>
            Task.FromResult(new Proto.HeartbeatResponse { Status = OkStatus() });

        public Task<Proto.QueryRouteResponse> QueryRouteAsync(Proto.QueryRouteRequest request, CancellationToken cancellationToken) => Throw<Proto.QueryRouteResponse>();
        public Task<Proto.QueryAssignmentResponse> QueryAssignmentAsync(Uri endpoint, Proto.QueryAssignmentRequest request, CancellationToken cancellationToken) => Throw<Proto.QueryAssignmentResponse>();
        public Task<IReadOnlyList<Proto.ReceiveMessageResponse>> ReceiveMessageAsync(Uri endpoint, Proto.ReceiveMessageRequest request, TimeSpan timeout, CancellationToken cancellationToken) => Throw<IReadOnlyList<Proto.ReceiveMessageResponse>>();
        public Task<Proto.AckMessageResponse> AckMessageAsync(Uri endpoint, Proto.AckMessageRequest request, CancellationToken cancellationToken) => Throw<Proto.AckMessageResponse>();
        public Task<Proto.ChangeInvisibleDurationResponse> ChangeInvisibleDurationAsync(Uri endpoint, Proto.ChangeInvisibleDurationRequest request, CancellationToken cancellationToken) => Throw<Proto.ChangeInvisibleDurationResponse>();
        public Task<Proto.ForwardMessageToDeadLetterQueueResponse> ForwardToDeadLetterQueueAsync(Uri endpoint, Proto.ForwardMessageToDeadLetterQueueRequest request, CancellationToken cancellationToken) => Throw<Proto.ForwardMessageToDeadLetterQueueResponse>();
        public Task<Proto.RecallMessageResponse> RecallMessageAsync(
            Uri endpoint,
            Proto.RecallMessageRequest request,
            CancellationToken cancellationToken)
        {
            RecallRequests.Enqueue((endpoint, request.Clone()));
            return Task.FromResult(new Proto.RecallMessageResponse
            {
                Status = OkStatus(),
                MessageId = "recalled-message"
            });
        }

        public Task<Proto.SyncLiteSubscriptionResponse> SyncLiteSubscriptionAsync(
            Uri endpoint,
            Proto.SyncLiteSubscriptionRequest request,
            CancellationToken cancellationToken) => Throw<Proto.SyncLiteSubscriptionResponse>();

        public ValueTask DisposeAsync() => ValueTask.CompletedTask;

        private static Task<T> Throw<T>() => Task.FromException<T>(new NotSupportedException());
    }

    private static (
        IGrpcTelemetrySession Session,
        TaskCompletionSource Completion) CreateTelemetrySession(Proto.Settings? serverSettings = null)
    {
        var completion = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var session = new Mock<IGrpcTelemetrySession>(MockBehavior.Strict);
        session.SetupGet(value => value.ServerSettings).Returns(serverSettings);
        session.SetupGet(value => value.Completion).Returns(completion.Task);
        session
            .Setup(value => value.WriteSettingsAsync(
                It.IsAny<Proto.Settings>(),
                It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask);
        session
            .Setup(value => value.WriteCommandAsync(
                It.IsAny<Proto.TelemetryCommand>(),
                It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask);
        session
            .Setup(value => value.DisposeAsync())
            .Callback(() => completion.TrySetResult())
            .Returns(ValueTask.CompletedTask);
        return (session.Object, completion);
    }
}
