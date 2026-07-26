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

using System.Text;
using System.Text.Json;
using EventHorizon.RocketMQ.Remoting.Consumer;
using EventHorizon.RocketMQ.Remoting.Consumer.Pull;
using EventHorizon.RocketMQ.Remoting.Consumer.Pull.Lite;
using EventHorizon.RocketMQ.Remoting.Protocol;
using Microsoft.Extensions.Options;
using Moq;
using Xunit;

namespace EventHorizon.RocketMQ.Remoting.Tests.Consumer.Pull.Lite;

public sealed class RemotingLitePullConsumerTests
{
    private const string ClientId = "127.0.0.1@lite-client";

    [Fact]
    public async Task StartAsync_HeartbeatsAndAllocatesQueuesForTheActiveConsumer()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var queues = new[]
        {
            new RemotingPullMessageQueue("orders", 0, "broker-a", "127.0.0.1:10911"),
            new RemotingPullMessageQueue("orders", 1, "broker-a", "127.0.0.1:10911")
        };
        byte[]? heartbeat = null;
        var engine = CreateEngine();
        engine
            .Setup(value => value.GetMessageQueuesAsync("orders", It.IsAny<CancellationToken>()))
            .ReturnsAsync(queues);
        engine
            .Setup(value => value.GetOffsetAsync(It.IsAny<RemotingPullMessageQueue>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(-1L);
        engine
            .Setup(value => value.QueryOffsetAsync(
                It.IsAny<RemotingPullMessageQueue>(),
                QueryOffsetPolicy.End,
                null,
                It.IsAny<CancellationToken>()))
            .ReturnsAsync(7L);
        var registration = CreateRegistration();
        var remoting = CreateRemotingClient(registration, request => request.Code switch
        {
            RequestCode.HeartBeat => CaptureHeartbeat(request, ref heartbeat),
            RequestCode.GetConsumerListByGroup => ConsumerListResponse(ClientId, "peer-client"),
            RequestCode.UnregisterClient => SuccessResponse(),
            _ => throw new InvalidOperationException($"Unexpected request code {request.Code}.")
        });
        var options = new RemotingLitePullConsumerOptions { GroupName = "orders-consumer" };
        options.Subscribe("orders");

        {
            await using var consumer = CreateConsumer(options, engine.Object, remoting.Object);
            await consumer.StartAsync(cancellationToken);

            var assigned = Assert.Single(consumer.Assignment);
            Assert.Equal(0, assigned.QueueId);
            var exception = await Assert.ThrowsAsync<InvalidOperationException>(() =>
                consumer.AssignAsync([queues[0]], cancellationToken));
            Assert.Contains("subscriptions", exception.Message, StringComparison.OrdinalIgnoreCase);
        }

        Assert.NotNull(heartbeat);
        using var document = JsonDocument.Parse(heartbeat);
        var consumerData = Assert.Single(document.RootElement.GetProperty("consumerDataSet").EnumerateArray());
        Assert.Equal("CONSUME_ACTIVELY", consumerData.GetProperty("consumeType").GetString());
        Assert.Equal("CLUSTERING", consumerData.GetProperty("messageModel").GetString());
        Assert.Equal("CONSUME_FROM_LAST_OFFSET", consumerData.GetProperty("consumeFromWhere").GetString());
        engine.Verify(value => value.StopAsync(CancellationToken.None), Times.Once);
        engine.Verify(value => value.DisposeAsync(), Times.Once);
        registration.Verify(value => value.Dispose(), Times.Once);
    }

    [Fact]
    public async Task PollAsync_TracksLocalPositionUntilCommitAsync()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var queue = new RemotingPullMessageQueue("orders", 0, "broker-a", "127.0.0.1:10911");
        var engine = CreateEngine();
        engine
            .Setup(value => value.GetMessageQueuesAsync("orders", It.IsAny<CancellationToken>()))
            .ReturnsAsync([queue]);
        engine
            .Setup(value => value.GetOffsetAsync(queue, It.IsAny<CancellationToken>()))
            .ReturnsAsync(-1L);
        engine
            .Setup(value => value.QueryOffsetAsync(
                queue,
                QueryOffsetPolicy.End,
                null,
                It.IsAny<CancellationToken>()))
            .ReturnsAsync(0L);
        engine
            .Setup(value => value.PullAsync(queue, 4, null, It.IsAny<CancellationToken>()))
            .ReturnsAsync(new RemotingPullResult([], 5, RemotingPullStatus.NoNewMessage));
        engine
            .Setup(value => value.UpdateOffsetAsync(queue, 5, It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask);
        var registration = CreateRegistration();
        var remoting = CreateRemotingClient(registration);
        var options = new RemotingLitePullConsumerOptions { GroupName = "orders-consumer" };

        {
            await using var consumer = CreateConsumer(options, engine.Object, remoting.Object);
            await consumer.StartAsync(cancellationToken);
            var discovered = Assert.Single(await consumer.GetMessageQueuesAsync("orders", cancellationToken));
            await consumer.AssignAsync([discovered], cancellationToken);

            consumer.Pause([queue]);
            await Assert.ThrowsAsync<InvalidOperationException>(() =>
                consumer.PollAsync(cancellationToken: cancellationToken));
            consumer.Resume([queue]);
            consumer.Seek(queue, 4);

            var result = await consumer.PollAsync(cancellationToken: cancellationToken);

            Assert.Equal(5, result.NextOffset);
            engine.Verify(
                value => value.UpdateOffsetAsync(
                    It.IsAny<RemotingPullMessageQueue>(),
                    It.IsAny<long>(),
                    It.IsAny<CancellationToken>()),
                Times.Never);
            await consumer.CommitAsync(queue, cancellationToken);
        }

        engine.Verify(value => value.UpdateOffsetAsync(queue, 5, It.IsAny<CancellationToken>()), Times.Once);
        engine.Verify(value => value.StopAsync(CancellationToken.None), Times.Once);
        engine.Verify(value => value.DisposeAsync(), Times.Once);
        registration.Verify(value => value.Dispose(), Times.Once);
    }

    [Fact]
    public async Task UnsubscribeAsync_CancelsAnActivePollForARevokedQueue()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var queue = new RemotingPullMessageQueue("orders", 0, "broker-a", "127.0.0.1:10911");
        var pullStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var unregisterCount = 0;
        var engine = CreateEngine();
        engine
            .Setup(value => value.GetMessageQueuesAsync("orders", It.IsAny<CancellationToken>()))
            .ReturnsAsync([queue]);
        engine
            .Setup(value => value.GetOffsetAsync(queue, It.IsAny<CancellationToken>()))
            .ReturnsAsync(-1L);
        engine
            .Setup(value => value.QueryOffsetAsync(
                queue,
                QueryOffsetPolicy.End,
                null,
                It.IsAny<CancellationToken>()))
            .ReturnsAsync(0L);
        engine
            .Setup(value => value.PullAsync(queue, 0, null, It.IsAny<CancellationToken>()))
            .Returns<RemotingPullMessageQueue, long, int?, CancellationToken>(async (_, _, _, token) =>
            {
                pullStarted.TrySetResult();
                await Task.Delay(Timeout.InfiniteTimeSpan, token);
                return new RemotingPullResult([], 0);
            });
        engine.Setup(value => value.RemoveSubscription("orders"));
        var registration = CreateRegistration();
        var remoting = CreateRemotingClient(registration, request => request.Code switch
        {
            RequestCode.HeartBeat => SuccessResponse(),
            RequestCode.GetConsumerListByGroup => ConsumerListResponse(ClientId),
            RequestCode.UnregisterClient => CountUnregister(ref unregisterCount),
            _ => throw new InvalidOperationException($"Unexpected request code {request.Code}.")
        });
        var options = new RemotingLitePullConsumerOptions { GroupName = "orders-consumer" };
        options.Subscribe("orders");

        {
            await using var consumer = CreateConsumer(options, engine.Object, remoting.Object);
            await consumer.StartAsync(cancellationToken);
            var poll = consumer.PollAsync(cancellationToken: cancellationToken);
            await pullStarted.Task.WaitAsync(cancellationToken);

            await consumer.UnsubscribeAsync("orders", cancellationToken);

            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => poll);
            Assert.Empty(consumer.Assignment);
        }

        engine.Verify(value => value.RemoveSubscription("orders"), Times.Once);
        Assert.Equal(2, unregisterCount);
        engine.Verify(value => value.StopAsync(CancellationToken.None), Times.Once);
        engine.Verify(value => value.DisposeAsync(), Times.Once);
    }

    [Fact]
    public async Task Seek_DoesNotAllowAnEarlierPollToOverwriteTheNewPosition()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var queue = new RemotingPullMessageQueue("orders", 0, "broker-a", "127.0.0.1:10911");
        var pullStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var completePull = new TaskCompletionSource<RemotingPullResult>(
            TaskCreationOptions.RunContinuationsAsynchronously);
        var engine = CreateEngine();
        engine
            .Setup(value => value.GetOffsetAsync(queue, It.IsAny<CancellationToken>()))
            .ReturnsAsync(-1L);
        engine
            .Setup(value => value.QueryOffsetAsync(
                queue,
                QueryOffsetPolicy.End,
                null,
                It.IsAny<CancellationToken>()))
            .ReturnsAsync(0L);
        engine
            .Setup(value => value.PullAsync(queue, 0, null, It.IsAny<CancellationToken>()))
            .Returns<RemotingPullMessageQueue, long, int?, CancellationToken>((_, _, _, _) =>
            {
                pullStarted.TrySetResult();
                return completePull.Task;
            });
        engine
            .Setup(value => value.UpdateOffsetAsync(queue, 10, It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask);
        var registration = CreateRegistration();
        var remoting = CreateRemotingClient(registration);
        var options = new RemotingLitePullConsumerOptions { GroupName = "orders-consumer" };

        {
            await using var consumer = CreateConsumer(options, engine.Object, remoting.Object);
            await consumer.StartAsync(cancellationToken);
            await consumer.AssignAsync([queue], cancellationToken);
            var poll = consumer.PollAsync(cancellationToken: cancellationToken);
            await pullStarted.Task.WaitAsync(cancellationToken);

            consumer.Seek(queue, 10);
            completePull.SetResult(new RemotingPullResult([], 3));

            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => poll);
            await consumer.CommitAsync(queue, cancellationToken);
        }

        engine.Verify(value => value.UpdateOffsetAsync(queue, 10, It.IsAny<CancellationToken>()), Times.Once);
        engine.Verify(value => value.StopAsync(CancellationToken.None), Times.Once);
        engine.Verify(value => value.DisposeAsync(), Times.Once);
    }

    private static Mock<IRemotingConsumerEngine> CreateEngine()
    {
        var engine = new Mock<IRemotingConsumerEngine>(MockBehavior.Strict);
        engine.Setup(value => value.StartAsync(It.IsAny<CancellationToken>())).Returns(ValueTask.CompletedTask);
        engine.Setup(value => value.StopAsync(CancellationToken.None)).Returns(ValueTask.CompletedTask);
        engine.Setup(value => value.DisposeAsync()).Returns(ValueTask.CompletedTask);
        return engine;
    }

    private static Mock<IDisposable> CreateRegistration()
    {
        var registration = new Mock<IDisposable>(MockBehavior.Strict);
        registration.Setup(value => value.Dispose());
        return registration;
    }

    private static Mock<IRemotingClient> CreateRemotingClient(
        Mock<IDisposable> registration,
        Func<RemotingCommand, RemotingCommand>? invoke = null)
    {
        var remoting = new Mock<IRemotingClient>(MockBehavior.Strict);
        remoting
            .Setup(value => value.RegisterRequestHandler(
                RequestCode.NotifyConsumerIdsChanged,
                It.IsAny<RemotingRequestHandler>()))
            .Returns(registration.Object);
        if (invoke is not null)
        {
            remoting
                .Setup(value => value.InvokeAsync(
                    It.IsAny<System.Net.EndPoint>(),
                    It.IsAny<RemotingCommand>(),
                    It.IsAny<TimeSpan>(),
                    It.IsAny<CancellationToken>()))
                .Returns<System.Net.EndPoint, RemotingCommand, TimeSpan, CancellationToken>(
                    (_, request, _, _) => Task.FromResult(invoke(request)));
        }

        return remoting;
    }

    private static RemotingLitePullConsumer CreateConsumer(
        RemotingLitePullConsumerOptions options,
        IRemotingConsumerEngine engine,
        IRemotingClient remoting)
    {
        return new RemotingLitePullConsumer(
            Options.Create(options),
            Options.Create(new RemotingClientOptions
            {
                ClientIP = "127.0.0.1",
                InstanceName = "lite-client"
            }),
            engine,
            remoting,
            TimeProvider.System);
    }

    private static RemotingCommand CaptureHeartbeat(RemotingCommand request, ref byte[]? heartbeat)
    {
        heartbeat = request.Body;
        return SuccessResponse();
    }

    private static RemotingCommand ConsumerListResponse(params string[] consumerIds) =>
        new()
        {
            Code = ResponseCodes.ResSuccess,
            Body = Encoding.UTF8.GetBytes($$"""{"consumerIdList":{{JsonSerializer.Serialize(consumerIds)}}}""")
        };

    private static RemotingCommand SuccessResponse() => new() { Code = ResponseCodes.ResSuccess };

    private static RemotingCommand CountUnregister(ref int unregisterCount)
    {
        unregisterCount++;
        return SuccessResponse();
    }
}
