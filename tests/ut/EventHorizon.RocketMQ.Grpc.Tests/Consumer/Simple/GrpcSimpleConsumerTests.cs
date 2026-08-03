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

using EventHorizon.RocketMQ.Grpc.Consumer;
using EventHorizon.RocketMQ.Grpc.Consumer.Simple;
using Microsoft.Extensions.Options;
using Moq;
using Xunit;
using Proto = Apache.Rocketmq.V2;

namespace EventHorizon.RocketMQ.Grpc.Tests.Consumer.Simple;

public sealed class GrpcSimpleConsumerTests
{
    [Fact]
    public async Task ReceiveAsync_AssignmentAndDurations_UsesConfiguredValues()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var filter = new FilterExpression("created");
        var queue = Queue();
        var assignment = new Proto.Assignment { MessageQueue = queue };
        var message = Message();
        var engine = new Mock<IGrpcReceiveConsumerEngine>(MockBehavior.Strict);
        engine.Setup(value => value.GetSubscriptions())
            .Returns([new KeyValuePair<string, FilterExpression>("orders", filter)]);
        engine.Setup(value => value.GetAssignmentsAsync("orders", cancellationToken))
            .ReturnsAsync([assignment]);
        engine.Setup(value => value.ReceiveAsync(
                queue,
                filter,
                4,
                TimeSpan.FromSeconds(45),
                TimeSpan.FromSeconds(7),
                false,
                cancellationToken))
            .ReturnsAsync([message]);
        engine.Setup(value => value.DisposeAsync()).Returns(ValueTask.CompletedTask);
        var consumer = new GrpcSimpleConsumer(Options.Create(new GrpcSimpleConsumerOptions
        {
            AwaitDuration = TimeSpan.FromSeconds(7),
            InvisibleDuration = TimeSpan.FromSeconds(45)
        }), engine.Object);

        var messages = await consumer.ReceiveAsync(4, cancellationToken: cancellationToken);

        Assert.Same(message, Assert.Single(messages));
        await consumer.DisposeAsync();
        engine.VerifyAll();
        engine.VerifyNoOtherCalls();
    }

    [Fact]
    public async Task ReceiveAsync_NoAssignments_ReturnsEmpty()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var engine = new Mock<IGrpcReceiveConsumerEngine>(MockBehavior.Strict);
        engine.Setup(value => value.GetSubscriptions())
            .Returns([new KeyValuePair<string, FilterExpression>("orders", FilterExpression.All)]);
        engine.Setup(value => value.GetAssignmentsAsync("orders", cancellationToken))
            .ReturnsAsync([]);
        engine.Setup(value => value.DisposeAsync()).Returns(ValueTask.CompletedTask);
        var consumer = new GrpcSimpleConsumer(
            Options.Create(new GrpcSimpleConsumerOptions()),
            engine.Object);

        var messages = await consumer.ReceiveAsync(1, cancellationToken: cancellationToken);

        Assert.Empty(messages);
        await consumer.DisposeAsync();
        engine.VerifyAll();
        engine.VerifyNoOtherCalls();
    }

    [Fact]
    public async Task ReceiveAsync_InvalidArgumentsAndMissingSubscription_Rejects()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var engine = new Mock<IGrpcReceiveConsumerEngine>(MockBehavior.Strict);
        engine.Setup(value => value.GetSubscriptions()).Returns([]);
        engine.Setup(value => value.DisposeAsync()).Returns(ValueTask.CompletedTask);
        var consumer = new GrpcSimpleConsumer(
            Options.Create(new GrpcSimpleConsumerOptions()),
            engine.Object);

        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(
            () => consumer.ReceiveAsync(0, cancellationToken: cancellationToken));
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(
            () => consumer.ReceiveAsync(1, TimeSpan.Zero, cancellationToken));
        await Assert.ThrowsAsync<InvalidOperationException>(
            () => consumer.ReceiveAsync(1, cancellationToken: cancellationToken));

        await consumer.DisposeAsync();
        engine.VerifyAll();
        engine.VerifyNoOtherCalls();
    }

    [Fact]
    public async Task MessageOperations_MessageValidation_ValidateAndForward()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var message = Message();
        var engine = new Mock<IGrpcReceiveConsumerEngine>(MockBehavior.Strict);
        engine.Setup(value => value.AckAsync(message, cancellationToken)).Returns(Task.CompletedTask);
        engine.Setup(value => value.ChangeInvisibleDurationAsync(
                message,
                TimeSpan.FromSeconds(20),
                cancellationToken))
            .Returns(Task.CompletedTask);
        engine.Setup(value => value.ForwardToDeadLetterQueueAsync(message, 5, cancellationToken))
            .Returns(Task.CompletedTask);
        engine.Setup(value => value.DisposeAsync()).Returns(ValueTask.CompletedTask);
        var consumer = new GrpcSimpleConsumer(
            Options.Create(new GrpcSimpleConsumerOptions()),
            engine.Object);

        await Assert.ThrowsAsync<ArgumentNullException>(() => consumer.AckAsync(null!, cancellationToken));
        await Assert.ThrowsAsync<ArgumentNullException>(
            () => consumer.ChangeInvisibleDurationAsync(null!, TimeSpan.FromSeconds(1), cancellationToken));
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(
            () => consumer.ChangeInvisibleDurationAsync(message, TimeSpan.Zero, cancellationToken));
        await Assert.ThrowsAsync<ArgumentNullException>(
            () => consumer.ForwardToDeadLetterQueueAsync(null!, 1, cancellationToken));
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(
            () => consumer.ForwardToDeadLetterQueueAsync(message, 0, cancellationToken));

        await consumer.AckAsync(message, cancellationToken);
        await consumer.ChangeInvisibleDurationAsync(message, TimeSpan.FromSeconds(20), cancellationToken);
        await consumer.ForwardToDeadLetterQueueAsync(message, 5, cancellationToken);
        await consumer.DisposeAsync();
        engine.VerifyAll();
        engine.VerifyNoOtherCalls();
    }

    [Fact]
    public async Task LifecycleAndSubscriptions_ForwardingAndDisposal_RemainIdempotent()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var filter = new FilterExpression("created");
        var engine = new Mock<IGrpcReceiveConsumerEngine>(MockBehavior.Strict);
        engine.Setup(value => value.StartAsync(cancellationToken)).Returns(ValueTask.CompletedTask);
        engine.Setup(value => value.StopAsync(cancellationToken)).Returns(ValueTask.CompletedTask);
        engine.Setup(value => value.SubscribeAsync("orders", filter, cancellationToken)).Returns(Task.CompletedTask);
        engine.Setup(value => value.UnsubscribeAsync("orders", cancellationToken)).Returns(Task.CompletedTask);
        engine.Setup(value => value.DisposeAsync()).Returns(ValueTask.CompletedTask);
        var consumer = new GrpcSimpleConsumer(
            Options.Create(new GrpcSimpleConsumerOptions()),
            engine.Object);

        await consumer.StartAsync(cancellationToken);
        await consumer.SubscribeAsync("orders", filter, cancellationToken);
        await consumer.UnsubscribeAsync("orders", cancellationToken);
        await consumer.StopAsync(cancellationToken);
        await consumer.DisposeAsync();
        await consumer.DisposeAsync();

        await Assert.ThrowsAsync<ObjectDisposedException>(async () => await consumer.StartAsync(cancellationToken));
        await Assert.ThrowsAsync<ObjectDisposedException>(
            () => consumer.SubscribeAsync("orders", filter, cancellationToken));
        await Assert.ThrowsAsync<ObjectDisposedException>(
            () => consumer.UnsubscribeAsync("orders", cancellationToken));
        engine.VerifyAll();
        engine.VerifyNoOtherCalls();
    }

    private static GrpcMessageView Message() => new(
        "orders",
        [1],
        "message-1",
        "receipt-1",
        null,
        [],
        new Dictionary<string, string>(),
        1,
        null,
        null,
        null,
        0,
        "broker-a",
        1,
        null,
        null,
        false,
        null,
        new Uri("http://127.0.0.1:8081"));

    private static Proto.MessageQueue Queue() => new()
    {
        Topic = new Proto.Resource { Name = "orders" },
        Id = 0,
        Broker = new Proto.Broker { Name = "broker-a" }
    };
}
