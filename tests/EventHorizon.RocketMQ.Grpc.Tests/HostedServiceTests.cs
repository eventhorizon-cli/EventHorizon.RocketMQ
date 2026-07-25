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

using EventHorizon.RocketMQ.Grpc.Consumer.Push;
using EventHorizon.RocketMQ.Grpc.Consumer.Simple;
using EventHorizon.RocketMQ.Grpc.Producer;
using Moq;
using Xunit;

namespace EventHorizon.RocketMQ.Grpc.Tests;

public sealed class HostedServiceTests
{
    [Fact]
    public async Task GrpcProducerHostedService_ForwardsLifecycleCancellationTokens()
    {
        using var start = new CancellationTokenSource();
        using var stop = new CancellationTokenSource();
        var producer = new Mock<IGrpcProducer>(MockBehavior.Strict);
        producer.Setup(value => value.StartAsync(start.Token)).Returns(ValueTask.CompletedTask);
        producer.Setup(value => value.StopAsync(stop.Token)).Returns(ValueTask.CompletedTask);
        var hostedService = new GrpcProducerHostedService(producer.Object);

        await hostedService.StartAsync(start.Token);
        await hostedService.StopAsync(stop.Token);

        producer.Verify(value => value.StartAsync(start.Token), Times.Once);
        producer.Verify(value => value.StopAsync(stop.Token), Times.Once);
        producer.VerifyNoOtherCalls();
    }

    [Fact]
    public async Task GrpcSimpleConsumerHostedService_ForwardsLifecycleCancellationTokens()
    {
        using var start = new CancellationTokenSource();
        using var stop = new CancellationTokenSource();
        var consumer = new Mock<IGrpcSimpleConsumer>(MockBehavior.Strict);
        consumer.Setup(value => value.StartAsync(start.Token)).Returns(ValueTask.CompletedTask);
        consumer.Setup(value => value.StopAsync(stop.Token)).Returns(ValueTask.CompletedTask);
        var hostedService = new GrpcSimpleConsumerHostedService(consumer.Object);

        await hostedService.StartAsync(start.Token);
        await hostedService.StopAsync(stop.Token);

        consumer.Verify(value => value.StartAsync(start.Token), Times.Once);
        consumer.Verify(value => value.StopAsync(stop.Token), Times.Once);
        consumer.VerifyNoOtherCalls();
    }

    [Fact]
    public async Task GrpcPushConsumerHostedService_ForwardsLifecycleCancellationTokens()
    {
        using var start = new CancellationTokenSource();
        using var stop = new CancellationTokenSource();
        var consumer = new Mock<IGrpcPushConsumer>(MockBehavior.Strict);
        consumer.Setup(value => value.StartAsync(start.Token)).Returns(ValueTask.CompletedTask);
        consumer.Setup(value => value.StopAsync(stop.Token)).Returns(ValueTask.CompletedTask);
        var hostedService = new GrpcPushConsumerHostedService(consumer.Object);

        await hostedService.StartAsync(start.Token);
        await hostedService.StopAsync(stop.Token);

        consumer.Verify(value => value.StartAsync(start.Token), Times.Once);
        consumer.Verify(value => value.StopAsync(stop.Token), Times.Once);
        consumer.VerifyNoOtherCalls();
    }
}
