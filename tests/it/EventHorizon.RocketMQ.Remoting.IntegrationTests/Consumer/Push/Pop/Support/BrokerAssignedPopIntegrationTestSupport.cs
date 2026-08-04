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

using EventHorizon.RocketMQ.IntegrationTestInfrastructure;
using EventHorizon.RocketMQ.Remoting.Consumer;
using EventHorizon.RocketMQ.Remoting.Consumer.Push;
using EventHorizon.RocketMQ.Remoting.Consumer.Push.Assignment;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace EventHorizon.RocketMQ.Remoting.IntegrationTests.Consumer.Push.Pop.Support;

internal static class BrokerAssignedPopIntegrationTestSupport
{
    internal static readonly TimeSpan PopInvisibleDuration = TimeSpan.FromSeconds(5);
    internal static readonly TimeSpan DeliveryTimeout = TimeSpan.FromSeconds(45);

    internal static RemotingRocketMQBuilder AddRemotingClient(
        ServiceCollection services,
        RocketMQSingleBrokerContainerFixture fixture,
        string instanceName) => services.AddRocketMQRemoting(options =>
        {
            options.NamesrvAddr = fixture.NameServerAddress;
            options.InstanceName = instanceName;
        });

    internal static void ConfigureBrokerAssignedPop(
        RemotingPushConsumerOptions options,
        string consumerGroup,
        string topic,
        string tag)
    {
        options.GroupName = consumerGroup;
        options.QueueAssignmentMode = RemotingPushQueueAssignmentMode.Broker;
        options.InitialPosition = ConsumeFromPosition.End;
        options.MaxConcurrency = 1;
        options.PopBatchSize = 1;
        options.PopInvisibleDuration = PopInvisibleDuration;
        options.PopMaxInflightMessagesPerAssignment = 32;
        options.ConsumeMessageBatchSize = 1;
        options.LongPollingTimeout = TimeSpan.FromSeconds(1);
        options.Subscribe(topic, new FilterExpression(tag));
    }

    internal static async Task AssertWildcardPopAssignmentAsync(
        IRemotingPushConsumer consumer,
        string topic,
        CancellationToken cancellationToken)
    {
        var pushConsumer = Assert.IsType<RemotingPushConsumer>(consumer);
        await WaitUntilAsync(
            () => Task.FromResult(pushConsumer.ReceiveAssignments.Any(assignment =>
                string.Equals(assignment.Topic, topic, StringComparison.Ordinal))),
            DeliveryTimeout,
            cancellationToken);
        var assignment = Assert.Single(pushConsumer.ReceiveAssignments, assignment =>
            string.Equals(assignment.Topic, topic, StringComparison.Ordinal));
        Assert.Equal("broker-a", assignment.BrokerName);
        Assert.Equal(-1, assignment.QueueId);
        Assert.Equal(RemotingPushReceiveMode.Pop, assignment.Mode);
    }

    internal static async Task WaitUntilAsync(
        Func<Task<bool>> condition,
        TimeSpan timeout,
        CancellationToken cancellationToken)
    {
        var deadline = DateTimeOffset.UtcNow + timeout;
        do
        {
            if (await condition())
            {
                return;
            }

            await Task.Delay(TimeSpan.FromMilliseconds(250), cancellationToken);
        }
        while (DateTimeOffset.UtcNow < deadline);

        Assert.Fail("The expected Broker state was not observed before the timeout.");
    }
}
