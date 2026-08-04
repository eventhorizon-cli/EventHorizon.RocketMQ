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

using System.Reflection;
using EventHorizon.RocketMQ.Grpc;
using EventHorizon.RocketMQ.Grpc.Consumer.Push;
using EventHorizon.RocketMQ.Remoting;
using EventHorizon.RocketMQ.Remoting.Consumer;
using EventHorizon.RocketMQ.Remoting.Consumer.LitePull;
using EventHorizon.RocketMQ.Remoting.Consumer.Push;
using Xunit;

namespace EventHorizon.RocketMQ.Compatibility.Tests;

public sealed class PushConsumerApiTests
{
    [Fact]
    public void PushConsumerOptions_DelegateHandlers_AreNotExposed()
    {
        Assert.Null(typeof(GrpcPushConsumerOptions).GetProperty("MessageHandler"));
        Assert.Null(typeof(RemotingPushConsumerOptions).GetProperty("MessageHandler"));
    }

    [Fact]
    public void RemotingPushConsumerOptions_QueueAssignmentContract_ExposesOnlyExplicitQueueNames()
    {
        var property = Assert.IsAssignableFrom<PropertyInfo>(
            typeof(RemotingPushConsumerOptions).GetProperty("QueueAssignmentMode"));

        Assert.Equal(typeof(RemotingPushQueueAssignmentMode), property.PropertyType);
        Assert.Equal(
            RemotingPushQueueAssignmentMode.Client,
            new RemotingPushConsumerOptions().QueueAssignmentMode);
        Assert.Null(typeof(RemotingPushConsumerOptions).GetProperty("AssignmentMode"));
        Assert.Null(typeof(IRemotingPushConsumer).Assembly.GetType(
            "EventHorizon.RocketMQ.Remoting.Consumer.Push.RemotingPushAssignmentMode"));
    }

    [Theory]
    [InlineData(typeof(GrpcRocketMQBuilderExtensions), "AddGrpcPushConsumer")]
    [InlineData(typeof(GrpcRocketMQBuilderExtensions), "AddGrpcLitePushConsumer")]
    [InlineData(typeof(RemotingRocketMQBuilderExtensions), "AddRemotingPushConsumer")]
    public void PushConsumerRegistrations_TypedHandlerOverloads_ExposeOnly(
        Type extensionType,
        string methodName)
    {
        var methods = extensionType.GetMethods(BindingFlags.Public | BindingFlags.Static)
            .Where(method => string.Equals(method.Name, methodName, StringComparison.Ordinal))
            .ToArray();

        Assert.NotEmpty(methods);
        Assert.All(methods, static method => Assert.True(method.IsGenericMethodDefinition));
    }

    [Fact]
    public void RemotingConsumerSurface_PublicApi_ExportsLitePullAndPushRoles()
    {
        var assembly = typeof(IRemotingPushConsumer).Assembly;
        var exportedTypeNames = assembly.GetExportedTypes()
            .Select(static type => type.FullName)
            .ToHashSet(StringComparer.Ordinal);

        Assert.Equal(
            "EventHorizon.RocketMQ.Remoting.Consumer.LitePull.IRemotingLitePullConsumer",
            typeof(IRemotingLitePullConsumer).FullName);
        Assert.Equal(
            "EventHorizon.RocketMQ.Remoting.Consumer.LitePull.RemotingLitePullConsumerOptions",
            typeof(RemotingLitePullConsumerOptions).FullName);
        Assert.Contains(typeof(IRemotingLitePullConsumer).FullName, exportedTypeNames);
        Assert.Contains(typeof(IRemotingPushConsumer).FullName, exportedTypeNames);
        Assert.Contains(typeof(RemotingConsumerQueue).FullName, exportedTypeNames);
        Assert.DoesNotContain("EventHorizon.RocketMQ.Remoting.Consumer.Pull.IRemotingPullConsumer", exportedTypeNames);
        Assert.DoesNotContain("EventHorizon.RocketMQ.Remoting.Consumer.Pull.RemotingPullConsumerOptions", exportedTypeNames);
        Assert.DoesNotContain("EventHorizon.RocketMQ.Remoting.Consumer.Pull.RemotingPullResult", exportedTypeNames);
        Assert.DoesNotContain(
            "EventHorizon.RocketMQ.Remoting.Consumer.Push.Pop.IRemotingPopConsumer",
            exportedTypeNames);
        Assert.DoesNotContain(
            "EventHorizon.RocketMQ.Remoting.Consumer.Push.Pop.RemotingPopConsumerOptions",
            exportedTypeNames);
        Assert.DoesNotContain(
            "EventHorizon.RocketMQ.Remoting.Consumer.Push.Pop.RemotingPopReceipt",
            exportedTypeNames);

        var registrationNames = typeof(RemotingRocketMQBuilderExtensions)
            .GetMethods(BindingFlags.Public | BindingFlags.Static)
            .Select(static method => method.Name)
            .ToHashSet(StringComparer.Ordinal);
        Assert.Contains("AddRemotingLitePullConsumer", registrationNames);
        Assert.Contains("AddRemotingPushConsumer", registrationNames);
        Assert.DoesNotContain("AddRemotingPullConsumer", registrationNames);
        Assert.DoesNotContain("AddRemotingPopConsumer", registrationNames);
    }
}
