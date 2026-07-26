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

using EventHorizon.RocketMQ.Grpc;
using EventHorizon.RocketMQ.Grpc.Producer;
using EventHorizon.RocketMQ.Remoting;
using EventHorizon.RocketMQ.Remoting.Producer;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Xunit;
using GrpcConsumeResult = EventHorizon.RocketMQ.Grpc.Consumer.ConsumeResult;
using GrpcConsumerOptions = EventHorizon.RocketMQ.Grpc.Consumer.ConsumerOptions;
using GrpcFilterExpression = EventHorizon.RocketMQ.Grpc.Consumer.FilterExpression;
using GrpcFilterExpressionType = EventHorizon.RocketMQ.Grpc.Consumer.FilterExpressionType;
using GrpcMessage = EventHorizon.RocketMQ.Grpc.Producer.Message;
using GrpcRocketMQClientException = EventHorizon.RocketMQ.Grpc.Exceptions.RocketMQClientException;
using RemotingConsumeResult = EventHorizon.RocketMQ.Remoting.Consumer.ConsumeResult;
using RemotingConsumerOptions = EventHorizon.RocketMQ.Remoting.Consumer.ConsumerOptions;
using RemotingFilterExpression = EventHorizon.RocketMQ.Remoting.Consumer.FilterExpression;
using RemotingFilterExpressionType = EventHorizon.RocketMQ.Remoting.Consumer.FilterExpressionType;
using RemotingMessage = EventHorizon.RocketMQ.Remoting.Producer.Message;
using RemotingQueryOffsetPolicy = EventHorizon.RocketMQ.Remoting.Consumer.QueryOffsetPolicy;
using RemotingRocketMQClientException = EventHorizon.RocketMQ.Remoting.Exceptions.RocketMQClientException;

namespace EventHorizon.RocketMQ.Compatibility.Tests;

public sealed class ProtocolTypeIsolationTests
{
    [Fact]
    public void ProtocolTypes_AreOwnedByTheirProtocolAssemblies()
    {
        Type[] grpcTypes =
        [
            typeof(GrpcClientOptions),
            typeof(GrpcConsumeResult),
            typeof(GrpcConsumerOptions),
            typeof(GrpcFilterExpression),
            typeof(GrpcFilterExpressionType),
            typeof(GrpcMessage),
            typeof(GrpcRocketMQClientException)
        ];
        Type[] remotingTypes =
        [
            typeof(RemotingClientOptions),
            typeof(RemotingConsumeResult),
            typeof(RemotingConsumerOptions),
            typeof(RemotingFilterExpression),
            typeof(RemotingFilterExpressionType),
            typeof(RemotingQueryOffsetPolicy),
            typeof(RemotingMessage),
            typeof(RemotingRocketMQClientException)
        ];

        Assert.All(grpcTypes, type =>
        {
            Assert.StartsWith("EventHorizon.RocketMQ.Grpc", type.Namespace!);
            Assert.Equal(typeof(IGrpcProducer).Assembly, type.Assembly);
        });
        Assert.All(remotingTypes, type =>
        {
            Assert.StartsWith("EventHorizon.RocketMQ.Remoting", type.Namespace!);
            Assert.Equal(typeof(IRemotingProducer).Assembly, type.Assembly);
        });
        Assert.Empty(grpcTypes.Select(type => type.FullName).Intersect(remotingTypes.Select(type => type.FullName)));
        Assert.Null(typeof(IGrpcProducer).Assembly.GetType("EventHorizon.RocketMQ.Grpc.Consumer.QueryOffsetPolicy"));
        Assert.Null(typeof(IGrpcProducer).Assembly.GetType("EventHorizon.RocketMQ.Grpc.RocketMQClientOptions"));
        Assert.Null(typeof(IRemotingProducer).Assembly.GetType("EventHorizon.RocketMQ.Remoting.RocketMQClientOptions"));

        string[] legacyTypeNames =
        [
            "EventHorizon.RocketMQ.RocketMQClientOptions",
            "EventHorizon.RocketMQ.Consumer.ConsumeResult",
            "EventHorizon.RocketMQ.Consumer.ConsumerOptions",
            "EventHorizon.RocketMQ.Consumer.FilterExpression",
            "EventHorizon.RocketMQ.Consumer.FilterExpressionType",
            "EventHorizon.RocketMQ.Consumer.QueryOffsetPolicy",
            "EventHorizon.RocketMQ.Producer.Message",
            "EventHorizon.RocketMQ.Exceptions.RocketMQClientException"
        ];
        Assert.All(
            new[] { typeof(IGrpcProducer).Assembly, typeof(IRemotingProducer).Assembly },
            assembly => Assert.All(legacyTypeNames, typeName => Assert.Null(assembly.GetType(typeName))));
    }

    [Fact]
    public void Messages_CanBeUsedInTheSameApplication()
    {
        var grpcMessage = new GrpcMessage("orders", [1]);
        var remotingMessage = new RemotingMessage("orders", [2]);

        Assert.Equal("orders", grpcMessage.Topic);
        Assert.Equal("orders", remotingMessage.Topic);
        Assert.NotEqual(grpcMessage.GetType(), remotingMessage.GetType());
    }

    [Fact]
    public void ClientOptions_ExposeOnlyProtocolRelevantIdentitySettings()
    {
        Assert.Equal(typeof(object), typeof(GrpcClientOptions).BaseType);
        Assert.Equal(typeof(object), typeof(RemotingClientOptions).BaseType);
        Assert.Null(typeof(GrpcClientOptions).GetProperty(nameof(RemotingClientOptions.ClientIP)));
        Assert.Null(typeof(GrpcClientOptions).GetProperty(nameof(RemotingClientOptions.InstanceName)));
        Assert.NotNull(typeof(RemotingClientOptions).GetProperty(nameof(RemotingClientOptions.ClientIP)));
        Assert.NotNull(typeof(RemotingClientOptions).GetProperty(nameof(RemotingClientOptions.InstanceName)));
    }

    [Fact]
    public void ProducerContracts_UseTheirProtocolMessageType()
    {
        Assert.Contains(
            typeof(IGrpcProducer).GetMethods(),
            method => method.Name == nameof(IGrpcProducer.SendAsync) &&
                      method.GetParameters().FirstOrDefault()?.ParameterType == typeof(GrpcMessage));
        Assert.Contains(
            typeof(IRemotingProducer).GetMethods(),
            method => method.Name == nameof(IRemotingProducer.SendAsync) &&
                      method.GetParameters().FirstOrDefault()?.ParameterType == typeof(RemotingMessage));
    }

    [Fact]
    public void DependencyInjectionRegistrations_CanCoexist()
    {
        var services = new ServiceCollection();

        services.AddRocketMQGrpc(options => options.Endpoint = "localhost:8081");
        services.AddRocketMQRemoting(options => options.NamesrvAddr = "localhost:9876");

        Assert.Contains(services, descriptor =>
            descriptor.ServiceType == typeof(IConfigureOptions<GrpcClientOptions>));
        Assert.Contains(services, descriptor =>
            descriptor.ServiceType == typeof(IConfigureOptions<RemotingClientOptions>));
    }
}
