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
using Xunit;
using GrpcConsumeResult = EventHorizon.RocketMQ.Grpc.Consumer.ConsumeResult;
using GrpcFilterExpression = EventHorizon.RocketMQ.Grpc.Consumer.FilterExpression;
using GrpcFilterExpressionType = EventHorizon.RocketMQ.Grpc.Consumer.FilterExpressionType;
using GrpcMessage = EventHorizon.RocketMQ.Grpc.Producer.Message;
using GrpcRocketMQClientException = EventHorizon.RocketMQ.Grpc.Exceptions.RocketMQClientException;
using RemotingConsumeResult = EventHorizon.RocketMQ.Remoting.Consumer.ConsumeResult;
using RemotingFilterExpression = EventHorizon.RocketMQ.Remoting.Consumer.FilterExpression;
using RemotingFilterExpressionType = EventHorizon.RocketMQ.Remoting.Consumer.FilterExpressionType;
using RemotingMessage = EventHorizon.RocketMQ.Remoting.Producer.Message;
using RemotingRocketMQClientException = EventHorizon.RocketMQ.Remoting.Exceptions.RocketMQClientException;

namespace EventHorizon.RocketMQ.Compatibility.Tests;

public sealed class ProtocolModelParityTests
{
    [Fact]
    public void ProtocolNeutralModelCopies_PublicApiParity_Matches()
    {
        AssertEquivalentPublicApi(typeof(GrpcMessage), typeof(RemotingMessage));
        AssertEquivalentPublicApi(typeof(GrpcFilterExpression), typeof(RemotingFilterExpression));
        AssertEquivalentPublicApi(typeof(GrpcFilterExpressionType), typeof(RemotingFilterExpressionType));
        AssertEquivalentPublicApi(typeof(GrpcRocketMQClientException), typeof(RemotingRocketMQClientException));
    }

    [Fact]
    public void ConsumeResults_ProtocolSpecificSemantics_ExposeExpectedOutcomes()
    {
        Assert.Equal(["Success", "Failure"], Enum.GetNames<GrpcConsumeResult>());
        Assert.Equal(["Success", "Retry", "DeadLetter"], Enum.GetNames<RemotingConsumeResult>());
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData(" ")]
    public void Message_ConstructorsInvalidTopics_Reject(string? topic)
    {
        Assert.ThrowsAny<ArgumentException>(() => new GrpcMessage(topic!, []));
        Assert.ThrowsAny<ArgumentException>(() => new RemotingMessage(topic!, []));
    }

    [Fact]
    public void Message_ConstructorsNullBodies_Reject()
    {
        Assert.Throws<ArgumentNullException>(() => new GrpcMessage("orders", null!));
        Assert.Throws<ArgumentNullException>(() => new RemotingMessage("orders", null!));
    }

    [Fact]
    public void ExceptionConstructors_MessageAndInnerException_Preserve()
    {
        var innerException = new InvalidOperationException("inner");
        var grpcException = new GrpcRocketMQClientException("failure", innerException);
        var remotingException = new RemotingRocketMQClientException("failure", innerException);

        Assert.Equal("failure", grpcException.Message);
        Assert.Same(innerException, grpcException.InnerException);
        Assert.Equal("failure", remotingException.Message);
        Assert.Same(innerException, remotingException.InnerException);
    }

    private static void AssertEquivalentPublicApi(Type grpcType, Type remotingType) =>
        Assert.Equal(GetPublicApi(grpcType), GetPublicApi(remotingType));

    private static string[] GetPublicApi(Type type)
    {
        const BindingFlags flags = BindingFlags.Public | BindingFlags.Instance | BindingFlags.Static |
                                   BindingFlags.DeclaredOnly;
        var members = new List<string>
        {
            $"base:{FormatType(type.BaseType)}"
        };
        members.AddRange(type.GetConstructors(flags).Select(constructor =>
            $"constructor:{FormatParameters(constructor.GetParameters())}"));
        members.AddRange(type.GetProperties(flags).Select(property =>
            $"property:{property.Name}:{FormatType(property.PropertyType)}:{property.CanRead}:{property.CanWrite}"));
        members.AddRange(type.GetMethods(flags).Where(method => !method.IsSpecialName).Select(method =>
            $"method:{method.Name}:{FormatType(method.ReturnType)}:{FormatParameters(method.GetParameters())}"));

        if (type.IsEnum)
        {
            members.AddRange(Enum.GetNames(type).Select(name =>
                $"enum:{name}:{Convert.ToInt64(Enum.Parse(type, name))}"));
        }

        return members.Order(StringComparer.Ordinal).ToArray();
    }

    private static string FormatParameters(IEnumerable<ParameterInfo> parameters) =>
        string.Join(",", parameters.Select(parameter => FormatType(parameter.ParameterType)));

    private static string FormatType(Type? type)
    {
        if (type is null)
        {
            return string.Empty;
        }

        if (type.IsArray)
        {
            return $"{FormatType(type.GetElementType())}[]";
        }

        if (type.IsGenericType)
        {
            var genericName = type.GetGenericTypeDefinition().FullName!.Split('`')[0];
            return $"{NormalizeProtocolName(genericName)}<{string.Join(",", type.GetGenericArguments().Select(FormatType))}>";
        }

        return NormalizeProtocolName(type.FullName ?? type.Name);
    }

    private static string NormalizeProtocolName(string typeName) =>
        typeName
            .Replace("EventHorizon.RocketMQ.Grpc", "EventHorizon.RocketMQ.Protocol", StringComparison.Ordinal)
            .Replace("EventHorizon.RocketMQ.Remoting", "EventHorizon.RocketMQ.Protocol", StringComparison.Ordinal);
}
