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

using EventHorizon.RocketMQ.Grpc.Exceptions;
using Xunit;

namespace EventHorizon.RocketMQ.Grpc.Tests.Exceptions;

public sealed class GrpcServiceExceptionTests
{
    [Fact]
    public void ExceptionType_RocketMQClientException_IsClientException()
    {
        var exception = new GrpcServiceException(401, "unauthorized");

        Assert.IsAssignableFrom<RocketMQClientException>(exception);
        Assert.Equal(401, exception.ResponseCode);
        Assert.Equal("unauthorized", exception.Message);
    }
}
