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
using EventHorizon.RocketMQ.Grpc.Protocol;
using Xunit;
using Proto = Apache.Rocketmq.V2;

namespace EventHorizon.RocketMQ.Grpc.Tests.Protocol;

public sealed class GrpcStatusTests
{
    [Fact]
    public void MultipleResultsIsAValidAggregateStatus()
    {
        GrpcStatus.EnsureSuccess(new Proto.Status { Code = Proto.Code.MultipleResults });
    }

    [Fact]
    public void ReceiveTreatsMessageNotFoundAsAnEmptyPoll()
    {
        GrpcStatus.EnsureReceiveSuccess(new Proto.Status { Code = Proto.Code.MessageNotFound });
    }

    [Fact]
    public void MessageNotFoundIsNotGenerallySuccessful()
    {
        var exception = Assert.Throws<GrpcServiceException>(() =>
            GrpcStatus.EnsureSuccess(new Proto.Status { Code = Proto.Code.MessageNotFound }));

        Assert.Equal((int)Proto.Code.MessageNotFound, exception.ResponseCode);
    }

    [Fact]
    public void ReceiveRejectsMissingStatus()
    {
        Assert.Throws<InvalidDataException>(() => GrpcStatus.EnsureReceiveSuccess(null));
    }

    [Fact]
    public void GeneralResponseRejectsMissingStatus()
    {
        Assert.Throws<InvalidDataException>(() => GrpcStatus.EnsureSuccess(null));
    }

    [Fact]
    public void ReceiveDoesNotHideIllegalPollingTime()
    {
        var exception = Assert.Throws<GrpcServiceException>(() =>
            GrpcStatus.EnsureReceiveSuccess(new Proto.Status { Code = Proto.Code.IllegalPollingTime }));

        Assert.Equal((int)Proto.Code.IllegalPollingTime, exception.ResponseCode);
    }
}
