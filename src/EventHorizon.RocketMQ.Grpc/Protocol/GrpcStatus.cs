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
using Proto = Apache.Rocketmq.V2;

namespace EventHorizon.RocketMQ.Grpc.Protocol;

internal static class GrpcStatus
{
    public static void EnsureReceiveSuccess(Proto.Status? status)
    {
        if (status?.Code == Proto.Code.MessageNotFound)
        {
            return;
        }

        EnsureSuccess(status);
    }

    public static void EnsureSuccess(Proto.Status? status)
    {
        if (status is null)
        {
            throw new InvalidDataException("RocketMQ returned a response without a status.");
        }

        if (status.Code is Proto.Code.Ok or Proto.Code.MultipleResults)
        {
            return;
        }

        throw new GrpcServiceException((int)status.Code, status.Message);
    }
}
