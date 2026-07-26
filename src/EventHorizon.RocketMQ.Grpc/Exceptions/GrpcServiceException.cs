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

namespace EventHorizon.RocketMQ.Grpc.Exceptions;

/// <summary>
/// Represents an unsuccessful RocketMQ service status returned over an otherwise completed gRPC call.
/// </summary>
public sealed class GrpcServiceException : RocketMQClientException
{
    /// <summary>
    /// Initializes a new instance of the <see cref="GrpcServiceException"/> class.
    /// </summary>
    /// <param name="responseCode">The numeric RocketMQ service response code.</param>
    /// <param name="message">The error message returned by the RocketMQ service.</param>
    public GrpcServiceException(int responseCode, string message) : base(message)
    {
        ResponseCode = responseCode;
    }

    /// <summary>
    /// Gets the numeric RocketMQ service response code.
    /// </summary>
    public int ResponseCode { get; }
}
