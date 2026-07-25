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

using EventHorizon.RocketMQ.Consumer;

namespace EventHorizon.RocketMQ.Grpc.Consumer.Simple;

/// <summary>
/// Configures a RocketMQ gRPC simple consumer.
/// </summary>
public sealed class GrpcSimpleConsumerOptions : ConsumerOptions
{
    /// <summary>
    /// Gets or sets the maximum time the server may wait for messages during a receive operation.
    /// </summary>
    public TimeSpan AwaitDuration { get; set; } = TimeSpan.FromSeconds(5);

    /// <summary>
    /// Gets or sets the default duration for which received messages remain invisible to other consumers.
    /// </summary>
    public TimeSpan InvisibleDuration { get; set; } = TimeSpan.FromSeconds(30);
}
