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

namespace EventHorizon.RocketMQ.Remoting.Consumer.Push.Pop;

/// <summary>
/// Specifies the outcome of a classic remoting POP operation.
/// </summary>
internal enum RemotingPopStatus
{
    /// <summary>
    /// One or more messages were made invisible and returned by the broker.
    /// </summary>
    Found,

    /// <summary>
    /// The broker completed polling without making a message available.
    /// </summary>
    NoNewMessage,

    /// <summary>
    /// The broker could not accept another POP polling request.
    /// </summary>
    PollingFull
}
