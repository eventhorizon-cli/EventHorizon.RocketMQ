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

namespace EventHorizon.RocketMQ.Remoting.Producer;

/// <summary>
/// Specifies the durability outcome reported by the broker for a classic remoting send operation.
/// </summary>
public enum RemotingSendStatus
{
    /// <summary>
    /// The broker accepted and stored the message successfully.
    /// </summary>
    SendOk,

    /// <summary>
    /// The broker did not flush the message to disk before the configured timeout.
    /// </summary>
    FlushDiskTimeout,

    /// <summary>
    /// The broker did not replicate the message to a slave before the configured timeout.
    /// </summary>
    FlushSlaveTimeout,

    /// <summary>
    /// No slave broker was available to acknowledge the message.
    /// </summary>
    SlaveNotAvailable,
}
