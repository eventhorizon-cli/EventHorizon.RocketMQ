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

namespace EventHorizon.RocketMQ.Remoting.Consumer;

/// <summary>
/// Specifies the outcome of processing a consumed message.
/// </summary>
public enum ConsumeResult
{
    /// <summary>
    /// Indicates that the message was processed successfully and can be acknowledged.
    /// </summary>
    Success,

    /// <summary>
    /// Indicates that message processing should be retried.
    /// </summary>
    Retry,

    /// <summary>
    /// Indicates that the message should be forwarded to the dead-letter queue.
    /// </summary>
    DeadLetter
}
