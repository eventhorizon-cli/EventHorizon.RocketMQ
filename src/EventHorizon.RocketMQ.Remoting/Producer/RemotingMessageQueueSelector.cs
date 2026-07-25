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

using EventHorizon.RocketMQ.Producer;

namespace EventHorizon.RocketMQ.Remoting.Producer;

/// <summary>
/// Selects a writable classic RocketMQ message queue for a send operation.
/// </summary>
/// <param name="messageQueues">The currently writable queues for the message topic.</param>
/// <param name="message">The message being sent.</param>
/// <param name="argument">The application-defined selector argument.</param>
/// <returns>A queue from <paramref name="messageQueues"/>.</returns>
public delegate RemotingMessageQueue RemotingMessageQueueSelector(
    IReadOnlyList<RemotingMessageQueue> messageQueues,
    Message message,
    object? argument);
