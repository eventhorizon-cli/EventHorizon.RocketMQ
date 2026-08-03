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
/// Represents the result of a classic remoting POP operation.
/// </summary>
internal sealed class RemotingPopResult
{
    internal RemotingPopResult(
        IReadOnlyList<RemotingPopMessage> messages,
        RemotingPopStatus status,
        long restNum)
    {
        ArgumentNullException.ThrowIfNull(messages);
        if (restNum < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(restNum));
        }

        Messages = Array.AsReadOnly(messages.ToArray());
        Status = status;
        RestNum = restNum;
    }

    /// <summary>
    /// Gets the messages made invisible and returned by the broker.
    /// </summary>
    public IReadOnlyList<RemotingPopMessage> Messages { get; }

    /// <summary>
    /// Gets the POP operation status returned by the broker.
    /// </summary>
    public RemotingPopStatus Status { get; }

    /// <summary>
    /// Gets the number of messages that the broker reported as remaining after the operation.
    /// </summary>
    public long RestNum { get; }
}
