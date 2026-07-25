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

namespace EventHorizon.RocketMQ.Remoting.Producer.Transactions;

/// <summary>
/// Represents the result of sending a classic RocketMQ transactional message.
/// </summary>
public sealed class RemotingTransactionSendResult
{
    internal RemotingTransactionSendResult(
        RemotingSendResult sendResult,
        RemotingTransactionResolution localTransactionResolution)
    {
        ArgumentNullException.ThrowIfNull(sendResult);
        SendResult = sendResult;
        LocalTransactionResolution = localTransactionResolution;
    }

    /// <summary>
    /// Gets the result of sending the transaction's half message.
    /// </summary>
    public RemotingSendResult SendResult { get; }

    /// <summary>
    /// Gets the outcome returned by the local transaction executor.
    /// </summary>
    public RemotingTransactionResolution LocalTransactionResolution { get; }
}
