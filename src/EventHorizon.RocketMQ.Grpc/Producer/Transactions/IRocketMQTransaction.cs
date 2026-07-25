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

using EventHorizon.RocketMQ.Grpc.Producer;

namespace EventHorizon.RocketMQ.Grpc.Producer.Transactions;

/// <summary>
/// Represents a pending RocketMQ transaction created by sending a transactional message.
/// </summary>
public interface IRocketMQTransaction
{
    /// <summary>
    /// Gets the receipt returned when the transactional message was sent.
    /// </summary>
    GrpcSendReceipt SendReceipt { get; }

    /// <summary>
    /// Gets the identifier of the transactional message.
    /// </summary>
    string MessageId { get; }

    /// <summary>
    /// Gets the server-assigned transaction identifier.
    /// </summary>
    string TransactionId { get; }

    /// <summary>
    /// Commits the transaction.
    /// </summary>
    /// <param name="cancellationToken">A token that cancels the operation.</param>
    /// <returns>A task that represents the asynchronous operation.</returns>
    Task CommitAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Rolls back the transaction.
    /// </summary>
    /// <param name="cancellationToken">A token that cancels the operation.</param>
    /// <returns>A task that represents the asynchronous operation.</returns>
    Task RollbackAsync(CancellationToken cancellationToken = default);
}
