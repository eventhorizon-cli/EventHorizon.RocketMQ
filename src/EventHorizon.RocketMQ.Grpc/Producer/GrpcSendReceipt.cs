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

namespace EventHorizon.RocketMQ.Grpc.Producer;

/// <summary>
/// Represents the server receipt returned after a message is sent through the RocketMQ gRPC protocol.
/// </summary>
public sealed class GrpcSendReceipt
{
    internal GrpcSendReceipt(
        string messageId,
        long offset,
        string? transactionId,
        string? recallHandle,
        IEnumerable<Uri> endpoints)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(messageId);
        ArgumentNullException.ThrowIfNull(endpoints);
        var endpointValues = endpoints.Distinct().ToArray();
        if (endpointValues.Length == 0)
        {
            throw new ArgumentException("At least one broker endpoint is required.", nameof(endpoints));
        }

        MessageId = messageId;
        Offset = offset;
        TransactionId = transactionId;
        RecallHandle = recallHandle;
        Endpoints = Array.AsReadOnly(endpointValues);
    }

    /// <summary>
    /// Gets the server-assigned message identifier.
    /// </summary>
    public string MessageId { get; }

    /// <summary>
    /// Gets the offset at which the message was stored.
    /// </summary>
    public long Offset { get; }

    /// <summary>
    /// Gets the transaction identifier, or <see langword="null"/> for a non-transactional message.
    /// </summary>
    public string? TransactionId { get; }

    /// <summary>
    /// Gets the handle used to recall a delayed message, or <see langword="null"/> when recall is not available.
    /// </summary>
    public string? RecallHandle { get; }

    /// <summary>
    /// Gets the broker endpoints associated with the send result.
    /// </summary>
    public IReadOnlyList<Uri> Endpoints { get; }

    /// <inheritdoc/>
    public override string ToString() =>
        $"{nameof(MessageId)}: {MessageId}, {nameof(RecallHandle)}: {RecallHandle}";
}
