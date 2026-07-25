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
using Proto = Apache.Rocketmq.V2;

namespace EventHorizon.RocketMQ.Grpc.Producer.Transactions;

internal sealed class RocketMQTransaction : IRocketMQTransaction
{
    private readonly GrpcProducer _producer;
    private readonly object _stateGate = new();
    private readonly SemaphoreSlim _resolutionGate = new(1, 1);
    private RocketMQTransactionState _state;
    private readonly TransactionReceipt _receipt;

    public RocketMQTransaction(GrpcProducer producer, TransactionReceipt receipt, GrpcSendReceipt sendReceipt)
    {
        _producer = producer;
        _receipt = receipt;
        SendReceipt = sendReceipt;
        _state = RocketMQTransactionState.Pending;
    }

    public GrpcSendReceipt SendReceipt { get; }
    public string MessageId => _receipt.MessageId;
    public string TransactionId => _receipt.TransactionId;

    public Task CommitAsync(CancellationToken cancellationToken = default) =>
        ResolveAsync(TransactionResolution.Commit, cancellationToken);

    public Task RollbackAsync(CancellationToken cancellationToken = default) =>
        ResolveAsync(TransactionResolution.Rollback, cancellationToken);

    private async Task ResolveAsync(TransactionResolution resolution, CancellationToken cancellationToken)
    {
        await _resolutionGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            lock (_stateGate)
            {
                if ((resolution == TransactionResolution.Commit && _state == RocketMQTransactionState.Committed) ||
                    (resolution == TransactionResolution.Rollback && _state == RocketMQTransactionState.RolledBack))
                {
                    return;
                }

                var canResolve = resolution == TransactionResolution.Commit
                    ? _state is RocketMQTransactionState.Pending or RocketMQTransactionState.CommitFailed
                    : _state is RocketMQTransactionState.Pending or RocketMQTransactionState.RollbackFailed;
                if (!canResolve)
                {
                    throw new InvalidOperationException($"Transaction cannot be resolved as {resolution} from state {_state}.");
                }

                _state = resolution == TransactionResolution.Commit
                    ? RocketMQTransactionState.Committing
                    : RocketMQTransactionState.RollingBack;
            }

            try
            {
                await _producer.EndTransactionAsync(
                    _receipt,
                    resolution,
                    Proto.TransactionSource.SourceClient,
                    cancellationToken).ConfigureAwait(false);
                lock (_stateGate)
                {
                    _state = resolution == TransactionResolution.Commit
                        ? RocketMQTransactionState.Committed
                        : RocketMQTransactionState.RolledBack;
                }
            }
            catch
            {
                lock (_stateGate)
                {
                    _state = resolution == TransactionResolution.Commit
                        ? RocketMQTransactionState.CommitFailed
                        : RocketMQTransactionState.RollbackFailed;
                }

                throw;
            }
        }
        finally
        {
            _resolutionGate.Release();
        }
    }
}
