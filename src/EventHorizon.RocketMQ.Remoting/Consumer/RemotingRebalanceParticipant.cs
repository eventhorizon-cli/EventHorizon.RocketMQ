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

using System.Diagnostics;
using Microsoft.Extensions.Logging;

namespace EventHorizon.RocketMQ.Remoting.Consumer;

internal sealed class RemotingRebalanceParticipant : IAsyncDisposable
{
    private readonly Func<CancellationToken, Task<bool>> _rebalance;
    private readonly TimeSpan _retryInterval;
    private readonly ILogger<RemotingRebalanceService> _logger;
    private readonly SemaphoreSlim _signal = new(0, 1);
    private readonly object _signalGate = new();
    private readonly CancellationTokenSource _stopping;
    private readonly Task _executionTask;
    private readonly Lazy<Task> _disposeTask;
    private int _disposed;

    public RemotingRebalanceParticipant(
        string consumerGroup,
        Func<CancellationToken, Task<bool>> rebalance,
        TimeSpan retryInterval,
        ILogger<RemotingRebalanceService> logger,
        CancellationToken serviceStopping)
    {
        ConsumerGroup = consumerGroup;
        _rebalance = rebalance;
        _retryInterval = retryInterval;
        _logger = logger;
        _stopping = CancellationTokenSource.CreateLinkedTokenSource(serviceStopping);
        _executionTask = RunAsync();
        _disposeTask = new Lazy<Task>(DisposeCoreAsync, LazyThreadSafetyMode.ExecutionAndPublication);
    }

    public string ConsumerGroup { get; }

    public void Wakeup()
    {
        lock (_signalGate)
        {
            if (Volatile.Read(ref _disposed) != 0)
            {
                return;
            }

            Signal();
        }
    }

    public ValueTask DisposeAsync() => new(_disposeTask.Value);

    private async Task DisposeCoreAsync()
    {
        Interlocked.Exchange(ref _disposed, 1);
        _stopping.Cancel();
        lock (_signalGate)
        {
            Signal();
        }

        try
        {
            await _executionTask.ConfigureAwait(false);
        }
        finally
        {
            lock (_signalGate)
            {
                _signal.Dispose();
            }

            _stopping.Dispose();
        }
    }

    private void Signal()
    {
        try
        {
            _signal.Release();
        }
        catch (SemaphoreFullException)
        {
        }
    }

    private async Task RunAsync()
    {
        var retry = false;
        var lastRebalanceTimestamp = Stopwatch.GetTimestamp();
        while (!_stopping.IsCancellationRequested)
        {
            try
            {
                if (retry)
                {
                    await _signal.WaitAsync(_retryInterval, _stopping.Token).ConfigureAwait(false);
                }
                else
                {
                    await _signal.WaitAsync(_stopping.Token).ConfigureAwait(false);
                }
            }
            catch (OperationCanceledException) when (_stopping.IsCancellationRequested)
            {
                return;
            }

            var elapsed = Stopwatch.GetElapsedTime(lastRebalanceTimestamp);
            if (elapsed < _retryInterval)
            {
                try
                {
                    await Task.Delay(_retryInterval - elapsed, _stopping.Token).ConfigureAwait(false);
                }
                catch (OperationCanceledException) when (_stopping.IsCancellationRequested)
                {
                    return;
                }
            }

            try
            {
                retry = !await _rebalance(_stopping.Token).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (_stopping.IsCancellationRequested)
            {
                return;
            }
            catch (Exception exception)
            {
                retry = true;
                _logger.LogWarning(
                    exception,
                    "Legacy consumer group {ConsumerGroup} rebalance failed; retrying",
                    ConsumerGroup);
            }

            lastRebalanceTimestamp = Stopwatch.GetTimestamp();
        }
    }
}
