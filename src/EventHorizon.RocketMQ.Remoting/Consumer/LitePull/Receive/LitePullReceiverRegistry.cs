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

namespace EventHorizon.RocketMQ.Remoting.Consumer.LitePull.Receive;

internal sealed class LitePullReceiverRegistry
{
    private readonly object _gate = new();
    private readonly HashSet<LitePullQueueState> _receivers = [];

    public void Start(
        LitePullQueueState state,
        Func<LitePullQueueState, Task> receive,
        Action<LitePullQueueState, Exception?>? receiverStoppedUnexpectedly = null)
    {
        ArgumentNullException.ThrowIfNull(state);
        ArgumentNullException.ThrowIfNull(receive);

        lock (_gate)
        {
            if (!_receivers.Add(state))
            {
                throw new InvalidOperationException("The Lite Pull queue receiver is already registered.");
            }

            var startSignal = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            state.ReceiverTask = RunAsync(
                state,
                receive,
                receiverStoppedUnexpectedly,
                startSignal.Task);
            startSignal.TrySetResult();
        }
    }

    public IReadOnlyList<LitePullQueueState> CancelAll()
    {
        lock (_gate)
        {
            var receivers = _receivers.ToArray();
            foreach (var state in receivers)
            {
                state.CancelReceive();
            }

            return receivers;
        }
    }

    public static async Task AwaitAsync(IEnumerable<LitePullQueueState> states)
    {
        ArgumentNullException.ThrowIfNull(states);
        foreach (var state in states)
        {
            try
            {
                await state.ReceiverTask.ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
            }
        }
    }

    public void Clear()
    {
        lock (_gate)
        {
            _receivers.Clear();
        }
    }

    private async Task RunAsync(
        LitePullQueueState state,
        Func<LitePullQueueState, Task> receive,
        Action<LitePullQueueState, Exception?>? receiverStoppedUnexpectedly,
        Task startSignal)
    {
        await startSignal.ConfigureAwait(false);
        Exception? failure = null;
        try
        {
            await receive(state).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (state.ReceiveCancellation.IsCancellationRequested)
        {
        }
        catch (Exception exception)
        {
            failure = exception;
        }
        finally
        {
            lock (_gate)
            {
                _receivers.Remove(state);
            }

            try
            {
                if (!state.ReceiveCancellation.IsCancellationRequested)
                {
                    receiverStoppedUnexpectedly?.Invoke(state, failure);
                }
            }
            finally
            {
                state.Dispose();
            }
        }
    }
}
