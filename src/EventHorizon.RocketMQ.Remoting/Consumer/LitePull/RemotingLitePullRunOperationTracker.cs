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

namespace EventHorizon.RocketMQ.Remoting.Consumer.LitePull;

internal sealed class RemotingLitePullRunOperationTracker
{
    private readonly object _gate = new();
    private TaskCompletionSource? _drained;
    private int _activeOperations;
    private bool _closed;

    public IDisposable Enter()
    {
        lock (_gate)
        {
            if (_closed)
            {
                throw new InvalidOperationException("The Lite Pull consumer run is stopping.");
            }

            _activeOperations++;
            return new Lease(this);
        }
    }

    public void Close()
    {
        TaskCompletionSource? drained = null;
        lock (_gate)
        {
            _closed = true;
            if (_activeOperations == 0)
            {
                drained = _drained;
            }
        }

        drained?.TrySetResult();
    }

    public Task DrainAsync()
    {
        lock (_gate)
        {
            if (!_closed)
            {
                throw new InvalidOperationException("The Lite Pull run operation tracker must be closed before draining.");
            }

            if (_activeOperations == 0)
            {
                return Task.CompletedTask;
            }

            return (_drained ??= new TaskCompletionSource(
                TaskCreationOptions.RunContinuationsAsynchronously)).Task;
        }
    }

    private void Exit()
    {
        TaskCompletionSource? drained = null;
        lock (_gate)
        {
            _activeOperations--;
            if (_closed && _activeOperations == 0)
            {
                drained = _drained;
            }
        }

        drained?.TrySetResult();
    }

    private sealed class Lease(RemotingLitePullRunOperationTracker owner) : IDisposable
    {
        private RemotingLitePullRunOperationTracker? _owner = owner;

        public void Dispose() => Interlocked.Exchange(ref _owner, null)?.Exit();
    }
}
