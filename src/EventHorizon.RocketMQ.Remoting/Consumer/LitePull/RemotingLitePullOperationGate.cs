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

internal sealed class RemotingLitePullOperationGate : IDisposable
{
    private readonly SemaphoreSlim _gate = new(1, 1);
    private int _closed;

    public bool IsOpen => Volatile.Read(ref _closed) == 0;

    public async ValueTask<IDisposable> EnterAsync(CancellationToken cancellationToken)
    {
        await _gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        if (!IsOpen)
        {
            _gate.Release();
            throw new InvalidOperationException("The Lite Pull consumer run is stopping.");
        }

        return new Lease(_gate);
    }

    public IDisposable Enter()
    {
        _gate.Wait();
        if (!IsOpen)
        {
            _gate.Release();
            throw new InvalidOperationException("The Lite Pull consumer run is stopping.");
        }

        return new Lease(_gate);
    }

    public IDisposable? TryEnter()
    {
        if (!_gate.Wait(0))
        {
            return null;
        }

        if (!IsOpen)
        {
            _gate.Release();
            throw new InvalidOperationException("The Lite Pull consumer run is stopping.");
        }

        return new Lease(_gate);
    }

    public void Close() => Interlocked.Exchange(ref _closed, 1);

    public async Task DrainAsync()
    {
        await _gate.WaitAsync(CancellationToken.None).ConfigureAwait(false);
        _gate.Release();
    }

    public void Dispose() => _gate.Dispose();

    private sealed class Lease(SemaphoreSlim gate) : IDisposable
    {
        private SemaphoreSlim? _gate = gate;

        public void Dispose() => Interlocked.Exchange(ref _gate, null)?.Release();
    }
}
