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

using System.Text.Json;
using EventHorizon.RocketMQ.Remoting.Consumer;

namespace EventHorizon.RocketMQ.Remoting.CrossProcessTestHost;

internal sealed class CrossProcessHostEventWriter(string member) : IDisposable
{
    private readonly SemaphoreSlim _gate = new(1, 1);

    public ValueTask WriteStartedAsync(CancellationToken cancellationToken) =>
        WriteAsync(new CrossProcessHostEvent("started", member), cancellationToken);

    public ValueTask WriteAssignmentAsync(
        IReadOnlyList<string> queues,
        CancellationToken cancellationToken) =>
        WriteAsync(new CrossProcessHostEvent("assignment", member) { Queues = queues }, cancellationToken);

    public ValueTask WriteDeliveryAsync(
        RemotingMessageView message,
        CancellationToken cancellationToken) =>
        WriteAsync(new CrossProcessHostEvent("delivery", member)
        {
            Body = System.Text.Encoding.UTF8.GetString(message.Body)
        }, cancellationToken);

    public ValueTask WriteStoppedAsync(CancellationToken cancellationToken) =>
        WriteAsync(new CrossProcessHostEvent("stopped", member), cancellationToken);

    public ValueTask WriteFaultedAsync(Exception exception, CancellationToken cancellationToken) =>
        WriteAsync(new CrossProcessHostEvent("faulted", member) { Message = exception.ToString() }, cancellationToken);

    public void Dispose()
    {
        _gate.Dispose();
    }

    private async ValueTask WriteAsync(CrossProcessHostEvent hostEvent, CancellationToken cancellationToken)
    {
        var line = JsonSerializer.Serialize(hostEvent);
        await _gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            await Console.Out.WriteLineAsync(line.AsMemory(), cancellationToken).ConfigureAwait(false);
            await Console.Out.FlushAsync(cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            _gate.Release();
        }
    }

    private sealed record CrossProcessHostEvent(string Kind, string Member)
    {
        public IReadOnlyList<string>? Queues { get; init; }

        public string? Body { get; init; }

        public string? Message { get; init; }
    }
}
