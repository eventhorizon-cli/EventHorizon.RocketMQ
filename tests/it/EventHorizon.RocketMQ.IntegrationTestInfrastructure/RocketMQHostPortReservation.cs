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

using System.Net;
using System.Net.Sockets;

namespace EventHorizon.RocketMQ.IntegrationTestInfrastructure;

/// <summary>
/// Coordinates dynamic host-port choices between container fixtures in the current test process.
/// </summary>
/// <remarks>
/// The probe listener closes after selecting a candidate, so this type records a process-local logical reservation; it
/// does not retain an operating-system socket lock.
/// </remarks>
internal sealed class RocketMQHostPortReservation : IDisposable
{
    private static readonly object SyncRoot = new();
    private static readonly HashSet<int> ReservedPorts = [];
    private readonly IReadOnlyList<int> _ports;
    private int _disposed;

    private RocketMQHostPortReservation(IReadOnlyList<int> ports)
    {
        _ports = ports;
    }

    public int this[int index] => _ports[index];

    public static RocketMQHostPortReservation Reserve(int count, int minimumPortDistance = 0)
    {
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(count, 0);
        ArgumentOutOfRangeException.ThrowIfNegative(minimumPortDistance);

        lock (SyncRoot)
        {
            var ports = new List<int>(count);
            while (ports.Count < count)
            {
                var port = GetAvailablePort();
                if (ReservedPorts.Contains(port) ||
                    ports.Any(existingPort => Math.Abs(existingPort - port) <= minimumPortDistance))
                {
                    continue;
                }

                ReservedPorts.Add(port);
                ports.Add(port);
            }

            return new RocketMQHostPortReservation(ports);
        }
    }

    public void Dispose()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
        {
            return;
        }

        lock (SyncRoot)
        {
            foreach (var port in _ports)
            {
                ReservedPorts.Remove(port);
            }
        }
    }

    private static int GetAvailablePort()
    {
        using var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        return ((IPEndPoint)listener.LocalEndpoint).Port;
    }
}
