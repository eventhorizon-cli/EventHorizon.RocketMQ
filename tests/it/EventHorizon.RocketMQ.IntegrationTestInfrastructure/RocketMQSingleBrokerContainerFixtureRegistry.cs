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

using Xunit;

namespace EventHorizon.RocketMQ.IntegrationTestInfrastructure;

/// <summary>
/// Lazily creates one single-Broker fixture for an integration-test assembly.
/// </summary>
/// <remarks>
/// Registering this holder instead of the container fixture keeps xUnit assembly initialization inexpensive and avoids
/// starting the baseline topology for a multi-Broker-only filtered run. Multi-Broker collection fixtures do not need
/// this indirection because xUnit already creates them only when their collection runs.
/// </remarks>
public sealed class RocketMQSingleBrokerContainerFixtureRegistry : IAsyncLifetime
{
    private readonly SemaphoreSlim _initializationGate = new(1, 1);
    private RocketMQSingleBrokerContainerFixture? _fixture;

    /// <inheritdoc />
    public ValueTask InitializeAsync()
    {
        return ValueTask.CompletedTask;
    }

    /// <summary>
    /// Gets the shared single-Broker fixture, starting it on first use.
    /// </summary>
    /// <param name="cancellationToken">The token used to cancel waiting for fixture initialization.</param>
    /// <returns>The initialized fixture.</returns>
    public async Task<RocketMQSingleBrokerContainerFixture> GetFixtureAsync(CancellationToken cancellationToken = default)
    {
        if (Volatile.Read(ref _fixture) is { } fixture)
        {
            return fixture;
        }

        await _initializationGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (_fixture is { } existingFixture)
            {
                return existingFixture;
            }

            var createdFixture = new RocketMQSingleBrokerContainerFixture();
            try
            {
                await createdFixture.InitializeAsync().ConfigureAwait(false);
                _fixture = createdFixture;
                return createdFixture;
            }
            catch
            {
                await createdFixture.DisposeAsync().ConfigureAwait(false);
                throw;
            }
        }
        finally
        {
            _initializationGate.Release();
        }
    }

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        await _initializationGate.WaitAsync().ConfigureAwait(false);
        try
        {
            if (_fixture is { } fixture)
            {
                _fixture = null;
                await fixture.DisposeAsync().ConfigureAwait(false);
            }
        }
        finally
        {
            _initializationGate.Release();
            _initializationGate.Dispose();
        }
    }
}
