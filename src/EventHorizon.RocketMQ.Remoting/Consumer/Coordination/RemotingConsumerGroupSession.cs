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

using System.Collections.Concurrent;

using EventHorizon.RocketMQ.Remoting.Consumer.Coordination.Rebalance;
using EventHorizon.RocketMQ.Remoting.Consumer.Route;

namespace EventHorizon.RocketMQ.Remoting.Consumer.Coordination;

internal sealed class RemotingConsumerGroupSession
{
    private readonly RemotingConsumerGroupClient _client;
    private readonly IRemotingRebalanceService _rebalanceService;
    private readonly ConcurrentDictionary<string, RemotingBrokerEndpoint> _knownBrokers =
        new(StringComparer.Ordinal);
    private readonly object _registrationGate = new();
    private IRemotingRebalanceRegistration? _rebalanceRegistration;
    private bool _rebalanceStopped;
    private long _subscriptionVersion;

    public RemotingConsumerGroupSession(
        RemotingConsumerGroupClient client,
        IRemotingRebalanceService rebalanceService,
        long subscriptionVersion)
    {
        ArgumentNullException.ThrowIfNull(client);
        ArgumentNullException.ThrowIfNull(rebalanceService);

        _client = client;
        _rebalanceService = rebalanceService;
        _subscriptionVersion = subscriptionVersion;
    }

    public string ClientId => _client.ClientId;

    public string ConsumerGroup => _client.ConsumerGroup;

    public long SubscriptionVersion => Interlocked.Read(ref _subscriptionVersion);

    public IReadOnlyCollection<RemotingBrokerEndpoint> KnownBrokers => _knownBrokers.Values.ToArray();

    public void AddKnownBroker(RemotingBrokerEndpoint broker) => _knownBrokers[broker.Address] = broker;

    public void AddKnownBrokers(IEnumerable<RemotingBrokerEndpoint> brokers)
    {
        ArgumentNullException.ThrowIfNull(brokers);
        foreach (var broker in brokers)
        {
            AddKnownBroker(broker);
        }
    }

    public void RegisterRebalance(Func<CancellationToken, Task<bool>> rebalance)
    {
        ArgumentNullException.ThrowIfNull(rebalance);
        lock (_registrationGate)
        {
            if (_rebalanceStopped)
            {
                throw new InvalidOperationException("The consumer group session has stopped rebalancing.");
            }

            if (_rebalanceRegistration is not null)
            {
                throw new InvalidOperationException("The consumer group session already has a rebalance registration.");
            }

            _rebalanceRegistration = _rebalanceService.Register(ConsumerGroup, rebalance);
        }
    }

    public void Wakeup()
    {
        lock (_registrationGate)
        {
            _rebalanceRegistration?.Wakeup();
        }
    }

    public void BumpSubscriptionVersion() => Interlocked.Increment(ref _subscriptionVersion);

    public Task SendHeartbeatsAsync(byte[] heartbeat, CancellationToken cancellationToken) =>
        _client.SendHeartbeatsAsync(KnownBrokers, heartbeat, cancellationToken);

    public Task<IReadOnlyList<string>> GetConsumerIdsAsync(
        RemotingConsumerQueue queue,
        RemotingBrokerEndpoint broker,
        CancellationToken cancellationToken) =>
        _client.GetConsumerIdsAsync(queue, broker, cancellationToken);

    public Task UnregisterAsync() => _client.UnregisterAsync(KnownBrokers);

    public async Task StopRebalanceAsync()
    {
        IRemotingRebalanceRegistration? registration;
        lock (_registrationGate)
        {
            _rebalanceStopped = true;
            registration = _rebalanceRegistration;
            _rebalanceRegistration = null;
        }

        if (registration is not null)
        {
            await registration.DisposeAsync().ConfigureAwait(false);
        }
    }
}
