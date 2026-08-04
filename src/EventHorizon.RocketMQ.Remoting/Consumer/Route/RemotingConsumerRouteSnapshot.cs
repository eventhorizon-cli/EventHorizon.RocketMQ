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

namespace EventHorizon.RocketMQ.Remoting.Consumer.Route;

internal sealed class RemotingConsumerRouteSnapshot
{
    private readonly IReadOnlyDictionary<string, IReadOnlyDictionary<long, string>> _brokerAddresses;

    public RemotingConsumerRouteSnapshot(
        string topic,
        IReadOnlyList<RemotingConsumerQueue> queues,
        IReadOnlyDictionary<string, IReadOnlyDictionary<long, string>> brokerAddresses)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(topic);
        ArgumentNullException.ThrowIfNull(queues);
        ArgumentNullException.ThrowIfNull(brokerAddresses);

        Topic = topic;
        Queues = queues;
        _brokerAddresses = brokerAddresses;
    }

    public string Topic { get; }

    public IReadOnlyList<RemotingConsumerQueue> Queues { get; }

    public IReadOnlyList<RemotingBrokerEndpoint> Brokers => _brokerAddresses
        .OrderBy(static broker => broker.Key, StringComparer.Ordinal)
        .Select(static broker => new RemotingBrokerEndpoint(broker.Key, SelectPrimaryAddress(broker.Value)))
        .ToArray();

    public IReadOnlyList<RemotingBrokerEndpoint> AllBrokerEndpoints => _brokerAddresses
        .OrderBy(static broker => broker.Key, StringComparer.Ordinal)
        .SelectMany(
            static broker => broker.Value.OrderBy(static address => address.Key),
            static (broker, address) => new RemotingBrokerEndpoint(broker.Key, address.Value))
        .ToArray();

    public RemotingBrokerEndpoint GetBroker(string brokerName)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(brokerName);
        return _brokerAddresses.TryGetValue(brokerName, out var addresses)
            ? new RemotingBrokerEndpoint(brokerName, SelectPrimaryAddress(addresses))
            : throw new InvalidOperationException(
                $"The route for topic '{Topic}' does not contain Broker '{brokerName}'.");
    }

    public bool TryGetBrokerAddresses(
        string brokerName,
        out IReadOnlyDictionary<long, string> brokerAddresses) =>
        _brokerAddresses.TryGetValue(brokerName, out brokerAddresses!);

    private static string SelectPrimaryAddress(IReadOnlyDictionary<long, string> addresses) =>
        addresses.TryGetValue(0, out var primary)
            ? primary
            : addresses.OrderBy(static address => address.Key).First().Value;
}
