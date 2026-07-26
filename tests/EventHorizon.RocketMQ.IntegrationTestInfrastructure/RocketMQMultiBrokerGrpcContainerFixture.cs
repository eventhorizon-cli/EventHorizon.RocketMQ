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

using System.Globalization;
using System.Text;
using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;
using DotNet.Testcontainers.Networks;
using Xunit;

namespace EventHorizon.RocketMQ.IntegrationTestInfrastructure;

/// <summary>
/// Provides a cluster-mode Proxy and three network-addressable Brokers for gRPC integration tests.
/// </summary>
public sealed class RocketMQMultiBrokerGrpcContainerFixture : IAsyncLifetime
{
    private const string Image = "apache/rocketmq:5.5.0";
    private const int NameServerPort = 9876;
    private const int BrokerPort = 10911;
    private const int GrpcPort = 8081;

    /// <summary>
    /// Gets the name of the first Broker in the test cluster.
    /// </summary>
    public const string BrokerAName = "broker-a";

    /// <summary>
    /// Gets the name of the second Broker in the test cluster.
    /// </summary>
    public const string BrokerBName = "broker-b";

    /// <summary>
    /// Gets the name of the third Broker in the test cluster.
    /// </summary>
    public const string BrokerCName = "broker-c";

    /// <summary>
    /// Gets the topic created on all Brokers for multi-Broker gRPC tests.
    /// </summary>
    public const string TestTopic = "rocketmq-dotnet-grpc-multi-broker-it";

    private readonly INetwork _network = new NetworkBuilder().Build();
    private readonly IContainer _nameServer;
    private readonly IContainer _brokerA;
    private readonly IContainer _brokerB;
    private readonly IContainer _brokerC;
    private readonly IContainer _proxy;
    private readonly RocketMQHostPortReservation _portReservation;
    private readonly int _proxyHostPort;

    /// <summary>
    /// Initializes a new instance of the <see cref="RocketMQMultiBrokerGrpcContainerFixture"/> class.
    /// </summary>
    public RocketMQMultiBrokerGrpcContainerFixture()
    {
        _portReservation = RocketMQHostPortReservation.Reserve(1);
        _proxyHostPort = _portReservation[0];
        _nameServer = new ContainerBuilder(Image)
            .WithNetwork(_network)
            .WithNetworkAliases("nameserver")
            .WithEnvironment("JAVA_OPT_EXT", "-Duser.home=/home/rocketmq -Xms256m -Xmx256m")
            .WithCommand("sh", "mqnamesrv")
            .WithWaitStrategy(Wait.ForUnixContainer().UntilInternalTcpPortIsAvailable(NameServerPort))
            .Build();

        _brokerA = CreateBroker(BrokerAName);
        _brokerB = CreateBroker(BrokerBName);
        _brokerC = CreateBroker(BrokerCName);

        var proxyConfiguration = Encoding.UTF8.GetBytes(
            "{\n" +
            "  \"rocketMQClusterName\": \"DefaultCluster\",\n" +
            "  \"proxyClusterName\": \"DefaultCluster\",\n" +
            $"  \"grpcServerPort\": {GrpcPort},\n" +
            "  \"useEndpointPortFromRequest\": true\n" +
            "}\n");
        _proxy = new ContainerBuilder(Image)
            .WithNetwork(_network)
            .WithNetworkAliases("proxy")
            .WithPortBinding(_proxyHostPort, GrpcPort)
            .WithEnvironment("NAMESRV_ADDR", $"nameserver:{NameServerPort}")
            .WithEnvironment("JAVA_OPT_EXT", "-Duser.home=/home/rocketmq -Xms512m -Xmx512m")
            .WithResourceMapping(proxyConfiguration, "/home/rocketmq/rocketmq-5.5.0/conf/rmq-proxy.json")
            .WithCommand(
                "sh",
                "mqproxy",
                "-pm",
                "cluster",
                "-n",
                $"nameserver:{NameServerPort}",
                "-pc",
                "/home/rocketmq/rocketmq-5.5.0/conf/rmq-proxy.json")
            .WithWaitStrategy(Wait.ForUnixContainer()
                .UntilInternalTcpPortIsAvailable(GrpcPort)
                .UntilMessageIsLogged("rocketmq-proxy startup successfully"))
            .Build();
    }

    /// <summary>
    /// Gets the host-reachable gRPC endpoint for the cluster-mode Proxy.
    /// </summary>
    public string GrpcEndpoint => $"{_proxy.Hostname}:{_proxy.GetMappedPublicPort(GrpcPort)}";

    /// <inheritdoc/>
    public async ValueTask InitializeAsync()
    {
        await _network.CreateAsync().ConfigureAwait(false);
        await _nameServer.StartAsync().ConfigureAwait(false);
        await Task.WhenAll(
            _brokerA.StartAsync(),
            _brokerB.StartAsync(),
            _brokerC.StartAsync()).ConfigureAwait(false);

        await WaitForClusterRegistrationAsync().ConfigureAwait(false);
        await CreateTopicAsync().ConfigureAwait(false);
        await _proxy.StartAsync().ConfigureAwait(false);
    }

    /// <inheritdoc/>
    public async ValueTask DisposeAsync()
    {
        try
        {
            await _proxy.DisposeAsync().ConfigureAwait(false);
            await _brokerC.DisposeAsync().ConfigureAwait(false);
            await _brokerB.DisposeAsync().ConfigureAwait(false);
            await _brokerA.DisposeAsync().ConfigureAwait(false);
            await _nameServer.DisposeAsync().ConfigureAwait(false);
            await _network.DisposeAsync().ConfigureAwait(false);
        }
        finally
        {
            _portReservation.Dispose();
        }
    }

    /// <summary>
    /// Waits until all three Brokers have stored at least one message in <paramref name="topic"/>.
    /// </summary>
    /// <param name="topic">The topic whose queue offsets to inspect.</param>
    /// <param name="timeout">The maximum time allowed for the offsets to become visible.</param>
    /// <param name="cancellationToken">The token used to cancel the operation.</param>
    /// <returns>The aggregate maximum queue offsets by Broker.</returns>
    public async Task<IReadOnlyDictionary<string, long>> WaitForMessagesOnAllBrokersAsync(
        string topic,
        TimeSpan timeout,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(topic);
        var deadline = DateTimeOffset.UtcNow + timeout;
        IReadOnlyDictionary<string, long> offsets;
        do
        {
            offsets = await GetBrokerMaxOffsetsAsync(topic, cancellationToken).ConfigureAwait(false);
            if (HasMessagesOnAllBrokers(offsets))
            {
                return offsets;
            }

            await Task.Delay(TimeSpan.FromMilliseconds(250), cancellationToken).ConfigureAwait(false);
        }
        while (DateTimeOffset.UtcNow < deadline);

        return await GetBrokerMaxOffsetsAsync(topic, cancellationToken).ConfigureAwait(false);
    }

    private IContainer CreateBroker(string brokerName)
    {
        var brokerConfiguration = Encoding.UTF8.GetBytes(
            "brokerClusterName=DefaultCluster\n" +
            $"brokerName={brokerName}\n" +
            "brokerId=0\n" +
            $"brokerIP1={brokerName}\n" +
            $"namesrvAddr=nameserver:{NameServerPort}\n" +
            $"listenPort={BrokerPort}\n" +
            "autoCreateTopicEnable=true\n");

        return new ContainerBuilder(Image)
            .WithNetwork(_network)
            .WithNetworkAliases(brokerName)
            .WithEnvironment("NAMESRV_ADDR", $"nameserver:{NameServerPort}")
            .WithEnvironment("JAVA_OPT_EXT", "-Duser.home=/home/rocketmq -Xms256m -Xmx256m")
            .WithResourceMapping(brokerConfiguration, "/tmp/broker.conf")
            .WithCommand("sh", "mqbroker", "-c", "/tmp/broker.conf")
            .WithWaitStrategy(Wait.ForUnixContainer().UntilInternalTcpPortIsAvailable(BrokerPort))
            .Build();
    }

    private async Task WaitForClusterRegistrationAsync()
    {
        ExecResult clusterList = default;
        for (var attempt = 0; attempt < 60; attempt++)
        {
            clusterList = await _brokerA.ExecAsync([
                "sh", "mqadmin", "clusterList", "-n", $"nameserver:{NameServerPort}"
            ]).ConfigureAwait(false);
            if (clusterList.ExitCode == 0 &&
                clusterList.Stdout.Contains(BrokerAName, StringComparison.Ordinal) &&
                clusterList.Stdout.Contains(BrokerBName, StringComparison.Ordinal) &&
                clusterList.Stdout.Contains(BrokerCName, StringComparison.Ordinal))
            {
                return;
            }

            await Task.Delay(TimeSpan.FromMilliseconds(500)).ConfigureAwait(false);
        }

        throw new InvalidOperationException(
            $"All three Brokers did not register with NameServer. stdout: {clusterList.Stdout} " +
            $"stderr: {clusterList.Stderr}");
    }

    private async Task CreateTopicAsync()
    {
        foreach (var brokerName in new[] { BrokerAName, BrokerBName, BrokerCName })
        {
            var createTopic = await _brokerA.ExecAsync([
                "sh", "mqadmin", "updateTopic",
                "-n", $"nameserver:{NameServerPort}",
                "-b", $"{brokerName}:{BrokerPort}",
                "-t", TestTopic,
                "-a", "+message.type=NORMAL"
            ]).ConfigureAwait(false);
            if (createTopic.ExitCode != 0)
            {
                throw new InvalidOperationException(
                    $"Unable to create the multi-Broker integration-test topic on '{brokerName}'. " +
                    $"stdout: {createTopic.Stdout} stderr: {createTopic.Stderr}");
            }
        }

        ExecResult topicRoute = default;
        for (var attempt = 0; attempt < 60; attempt++)
        {
            topicRoute = await _brokerA.ExecAsync([
                "sh", "mqadmin", "topicRoute",
                "-n", $"nameserver:{NameServerPort}",
                "-t", TestTopic
            ]).ConfigureAwait(false);
            if (topicRoute.ExitCode == 0 &&
                topicRoute.Stdout.Contains(BrokerAName, StringComparison.Ordinal) &&
                topicRoute.Stdout.Contains(BrokerBName, StringComparison.Ordinal) &&
                topicRoute.Stdout.Contains(BrokerCName, StringComparison.Ordinal))
            {
                return;
            }

            await Task.Delay(TimeSpan.FromMilliseconds(500)).ConfigureAwait(false);
        }

        throw new InvalidOperationException(
            $"Multi-Broker test topic has an incomplete route. stdout: {topicRoute.Stdout} stderr: {topicRoute.Stderr}");
    }

    private async Task<IReadOnlyDictionary<string, long>> GetBrokerMaxOffsetsAsync(
        string topic,
        CancellationToken cancellationToken)
    {
        var result = await _brokerA.ExecAsync([
            "sh", "mqadmin", "topicStatus",
            "-n", $"nameserver:{NameServerPort}",
            "-t", topic
        ], cancellationToken).ConfigureAwait(false);
        if (result.ExitCode != 0)
        {
            throw new InvalidOperationException(
                $"Unable to inspect multi-Broker topic status. stdout: {result.Stdout} stderr: {result.Stderr}");
        }

        var offsets = new Dictionary<string, long>(StringComparer.Ordinal);
        foreach (var line in result.Stdout.Split('\n', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries))
        {
            var fields = line.Split((char[]?)null, StringSplitOptions.RemoveEmptyEntries);
            if (fields.Length < 4 ||
                (fields[0] != BrokerAName && fields[0] != BrokerBName && fields[0] != BrokerCName) ||
                !long.TryParse(fields[3], NumberStyles.Integer, CultureInfo.InvariantCulture, out var maxOffset))
            {
                continue;
            }

            offsets.TryGetValue(fields[0], out var totalOffset);
            offsets[fields[0]] = totalOffset + maxOffset;
        }

        return offsets;
    }

    private static bool HasMessagesOnAllBrokers(IReadOnlyDictionary<string, long> offsets) =>
        offsets.TryGetValue(BrokerAName, out var brokerAOffset) && brokerAOffset > 0 &&
        offsets.TryGetValue(BrokerBName, out var brokerBOffset) && brokerBOffset > 0 &&
        offsets.TryGetValue(BrokerCName, out var brokerCOffset) && brokerCOffset > 0;
}
