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
using System.Text;
using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;
using DotNet.Testcontainers.Networks;
using Xunit;

namespace EventHorizon.RocketMQ.IntegrationTestInfrastructure;

/// <summary>
/// Provides a NameServer and three host-reachable classic Remoting Brokers for integration tests.
/// </summary>
public sealed class RocketMQMultiBrokerRemotingContainerFixture : IAsyncLifetime
{
    private const string Image = "apache/rocketmq:5.5.0";
    private const int NameServerPort = 9876;

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
    /// Gets the topic created on all Brokers for multi-Broker Remoting tests.
    /// </summary>
    public const string TestTopic = "rocketmq-dotnet-remoting-multi-broker-it";

    private readonly INetwork _network = new NetworkBuilder().Build();
    private readonly IContainer _nameServer;
    private readonly IContainer _brokerA;
    private readonly IContainer _brokerB;
    private readonly IContainer _brokerC;
    private readonly int _brokerAPort;
    private readonly int _brokerBPort;
    private readonly int _brokerCPort;

    /// <summary>
    /// Initializes a new instance of the <see cref="RocketMQMultiBrokerRemotingContainerFixture"/> class.
    /// </summary>
    public RocketMQMultiBrokerRemotingContainerFixture()
    {
        _brokerAPort = GetAvailablePort();
        do
        {
            _brokerBPort = GetAvailablePort();
        }
        while (_brokerBPort == _brokerAPort);

        do
        {
            _brokerCPort = GetAvailablePort();
        }
        while (_brokerCPort == _brokerAPort || _brokerCPort == _brokerBPort);

        _nameServer = new ContainerBuilder(Image)
            .WithNetwork(_network)
            .WithNetworkAliases("nameserver")
            .WithPortBinding(NameServerPort, true)
            .WithEnvironment("JAVA_OPT_EXT", "-Duser.home=/home/rocketmq -Xms256m -Xmx256m")
            .WithCommand("sh", "mqnamesrv")
            .WithWaitStrategy(Wait.ForUnixContainer().UntilInternalTcpPortIsAvailable(NameServerPort))
            .Build();

        _brokerA = CreateBroker(BrokerAName, _brokerAPort);
        _brokerB = CreateBroker(BrokerBName, _brokerBPort);
        _brokerC = CreateBroker(BrokerCName, _brokerCPort);
    }

    /// <summary>
    /// Gets the host-reachable NameServer address for classic Remoting clients.
    /// </summary>
    public string NameServerAddress => $"{_nameServer.Hostname}:{_nameServer.GetMappedPublicPort(NameServerPort)}";

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
    }

    /// <inheritdoc/>
    public async ValueTask DisposeAsync()
    {
        await _brokerC.DisposeAsync().ConfigureAwait(false);
        await _brokerB.DisposeAsync().ConfigureAwait(false);
        await _brokerA.DisposeAsync().ConfigureAwait(false);
        await _nameServer.DisposeAsync().ConfigureAwait(false);
        await _network.DisposeAsync().ConfigureAwait(false);
    }

    private IContainer CreateBroker(string brokerName, int port)
    {
        var brokerConfiguration = Encoding.UTF8.GetBytes(
            "brokerClusterName=DefaultCluster\n" +
            $"brokerName={brokerName}\n" +
            "brokerId=0\n" +
            "brokerIP1=127.0.0.1\n" +
            $"namesrvAddr=nameserver:{NameServerPort}\n" +
            $"listenPort={port}\n" +
            "autoCreateTopicEnable=true\n");

        return new ContainerBuilder(Image)
            .WithNetwork(_network)
            .WithNetworkAliases(brokerName)
            .WithPortBinding(port, port)
            .WithEnvironment("NAMESRV_ADDR", $"nameserver:{NameServerPort}")
            .WithEnvironment("JAVA_OPT_EXT", "-Duser.home=/home/rocketmq -Xms256m -Xmx256m")
            .WithResourceMapping(brokerConfiguration, "/tmp/broker.conf")
            .WithCommand("sh", "mqbroker", "-c", "/tmp/broker.conf")
            .WithWaitStrategy(Wait.ForUnixContainer().UntilInternalTcpPortIsAvailable(port))
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
        foreach (var (brokerName, port) in new[]
                 {
                     (BrokerAName, _brokerAPort),
                     (BrokerBName, _brokerBPort),
                     (BrokerCName, _brokerCPort)
                 })
        {
            var createTopic = await _brokerA.ExecAsync([
                "sh", "mqadmin", "updateTopic",
                "-n", $"nameserver:{NameServerPort}",
                "-b", $"{brokerName}:{port}",
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

    private static int GetAvailablePort()
    {
        using var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        return ((IPEndPoint)listener.LocalEndpoint).Port;
    }
}
