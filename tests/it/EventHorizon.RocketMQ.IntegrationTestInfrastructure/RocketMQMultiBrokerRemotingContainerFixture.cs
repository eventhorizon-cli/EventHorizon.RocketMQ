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

using System.Text;
using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;
using DotNet.Testcontainers.Networks;
using Xunit;

namespace EventHorizon.RocketMQ.IntegrationTestInfrastructure;

/// <summary>
/// Provides two NameServers and three host-reachable classic Remoting Brokers for integration tests.
/// </summary>
/// <remarks>
/// Each Broker advertises loopback with a distinct dynamically allocated host port. A separate Proxy container would
/// resolve loopback to itself rather than to these three Brokers, so this topology remains separate from
/// <see cref="RocketMQMultiBrokerGrpcContainerFixture"/>. xUnit owns this type directly as a collection fixture; an
/// additional lazy registry would not extend its lifecycle.
/// </remarks>
public sealed class RocketMQMultiBrokerRemotingContainerFixture : IAsyncLifetime
{
    private const string Image = "apache/rocketmq:5.5.0";
    private const int NameServerPort = 9876;
    private const string NameServerAAlias = "nameserver-a";
    private const string NameServerBAlias = "nameserver-b";

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

    /// <summary>
    /// Gets the number of readable and writable queues created on each Broker for the test topic.
    /// </summary>
    public const int QueuesPerBroker = 3;

    private readonly INetwork _network = new NetworkBuilder().Build();
    private readonly IContainer _nameServerA;
    private readonly IContainer _nameServerB;
    private readonly IContainer _brokerA;
    private readonly IContainer _brokerB;
    private readonly IContainer _brokerC;
    private readonly RocketMQHostPortReservation _portReservation;
    private readonly int _nameServerAHostPort;
    private readonly int _nameServerBHostPort;
    private readonly int _brokerAPort;
    private readonly int _brokerBPort;
    private readonly int _brokerCPort;

    /// <summary>
    /// Initializes a new instance of the <see cref="RocketMQMultiBrokerRemotingContainerFixture"/> class.
    /// </summary>
    public RocketMQMultiBrokerRemotingContainerFixture()
    {
        _portReservation = RocketMQHostPortReservation.Reserve(5);
        _nameServerAHostPort = _portReservation[0];
        _nameServerBHostPort = _portReservation[1];
        _brokerAPort = _portReservation[2];
        _brokerBPort = _portReservation[3];
        _brokerCPort = _portReservation[4];

        _nameServerA = CreateNameServer(NameServerAAlias, _nameServerAHostPort);
        _nameServerB = CreateNameServer(NameServerBAlias, _nameServerBHostPort);

        _brokerA = CreateBroker(BrokerAName, _brokerAPort);
        _brokerB = CreateBroker(BrokerBName, _brokerBPort);
        _brokerC = CreateBroker(BrokerCName, _brokerCPort);
    }

    /// <summary>
    /// Gets the host-reachable address of the first NameServer.
    /// </summary>
    public string NameServerAAddress =>
        $"{_nameServerA.Hostname}:{_nameServerA.GetMappedPublicPort(NameServerPort)}";

    /// <summary>
    /// Gets the host-reachable address of the second NameServer.
    /// </summary>
    public string NameServerBAddress =>
        $"{_nameServerB.Hostname}:{_nameServerB.GetMappedPublicPort(NameServerPort)}";

    /// <summary>
    /// Gets both host-reachable NameServer addresses as a semicolon-separated client setting.
    /// </summary>
    public string NameServerAddresses => $"{NameServerAAddress};{NameServerBAddress}";

    /// <inheritdoc/>
    public async ValueTask InitializeAsync()
    {
        await _network.CreateAsync().ConfigureAwait(false);
        await Task.WhenAll(
            _nameServerA.StartAsync(),
            _nameServerB.StartAsync()).ConfigureAwait(false);
        await Task.WhenAll(
            _brokerA.StartAsync(),
            _brokerB.StartAsync(),
            _brokerC.StartAsync()).ConfigureAwait(false);

        await WaitForClusterRegistrationAsync(NameServerAAlias).ConfigureAwait(false);
        await WaitForClusterRegistrationAsync(NameServerBAlias).ConfigureAwait(false);
        await CreateTopicAsync().ConfigureAwait(false);
        await WaitForTopicRouteAsync(NameServerAAlias).ConfigureAwait(false);
        await WaitForTopicRouteAsync(NameServerBAlias).ConfigureAwait(false);
    }

    /// <summary>
    /// Stops the first NameServer so a test can verify client failover to the second address.
    /// </summary>
    /// <param name="cancellationToken">The token used to cancel the stop operation.</param>
    /// <returns>A task that represents the asynchronous stop operation.</returns>
    public Task StopNameServerAAsync(CancellationToken cancellationToken = default) =>
        _nameServerA.StopAsync(cancellationToken);

    /// <summary>
    /// Restarts the first NameServer and waits until its Broker and topic routes are complete again.
    /// </summary>
    /// <param name="cancellationToken">The token used to cancel recovery.</param>
    /// <returns>A task that represents the asynchronous recovery operation.</returns>
    public async Task RestoreNameServerAAsync(CancellationToken cancellationToken = default)
    {
        await _nameServerA.StartAsync(cancellationToken).ConfigureAwait(false);
        await WaitForClusterRegistrationAsync(NameServerAAlias, cancellationToken).ConfigureAwait(false);
        await WaitForTopicRouteAsync(NameServerAAlias, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc/>
    public async ValueTask DisposeAsync()
    {
        try
        {
            await _brokerC.DisposeAsync().ConfigureAwait(false);
            await _brokerB.DisposeAsync().ConfigureAwait(false);
            await _brokerA.DisposeAsync().ConfigureAwait(false);
            await _nameServerB.DisposeAsync().ConfigureAwait(false);
            await _nameServerA.DisposeAsync().ConfigureAwait(false);
            await _network.DisposeAsync().ConfigureAwait(false);
        }
        finally
        {
            _portReservation.Dispose();
        }
    }

    private IContainer CreateNameServer(string networkAlias, int hostPort) =>
        new ContainerBuilder(Image)
            .WithNetwork(_network)
            .WithNetworkAliases(networkAlias)
            .WithPortBinding(hostPort, NameServerPort)
            .WithEnvironment("JAVA_OPT_EXT", "-Duser.home=/home/rocketmq -Xms256m -Xmx256m")
            .WithCommand("sh", "mqnamesrv")
            .WithWaitStrategy(Wait.ForUnixContainer().UntilInternalTcpPortIsAvailable(NameServerPort))
            .Build();

    private IContainer CreateBroker(string brokerName, int port)
    {
        var nameServerAddresses = $"{NameServerAAlias}:{NameServerPort};{NameServerBAlias}:{NameServerPort}";
        var brokerConfiguration = Encoding.UTF8.GetBytes(
            "brokerClusterName=DefaultCluster\n" +
            $"brokerName={brokerName}\n" +
            "brokerId=0\n" +
            "brokerIP1=127.0.0.1\n" +
            $"namesrvAddr={nameServerAddresses}\n" +
            $"listenPort={port}\n" +
            "autoCreateTopicEnable=true\n" +
            "autoCreateSubscriptionGroup=true\n");

        return new ContainerBuilder(Image)
            .WithNetwork(_network)
            .WithNetworkAliases(brokerName)
            .WithPortBinding(port, port)
            .WithEnvironment("NAMESRV_ADDR", nameServerAddresses)
            .WithEnvironment("JAVA_OPT_EXT", "-Duser.home=/home/rocketmq -Xms256m -Xmx256m")
            .WithResourceMapping(brokerConfiguration, "/tmp/broker.conf")
            .WithCommand("sh", "mqbroker", "-c", "/tmp/broker.conf")
            .WithWaitStrategy(Wait.ForUnixContainer().UntilInternalTcpPortIsAvailable(port))
            .Build();
    }

    private async Task WaitForClusterRegistrationAsync(
        string nameServerAlias,
        CancellationToken cancellationToken = default)
    {
        ExecResult clusterList = default;
        for (var attempt = 0; attempt < 120; attempt++)
        {
            clusterList = await _brokerA.ExecAsync([
                "sh", "mqadmin", "clusterList", "-n", $"{nameServerAlias}:{NameServerPort}"
            ]).ConfigureAwait(false);
            if (clusterList.ExitCode == 0 &&
                clusterList.Stdout.Contains(BrokerAName, StringComparison.Ordinal) &&
                clusterList.Stdout.Contains(BrokerBName, StringComparison.Ordinal) &&
                clusterList.Stdout.Contains(BrokerCName, StringComparison.Ordinal))
            {
                return;
            }

            await Task.Delay(TimeSpan.FromMilliseconds(500), cancellationToken).ConfigureAwait(false);
        }

        throw new InvalidOperationException(
            $"All three Brokers did not register with NameServer '{nameServerAlias}'. stdout: {clusterList.Stdout} " +
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
                "-n", $"{NameServerAAlias}:{NameServerPort}",
                "-b", $"{brokerName}:{port}",
                "-t", TestTopic,
                "-r", QueuesPerBroker.ToString(),
                "-w", QueuesPerBroker.ToString(),
                "-a", "+message.type=NORMAL"
            ]).ConfigureAwait(false);
            if (createTopic.ExitCode != 0)
            {
                throw new InvalidOperationException(
                    $"Unable to create the multi-Broker integration-test topic on '{brokerName}'. " +
                    $"stdout: {createTopic.Stdout} stderr: {createTopic.Stderr}");
            }
        }
    }

    private async Task WaitForTopicRouteAsync(
        string nameServerAlias,
        CancellationToken cancellationToken = default)
    {
        ExecResult topicRoute = default;
        for (var attempt = 0; attempt < 120; attempt++)
        {
            topicRoute = await _brokerA.ExecAsync([
                "sh", "mqadmin", "topicRoute",
                "-n", $"{nameServerAlias}:{NameServerPort}",
                "-t", TestTopic
            ]).ConfigureAwait(false);
            if (topicRoute.ExitCode == 0 &&
                topicRoute.Stdout.Contains(BrokerAName, StringComparison.Ordinal) &&
                topicRoute.Stdout.Contains(BrokerBName, StringComparison.Ordinal) &&
                topicRoute.Stdout.Contains(BrokerCName, StringComparison.Ordinal))
            {
                return;
            }

            await Task.Delay(TimeSpan.FromMilliseconds(500), cancellationToken).ConfigureAwait(false);
        }

        throw new InvalidOperationException(
            $"Multi-Broker test topic has an incomplete route on NameServer '{nameServerAlias}'. " +
            $"stdout: {topicRoute.Stdout} stderr: {topicRoute.Stderr}");
    }

}
