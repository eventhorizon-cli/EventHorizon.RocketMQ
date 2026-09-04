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
/// Provides a Broker-integrated local-mode Proxy topology for gRPC integration tests.
/// </summary>
/// <remarks>
/// At the official <c>rocketmq-all-5.5.0</c> release tag, RocketMQ starts the local Proxy through
/// <c>mqbroker --enable-proxy</c>. The Proxy embeds the Broker in the same process, while the NameServer remains an
/// independent container on the test network. Local-mode routes advertise the configured gRPC port directly, so the
/// fixture uses the same dynamically reserved port inside the container and on the host.
/// </remarks>
public sealed class RocketMQLocalProxyContainerFixture : IAsyncLifetime
{
    private const string Image = "apache/rocketmq:5.5.0";
    private const string NameServerAlias = "nameserver";
    private const int NameServerPort = 9876;
    private const int BrokerPort = 10911;

    /// <summary>
    /// Gets the name of the embedded Broker.
    /// </summary>
    public const string BrokerName = "broker-a";

    /// <summary>
    /// Gets the normal topic created for the local-mode Proxy integration test.
    /// </summary>
    public const string TestTopic = "rocketmq-dotnet-grpc-local-proxy-it";

    private readonly INetwork _network = new NetworkBuilder().Build();
    private readonly IContainer _nameServer;
    private readonly IContainer _broker;
    private readonly RocketMQHostPortReservation _portReservation;
    private readonly int _grpcHostPort;

    /// <summary>
    /// Initializes a new instance of the <see cref="RocketMQLocalProxyContainerFixture"/> class.
    /// </summary>
    public RocketMQLocalProxyContainerFixture()
    {
        _portReservation = RocketMQHostPortReservation.Reserve(1);
        _grpcHostPort = _portReservation[0];

        _nameServer = new ContainerBuilder(Image)
            .WithNetwork(_network)
            .WithNetworkAliases(NameServerAlias)
            .WithEnvironment("JAVA_OPT_EXT", "-Duser.home=/home/rocketmq -Xms256m -Xmx256m")
            .WithCommand("sh", "mqnamesrv")
            .WithWaitStrategy(Wait.ForUnixContainer().UntilInternalTcpPortIsAvailable(NameServerPort))
            .Build();

        var brokerConfiguration = Encoding.UTF8.GetBytes(
            "brokerClusterName=DefaultCluster\n" +
            $"brokerName={BrokerName}\n" +
            "brokerId=0\n" +
            "brokerIP1=127.0.0.1\n" +
            $"namesrvAddr={NameServerAlias}:{NameServerPort}\n" +
            $"listenPort={BrokerPort}\n" +
            "autoCreateTopicEnable=true\n" +
            "autoCreateSubscriptionGroup=true\n" +
            "enablePropertyFilter=true\n" +
            "timerWheelEnable=true\n" +
            "serverLoadBalancerEnable=true\n" +
            "defaultMessageRequestMode=PULL\n");
        var proxyConfiguration = Encoding.UTF8.GetBytes(
            "{\n" +
            "  \"rocketMQClusterName\": \"DefaultCluster\",\n" +
            "  \"proxyClusterName\": \"DefaultCluster\",\n" +
            $"  \"grpcServerPort\": {_grpcHostPort}\n" +
            "}\n");

        _broker = new ContainerBuilder(Image)
            .WithNetwork(_network)
            .WithNetworkAliases("broker")
            .WithPortBinding(_grpcHostPort, _grpcHostPort)
            .WithEnvironment("NAMESRV_ADDR", $"{NameServerAlias}:{NameServerPort}")
            .WithEnvironment("JAVA_OPT_EXT", "-Duser.home=/home/rocketmq -Xms512m -Xmx512m")
            .WithResourceMapping(brokerConfiguration, "/tmp/broker.conf")
            .WithResourceMapping(proxyConfiguration, "/home/rocketmq/rocketmq-5.5.0/conf/rmq-proxy.json")
            .WithCommand(
                "sh",
                "mqbroker",
                "--enable-proxy",
                "-n",
                $"{NameServerAlias}:{NameServerPort}",
                "-c",
                "/tmp/broker.conf")
            .WithWaitStrategy(Wait.ForUnixContainer()
                .UntilInternalTcpPortIsAvailable(BrokerPort)
                .UntilInternalTcpPortIsAvailable(_grpcHostPort)
                .UntilMessageIsLogged("rocketmq-proxy startup successfully"))
            .Build();
    }

    /// <summary>
    /// Gets the host-reachable gRPC endpoint of the Broker-integrated local-mode Proxy.
    /// </summary>
    public string GrpcEndpoint => $"{_broker.Hostname}:{_broker.GetMappedPublicPort(_grpcHostPort)}";

    /// <summary>
    /// Creates a consumer group for a local-mode gRPC integration test.
    /// </summary>
    /// <param name="group">The consumer group to create.</param>
    /// <param name="cancellationToken">The token used to cancel the Broker administration request.</param>
    /// <returns>A task that represents the asynchronous administration request.</returns>
    public async Task CreateConsumerGroupAsync(string group, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(group);
        var createGroup = await _broker.ExecAsync([
            "sh", "mqadmin", "updateSubGroup",
            "-n", $"{NameServerAlias}:{NameServerPort}",
            "-c", "DefaultCluster",
            "-g", group
        ], cancellationToken).ConfigureAwait(false);
        if (createGroup.ExitCode != 0)
        {
            throw new InvalidOperationException(
                $"Unable to create local-mode integration-test consumer group '{group}'. " +
                $"stdout: {createGroup.Stdout} stderr: {createGroup.Stderr}");
        }
    }

    /// <inheritdoc/>
    public async ValueTask InitializeAsync()
    {
        await _network.CreateAsync().ConfigureAwait(false);
        await _nameServer.StartAsync().ConfigureAwait(false);
        await _broker.StartAsync().ConfigureAwait(false);

        await WaitForBrokerRegistrationAsync().ConfigureAwait(false);
        await CreateTopicAsync().ConfigureAwait(false);
    }

    /// <inheritdoc/>
    public async ValueTask DisposeAsync()
    {
        try
        {
            await _broker.DisposeAsync().ConfigureAwait(false);
            await _nameServer.DisposeAsync().ConfigureAwait(false);
            await _network.DisposeAsync().ConfigureAwait(false);
        }
        finally
        {
            _portReservation.Dispose();
        }
    }

    private async Task WaitForBrokerRegistrationAsync()
    {
        ExecResult clusterList = default;
        for (var attempt = 0; attempt < 60; attempt++)
        {
            clusterList = await _broker.ExecAsync([
                "sh", "mqadmin", "clusterList", "-n", $"{NameServerAlias}:{NameServerPort}"
            ]).ConfigureAwait(false);
            if (clusterList.ExitCode == 0 &&
                clusterList.Stdout.Contains(BrokerName, StringComparison.Ordinal))
            {
                return;
            }

            await Task.Delay(TimeSpan.FromMilliseconds(500)).ConfigureAwait(false);
        }

        throw new InvalidOperationException(
            $"Broker '{BrokerName}' did not register with NameServer. stdout: {clusterList.Stdout} " +
            $"stderr: {clusterList.Stderr}");
    }

    private async Task CreateTopicAsync()
    {
        var createTopic = await _broker.ExecAsync([
            "sh", "mqadmin", "updateTopic",
            "-n", $"{NameServerAlias}:{NameServerPort}",
            "-c", "DefaultCluster",
            "-t", TestTopic,
            "-a", "+message.type=NORMAL"
        ]).ConfigureAwait(false);
        if (createTopic.ExitCode != 0)
        {
            throw new InvalidOperationException(
                $"Unable to create local-mode integration-test topic. stdout: {createTopic.Stdout} " +
                $"stderr: {createTopic.Stderr}");
        }

        ExecResult topicRoute = default;
        for (var attempt = 0; attempt < 60; attempt++)
        {
            topicRoute = await _broker.ExecAsync([
                "sh", "mqadmin", "topicRoute",
                "-n", $"{NameServerAlias}:{NameServerPort}",
                "-t", TestTopic
            ]).ConfigureAwait(false);
            if (topicRoute.ExitCode == 0 &&
                topicRoute.Stdout.Contains(BrokerName, StringComparison.Ordinal))
            {
                return;
            }

            await Task.Delay(TimeSpan.FromMilliseconds(500)).ConfigureAwait(false);
        }

        throw new InvalidOperationException(
            $"Local-mode integration-test topic has no route. stdout: {topicRoute.Stdout} " +
            $"stderr: {topicRoute.Stderr}");
    }
}
