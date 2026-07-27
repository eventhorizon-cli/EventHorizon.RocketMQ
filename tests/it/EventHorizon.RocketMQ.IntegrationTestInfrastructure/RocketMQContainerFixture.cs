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

public sealed class RocketMQContainerFixture : IAsyncLifetime
{
    private const string Image = "apache/rocketmq:5.5.0";
    private const int NameServerPort = 9876;

    /// <summary>
    /// Gets the shared topic used only by the legacy serial Remoting integration-test suite.
    /// </summary>
    public const string TestTopic = "rocketmq-dotnet-it";

    /// <summary>
    /// Gets the shared transaction topic used by isolated transaction tests.
    /// </summary>
    public const string TransactionTopic = "rocketmq-dotnet-transaction-it";

    /// <summary>
    /// Gets the shared FIFO topic used by isolated FIFO tests.
    /// </summary>
    public const string FifoTopic = "rocketmq-dotnet-fifo-it";

    /// <summary>
    /// Gets the shared delay topic used by isolated delay tests.
    /// </summary>
    public const string DelayTopic = "rocketmq-dotnet-delay-it";

    /// <summary>
    /// Gets the shared LitePush parent topic used by isolated LitePush tests.
    /// </summary>
    public const string LiteParentTopic = "rocketmq-dotnet-lite-parent-it";

    private readonly SemaphoreSlim _administrationGate = new(4, 4);
    private readonly INetwork _network = new NetworkBuilder().Build();
    private readonly IContainer _nameServer;
    private readonly IContainer _broker;
    private readonly RocketMQHostPortReservation _portReservation;
    private readonly int _nameServerHostPort;
    private readonly int _brokerPort;
    private readonly int _grpcPort;

    public RocketMQContainerFixture()
    {
        _portReservation = RocketMQHostPortReservation.Reserve(3, minimumPortDistance: 10);
        _nameServerHostPort = _portReservation[0];
        _brokerPort = _portReservation[1];
        _grpcPort = _portReservation[2];
        _nameServer = new ContainerBuilder(Image)
            .WithNetwork(_network)
            .WithNetworkAliases("nameserver")
            .WithPortBinding(_nameServerHostPort, NameServerPort)
            .WithEnvironment("JAVA_OPT_EXT", "-Duser.home=/home/rocketmq -Xms256m -Xmx256m")
            .WithCommand("sh", "mqnamesrv")
            .WithWaitStrategy(Wait.ForUnixContainer().UntilInternalTcpPortIsAvailable(NameServerPort))
            .Build();

        var brokerConfiguration = Encoding.UTF8.GetBytes(
            "brokerClusterName=DefaultCluster\n" +
            "brokerName=broker-a\n" +
            "brokerId=0\n" +
            "brokerIP1=127.0.0.1\n" +
            $"namesrvAddr=nameserver:{NameServerPort}\n" +
            $"listenPort={_brokerPort}\n" +
            "autoCreateTopicEnable=true\n" +
            "autoCreateSubscriptionGroup=true\n" +
            "enableLmq=true\n" +
            "enableMultiDispatch=true\n" +
            "recallMessageEnable=true\n");
        var proxyConfiguration = Encoding.UTF8.GetBytes(
            "{\n" +
            "  \"rocketMQClusterName\": \"DefaultCluster\",\n" +
            "  \"proxyClusterName\": \"DefaultCluster\",\n" +
            $"  \"grpcServerPort\": {_grpcPort},\n" +
            "  \"useEndpointPortFromRequest\": true\n" +
            "}\n");

        _broker = new ContainerBuilder(Image)
            .WithNetwork(_network)
            .WithPortBinding(_brokerPort, _brokerPort)
            .WithPortBinding(_grpcPort, _grpcPort)
            .WithEnvironment("NAMESRV_ADDR", $"nameserver:{NameServerPort}")
            .WithEnvironment("JAVA_OPT_EXT", "-Duser.home=/home/rocketmq -Xms512m -Xmx512m")
            .WithResourceMapping(brokerConfiguration, "/tmp/broker.conf")
            .WithResourceMapping(proxyConfiguration, "/home/rocketmq/rocketmq-5.5.0/conf/rmq-proxy.json")
            .WithCommand(
                "sh",
                "-c",
                $"sh /home/rocketmq/rocketmq-5.5.0/bin/mqbroker -c /tmp/broker.conf & " +
                $"for attempt in $(seq 1 60); do " +
                $"if sh /home/rocketmq/rocketmq-5.5.0/bin/mqadmin clusterList -n nameserver:{NameServerPort} " +
                "2>/dev/null | grep -q broker-a; then " +
                $"exec sh /home/rocketmq/rocketmq-5.5.0/bin/mqproxy -pm cluster " +
                $"-n nameserver:{NameServerPort} -pc /home/rocketmq/rocketmq-5.5.0/conf/rmq-proxy.json; " +
                "fi; sleep 1; done; exit 1")
            .WithWaitStrategy(Wait.ForUnixContainer()
                .UntilInternalTcpPortIsAvailable(_brokerPort)
                .UntilInternalTcpPortIsAvailable(_grpcPort)
                .UntilMessageIsLogged("rocketmq-proxy startup successfully"))
            .Build();
    }

    public string NameServerAddress => $"{_nameServer.Hostname}:{_nameServer.GetMappedPublicPort(NameServerPort)}";

    public string BrokerAddress => $"{_broker.Hostname}:{_broker.GetMappedPublicPort(_brokerPort)}";

    public string GrpcEndpoint => $"{_broker.Hostname}:{_broker.GetMappedPublicPort(_grpcPort)}";

    /// <summary>
    /// Creates a test-specific topic and group-name scope for one integration test.
    /// </summary>
    /// <param name="topicType">The message type configured for the topic.</param>
    /// <param name="cancellationToken">The token used to cancel Broker administration requests.</param>
    /// <returns>The isolated integration-test scope.</returns>
    public Task<RocketMQTestScope> CreateTestScopeAsync(
        RocketMQTestTopicType topicType,
        CancellationToken cancellationToken = default)
    {
        return CreateTestScopeCoreAsync(topicType, cancellationToken);
    }

    private async Task<RocketMQTestScope> CreateTestScopeCoreAsync(
        RocketMQTestTopicType topicType,
        CancellationToken cancellationToken)
    {
        var suffix = Guid.NewGuid().ToString("N");
        var topic = topicType switch
        {
            RocketMQTestTopicType.Normal => $"rocketmq-dotnet-it-{suffix}",
            RocketMQTestTopicType.Transaction => TransactionTopic,
            RocketMQTestTopicType.Fifo => FifoTopic,
            RocketMQTestTopicType.Delay => DelayTopic,
            RocketMQTestTopicType.Lite => LiteParentTopic,
            _ => throw new ArgumentOutOfRangeException(nameof(topicType), topicType, null)
        };
        if (topicType == RocketMQTestTopicType.Normal)
        {
            await CreateTopicAsync(topic, GetMessageType(topicType), cancellationToken).ConfigureAwait(false);
        }

        return new RocketMQTestScope(this, topic, suffix);
    }

    public async Task<string> GetTopicListAsync(CancellationToken cancellationToken)
    {
        var result = await _broker.ExecAsync([
            "sh", "mqadmin", "topicList", "-n", $"nameserver:{NameServerPort}"
        ], cancellationToken).ConfigureAwait(false);
        if (result.ExitCode != 0)
        {
            throw new InvalidOperationException(
                $"Unable to list integration-test topics. stdout: {result.Stdout} stderr: {result.Stderr}");
        }

        return result.Stdout;
    }

    public async Task<string> GetTopicStatusAsync(string topic, CancellationToken cancellationToken)
    {
        var result = await _broker.ExecAsync([
            "sh", "mqadmin", "topicStatus",
            "-n", $"nameserver:{NameServerPort}",
            "-t", topic
        ], cancellationToken).ConfigureAwait(false);
        return $"exit={result.ExitCode} stdout={result.Stdout} stderr={result.Stderr}";
    }

    public async Task<string> GetConsumerProgressAsync(string group, CancellationToken cancellationToken)
    {
        var result = await _broker.ExecAsync([
            "sh", "mqadmin", "consumerProgress",
            "-n", $"nameserver:{NameServerPort}",
            "-g", group
        ], cancellationToken).ConfigureAwait(false);
        return $"exit={result.ExitCode} stdout={result.Stdout} stderr={result.Stderr}";
    }

    public async Task<(bool Committed, string Progress)> WaitForConsumerCommitAsync(
        string group,
        string topic,
        string brokerName,
        int queueId,
        TimeSpan timeout,
        CancellationToken cancellationToken)
    {
        var deadline = DateTimeOffset.UtcNow + timeout;
        string progress;
        do
        {
            progress = await GetConsumerProgressAsync(group, cancellationToken).ConfigureAwait(false);
            if (HasZeroLag(progress, topic, brokerName, queueId))
            {
                return (true, progress);
            }

            await Task.Delay(TimeSpan.FromMilliseconds(250), cancellationToken).ConfigureAwait(false);
        }
        while (DateTimeOffset.UtcNow < deadline);

        progress = await GetConsumerProgressAsync(group, cancellationToken).ConfigureAwait(false);
        return (HasZeroLag(progress, topic, brokerName, queueId), progress);
    }

    public async ValueTask InitializeAsync()
    {
        await _network.CreateAsync().ConfigureAwait(false);
        await _nameServer.StartAsync().ConfigureAwait(false);
        await _broker.StartAsync().ConfigureAwait(false);

        await Task.WhenAll(
            CreateTopicAsync(TestTopic, "NORMAL", CancellationToken.None),
            CreateTopicAsync(TransactionTopic, "TRANSACTION", CancellationToken.None),
            CreateTopicAsync(FifoTopic, "FIFO", CancellationToken.None),
            CreateTopicAsync(DelayTopic, "DELAY", CancellationToken.None),
            CreateTopicAsync(LiteParentTopic, "LITE", CancellationToken.None)).ConfigureAwait(false);
        foreach (var group in new[]
                 {
                     "remoting-pull-consumer-it",
                     "legacy-push-consumer-it",
                     "legacy-push-cluster-it"
                 })
        {
            await CreateConsumerGroupAsync(group, null, CancellationToken.None).ConfigureAwait(false);
        }
    }

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
            _administrationGate.Dispose();
            _portReservation.Dispose();
        }
    }

    internal async Task CreateConsumerGroupAsync(
        string group,
        string? liteParentTopic,
        CancellationToken cancellationToken)
    {
        await _administrationGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            var createGroupCommand = new List<string>
            {
                "sh", "mqadmin", "updateSubGroup",
                "-n", $"nameserver:{NameServerPort}",
                "-c", "DefaultCluster",
                "-g", group
            };
            if (liteParentTopic is not null)
            {
                createGroupCommand.Add("--attributes");
                createGroupCommand.Add($"+lite.bind.topic={liteParentTopic}");
            }

            var createGroup = await _broker.ExecAsync(createGroupCommand, cancellationToken).ConfigureAwait(false);
            if (createGroup.ExitCode != 0)
            {
                throw new InvalidOperationException(
                    $"Unable to create integration-test consumer group '{group}'. stdout: {createGroup.Stdout} stderr: {createGroup.Stderr}");
            }
        }
        finally
        {
            _administrationGate.Release();
        }
    }

    private static bool HasZeroLag(string progress, string topic, string brokerName, int queueId)
    {
        foreach (var line in progress.Split('\n', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries))
        {
            var fields = line.Split((char[]?)null, StringSplitOptions.RemoveEmptyEntries);
            if (fields.Length < 6 ||
                !string.Equals(fields[0], topic, StringComparison.Ordinal) ||
                !string.Equals(fields[1], brokerName, StringComparison.Ordinal) ||
                !int.TryParse(fields[2], out var actualQueueId) ||
                actualQueueId != queueId ||
                !long.TryParse(fields[3], out var brokerOffset) ||
                !long.TryParse(fields[4], out var consumerOffset) ||
                !long.TryParse(fields[5], out var difference))
            {
                continue;
            }

            return consumerOffset >= brokerOffset && difference == 0;
        }

        return false;
    }

    private async Task CreateTopicAsync(string topic, string messageType, CancellationToken cancellationToken)
    {
        await _administrationGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            var createTopic = await _broker.ExecAsync([
                "sh", "mqadmin", "updateTopic",
                "-n", $"nameserver:{NameServerPort}",
                "-c", "DefaultCluster",
                "-t", topic,
                "-a", $"+message.type={messageType}"
            ], cancellationToken).ConfigureAwait(false);
            if (createTopic.ExitCode != 0)
            {
                throw new InvalidOperationException(
                    $"Unable to create integration-test topic '{topic}'. stdout: {createTopic.Stdout} stderr: {createTopic.Stderr}");
            }

            var topicRoute = await _broker.ExecAsync([
                "sh", "mqadmin", "topicRoute",
                "-n", $"nameserver:{NameServerPort}",
                "-t", topic
            ], cancellationToken).ConfigureAwait(false);
            if (topicRoute.ExitCode != 0 || !topicRoute.Stdout.Contains("broker-a", StringComparison.Ordinal))
            {
                throw new InvalidOperationException(
                    $"Test topic '{topic}' has no route. stdout: {topicRoute.Stdout} stderr: {topicRoute.Stderr}");
            }
        }
        finally
        {
            _administrationGate.Release();
        }
    }

    private static string GetMessageType(RocketMQTestTopicType topicType)
    {
        return topicType switch
        {
            RocketMQTestTopicType.Normal => "NORMAL",
            RocketMQTestTopicType.Transaction => "TRANSACTION",
            RocketMQTestTopicType.Fifo => "FIFO",
            RocketMQTestTopicType.Delay => "DELAY",
            RocketMQTestTopicType.Lite => "LITE",
            _ => throw new ArgumentOutOfRangeException(nameof(topicType), topicType, null)
        };
    }
}
