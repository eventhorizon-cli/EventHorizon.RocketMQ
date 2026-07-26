# Local and Integration Testing

[English](local-and-integration-testing.md) | [Simplified Chinese](../../zh-CN/testing/local-and-integration-testing.md)

## Context

The two client projects have different runtime dependencies. A gRPC integration path requires a
working RocketMQ Proxy; a Remoting integration path requires a NameServer, reachable Broker routes,
and a classic Broker transport. Unit tests cannot establish those guarantees, while a permanently
running local cluster makes automated tests less isolated and harder to reproduce.

The repository therefore uses two complementary environments: Docker-backed Testcontainers for
integration tests and Docker Compose for manual exploration and samples.

## Decision

```text
Unit and compatibility tests
    -> protocol-specific test projects
    -> one project compiling against both protocol assemblies
    -> deterministic collaborators, no RocketMQ network

Integration tests
    -> protocol-isolated Testcontainers fixtures
    -> single-Broker baseline and three-Broker route coverage
    -> protocol-specific integration test assemblies

Manual samples
    -> test-environments/rocketmq Compose stack
    -> stable localhost ports and optional persistent volumes
```

The Testcontainers infrastructure Project is not another protocol client. It must not
reference either production protocol project. The gRPC and Remoting integration-test assemblies
reference it and keep their assertions protocol-specific.

## How It Works

### Unit tests

Protocol behavior has matching unit-test projects, while one compatibility project references both
protocol Projects:

- [`tests/EventHorizon.RocketMQ.Grpc.Tests`](../../../tests/EventHorizon.RocketMQ.Grpc.Tests)
- [`tests/EventHorizon.RocketMQ.Remoting.Tests`](../../../tests/EventHorizon.RocketMQ.Remoting.Tests)
- [`tests/EventHorizon.RocketMQ.Compatibility.Tests`](../../../tests/EventHorizon.RocketMQ.Compatibility.Tests)

They validate isolated behavior such as serialization, route caching, client lifecycle, handler
lifetimes, and public contracts without depending on a Broker. The compatibility tests compile
against both protocol Projects to verify both public APIs, namespace separation, and dual-Package
consumption. Replaceable collaborators normally use Moq;
purpose-built fakes remain appropriate for stateful framing, streaming, or concurrency behavior
that would be less clear with a mock.

### Integration tests and Testcontainers

[`RocketMQContainerFixture`](../../../tests/EventHorizon.RocketMQ.IntegrationTestInfrastructure/RocketMQContainerFixture.cs)
creates an isolated Docker network, starts an Apache RocketMQ 5.5.0 NameServer and Broker, then
starts a cluster-mode Proxy after the Broker registers. It creates the normal, FIFO, delayed,
transactional, and Lite topic resources used by integration tests and prepares the required
consumer groups.

The fixture obtains host ports dynamically, so parallel or repeated test runs are not tied to the
manual Compose ports. The protocol-specific test projects are:

- [`tests/EventHorizon.RocketMQ.Grpc.IntegrationTests`](../../../tests/EventHorizon.RocketMQ.Grpc.IntegrationTests)
- [`tests/EventHorizon.RocketMQ.Remoting.IntegrationTests`](../../../tests/EventHorizon.RocketMQ.Remoting.IntegrationTests)

The multi-Broker fixtures complement the baseline fixture:

- [`RocketMQMultiBrokerRemotingContainerFixture`](../../../tests/EventHorizon.RocketMQ.IntegrationTestInfrastructure/RocketMQMultiBrokerRemotingContainerFixture.cs)
  starts a NameServer and three host-reachable master Brokers. The Remoting test requires a complete route for
  `broker-a`, `broker-b`, and `broker-c`, then sends to an explicit queue on each Broker.
- [`RocketMQMultiBrokerGrpcContainerFixture`](../../../tests/EventHorizon.RocketMQ.IntegrationTestInfrastructure/RocketMQMultiBrokerGrpcContainerFixture.cs)
  starts a NameServer, three master Brokers, and a cluster-mode Proxy on an isolated Docker network. The gRPC test
  sends through the Proxy and confirms that every Broker has stored messages.

The fixtures create the test topic on each Broker with `mqadmin updateTopic -b` and wait for the complete route.
Do not assume that `updateTopic -c DefaultCluster` creates a topic on every master Broker.

Run the relevant project when a change affects live Proxy, Broker, NameServer, wire transport, or
interoperability behavior:

```shell
dotnet test tests/EventHorizon.RocketMQ.Grpc.IntegrationTests/EventHorizon.RocketMQ.Grpc.IntegrationTests.csproj --no-restore
dotnet test tests/EventHorizon.RocketMQ.Remoting.IntegrationTests/EventHorizon.RocketMQ.Remoting.IntegrationTests.csproj --no-restore
```

Docker must be available to run these tests. The manual Compose stack is not a prerequisite for
them.

### CI coverage

GitHub Actions runs the Docker-backed gRPC and Remoting integration-test projects in four parallel
`ubuntu-latest` matrix jobs after formatting, build, and unit-test validation complete: one
single-Broker and one multi-Broker job for each protocol. A class-level `Topology=MultiBroker` trait
selects the multi-Broker tests; the single-Broker jobs run the complementary set. Each job restores
and builds only its protocol-specific integration-test dependency graph, has its own timeout budget,
and reuses the repository's NuGet package cache. Testcontainers still starts isolated Broker,
NameServer, and Proxy fixtures for every job. All four jobs collect Cobertura reports and upload them
to Codecov with distinct upload names under the shared `integration-tests` flag. Codecov combines
those reports with the `unit-tests` reports; the repository configuration continues to exclude
`samples` from coverage.

### Manual Compose environment

`test-environments` groups self-contained manual Compose stacks. Choose
[`rocketmq`](../../../test-environments/rocketmq) for general single-Broker samples,
[`rocketmq-litepush`](../../../test-environments/rocketmq-litepush) for the self-initializing LitePush sample, and
[`rocketmq-multi-broker`](../../../test-environments/rocketmq-multi-broker) for manual three-Broker route inspection.

The single-Broker environment exposes stable loopback endpoints:

| Client path | Local endpoint |
| --- | --- |
| NameServer for classic Remoting | `localhost:9876` |
| Broker remoting port | `localhost:10911` |
| RocketMQ Proxy gRPC endpoint | `localhost:8081` |

The Compose file starts the Broker and cluster-mode Proxy in the same container after Broker
registration. A one-shot `volume-init` service prepares ownership for the image's non-root user.
By default, Docker named volumes keep logs and message data across `docker compose down`; the
optional host-volume override stores the same data under `test-environments/rocketmq/data` for
inspection.

The [test-environment index](../../../test-environments/README.md) explains the purpose, ports, and startup
constraints of each stack. The [samples index](../../../samples/README.md) links each runnable project to its exact
Broker or Proxy requirements.

## Trade-offs and Constraints

- Unit tests are fast and deterministic but cannot prove Docker, network, Broker, Proxy, or server
  version compatibility.
- Integration tests exercise the real stack but need Docker and take longer. Keep them focused on
  behavior that actually crosses the transport boundary.
- Compose is intentionally manual and persistent by default. It is useful for learning and samples,
  but it is not a replacement for isolated integration-test fixtures.
- The dedicated LitePush environment creates the required parent topic and consumer group during Compose startup.
  It does not turn every protobuf RPC declared upstream into an available server feature.
- Configuration changes require validating the rendered Compose file for the affected environment.

## Related Reading

- [Test-environment index](../../../test-environments/README.md)
- [Runnable samples](../../../samples/README.md)
- [gRPC consumer model](../grpc/consumer-model.md)
- [Classic Remoting transport and client roles](../remoting/transport-and-client-roles.md)
