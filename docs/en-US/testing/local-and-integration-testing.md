# Local and Integration Testing

[English](local-and-integration-testing.md) | [Simplified Chinese](../../zh-CN/testing/local-and-integration-testing.md)

## Context

The two client packages have different runtime dependencies. A gRPC integration path requires a
working RocketMQ Proxy; a Remoting integration path requires a NameServer, reachable Broker routes,
and a classic Broker transport. Unit tests cannot establish those guarantees, while a permanently
running local cluster makes automated tests less isolated and harder to reproduce.

The repository therefore uses two complementary environments: Docker-backed Testcontainers for
integration tests and Docker Compose for manual exploration and samples.

## Decision

```text
Unit tests
    -> protocol-specific test projects
    -> deterministic collaborators, no RocketMQ network

Integration tests
    -> shared Testcontainers fixture
    -> isolated NameServer + Broker + cluster Proxy
    -> protocol-specific integration test assemblies

Manual samples
    -> test-environments/rocketmq Compose stack
    -> stable localhost ports and optional persistent volumes
```

The shared Testcontainers project is infrastructure, not another protocol client. It must not
reference either production protocol package. The gRPC and Remoting integration-test assemblies
reference it and keep their assertions protocol-specific.

## How It Works

### Unit tests

The production packages have matching unit-test projects:

- [`tests/EventHorizon.RocketMQ.Shared.Tests`](../../../tests/EventHorizon.RocketMQ.Shared.Tests)
- [`tests/EventHorizon.RocketMQ.Grpc.Tests`](../../../tests/EventHorizon.RocketMQ.Grpc.Tests)
- [`tests/EventHorizon.RocketMQ.Remoting.Tests`](../../../tests/EventHorizon.RocketMQ.Remoting.Tests)

They validate isolated behavior such as serialization, route caching, client lifecycle, handler
lifetimes, and public contracts without depending on a Broker. Replaceable collaborators normally
use Moq; purpose-built fakes remain appropriate for stateful framing, streaming, or concurrency
behavior that would be less clear with a mock.

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

Run the relevant project when a change affects live Proxy, Broker, NameServer, wire transport, or
interoperability behavior:

```shell
dotnet test tests/EventHorizon.RocketMQ.Grpc.IntegrationTests/EventHorizon.RocketMQ.Grpc.IntegrationTests.csproj --no-restore
dotnet test tests/EventHorizon.RocketMQ.Remoting.IntegrationTests/EventHorizon.RocketMQ.Remoting.IntegrationTests.csproj --no-restore
```

Docker must be available to run these tests. The manual Compose stack is not a prerequisite for
them.

### Manual Compose environment

`test-environments` groups self-contained manual Compose stacks. Its current
[`rocketmq`](../../../test-environments/rocketmq) environment is designed for people running the samples or
inspecting a local cluster. It exposes stable loopback endpoints:

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

The [local environment guide](../../../test-environments/rocketmq/README.md) describes startup,
resource preparation, persistence, reset behavior, and LitePush-specific setup. The
[samples index](../../../samples/README.md) links each runnable project to its exact Broker or
Proxy requirements.

## Trade-offs and Constraints

- Unit tests are fast and deterministic but cannot prove Docker, network, Broker, Proxy, or server
  version compatibility.
- Integration tests exercise the real stack but need Docker and take longer. Keep them focused on
  behavior that actually crosses the transport boundary.
- Compose is intentionally manual and persistent by default. It is useful for learning and samples,
  but it is not a replacement for isolated integration-test fixtures.
- The local 5.5.0 Proxy supports the gRPC paths documented by the samples, including LitePush with
  the required parent topic and group configuration. It does not turn every protobuf RPC declared
  upstream into an available server feature.
- Configuration changes to the Compose stack require validating the rendered configuration with
  `docker compose -f test-environments/rocketmq/compose.yaml config --quiet`.

## Related Reading

- [Local RocketMQ environment guide](../../../test-environments/rocketmq/README.md)
- [Runnable samples](../../../samples/README.md)
- [gRPC consumer model](../grpc/consumer-model.md)
- [Classic Remoting transport and client roles](../remoting/transport-and-client-roles.md)
