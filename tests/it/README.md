# Integration Tests

[English](README.md) | [Simplified Chinese](README.zh-CN.md)

`tests/it` contains the `net10.0` Docker-backed integration-test projects. They validate protocol-specific behavior
against real RocketMQ NameServer, Broker, and Proxy processes while keeping container orchestration separate from
production client code.

## Projects

| Project | Responsibility |
| --- | --- |
| `EventHorizon.RocketMQ.Grpc.IntegrationTests` | Exercises the gRPC client through a real cluster-mode Proxy. |
| `EventHorizon.RocketMQ.Remoting.IntegrationTests` | Exercises classic NameServer and Broker Remoting behavior. |
| `EventHorizon.RocketMQ.IntegrationTestInfrastructure` | Owns reusable Testcontainers topologies, host-port allocation, and test resource names. |
| `EventHorizon.RocketMQ.Remoting.CrossProcessTestHost` | Runs test-only Remoting consumers in separate operating-system processes for cross-process Rebalance coverage. |

The infrastructure project is a non-packable support library, not a test assembly. It deliberately has no reference
to either production client project and contains no protocol client assertions. Each protocol test project references
the infrastructure project and its own production client; the Remoting tests also own the cross-process test host:

```text
Grpc.IntegrationTests ------> EventHorizon.RocketMQ.Grpc
    `-----------------------> IntegrationTestInfrastructure

Remoting.IntegrationTests --> EventHorizon.RocketMQ.Remoting
    |-----------------------> IntegrationTestInfrastructure
    `-----------------------> Remoting.CrossProcessTestHost
                                  `--> EventHorizon.RocketMQ.Remoting

IntegrationTestInfrastructure --> Testcontainers
                              --> xUnit lifecycle abstractions
```

See [Local and Integration Testing](../../docs/en-US/testing/local-and-integration-testing.md) for the repository-wide
test topology and CI policy.

## Infrastructure structure

`EventHorizon.RocketMQ.IntegrationTestInfrastructure` has six public top-level types and one internal helper.

| Type | Responsibility |
| --- | --- |
| `RocketMQSingleBrokerContainerFixtureRegistry` | Lazily owns the shared single-Broker fixture for one test assembly. |
| `RocketMQSingleBrokerContainerFixture` | Runs the ordinary NameServer, single Broker, and cluster-mode Proxy topology. |
| `RocketMQTestScope` | Produces test-specific topic and producer or consumer group names. |
| `RocketMQTestTopicType` | Selects normal, transaction, FIFO, delay, or Lite topic provisioning. |
| `RocketMQHostPortReservation` | Coordinates dynamic host-port choices within the current test process. |
| `RocketMQMultiBrokerGrpcContainerFixture` | Runs three Docker-network Brokers behind a cluster-mode Proxy. |
| `RocketMQMultiBrokerRemotingContainerFixture` | Runs three host-reachable classic Remoting Brokers. |

The internal type dependencies are:

```text
RocketMQSingleBrokerContainerFixtureRegistry
    `-- lazily owns --> RocketMQSingleBrokerContainerFixture
                           |-- uses --> RocketMQHostPortReservation
                           |-- consumes --> RocketMQTestTopicType
                           `-- creates --> RocketMQTestScope
                                              `-- calls back --> RocketMQSingleBrokerContainerFixture

RocketMQMultiBrokerGrpcContainerFixture
    `-- uses --> RocketMQHostPortReservation

RocketMQMultiBrokerRemotingContainerFixture
    `-- uses --> RocketMQHostPortReservation
```

`RocketMQTestScope` calls back into `RocketMQSingleBrokerContainerFixture` only when it must create a Lite consumer group with a
Broker-side parent-topic binding. It does not own or dispose Broker resources. Topics and groups live until the owning
container fixture is disposed.

## Fixture ownership and lifecycle

### Single-Broker baseline

Both protocol integration-test assemblies register `RocketMQSingleBrokerContainerFixtureRegistry` as an xUnit assembly fixture.
xUnit therefore creates one registry per test assembly. The two assemblies do not share a registry instance or a live
Docker topology.

The registry is intentionally a lazy holder rather than a general-purpose fixture catalog:

```text
Test assembly starts
    -> xUnit creates RocketMQSingleBrokerContainerFixtureRegistry
    -> registry InitializeAsync completes without starting Docker
    -> first GetFixtureAsync call wins the initialization gate
    -> registry creates and initializes RocketMQSingleBrokerContainerFixture
         -> reserve three host ports
         -> create Docker network
         -> start NameServer
         -> start the Broker and Proxy container
         -> provision predefined topics and legacy Remoting groups
    -> independent test classes share the initialized fixture
Test assembly finishes
    -> registry disposes the fixture, initialization gate, and owned resources
```

`GetFixtureAsync` uses a semaphore and double-checked access so parallel test classes initialize exactly one fixture.
If initialization fails, the registry disposes the partially initialized fixture before propagating the failure. A
multi-Broker-only filtered run still creates the inexpensive assembly registry, but never starts the single-Broker
topology because it does not call `GetFixtureAsync`.

`RocketMQSingleBrokerContainerFixture` provisions a unique normal topic for each scope. Transaction, FIFO, delay, and Lite topics
are predefined because their Broker semantics require configuration, but every scope still receives a unique suffix
for group and message identity isolation. Broker administration is limited to four concurrent operations to match the
parallel integration-test workload without serializing the entire assembly.

### Multi-Broker collections

The multi-Broker fixtures are registered directly as xUnit collection fixtures:

```text
gRPC multi-Broker collection
    `-- RocketMQMultiBrokerGrpcContainerFixture

Remoting multi-Broker collection
    `-- RocketMQMultiBrokerRemotingContainerFixture
```

xUnit creates each fixture only when its collection has selected tests, calls `InitializeAsync`, injects the same
instance into that collection's tests, and calls `DisposeAsync` after the collection finishes. A registry wrapper would
not add laziness or sharing because the collection fixture already provides both.

The gRPC multi-Broker initialization order is:

```text
reserve one Proxy host port
    -> network -> NameServer -> Broker A/B/C in parallel
    -> wait for all Broker registrations
    -> create the topic on every Broker and wait for the complete route
    -> start Proxy
```

Disposal runs in the reverse ownership direction: Proxy, Brokers, NameServer, network, then the port reservation.

The Remoting multi-Broker initialization order is:

```text
reserve one NameServer and three Broker host ports
    -> network -> NameServer -> Broker A/B/C in parallel
    -> wait for all Broker registrations
    -> create a three-read-queue and three-write-queue topic on every Broker
    -> wait for the complete route
```

Disposal releases the Brokers, NameServer, network, and port reservation.

## Why the single-Broker fixture is shared

The single-Broker fixture can provide one valid superset topology for both protocols because the Broker and Proxy run
inside the same container. The Broker advertises `127.0.0.1` and uses the same numeric port inside the container and on
the host:

```text
host Remoting client -> mapped Broker port -> Broker

host gRPC client -> mapped Proxy port -> Proxy
                                      -> loopback Broker port in the same container
```

All exposed endpoints are valid for the fixture's entire lifetime. The protocol test assemblies reuse the fixture type
and provisioning implementation, while their production clients and assertions remain separate. Each assembly still
starts its own containers at runtime. The Remoting assembly consequently starts an otherwise unused Proxy, which is an
accepted cost of the simple shared baseline topology.

## Why the multi-Broker fixtures remain separate

The multi-Broker topologies have incompatible Broker address-advertisement requirements:

| Concern | gRPC multi-Broker | Remoting multi-Broker |
| --- | --- | --- |
| Client entry point | Proxy | NameServer and Brokers |
| Advertised Broker host | Docker alias such as `broker-a` | `127.0.0.1` |
| Broker ports | Fixed `10911` in separate containers | Distinct dynamically allocated host ports |
| Host mappings | Proxy only | NameServer and every Broker |
| Additional behavior | Proxy startup and per-Broker offset inspection | Explicit queue counts and host-reachable routes |

A separate Proxy container can resolve `broker-a`, `broker-b`, and `broker-c`, while a host Remoting client cannot. If
the Brokers advertise `127.0.0.1`, the host can reach them through distinct mapped ports, but `127.0.0.1` inside the
Proxy container refers to the Proxy container itself and cannot identify three other Broker containers.

Combining the public fixtures would therefore produce a mode-switched type with conditional construction and members
that are invalid in one mode, such as a gRPC endpoint in Remoting mode. It would reduce type count without unifying the
runtime topology. Keep the two protocol-specific public fixtures. Shared registration polling, route polling, or
container orchestration may be extracted into a focused internal collaborator if repeated maintenance demonstrates a
real benefit; do not replace the explicit fixtures with a generic mode selector merely to remove similar code.

## Running integration tests

Docker must be available. Run the protocol-specific integration-test project from the repository root:

```shell
dotnet test tests/it/EventHorizon.RocketMQ.Grpc.IntegrationTests/EventHorizon.RocketMQ.Grpc.IntegrationTests.csproj --no-restore
dotnet test tests/it/EventHorizon.RocketMQ.Remoting.IntegrationTests/EventHorizon.RocketMQ.Remoting.IntegrationTests.csproj --no-restore
```
