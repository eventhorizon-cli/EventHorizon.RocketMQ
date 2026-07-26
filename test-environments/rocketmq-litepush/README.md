# RocketMQ LitePush test environment

[All test environments](../README.md) | [Simplified Chinese](README.zh-CN.md)

This environment is a self-contained Apache RocketMQ 5.5.0 stack for the repository's gRPC
[`LitePushConsumer` sample](../../samples/grpc/LitePushConsumer/README.md).

Run `docker compose up -d --wait` once. The stack starts a cluster-mode Proxy and automatically prepares the
resources required by the sample; no `mqadmin` command is needed from the developer.

It deliberately exposes the sample's default gRPC endpoint, `localhost:8081`. The NameServer diagnostic port is
`localhost:19876`, so it does not conflict with the default single- or multi-Broker NameServer port.

RocketMQ Dashboard is available at `http://localhost:8082`. Its browser endpoint is separate from the gRPC Proxy
endpoint.

## Automatically prepared resources

The one-shot `resource-init` service waits for the Broker and cluster-mode Proxy health checks to pass. It then
creates or updates the following idempotently:

| Resource | Name | Required setting |
| --- | --- | --- |
| LITE parent Topic | `eventhorizon-test-lite-parent-topic` | `message.type=LITE` |
| Consumer Group | `eventhorizon-test-lite-push-consumer` | `lite.bind.topic=eventhorizon-test-lite-parent-topic` |
| LiteTopic used by the sample | `eventhorizon-test-lite-topic` | Synchronized by `IGrpcLitePushConsumer` through `SyncLiteSubscription` when the sample starts. |

`eventhorizon-test-lite-topic` is a logical LiteTopic below the LITE parent Topic. It is not an independent ordinary
Topic that `resource-init` needs to create.

The Broker enables `enableLmq=true` and `enableMultiDispatch=true`. The Proxy starts separately with
`mqproxy -pm cluster`; RocketMQ 5.5.0's Broker-integrated Proxy does not provide `SyncLiteSubscription`.

## Topology

```mermaid
flowchart TB
    Host[Developer host]
    Sample[LitePushConsumer sample\nEndpoint: localhost:8081]
    DashboardUi[Browser\nhttp://localhost:8082]

    subgraph Environment[test-environments/rocketmq-litepush]
        VolumeInit[volume-init\none-shot volume ownership setup]
        NameServer[nameserver\ncontainer port 9876]
        Broker[broker: mqbroker\ninternal route broker:10911]
        Proxy[proxy: mqproxy -pm cluster\ngRPC port 8081]
        Dashboard[dashboard\nweb port 8082]
        ResourceInit[resource-init\ncreates LITE resources once]
        Volumes[(Docker named volumes\nlogs and Broker store)]
    end

    Host --> Sample
    Host --> DashboardUi
    Sample -->|gRPC :8081| Proxy
    DashboardUi -->|Dashboard :8082| Dashboard
    Host -->|diagnostics :19876| NameServer
    VolumeInit --> Volumes
    VolumeInit -->|finishes before startup| NameServer
    NameServer -->|healthy| Broker
    Broker -->|registers its route| NameServer
    Proxy -->|waits for Broker route| NameServer
    Proxy -->|Broker operations| Broker
    Dashboard -->|admin route lookup| NameServer
    Dashboard -->|Broker administration| Broker
    Dashboard -->|RocketMQ 5 administration| Proxy
    Proxy -->|healthy| ResourceInit
    ResourceInit -->|creates Topic and Consumer Group| Broker
    Broker --> Volumes
    Proxy --> Volumes
```

The Broker advertises the Compose-network address `broker:10911`. It is intentionally not a host-reachable Remoting
endpoint; this environment is for gRPC LitePush.

## Start

Run the following from this directory:

The first run also pulls `apacherocketmq/rocketmq-dashboard:2.1.0` when it is not already available locally.

```shell
docker compose up -d --wait
docker compose ps --all
```

`resource-init` should show `Exited (0)`. That completed state is expected, and `--wait` does not return until its
resource setup has finished successfully.

Open [RocketMQ Dashboard](http://localhost:8082) to inspect the cluster and automatically prepared resources.
Dashboard can change Topics and Consumer Groups, so use it only as a local development tool.

To inspect the automatic setup without issuing any administration commands, read its log:

```shell
docker compose logs --no-color resource-init
```

Run the sample from the repository root without changing its default `appsettings.json`:

```shell
dotnet run --project samples/grpc/LitePushConsumer
```

The Consumer synchronizes `eventhorizon-test-lite-topic` at startup. To receive a message, a compatible gRPC
Producer must send a Lite message under `eventhorizon-test-lite-parent-topic` with that LiteTopic; the standard Producer
sample sends ordinary messages and therefore does not populate the LiteTopic.

## Endpoints and limits

| Purpose | Host endpoint | Notes |
| --- | --- | --- |
| gRPC LitePush sample | `localhost:8081` | The default `RocketMQ:Client:Endpoint`. |
| NameServer diagnostics | `localhost:19876` | Optional inspection endpoint, not the gRPC client endpoint. |
| RocketMQ Dashboard | `http://localhost:8082` | Local management interface for the cluster. |
| Broker | Not published | Reachable only as `broker:10911` inside the Compose network. |

- Docker Compose must be available. The first run pulls `apache/rocketmq:5.5.0` and
  `apacherocketmq/rocketmq-dashboard:2.1.0` when necessary.
- This environment cannot run at the same time as an environment that publishes host port `8081`, including
  `rocketmq` and `rocketmq-multi-broker`.
- All published ports bind to `127.0.0.1`; the stack is intended only for local development.
- Dashboard can modify RocketMQ resources and has no production authentication or exposure configuration in this stack.
- Do not configure a classic Remoting client with this environment's NameServer. The Broker route is deliberately
  internal to Docker and is not reachable by a host Remoting client.

## Persistence and reset

By default, NameServer logs, Broker logs and store files, and Proxy logs use Docker named volumes. `docker compose
down` stops the stack without removing those data.

Use the following command for a clean state. The next `up -d --wait` run recreates the LITE parent Topic and Consumer
Group automatically.

```shell
docker compose down -v --remove-orphans
```

To store data below this directory instead, use the host-volume override:

```shell
docker compose -f compose.yaml -f compose.host-volumes.yaml up -d --wait
```

It writes data under `./data/`. Keep using both Compose files for later commands. `docker compose down -v` does not
remove bind-mounted host data; remove `./data` explicitly when a full host-directory reset is required.

## Stop

Stop services while retaining the default named volumes:

```shell
docker compose down
```

For the protocol constraints behind this environment, see the
[gRPC guide](../../src/EventHorizon.RocketMQ.Grpc/README.md) and the
[RocketMQ architecture note](../../docs/en-US/architecture/rocketmq-architecture.md).
