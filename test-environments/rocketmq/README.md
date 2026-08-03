# RocketMQ test environment

[All test environments](../README.md) | [简体中文](README.zh-CN.md)

This directory defines a local Apache RocketMQ 5.5.0 environment for manual client testing. It starts:

- a NameServer on `localhost:9876`;
- a Broker on `localhost:10911`;
- a cluster-mode Proxy on `localhost:8080` (remoting) and `localhost:8081` (gRPC).
- RocketMQ Dashboard on `http://localhost:8082`.

An additional one-shot `volume-init` service gives the image's non-root `rocketmq` user ownership
of the persistent log and store volumes. It exits before the NameServer starts.

The Broker and Proxy run as separate processes in the same container. The Broker starts first; once it has
registered with the NameServer, the container starts `mqproxy -pm cluster`. This preserves the host-reachable
Broker address advertised by the NameServer while providing the cluster Proxy services required by LitePush. It is
intentionally different from starting the Broker with `mqbroker --enable-proxy`.

Dashboard runs in a separate container but shares the Broker network namespace. It can therefore use the
`127.0.0.1:10911` Broker route returned by the NameServer. It listens on container port `8082` so it does not
conflict with the Proxy's remoting port.

For the component roles, protocol paths, and complete local deployment topology, see the
[RocketMQ architecture note](../../docs/en-US/architecture/rocketmq-architecture.md).

For the bundled LitePush sample, prefer the self-initializing
[`rocketmq-litepush`](../rocketmq-litepush) environment. Its Compose startup creates the sample's LITE parent topic
and consumer group automatically.

This environment also includes a one-shot `resource-init` service for the standard samples. After the Broker and
Proxy are healthy, it idempotently creates the ordinary shared `eventhorizon-test-topic` topic.

## Start and inspect

Run these commands from this directory, or add `-f test-environments/rocketmq/compose.yaml` when
running them from the repository root.

The first run also pulls `apacherocketmq/rocketmq-dashboard:2.1.0` when it is not already available locally.

```shell
docker compose config --quiet
docker compose up -d --wait
docker compose ps --all
```

`resource-init` should show `Exited (0)`. That completed state is expected: `docker compose up -d --wait` does not
return until the shared standard sample topic has been created.

Confirm that the Broker registered with the NameServer:

```shell
docker compose exec broker sh mqadmin clusterList -n nameserver:9876
```

Open [RocketMQ Dashboard](http://localhost:8082) to inspect the cluster, Topics, Consumer Groups, and messages.
Dashboard can also modify RocketMQ resources, so use it only as a local development tool.

## Optional host-directory persistence

By default, logs and Broker data use Docker named volumes. To keep the files in this directory for inspection or
backup, add the host-volume override when starting the environment:

```shell
docker compose -f compose.yaml -f compose.host-volumes.yaml up -d --wait
```

Use the same two `-f` arguments for later Compose commands. The override replaces the default named-volume mounts
at the same container paths and stores files under:

- `./data/nameserver-logs` for NameServer file logs;
- `./data/broker-logs` for Broker and Proxy file logs;
- `./data/broker-store` for Broker message data, including commit logs and consume queues.

`volume-init` creates the directories and assigns them to the image's non-root RocketMQ user. The `data/`
directory is ignored by Git. Unlike named volumes, `docker compose down -v` does not remove host-directory data;
remove `./data` explicitly when a full reset is needed.

## Prepare manual tests

All standard samples can use `eventhorizon-test-topic` immediately. Create a separate standard topic and consumer
groups before testing an independent scenario:

```shell
docker compose exec broker sh mqadmin updateTopic \
  -n nameserver:9876 -c DefaultCluster -t eventhorizon-test-topic

docker compose exec broker sh mqadmin updateSubGroup \
  -n nameserver:9876 -c DefaultCluster -g rocketmq-dotnet-lite-pull

docker compose exec broker sh mqadmin updateSubGroup \
  -n nameserver:9876 -c DefaultCluster -g rocketmq-dotnet-push

docker compose exec broker sh mqadmin topicRoute \
  -n nameserver:9876 -t eventhorizon-test-topic
```

The environment keeps the normal Broker request mode at PULL. The Remoting LitePull sample and Push with either the
default `QueueAssignmentMode` value `Client` or the sample's `Broker` value therefore run without POP configuration; Broker
assignment returns PULL by default. To exercise POP, an operator must configure POP request mode for the target
topic/group. The runtime Consumer never sends `SET_MESSAGE_REQUEST_MODE`. Use the isolated Remoting integration tests
for a reproducible local POP exercise rather than changing this shared sample environment's default.

For manual gRPC LitePush experiments in this general environment, create a dedicated LITE parent topic and configure
the consumer group's bind topic. This is not required for the bundled LitePush sample when it uses
[`rocketmq-litepush`](../rocketmq-litepush): that environment creates its default resources automatically.

```shell
docker compose exec broker sh mqadmin updateTopic \
  -n nameserver:9876 -c DefaultCluster -t eventhorizon-test-lite-parent-topic -a +message.type=LITE

docker compose exec broker sh mqadmin updateSubGroup \
  -n nameserver:9876 -c DefaultCluster -g eventhorizon-test-lite-push-consumer \
  --attributes +lite.bind.topic=eventhorizon-test-lite-parent-topic
```

The supplied Broker configuration enables `enableLmq=true` and `enableMultiDispatch=true`, which are required for
Lite messages to create and populate their LMQ queues. LitePush also requires the cluster-mode Proxy used by this
environment; RocketMQ 5.5.0's Broker-integrated Proxy does not implement `SyncLiteSubscription`.

Use these client endpoints:

| Client path | Endpoint |
| --- | --- |
| Classic Remoting producer, LitePull, or Push | NameServer `localhost:9876` |
| RocketMQ 5 gRPC producer/consumer | Proxy `localhost:8081` |
| RocketMQ Dashboard | Browser `http://localhost:8082` |

The RocketMQ 5.5.0 Proxy supports the standard gRPC send, simple-consumer, push-consumer, and LitePush paths when
the Lite parent topic and consumer group are prepared as above.

## Stop and reset

Stop the services while retaining Broker data:

```shell
docker compose down
```

Remove the services and all local RocketMQ data:

```shell
docker compose down -v --remove-orphans
```

The published ports are bound to `127.0.0.1` and are intended only for local development. The
fixed Broker port is required because the NameServer advertises `127.0.0.1:10911` to host clients.
