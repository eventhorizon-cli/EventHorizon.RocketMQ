# RocketMQ test environment

[All test environments](../README.md)

This directory defines a local Apache RocketMQ 5.5.0 environment for manual client testing. It starts:

- a NameServer on `localhost:9876`;
- a Broker on `localhost:10911`;
- a cluster-mode Proxy on `localhost:8080` (remoting) and `localhost:8081` (gRPC).

An additional one-shot `volume-init` service gives the image's non-root `rocketmq` user ownership
of the persistent log and store volumes. It exits before the NameServer starts.

The Broker and Proxy run as separate processes in the same container. The Broker starts first; once it has
registered with the NameServer, the container starts `mqproxy -pm cluster`. This preserves the host-reachable
Broker address advertised by the NameServer while providing the cluster Proxy services required by LitePush. It is
intentionally different from starting the Broker with `mqbroker --enable-proxy`.

For the component roles, protocol paths, and complete local deployment topology, see the
[RocketMQ architecture note](../../docs/en-US/architecture/rocketmq-architecture.md).

## Start and inspect

Run these commands from this directory, or add `-f test-environments/rocketmq/compose.yaml` when
running them from the repository root.

```shell
docker compose config --quiet
docker compose up -d --wait
docker compose ps
```

Confirm that the Broker registered with the NameServer:

```shell
docker compose exec broker sh mqadmin clusterList -n nameserver:9876
```

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

Create a standard topic and consumer groups before testing standard pull and push consumers:

```shell
docker compose exec broker sh mqadmin updateTopic \
  -n nameserver:9876 -c DefaultCluster -t rocketmq-dotnet-manual

docker compose exec broker sh mqadmin updateSubGroup \
  -n nameserver:9876 -c DefaultCluster -g rocketmq-dotnet-pull

docker compose exec broker sh mqadmin updateSubGroup \
  -n nameserver:9876 -c DefaultCluster -g rocketmq-dotnet-push

docker compose exec broker sh mqadmin topicRoute \
  -n nameserver:9876 -t rocketmq-dotnet-manual
```

For gRPC LitePush, create a dedicated LITE parent topic and configure the consumer group's bind topic:

```shell
docker compose exec broker sh mqadmin updateTopic \
  -n nameserver:9876 -c DefaultCluster -t rocketmq-dotnet-lite-manual -a +message.type=LITE

docker compose exec broker sh mqadmin updateSubGroup \
  -n nameserver:9876 -c DefaultCluster -g rocketmq-dotnet-grpc-lite-push-sample \
  --attributes +lite.bind.topic=rocketmq-dotnet-lite-manual
```

The supplied Broker configuration enables `enableLmq=true` and `enableMultiDispatch=true`, which are required for
Lite messages to create and populate their LMQ queues. LitePush also requires the cluster-mode Proxy used by this
environment; RocketMQ 5.5.0's Broker-integrated Proxy does not implement `SyncLiteSubscription`.

Use these client endpoints:

| Client path | Endpoint |
| --- | --- |
| Legacy remoting producer/consumer | NameServer `localhost:9876` |
| RocketMQ 5 gRPC producer/consumer | Proxy `localhost:8081` |

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
