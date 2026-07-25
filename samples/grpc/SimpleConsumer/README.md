# gRPC SimpleConsumer sample

[English](README.md) | [简体中文](README.zh-CN.md)

This Generic Host sample registers `IGrpcSimpleConsumer` through `AddRocketMQGrpc` and
`AddGrpcSimpleConsumer`. The hosted worker explicitly calls `ReceiveAsync`, processes each message,
and then calls `AckAsync`; the application, rather than the client, controls those steps.

## Configuration

| Key | Default | Purpose |
| --- | --- | --- |
| `RocketMQ:Client:Endpoint` | `localhost:8081` | RocketMQ Proxy gRPC endpoint. |
| `RocketMQ:Client:RequestTimeout` | `00:00:03` | Deadline for ordinary client requests. |
| `RocketMQ:Client:UseTLS` | `false` | Enables TLS for the Proxy connection. |
| `RocketMQ:Consumer:GroupName` | `rocketmq-dotnet-grpc-simple-sample` | Consumer group that owns acknowledgements and retry state. |
| `RocketMQ:Consumer:AwaitDuration` | `00:00:05` | Server-side receive long-poll duration. |
| `RocketMQ:Consumer:InvisibleDuration` | `00:00:30` | Initial lease duration for a received message. |
| `Sample:Subscriptions:0:Topic` | `rocketmq-dotnet-manual` | Standard topic to receive. |
| `Sample:Subscriptions:0:Filter` | `sample` | Tag filter expression. |
| `Sample:BatchSize` | `16` | Maximum messages requested by each receive call. |
| `Sample:MaxDeliveryAttempts` | `16` | Threshold passed when forwarding a corrupted message to the dead-letter queue. |

The producer sample's default tag is `sample`, so its default messages match this subscription.

## Prerequisites

- A RocketMQ 5 Proxy must be reachable at `RocketMQ:Client:Endpoint`; the client does not connect
  directly to a NameServer.
- The standard topic and consumer group must exist, or the Broker must permit their automatic
  creation. Create them explicitly when that policy is disabled:

  ```shell
  docker compose -f test-environments/rocketmq/compose.yaml exec broker sh mqadmin updateTopic \
    -n nameserver:9876 -c DefaultCluster -t rocketmq-dotnet-manual

  docker compose -f test-environments/rocketmq/compose.yaml exec broker sh mqadmin updateSubGroup \
    -n nameserver:9876 -c DefaultCluster -g rocketmq-dotnet-grpc-simple-sample
  ```

- The supplied RocketMQ 5.5.0 Compose environment supports SimpleConsumer at `localhost:8081`.
  Its Broker enables automatic topic and subscription-group creation, but production deployments
  commonly require administrators to provision both resources first.

Start the supplied environment from the repository root:

```shell
docker compose -f test-environments/rocketmq/compose.yaml up -d --wait
```

## Run

From the repository root:

```shell
dotnet run --project samples/grpc/SimpleConsumer
```

The sample continues until Ctrl+C. Send a message with the Producer sample, or publish a standard
message tagged `sample` to `rocketmq-dotnet-manual` through another client.

## Semantics and common pitfalls

- `ReceiveAsync` leases messages but does not acknowledge them. This sample acknowledges only after
  successful logging; an unacknowledged message becomes visible again when its 30-second invisible
  duration expires.
- The worker explicitly forwards corrupted messages to the group's dead-letter queue using
  `MaxDeliveryAttempts`. It does not treat ordinary processing failures as successful; a failed
  acknowledgement leaves the message eligible for redelivery.
- The bundled RocketMQ 5.5.0 Proxy has a five-second minimum long-poll interval. Keep
  `AwaitDuration` at five seconds or longer when using that server.
- A consumer group is shared state. Running several instances with the same group distributes work;
  use a different group to independently replay or inspect the same topic.
- This is application-driven receive/acknowledgement, not Broker push. Use the PushConsumer sample
  when the client should manage long polling and dispatch automatically.

For runtime subscription changes and the complete API, see the
[gRPC guide](../../../src/EventHorizon.RocketMQ.Grpc/README.md).
