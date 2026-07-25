# gRPC PushConsumer sample

[English](README.md) | [简体中文](README.zh-CN.md)

This Generic Host sample registers `IGrpcPushConsumer` through `AddRocketMQGrpc` and
`AddGrpcPushConsumer<PushConsumerMessageHandler>`. The client polls assignments and dispatches
messages to a DI-managed scoped handler; it is not a Broker-initiated network push subscription.

## Configuration

| Key | Default | Purpose |
| --- | --- | --- |
| `RocketMQ:Client:Endpoint` | `localhost:8081` | RocketMQ Proxy gRPC endpoint. |
| `RocketMQ:Client:RequestTimeout` | `00:00:03` | Deadline for ordinary client requests. |
| `RocketMQ:Client:UseTLS` | `false` | Enables TLS for the Proxy connection. |
| `RocketMQ:Consumer:GroupName` | `rocketmq-dotnet-grpc-push-sample` | Consumer group used for assignment, acknowledgement, retry, and dead-letter state. |
| `RocketMQ:Consumer:BatchSize` | `16` | Maximum messages requested by each receive operation. |
| `RocketMQ:Consumer:MaxConcurrency` | `4` | Maximum concurrent handler executions. |
| `RocketMQ:Consumer:MaxDeliveryAttempts` | `16` | Fallback delivery-attempt limit before dead-lettering. |
| `RocketMQ:Consumer:InvisibleDuration` | `00:00:30` | Initial lease duration; the client renews it while handling a message. |
| `RocketMQ:Consumer:LongPollingTimeout` | `00:00:15` | Maximum server wait for a receive operation. |
| `Sample:Subscriptions:0:Topic` | `rocketmq-dotnet-manual` | Standard topic to receive. |
| `Sample:Subscriptions:0:Filter` | `sample` | Tag filter expression. |

The Producer sample's default tag is `sample`, so its default messages match this subscription.

## Prerequisites

- A RocketMQ 5 Proxy must be reachable at `RocketMQ:Client:Endpoint`; gRPC consumers connect to a
  Proxy, not directly to a NameServer.
- The standard topic and consumer group must exist, or the Broker must permit automatic creation.
  Provision them explicitly when that policy is disabled:

  ```shell
  docker compose -f test-environments/rocketmq/compose.yaml exec broker sh mqadmin updateTopic \
    -n nameserver:9876 -c DefaultCluster -t rocketmq-dotnet-manual

  docker compose -f test-environments/rocketmq/compose.yaml exec broker sh mqadmin updateSubGroup \
    -n nameserver:9876 -c DefaultCluster -g rocketmq-dotnet-grpc-push-sample
  ```

- The supplied RocketMQ 5.5.0 Compose environment supports this standard PushConsumer path at
  `localhost:8081`. Its Broker enables automatic topic and subscription-group creation, but
  production deployments should normally provision those resources explicitly.

Start the supplied environment from the repository root:

```shell
docker compose -f test-environments/rocketmq/compose.yaml up -d --wait
```

## Run

From the repository root:

```shell
dotnet run --project samples/grpc/PushConsumer
```

The host continues until Ctrl+C. Send a standard message tagged `sample` to
`rocketmq-dotnet-manual`, for example through the Producer sample.

## Semantics and common pitfalls

- Despite its name, this is client-initiated long polling: the client queries assignments and
  repeatedly calls `ReceiveMessage`, then dispatches locally. The Broker does not open a push
  connection to the application.
- `PushConsumerMessageHandler` is registered as `Scoped`; each handling attempt receives a fresh
  async DI scope. Do not inject a scoped service into a singleton handler, and make a singleton
  handler thread-safe if changing its lifetime.
- Returning `ConsumeResult.Success` acknowledges the message. Returning `Retry` makes it eligible
  for redelivery, and `DeadLetter` forwards it to the consumer group's dead-letter queue. This
  sample returns `DeadLetter` for corrupted messages and `Success` after logging normal messages.
- `MaxConcurrency` controls local handler parallelism, not Broker queue count. Messages in a FIFO
  message group retain their ordering constraints even when other messages run concurrently.
- Running multiple instances with the same group distributes messages and shares retry state. Use a
  new group when independently observing the topic.

For handler lifetimes, retry behavior, and runtime subscriptions, see the
[gRPC guide](../../../src/EventHorizon.RocketMQ.Grpc/README.md).
