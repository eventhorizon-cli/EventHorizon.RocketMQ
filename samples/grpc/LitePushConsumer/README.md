# gRPC LitePushConsumer

[English](README.md) | [简体中文](README.zh-CN.md)

`IGrpcLitePushConsumer` automatically dispatches messages from service-managed LiteTopics beneath one configured LITE
parent, or bind, topic. It reuses the gRPC Push handler and long-poll dispatcher, but adds a Lite subscription control
plane through `SyncLiteSubscription`. LiteTopics are not ordinary RocketMQ topics and are not tag filters.

## When to use it

Use LitePushConsumer only when the RocketMQ deployment and resource model are intentionally configured for LITE
messages, and the application wants automatic handler dispatch from one or more logical LiteTopics under the same
parent topic.

Use `IGrpcPushConsumer` for ordinary topics with tag or SQL filters. Use `IGrpcSimpleConsumer` when the application
must drive receive and settlement itself. A Proxy that does not implement `SyncLiteSubscription` cannot support this
role through a client-side setting.

## SDK workflow

Register the gRPC transport and a LitePush role. `BindTopic` identifies the LITE parent topic; `LiteTopics` is the
initial complete logical subscription set:

```csharp
rocketMQ.AddGrpcLitePushConsumer<OrderMessageHandler>(ServiceLifetime.Scoped, options =>
{
    options.GroupName = "orders-lite-push-consumer";
    options.BindTopic = "orders-lite";
    options.LiteTopics.Add("orders-us");
    options.LiteTopics.Add("orders-eu");
    options.MaxConcurrency = 8;
});
```

Do not call the inherited `ConsumerOptions.Subscribe` for this role. LitePush receives through `BindTopic` and manages
logical subscriptions with `LiteTopics`, `SubscribeLiteAsync`, and `UnsubscribeLiteAsync`; it does not accept ordinary
topic filters.

The Generic Host integration starts the role, synchronizes the configured set, and stops it with the host. Outside a
host, manage `StartAsync` and `StopAsync` through `IGrpcLitePushConsumer`. `LiteTopics` returns a snapshot of the current
local set.

At runtime, a new LiteTopic can include an initial offset selection:

```csharp
await consumer.SubscribeLiteAsync(
    "orders-apac",
    GrpcLiteOffsetOption.Last,
    cancellationToken);
```

`GrpcLiteOffsetOption` supports the service-defined recent position (`Last`), earliest or latest available position
(`Min` / `Max`), an exact non-negative queue offset (`AtOffset`), a recent message count (`Tail`), or the first stored
message at or after a timestamp (`AtTimestamp`). The option applies when that LiteTopic subscription is created.

The client sends the complete configured LiteTopic set at startup and reconciles it every
`SubscriptionSyncInterval`. The service remains authoritative for name and quota rules. A rejected runtime change
throws `GrpcServiceException` and does not update the local set. `SyncLiteSubscription` controls subscriptions; message
delivery still uses client-initiated long polling.

## Delivery and failure semantics

LitePush uses the same `IGrpcPushMessageHandler` contract and dependency-injection lifetime rules as standard Push.
`ConsumeResult.Success` acknowledges the message, `Retry` requests another attempt, and `DeadLetter` forwards it to the
consumer group's dead-letter queue. Handler exceptions are treated as retry requests, and failed completion calls can
produce duplicate delivery, so processing must be idempotent.

Corrupted messages bypass the application handler. The client retries non-FIFO corrupted messages and dead-letters
corrupted FIFO messages before releasing the next member of that message group.

The client renews invisibility while handling a message. For non-FIFO work, `ConsumeTimeout` cancels the handler token,
stops renewal, and requests retry; it cannot forcibly terminate code that ignores cancellation. FIFO message groups
remain attached to preserve order. The same local concurrency, bounded-cache, and server-policy fallback behavior as
`IGrpcPushConsumer` applies.

## Required RocketMQ capabilities

LitePush requires matching client, Proxy, Broker, Topic, and consumer-group configuration:

- Every Broker storing the parent topic must enable `enableLmq=true` and `enableMultiDispatch=true`.
- The parent topic must be created with `message.type=LITE`.
- The consumer group must set `lite.bind.topic=<parent-topic>`, matching `GrpcLitePushConsumerOptions.BindTopic`.
- The client must connect to a cluster-mode Proxy that implements `SyncLiteSubscription`.

In RocketMQ 5.5.0, the Broker-integrated Proxy started by `mqbroker --enable-proxy` does not implement this RPC; use
`mqproxy -pm cluster` or a compatible newer Proxy. A separate cluster-mode Proxy may share a container or Compose
service with a Broker.

## Key SDK options

| Option | What it controls |
| --- | --- |
| `GrpcClientOptions.Endpoint` | The RocketMQ Proxy endpoint; capability must include `SyncLiteSubscription`. |
| `GrpcLitePushConsumerOptions.GroupName` | The consumer group bound to the LITE parent topic. |
| `BindTopic` | The single parent topic used for assignment and message reception. |
| `LiteTopics` | The complete initial logical LiteTopic set synchronized at startup. |
| `SubscriptionSyncInterval` | Periodic reconciliation interval for the complete LiteTopic set. |
| `MaxConcurrency`, `BatchSize`, and cache limits | Local handler parallelism, receive batch size, and bounded buffering inherited from Push. |
| `InvisibleDuration` / `ConsumeTimeout` | Initial invisibility and the non-FIFO handler timeout behavior inherited from Push. |
| `MaxDeliveryAttempts` / `RetryDelay` | Fallback delivery policy when the Proxy does not supply one. |
| `LongPollingTimeout` | Maximum server wait for each receive long poll. |

## Run the sample

Use the dedicated [LitePush environment](../../../test-environments/rocketmq-litepush/README.md). It enables the Broker
features, starts a cluster-mode Proxy at `localhost:8081`, and provisions the required parent topic and consumer group.

```shell
docker compose -f test-environments/rocketmq-litepush/compose.yaml up -d --wait
dotnet run --project samples/grpc/LitePushConsumer
```

The runnable code binds to `eventhorizon-test-lite-parent-topic` and subscribes to
`eventhorizon-test-lite-topic`. Observing delivery requires a producer that can publish a Lite message under that
parent; the ordinary Producer sample does not populate LiteTopics.

For the complete API and deployment constraints, see the
[gRPC guide](../../../src/EventHorizon.RocketMQ.Grpc/README.md) and
[gRPC consumer model](../../../docs/en-US/grpc/consumer-model.md).
