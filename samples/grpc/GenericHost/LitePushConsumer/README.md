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

`BindTopic` identifies the one LITE parent topic. `LiteTopics` can provide an initial logical subscription set, but this
sample deliberately starts with an empty set: a Web API represents application events such as a session connecting or
disconnecting.

```csharp
rocketMQ.AddGrpcLitePushConsumer<LitePushConsumerMessageHandler>(ServiceLifetime.Scoped, options =>
{
    options.GroupName = "eventhorizon-test-lite-push-consumer";
    options.BindTopic = "eventhorizon-test-lite-parent-topic";
    options.MaxConcurrency = 4;
});
```

The API calls the public runtime subscription methods after the SDK-hosted service has started the consumer:

```csharp
await consumer.SubscribeLiteAsync(liteTopic, cancellationToken: cancellationToken);
await consumer.UnsubscribeLiteAsync(liteTopic, cancellationToken);
```

`POST /subscriptions/{liteTopic}` adds a subscription, `DELETE /subscriptions/{liteTopic}` removes one, and
`GET /subscriptions` returns the current local snapshot. LiteTopic names use only letters, digits, hyphens, and
underscores, so `chat-session-123` is suitable for a session-oriented flow. Each successful mutation has already
synchronized with RocketMQ; an invalid name such as `chat.session-123` returns HTTP 400, while an SDK failure becomes
HTTP 503 and leaves the local set unchanged.

Do not call the inherited `ConsumerOptions.Subscribe` for this role. LitePush receives through `BindTopic` and manages
logical subscriptions with `LiteTopics`, `SubscribeLiteAsync`, and `UnsubscribeLiteAsync`; it does not accept ordinary
topic filters. The same `AddRocketMQGrpc` registration may add multiple independently configured LitePush consumers
while reusing its underlying gRPC channels. Consumers in the same group must use the same `BindTopic`; their LiteTopic
sets may differ. Different groups receive independently.

The Generic Host starts and stops the consumer through its SDK-hosted service. The API changes subscriptions but does
not manage that lifecycle. Runtime subscriptions are local application state, so establish them again after a Consumer
restart. `LiteTopics` returns a snapshot of the current local set.

For a newly added LiteTopic, production code can select an initial offset with `GrpcLiteOffsetOption`, for example
`GrpcLiteOffsetOption.Last`. The sample uses the service default to keep the session-subscription path focused. The
client reconciles the complete local set every `SubscriptionSyncInterval`; `SyncLiteSubscription` controls
subscriptions while delivery remains client-initiated long polling.

## Delivery and failure semantics

LitePush uses the same `IGrpcPushMessageHandler` contract and dependency-injection lifetime rules as standard Push.
`ConsumeResult.Success` acknowledges the message. Non-FIFO `Failure` and `Suspend` use service-owned retry/DLQ
progression, and Suspend ignores its requested duration. FIFO `Failure` retries the handler locally and forwards the
message to DLQ after the effective attempt limit; completion failures retry at a fixed one-second interval, and a
terminal failure leaves the message unsettled while releasing the FIFO successor. FIFO `ConsumeResult.Suspend(duration)` changes invisibility with `suspend=true`; the service
owns the delivery attempt reported by the next receive. The minimum duration is 50 milliseconds. It also covers every
unprocessed same-LiteTopic message from the current receive batch without invoking those sibling handlers. Handler
exceptions are treated as failures, and failed completion calls can produce duplicate delivery, so processing must be
idempotent.

Corrupted messages bypass the application handler. The client retries non-FIFO corrupted messages and forwards corrupted
FIFO messages to DLQ before releasing the next delivery for that LiteTopic. The successor waits while completion retry is
in progress; a terminal completion failure releases it, while the failed message remains unsettled and may be redelivered.

LitePush requests set `AutoRenew=true`, so a compatible Proxy renews the receipt while the handler runs; the .NET
client does not run a second renewal timer. For non-FIFO work, `ConsumeTimeout` cancels the handler token and requests
another delivery; it cannot forcibly terminate code that ignores cancellation. FIFO LiteTopics remain attached to
preserve order. The same local concurrency, bounded-cache, and server-policy fallback behavior as
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
| `LiteTopics` | Optional initial logical LiteTopic set synchronized at startup; this sample intentionally leaves it empty. |
| `SubscriptionSyncInterval` | Periodic reconciliation interval for the complete LiteTopic set. |
| `MaxConcurrency`, `BatchSize`, and cache limits | Local handler parallelism, receive batch size, and bounded buffering inherited from Push. |
| `InvisibleDuration` / `ConsumeTimeout` | Initial invisibility and the non-FIFO handler timeout behavior inherited from Push. |
| `MaxDeliveryAttempts` / `RetryDelay` | Fallback FIFO local-attempt limit and retry delay when the Proxy does not supply a policy. Non-FIFO dead-letter progression remains service-owned. |
| `LongPollingTimeout` | Maximum server wait for each receive long poll. |

## Run the sample

Use the dedicated [LitePush environment](../../../../test-environments/rocketmq-litepush/README.md). It enables the Broker
features, starts a cluster-mode Proxy at `localhost:8081`, and provisions the required parent topic and consumer group.

```shell
docker compose -f test-environments/rocketmq-litepush/compose.yaml up -d --wait
dotnet run --project samples/grpc/GenericHost/LitePushConsumer
```

The Consumer Web API runs at `http://localhost:5233`; Swagger is available at
`http://localhost:5233/swagger`. In another terminal, add a session subscription before sending a matching message:

```shell
curl http://localhost:5233/subscriptions
curl --request POST http://localhost:5233/subscriptions/chat-session-123
curl http://localhost:5233/subscriptions
```

Start LiteProducer in a third terminal, then post the matching message from a fourth terminal:

```shell
dotnet run --project samples/grpc/GenericHost/LiteProducer
```

```shell
curl --request POST http://localhost:5232/messages \
  --header 'Content-Type: application/json' \
  --data '{"liteTopic":"chat-session-123","message":"hello Lite"}'
```

To end the session, call `curl --request DELETE http://localhost:5233/subscriptions/chat-session-123`.
Only the Proxy connection settings are externalized in `appsettings.json`; the parent-topic binding remains visible
beside its registration in `Program.cs`. The handler logs the parent topic and LiteTopic, then returns `Success`, so
the SDK acknowledges the message after processing. The host continues running until Ctrl+C.

For the complete API and deployment constraints, see the
[gRPC guide](../../../../src/EventHorizon.RocketMQ.Grpc/README.md) and
[gRPC consumer model](../../../../docs/en-US/grpc/consumer-model.md).
