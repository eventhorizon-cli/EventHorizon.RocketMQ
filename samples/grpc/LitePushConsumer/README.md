# gRPC LitePushConsumer sample

[English](README.md) | [简体中文](README.zh-CN.md)

This Generic Host sample registers `IGrpcLitePushConsumer` through `AddRocketMQGrpc` and
`AddGrpcLitePushConsumer<LitePushConsumerMessageHandler>`. It automatically dispatches LiteTopic
messages that belong to one LITE parent/bind topic and synchronizes its complete LiteTopic set with
the service through `SyncLiteSubscription`.

## Configuration

| Key | Default | Purpose |
| --- | --- | --- |
| `RocketMQ:Client:Endpoint` | `localhost:8081` | RocketMQ Proxy gRPC endpoint. |
| `RocketMQ:Client:RequestTimeout` | `00:00:03` | Deadline for ordinary client requests. |
| `RocketMQ:Client:UseTLS` | `false` | Enables TLS for the Proxy connection. |
| `RocketMQ:Consumer:GroupName` | `eventhorizon-test-lite-push-consumer` | Consumer group, which must be bound to the LITE parent topic. |
| `RocketMQ:Consumer:BatchSize` | `16` | Maximum messages requested by each receive operation. |
| `RocketMQ:Consumer:MaxConcurrency` | `4` | Maximum concurrent handler executions. |
| `RocketMQ:Consumer:MaxDeliveryAttempts` | `16` | Fallback delivery-attempt limit before dead-lettering. |
| `RocketMQ:Consumer:InvisibleDuration` | `00:00:30` | Initial lease duration for a received message. |
| `RocketMQ:Consumer:LongPollingTimeout` | `00:00:15` | Maximum server wait for a receive operation. |
| `RocketMQ:Consumer:SubscriptionSyncInterval` | `00:00:30` | Interval for reconciling the complete LiteTopic set with the service. |

The sample fixes its LITE parent topic to `eventhorizon-test-lite-parent-topic` and synchronizes
`eventhorizon-test-lite-topic` on startup. These names are deliberately set in code. The parent topic is not a
regular subscription; production applications can manage their LiteTopic set through `SubscribeLiteAsync` and
`UnsubscribeLiteAsync` instead.

## Prerequisites

- A cluster-mode RocketMQ 5 Proxy that implements `SyncLiteSubscription` must be reachable at
  `RocketMQ:Client:Endpoint`. A Broker-integrated Proxy started with `mqbroker --enable-proxy` in
  RocketMQ 5.5.0 does not implement this service.
- Every Broker that stores the parent topic must enable `enableLmq=true` and
  `enableMultiDispatch=true`.
- Use the dedicated LitePush environment. It enables the two Broker flags above, starts
  `mqproxy -pm cluster` at `localhost:8081`, and creates the fixed LITE parent Topic and Consumer Group during
  startup.
- Stop any environment that already publishes host port `8081` before starting this one.

Start the dedicated environment from the repository root:

```shell
docker compose -f test-environments/rocketmq-litepush/compose.yaml up -d --wait
```

The completed `resource-init` service creates `eventhorizon-test-lite-parent-topic` with `message.type=LITE` and binds
`eventhorizon-test-lite-push-consumer` to that parent Topic. `eventhorizon-test-lite-topic` is a logical LiteTopic,
which this Consumer synchronizes when it starts rather than an independent ordinary Topic to create.

See the [LitePush environment guide](../../../test-environments/rocketmq-litepush/README.md) for the topology,
persistence behavior, and local port limits.

## Run

After the environment is ready, run from the repository root:

```shell
dotnet run --project samples/grpc/LitePushConsumer
```

The host continues until Ctrl+C. It logs its fixed LiteTopic on startup. To observe messages,
publish Lite messages for `eventhorizon-test-lite-topic` under the
`eventhorizon-test-lite-parent-topic` parent topic with a compatible producer; the standard Producer sample
only sends ordinary messages and does not populate a LiteTopic.

## Semantics and common pitfalls

- LiteTopics are service-managed logical subscriptions under the LITE parent topic. They are not
  independent ordinary topics and cannot be replaced by a normal tag filter.
- The client sends its complete LiteTopic set on startup and every
  `SubscriptionSyncInterval`; the service can reject a subscription because of name or quota rules.
  A rejected change raises `GrpcServiceException` and does not update the local set.
- Like standard PushConsumer, LitePush is client-initiated long polling, not Broker network push.
  The handler returns `Success` to acknowledge, `Retry` to request redelivery, or `DeadLetter` to
  forward to the group dead-letter queue. This sample dead-letters corrupted messages.
- `BindTopic`, the Broker `message.type=LITE` attribute, and the group `lite.bind.topic` attribute
  must name the same parent topic. A spelling mismatch is a configuration error, not a way to
  subscribe to a second parent topic.
- The handler is registered as `Scoped`, so each handling attempt gets its own async DI scope.

For Lite message production and the complete API, see the
[gRPC guide](../../../src/EventHorizon.RocketMQ.Grpc/README.md).
