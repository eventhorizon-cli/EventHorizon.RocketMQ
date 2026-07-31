# gRPC LitePushConsumer without Generic Host

[English](README.md) | [简体中文](README.zh-CN.md)

This console sample runs `IGrpcLitePushConsumer` without constructing a Generic Host. It builds a plain
`ServiceProvider`, explicitly starts the role so its initial LiteTopic set is synchronized, keeps the handler running
until Ctrl+C, explicitly stops the role, and asynchronously disposes the service provider.

## When to use this shape

Use this form only when the process owns a non-Generic-Host lifetime. A normal .NET Generic Host starts and stops the
role through the `IHostedService` registered by `AddGrpcLitePushConsumer`; application code must not invoke
`IGrpcLitePushConsumer.StartAsync` or `StopAsync` again. See the
[Generic Host LitePushConsumer sample](../../GenericHost/LitePushConsumer/README.md).

`BuildServiceProvider` does not start registered hosted services. That is why `Program.cs` explicitly owns the lifecycle
in this non-Host process.

## Delivery and Lite subscriptions

`BindTopic` identifies the LITE parent topic. `LiteTopics` is the complete initial logical subscription set; it is not
a list of ordinary topics or filters. The sample's scoped `IGrpcPushMessageHandler` logs a message and returns
`ConsumeResult.Success`, which makes the SDK acknowledge it. `Retry` requests another attempt and `DeadLetter` moves
the message to the consumer group's dead-letter queue. Handler exceptions are retried, so processing must be idempotent.

Do not call ordinary `ConsumerOptions.Subscribe` for LitePush. Use `LiteTopics`, `SubscribeLiteAsync`, and
`UnsubscribeLiteAsync` with one bind topic instead. Lite delivery still uses client-initiated long polling through a
RocketMQ 5 Proxy.

## Run the sample

Use the dedicated [LitePush environment](../../../../test-environments/rocketmq-litepush/README.md). It provisions the
LITE parent topic, consumer group, Broker features, and a compatible cluster-mode Proxy:

```shell
docker compose -f test-environments/rocketmq-litepush/compose.yaml up -d --wait
dotnet run --project samples/grpc/NonHost/LitePushConsumer
```

Press Ctrl+C to begin graceful shutdown. The sample passes a live token to `StopAsync` because the Ctrl+C token is
already cancelled. Start the Generic Host [LiteProducer](../../GenericHost/LiteProducer/README.md) to publish a Lite
message under `eventhorizon-test-lite-parent-topic` for this handler to receive; the ordinary non-Host
[Producer](../Producer/README.md) deliberately sends a standard message instead.

LitePush requires `enableLmq`, `enableMultiDispatch`, a `message.type=LITE` parent topic, a group with matching
`lite.bind.topic`, and a Proxy implementing `SyncLiteSubscription`. See the
[gRPC protocol guide](../../../../src/EventHorizon.RocketMQ.Grpc/README.md) for the complete contract.
