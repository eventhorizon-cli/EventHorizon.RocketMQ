# gRPC Samples without Generic Host

[English](README.md) | [简体中文](README.zh-CN.md)

These runnable console projects demonstrate the four gRPC lifecycle-managed roles in a process that deliberately does
not construct a Generic Host. Each project uses `ServiceCollection` and `BuildServiceProvider`, resolves one
protocol-specific public role, explicitly calls `StartAsync` and `StopAsync` in a `try`/`finally`, and asynchronously
disposes the provider.

`AddRocketMQGrpc` and each role registration still add an SDK `IHostedService`. A plain `ServiceProvider` only creates
that registration; it does not start or stop it. The explicit calls shown here are therefore required for a non-Host
process. Do not copy them into a normal Generic Host application: after `RunAsync` or `StartAsync`, the Host owns the
same role lifecycle.

| Sample | Public role | Demonstrated workflow |
| --- | --- | --- |
| [Producer](Producer/README.md) | `IGrpcProducer` | Start, send one tagged message, await the receipt, stop. |
| [SimpleConsumer](SimpleConsumer/README.md) | `IGrpcSimpleConsumer` | Start, long-poll, acknowledge valid messages, leave failed messages unsettled, stop. |
| [PushConsumer](PushConsumer/README.md) | `IGrpcPushConsumer` | Start, dispatch messages to a scoped handler until Ctrl+C, then stop. |
| [LitePushConsumer](LitePushConsumer/README.md) | `IGrpcLitePushConsumer` | Start, synchronize LiteTopics, dispatch to a scoped handler until Ctrl+C, then stop. |

The first three samples use the standard [local RocketMQ environment](../../../test-environments/rocketmq/README.md).
LitePush requires the dedicated [LitePush environment](../../../test-environments/rocketmq-litepush/README.md), because
it needs LITE resources and a Proxy implementing `SyncLiteSubscription`.

Use the sibling gRPC samples when the application is a normal Generic Host. The protocol guide explains the complete
role APIs, delivery behavior, retries, and server constraints:
[EventHorizon.RocketMQ.Grpc](../../../src/EventHorizon.RocketMQ.Grpc/README.md).
