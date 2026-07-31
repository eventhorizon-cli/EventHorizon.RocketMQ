# 不使用 Generic Host 的 gRPC 示例

[English](README.md) | [简体中文](README.zh-CN.md)

这些可运行的控制台项目演示了进程刻意不构造 Generic Host 时，四种受生命周期管理的 gRPC 角色。每个项目都使用
`ServiceCollection` 和 `BuildServiceProvider`，解析一个协议专属公开角色，在 `try`/`finally` 中显式调用
`StartAsync` 和 `StopAsync`，并异步释放 provider。

`AddRocketMQGrpc` 和每种角色注册仍会添加 SDK `IHostedService`。普通 `ServiceProvider` 只会创建该注册，不会启动或停止它。
因此，这里展示的显式调用对非 Host 进程是必需的。不要把它们复制到常规 Generic Host 应用：在 `RunAsync` 或 `StartAsync` 之后，
Host 会拥有同一角色的生命周期。

| 示例 | 公开角色 | 演示的工作流 |
| --- | --- | --- |
| [Producer](Producer/README.zh-CN.md) | `IGrpcProducer` | 启动，发送一条带 tag 的消息，等待回执，停止。 |
| [SimpleConsumer](SimpleConsumer/README.zh-CN.md) | `IGrpcSimpleConsumer` | 启动，长轮询，确认正常消息，将损坏消息转入死信队列，停止。 |
| [PushConsumer](PushConsumer/README.zh-CN.md) | `IGrpcPushConsumer` | 启动，持续向 scoped handler 分发消息直到 Ctrl+C，再停止。 |
| [LitePushConsumer](LitePushConsumer/README.zh-CN.md) | `IGrpcLitePushConsumer` | 启动，同步 LiteTopic，持续向 scoped handler 分发消息直到 Ctrl+C，再停止。 |

前三个示例使用标准的[本地 RocketMQ 环境](../../../test-environments/rocketmq/README.zh-CN.md)。LitePush 需要专用的
[LitePush 环境](../../../test-environments/rocketmq-litepush/README.zh-CN.md)，因为它需要 LITE 资源及实现
`SyncLiteSubscription` 的 Proxy。

应用是常规 Generic Host 时，请使用同级 gRPC 示例。完整的角色 API、投递行为、重试和服务端约束请参阅
[EventHorizon.RocketMQ.Grpc](../../../src/EventHorizon.RocketMQ.Grpc/README.zh-CN.md)。
