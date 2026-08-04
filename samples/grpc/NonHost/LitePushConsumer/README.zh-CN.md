# 不使用 Generic Host 的 gRPC LitePushConsumer

[English](README.md) | [简体中文](README.zh-CN.md)

此控制台示例不构造 Generic Host，而是运行 `IGrpcLitePushConsumer`。它构建普通 `ServiceProvider`，显式启动角色以同步初始
LiteTopic 集合，持续运行 handler 直到 Ctrl+C，显式停止角色，并异步释放 service provider。

## 何时使用这种形式

只有进程自身管理非 Generic Host 生命周期时，才应使用此形式。常规 .NET Generic Host 会通过
`AddGrpcLitePushConsumer` 注册的 `IHostedService` 启动和停止角色；应用代码不能再次调用
`IGrpcLitePushConsumer.StartAsync` 或 `StopAsync`。请参阅
[Generic Host LitePushConsumer 示例](../../GenericHost/LitePushConsumer/README.zh-CN.md)。

`BuildServiceProvider` 不会启动已注册的 hosted service，因此这个非 Host 进程中的 `Program.cs` 必须显式管理生命周期。

## 投递与 Lite 订阅

`BindTopic` 指定 LITE parent topic。`LiteTopics` 是完整的初始逻辑订阅集合，不是普通 topic 或 filter 的列表。示例的 scoped
`IGrpcPushMessageHandler` 记录消息并返回 `ConsumeResult.Success`，SDK 随后会确认消息。`Failure` 交由当前生效的重试
策略安排再次投递。handler 异常会触发重试，因此处理必须是幂等的。

不要为 LitePush 调用普通的 `ConsumerOptions.Subscribe`。应通过一个 bind topic 下的 `LiteTopics`、`SubscribeLiteAsync` 和
`UnsubscribeLiteAsync` 管理订阅。Lite 投递仍通过 RocketMQ 5 Proxy 的客户端发起长轮询完成。

## 运行示例

请使用专用的 [LitePush 环境](../../../../test-environments/rocketmq-litepush/README.zh-CN.md)。它会准备 LITE parent topic、
consumer group、Broker 功能和兼容的 cluster-mode Proxy：

```shell
docker compose -f test-environments/rocketmq-litepush/compose.yaml up -d --wait
dotnet run --project samples/grpc/NonHost/LitePushConsumer
```

按 Ctrl+C 会开始优雅停止。Ctrl+C token 已被取消，因此示例会向 `StopAsync` 传入未取消的 token。可启动 Generic Host
[LiteProducer](../../GenericHost/LiteProducer/README.zh-CN.md)，在 `eventhorizon-test-lite-parent-topic` 下发送 Lite message，
使 handler 收到消息；普通的非 Host [Producer](../Producer/README.zh-CN.md) 刻意发送标准消息。

LitePush 要求启用 `enableLmq`、`enableMultiDispatch`，存在 `message.type=LITE` parent topic 和具有匹配
`lite.bind.topic` 的 group，并使用实现 `SyncLiteSubscription` 的 Proxy。完整契约请参阅
[gRPC 协议指南](../../../../src/EventHorizon.RocketMQ.Grpc/README.zh-CN.md)。
