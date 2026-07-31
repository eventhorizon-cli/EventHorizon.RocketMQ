# 不使用 Generic Host 的 gRPC Push Consumer

[English](README.md) | [简体中文](README.zh-CN.md)

这是 [Generic Host Push Consumer 示例](../../GenericHost/PushConsumer/README.zh-CN.md)对应的可运行控制台示例。它直接使用
`IGrpcPushConsumer`，但刻意不构造 `IHost`。

`AddGrpcPushConsumer` 仍会为常规 Generic Host 应用注册 `IHostedService`。普通 `ServiceProvider` 的构造不会启动该服务，
因此主程序会解析协议专用 Consumer，调用 `StartAsync`，等待 Ctrl+C，再使用未取消的 token 调用 `StopAsync`，最后异步释放
容器。

## 何时使用这种形式

只有应用自身使用非 Generic Host 生命周期时，例如嵌入式运行时或已有进程宿主，才应使用此模式。常规 .NET Generic Host
应使用[对应的 Host 示例](../../GenericHost/PushConsumer/README.zh-CN.md)：其 `RunAsync` 会调用 SDK 注册的生命周期服务，所以应用代码不能再次调用
`StartAsync` 或 `StopAsync`。

Consumer 本身仍负责分配轮询、长轮询接收、handler 并发、确认、重试和死信结算。scoped handler 只会在示例处理完成后返回
`Success`。

## 运行示例

先启动[本地 RocketMQ 环境](../../../../test-environments/rocketmq/README.zh-CN.md)，再运行 Consumer，然后发送带有匹配
`sample` tag 的消息：

```shell
docker compose -f test-environments/rocketmq/compose.yaml up -d --wait
dotnet run --project samples/grpc/NonHost/PushConsumer
```

可使用[非 Host gRPC Producer 示例](../Producer/README.zh-CN.md)发送消息。按一次 Ctrl+C 会开始优雅停止；即使启动被取消或中途失败，
示例仍会调用 `StopAsync`。

默认 Proxy 端点为 `localhost:8081`。外部部署必须暴露 RocketMQ 5 Proxy、创建 `eventhorizon-test-topic`，并授予路由查询和
消费权限；启用重试和死信结果时还需要相应访问权限。可使用标准 .NET 配置覆盖，例如
`RocketMQ__Client__Endpoint=proxy.example:8081`。

完整 API 和投递契约请参阅 [gRPC 协议指南](../../../../src/EventHorizon.RocketMQ.Grpc/README.zh-CN.md)。
