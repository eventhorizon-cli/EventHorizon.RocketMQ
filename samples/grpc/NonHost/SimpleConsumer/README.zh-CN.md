# 不使用 Generic Host 的 gRPC SimpleConsumer

[English](README.md) | [简体中文](README.zh-CN.md)

此控制台示例不构造 Generic Host，而是直接驱动 gRPC `IGrpcSimpleConsumer`。它构建普通 `ServiceProvider`，显式启动
Consumer，对 Proxy 执行长轮询，确认正常消息，将损坏消息转入死信队列，显式停止 Consumer，并异步释放 service provider。

## 何时使用这种形式

只有进程自身管理非 Generic Host 生命周期时，才应使用此形式。常规 .NET Generic Host 中，`AddGrpcSimpleConsumer` 会注册 SDK
生命周期服务，并由 Host 启动和停止该角色。应用代码不能再次调用 `IGrpcSimpleConsumer.StartAsync` 或 `StopAsync`；请参阅
[Generic Host SimpleConsumer 示例](../../GenericHost/SimpleConsumer/README.zh-CN.md)。

`BuildServiceProvider` 只会构造已注册服务，不会运行其 `IHostedService` 注册。因此本示例中的手动生命周期调用是必需的。

## 接收与结算

`ReceiveAsync` 会长轮询 RocketMQ 5 Proxy，返回暂时不可见的 `GrpcMessageView`，但不会确认它们。示例记录每条正常消息后调用
`AckAsync`；真实应用只能在业务效果已持久化后确认。它会在读取 body 前检查 `IsCorrupted`，并对损坏 payload 调用
`ForwardToDeadLetterQueueAsync`。结算失败或未结算时，消息会在不可见窗口结束后再次可见，因此处理必须是幂等的。

SimpleConsumer 由应用驱动：进程控制接收批量、接收并发和结算。需要 handler 风格的自动分发和结算时，应选择
`IGrpcPushConsumer`。

## 运行示例

先启动[本地 RocketMQ 环境](../../../../test-environments/rocketmq/README.zh-CN.md)，再运行 Consumer，随后用非 Host
[Producer](../Producer/README.zh-CN.md) 发送带有 `sample` tag 的消息：

```shell
docker compose -f test-environments/rocketmq/compose.yaml up -d --wait
dotnet run --project samples/grpc/NonHost/SimpleConsumer
```

按 Ctrl+C 会取消接收循环。Ctrl+C token 已被取消，因此示例使用未取消的 token 调用 `StopAsync`。可以使用正常 .NET 配置覆盖
Proxy 端点，例如 `RocketMQ__Client__Endpoint=proxy.example:8081`。

运行时订阅变更、不可见时间续期和完整 SimpleConsumer 契约请参阅
[gRPC 协议指南](../../../../src/EventHorizon.RocketMQ.Grpc/README.zh-CN.md)。
