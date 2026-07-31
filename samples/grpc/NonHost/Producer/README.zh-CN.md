# 不使用 Generic Host 的 gRPC Producer

[English](README.md) | [简体中文](README.zh-CN.md)

此控制台示例不构造 Generic Host，而是通过 `IGrpcProducer` 发布一条普通消息。它使用普通
`ServiceCollection` 和 `ServiceProvider`，显式启动 Producer，等待发送回执，在 `finally` 中停止 Producer，最后异步释放
service provider。

## 何时使用这种形式

只有进程自身管理非 Generic Host 生命周期时，例如嵌入式运行时或已有应用宿主，才应使用此模式。常规 .NET Generic Host 中，
`AddGrpcProducer` 会注册 SDK 生命周期服务，`RunAsync` 会启动和停止该角色；应用代码不应再次调用
`IGrpcProducer.StartAsync` 或 `StopAsync`。对应集成请参阅 [Generic Host Producer 示例](../../GenericHost/Producer/README.zh-CN.md)。

`BuildServiceProvider` 只创建注册，不会运行已注册的 `IHostedService`。因此 `Program.cs` 中的显式生命周期调用在这里是必需的，
它不是在 Host 已拥有角色时使用的第二套管理方式。

## 工作流与投递

示例创建带有 `sample` tag 的协议专属 `Message`，调用 `SendAsync`，并记录得到的 `GrpcSendReceipt`。回执完成表示 RocketMQ
已接受该消息，不代表 Consumer 已处理它。生成的 message key 仅用于查询和关联，不保证去重。

`IGrpcProducer` 通过 `RocketMQ:Client:Endpoint` 连接 RocketMQ 5 Proxy，不会直接连接 Broker 或 NameServer。Producer 不使用
consumer group。

## 运行示例

先启动[本地 RocketMQ 环境](../../../../test-environments/rocketmq/README.zh-CN.md)，再运行项目：

```shell
docker compose -f test-environments/rocketmq/compose.yaml up -d --wait
dotnet run --project samples/grpc/NonHost/Producer
```

该环境会创建 `eventhorizon-test-topic`，非 Host [SimpleConsumer](../SimpleConsumer/README.zh-CN.md) 和
[PushConsumer](../PushConsumer/README.zh-CN.md) 示例也会消费它。可使用标准 .NET 配置修改 Proxy 连接，例如
`RocketMQ__Client__Endpoint=proxy.example:8081`。

消息模式、发送失败处理和完整 Producer options 请参阅
[gRPC 协议指南](../../../../src/EventHorizon.RocketMQ.Grpc/README.zh-CN.md)。
