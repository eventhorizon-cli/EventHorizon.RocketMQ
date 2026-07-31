# gRPC Lite Producer

[English](README.md) | [简体中文](README.zh-CN.md)

`IGrpcProducer` 通过向 LITE parent topic 发布消息并设置消息的 `LiteTopic` 来发送 Lite message。这个 Generic Host Web API
示例向 `eventhorizon-test-lite-parent-topic` 发布消息，并使用逻辑 LiteTopic `eventhorizon-test-lite-topic`。请与配套的
[LitePushConsumer](../LitePushConsumer/README.zh-CN.md) 一起运行，以覆盖完整的 Lite 发布和分发路径。

## 适用场景

当 RocketMQ 5 gRPC 应用需要向启用 LITE 的部署发布消息时，适合使用这种形式。parent topic 标识 Broker 资源；`LiteTopic`
标识该 parent 下的逻辑目标。LiteTopic 不是需要单独创建的普通 topic，也不是 tag filter。

不以 LiteTopic 为目标的普通、FIFO、延迟、优先级或事务工作流应使用普通的 [Producer](../Producer/README.zh-CN.md)。该示例
刻意不设置 `Message.LiteTopic`，因此其消息不会到达此环境中 LitePushConsumer 的订阅。

## 注册与生命周期

示例通过 `AddRocketMQGrpc` 和 `AddGrpcProducer` 注册 `IGrpcProducer`。它的 `WebApplication` 是 Generic Host，因此 SDK 的
hosted service 会在应用启动时启动 Producer，并在 Host 关闭时停止它。应用代码只注入并使用 `IGrpcProducer`，不能手动调用
`StartAsync` 或 `StopAsync`。

## 发送 Lite message

HTTP endpoint 先用 LITE parent topic 创建协议专属的 `Message`，再在调用 `SendAsync` 前设置 `LiteTopic`：

```csharp
var message = new Message(
    "eventhorizon-test-lite-parent-topic",
    Encoding.UTF8.GetBytes(body))
{
    LiteTopic = "eventhorizon-test-lite-topic"
};

GrpcSendReceipt receipt = await producer.SendAsync(message, cancellationToken);
```

设置 `LiteTopic` 会选择 Lite message type。它与 `MessageGroup`、`DeliveryTimestamp` 和 `Priority` 互斥，不能在一条消息上
组合这些专用属性。发送回执只说明 RocketMQ 已接受消息，并不代表 Consumer 已完成处理。

## 前置条件与运行

请使用专用的 [LitePush 环境](../../../../test-environments/rocketmq-litepush/README.zh-CN.md)。它会在 `localhost:8081` 启动
cluster-mode Proxy，创建 LITE parent topic，并将 LitePush consumer group 绑定到该 parent topic。

先启动配套的 Consumer：

```shell
docker compose -f test-environments/rocketmq-litepush/compose.yaml up -d --wait
dotnet run --project samples/grpc/GenericHost/LitePushConsumer
```

在第二个终端启动此 Web API 并提交消息：

```shell
dotnet run --project samples/grpc/GenericHost/LiteProducer
curl --request POST http://localhost:5232/messages \
  --header 'Content-Type: application/json' \
  --data '{"message":"hello Lite"}'
```

API 会返回发送回执数据。LitePushConsumer 完成 LiteTopic 订阅同步后，会记录收到的消息。示例只在 `appsettings.json` 中保留
Proxy 连接设置；parent topic 和 LiteTopic 保留在消息构造代码旁，使 Lite 路由决策清晰可见。

服务端能力要求和完整 Producer 契约请参阅
[gRPC 指南](../../../../src/EventHorizon.RocketMQ.Grpc/README.zh-CN.md)与
[LitePushConsumer 示例](../LitePushConsumer/README.zh-CN.md)。
