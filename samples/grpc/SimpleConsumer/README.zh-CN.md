# gRPC SimpleConsumer

[English](README.md) | [简体中文](README.zh-CN.md)

`IGrpcSimpleConsumer` 是由应用驱动的 gRPC 消费模型。应用主动向 RocketMQ Proxy 请求一批消息，自行判断业务处理何时完成，
并显式处理每条消息的最终状态。它不是经典的 queue/offset Pull，Broker 也不会通过到应用的连接主动推送消息。

## 适用场景

当确认边界必须由应用代码控制时，适合使用 SimpleConsumer。常见场景包括：将确认与持久化写入绑定、为每次接收决定批量大小、
在长时间任务期间延长不可见时间，或者采用应用自己的并发和背压策略。

如果更适合使用 handler，并希望客户端自动接收、分发和处理消息状态，请改用 `IGrpcPushConsumer`。如果应用必须直接选择
Broker queue 并管理数字 offset，请使用经典 Remoting Pull。

## SDK 工作流

先通过 `AddRocketMQGrpc` 注册 gRPC 传输，再为该注册添加 SimpleConsumer 角色：

```csharp
rocketMQ.AddGrpcSimpleConsumer(options =>
{
    options.GroupName = "orders-simple-consumer";
    options.AwaitDuration = TimeSpan.FromSeconds(5);
    options.InvisibleDuration = TimeSpan.FromSeconds(30);
    options.Subscribe("orders", new FilterExpression("created"));
});
```

同一个 `AddRocketMQGrpc` 注册可以包含多个 Consumer 角色。每次调用 `AddGrpcSimpleConsumer` 都有独立的 group、
订阅、options 和生命周期，底层 gRPC channel 则由该注册复用。需要让每个 Consumer 独立观察同一 topic 时，应使用
不同 group。

Generic Host 集成会启动和停止已注册的角色。不使用 Host 时，需要解析 `IGrpcSimpleConsumer`，并显式调用 `StartAsync` 和
`StopAsync`。

公开 API 将接收与最终状态处理分开：

| API | 职责 |
| --- | --- |
| `SubscribeAsync` / `UnsubscribeAsync` | 启动后修改当前生效的 topic 和过滤条件。Options 上的 `Subscribe` 用于定义初始订阅。 |
| `ReceiveAsync` | 向 Proxy 发起长轮询，最多返回请求数量的 `GrpcMessageView`；返回空集合是正常结果。 |
| `AckAsync` | 在业务副作用已经持久化后确认一条消息。 |
| `ChangeInvisibleDurationAsync` | 处理需要更多时间时，替换消息当前的不可见时长。 |
| `ForwardToDeadLetterQueueAsync` | 使用传入的投递次数阈值，将消息显式转入该 consumer group 的死信队列。 |

读取消息体前应检查 `GrpcMessageView.IsCorrupted`。损坏的 payload 仍需由应用明确决定如何处理，SimpleConsumer 不会自动
确认它。

## 完成与失败语义

`ReceiveAsync` 只会让消息暂时不可见，不会确认消息。如果业务处理失败、进程停止，或者 `AckAsync` 没有成功完成，消息可能在
不可见时长结束后再次可见。因此，应用必须保证处理逻辑幂等，并且只在持久化处理完成后确认。

如果任务可能超过当前不可见窗口，应在窗口到期前调用 `ChangeInvisibleDurationAsync`。转入死信队列同样是应用的显式决定；
普通处理异常不会让 SDK 代替应用调用 `ForwardToDeadLetterQueueAsync`。

consumer group 保存确认和重试状态。使用同一个 group 的多个实例会分摊消息；使用不同 group 才会独立观察同一个 topic。

## 关键 SDK options

| Option | 控制内容 |
| --- | --- |
| `GrpcClientOptions.Endpoint` | RocketMQ Proxy 端点；这里不能填写 NameServer 地址。 |
| `GrpcClientOptions.RequestTimeout` / `UseTLS` | 普通 gRPC 请求的截止时间和传输安全设置。 |
| `GrpcSimpleConsumerOptions.GroupName` | 保存投递和确认状态的 consumer group。 |
| `GrpcSimpleConsumerOptions.AwaitDuration` | Proxy 在 Receive 长轮询中可以等待的时间。本仓库提供的 RocketMQ 5.5.0 Proxy 要求至少五秒。 |
| `GrpcSimpleConsumerOptions.InvisibleDuration` | 接收消息时默认请求的不可见时长；`ReceiveAsync` 可以为单次调用覆盖该值。 |
| `ConsumerOptions.Subscribe` | 初始 topic 订阅和 `FilterExpression`；省略表达式会匹配全部消息。 |

## 运行示例

本仓库提供的 [RocketMQ 环境](../../../test-environments/rocketmq/README.zh-CN.md) 会在 `localhost:8081` 提供 Proxy，
并创建示例 Topic。使用外部部署时，必须提供兼容的 RocketMQ 5 Proxy；如果禁用了自动创建，还需要预先创建 Topic 和
consumer group。

```shell
docker compose -f test-environments/rocketmq/compose.yaml up -d --wait
dotnet run --project samples/grpc/SimpleConsumer
```

`appsettings.json` 只保存 Proxy 连接设置；可运行 Consumer 的 group、时间和订阅值都直接写在 `Program.cs` 的注册旁。
它订阅 `eventhorizon-test-topic`，并使用 `sample` tag 过滤。可通过
[Producer 示例](../Producer/README.zh-CN.md)或其他兼容 Producer 发送匹配消息。

完整 Consumer API 和 Proxy 约束请参阅
[gRPC 指南](../../../src/EventHorizon.RocketMQ.Grpc/README.zh-CN.md)与
[gRPC 消费模型](../../../docs/zh-CN/grpc/consumer-model.md)。
