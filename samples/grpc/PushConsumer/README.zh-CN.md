# gRPC PushConsumer

[English](README.md) | [简体中文](README.zh-CN.md)

`IGrpcPushConsumer` 是自动分发的 gRPC 消费模型。客户端查询分配、向 RocketMQ Proxy 发起长轮询、在本地缓冲收到的消息，
再调用应用提供的 handler。“Push”描述的是应用 API；Broker 不会向应用建立一个由服务端发起的连接。

## 适用场景

当消息处理适合表示成 handler，并希望 SDK 负责接收循环、背压、并发、不可见时间续期及完成操作时，适合使用
PushConsumer。对于持续运行、每条消息都能由 handler 返回一个处理结果的服务，它通常是更简单的选择。

如果应用必须自行决定何时接收、直接控制批量，或者将确认交给自己的调度器，请改用 `IGrpcSimpleConsumer`。如果应用必须选择
Broker queue 并管理数字 offset，请使用经典 Remoting Pull。

## SDK 工作流

注册 gRPC 传输，并为类型化 handler 指定明确的依赖注入生命周期：

```csharp
rocketMQ.AddGrpcPushConsumer<OrderMessageHandler>(ServiceLifetime.Scoped, options =>
{
    options.GroupName = "orders-push-consumer";
    options.MaxConcurrency = 8;
    options.Subscribe("orders", new FilterExpression("created"));
});

public sealed class OrderMessageHandler : IGrpcPushMessageHandler
{
    public async ValueTask<ConsumeResult> HandleAsync(
        GrpcMessageView message,
        CancellationToken cancellationToken)
    {
        await ProcessAsync(message.Body, cancellationToken);
        return ConsumeResult.Success;
    }

    private static Task ProcessAsync(byte[] body, CancellationToken cancellationToken) =>
        Task.CompletedTask;
}
```

`Scoped` 和 `Transient` handler 会在每次处理尝试中新建异步 scope，再从中解析。`Singleton` handler 会被并发调用共享，
因此必须线程安全。对于很小的无状态回调，可以使用非泛型注册并设置 `GrpcPushConsumerOptions.MessageHandler`；委托和类型化
handler 不能同时配置。

Generic Host 集成会为已注册角色调用 `StartAsync` 和 `StopAsync`。不使用 Host 时，应用必须通过 `IGrpcPushConsumer`
管理生命周期。`SubscribeAsync` 和 `UnsubscribeAsync` 用于在启动后修改当前 topic 和过滤条件；Options 上的 `Subscribe`
用于定义初始订阅。

## 投递、完成与失败

每个 handler 结果都会要求 Consumer 执行一种明确的完成操作：

| 结果 | SDK 行为 |
| --- | --- |
| `ConsumeResult.Success` | 确认消息。只有业务副作用已经持久化后才能返回该结果。 |
| `ConsumeResult.Retry` | 按当前生效的重试策略，让消息可以再次投递。 |
| `ConsumeResult.DeadLetter` | 将消息转入该 consumer group 的死信队列。 |

未处理的 handler 异常会被当作 `Retry`。如果确认、重试或死信完成操作失败，消息可能再次投递。因此，即使 handler 返回
`Success`，处理逻辑仍必须幂等。

损坏的消息不会进入 `IGrpcPushMessageHandler`。非 FIFO 损坏消息会重新变为可投递状态；FIFO 损坏消息会在释放同一
message group 的下一条消息前转入死信队列。

handler 运行期间，客户端会续期消息的不可见时间。对于非 FIFO 消息，`ConsumeTimeout` 限制 dispatcher 等待 handler
的时长：超时后会取消 handler token、停止续期、请求重试，并忽略延迟返回的成功结果。SDK 无法强制停止忽略取消的代码，
因此旧调用可能与重新投递重叠。FIFO message group 不使用这种超时后脱离 handler 的行为，因为释放下一条消息可能破坏顺序。

`MaxConcurrency` 控制本地 handler 并发，不代表 Broker queue 数量。FIFO 顺序只在一个 message group 内成立，不是跨
queue、consumer group 或 Consumer 实例的全局顺序。使用相同 consumer group 的实例会共享分配和重试状态。

## 关键 SDK options

| Option | 控制内容 |
| --- | --- |
| `GrpcClientOptions.Endpoint` | RocketMQ Proxy 端点；这里不能填写 NameServer 地址。 |
| `GrpcPushConsumerOptions.GroupName` | 用于分配和投递状态的 consumer group。 |
| `ConsumerOptions.Subscribe` | 初始 topic 以及 Tag 或 SQL 过滤订阅。 |
| `MaxConcurrency` | 最大 handler 调度并发数。已经超时且忽略取消的 handler 仍可能留在进程中。 |
| `BatchSize` | 单次 Receive 请求的最大消息数。 |
| `MaxCachedMessages` / `MaxCachedMessageBytes` | 有界本地消息缓冲的数量和消息体字节数限制。 |
| `InvisibleDuration` | 初始消息不可见时长；处理期间客户端会续期。 |
| `ConsumeTimeout` | 非 FIFO handler 在请求取消和重试前允许运行的最长时间。 |
| `MaxDeliveryAttempts` / `RetryDelay` | Proxy 没有提供策略时使用的死信阈值和重试延迟回退值。 |
| `LongPollingTimeout` | 每次 Receive 长轮询允许服务端等待的最长时间。 |

## 运行示例

本仓库提供的 [RocketMQ 环境](../../../test-environments/rocketmq/README.zh-CN.md) 会在 `localhost:8081` 提供兼容
Proxy，并创建示例 Topic。使用外部部署且禁用了自动创建时，需要预先创建 Topic 和 consumer group。

```shell
docker compose -f test-environments/rocketmq/compose.yaml up -d --wait
dotnet run --project samples/grpc/PushConsumer
```

可运行代码订阅 `eventhorizon-test-topic`，并使用 `sample` tag 过滤。可通过
[Producer 示例](../Producer/README.zh-CN.md)或其他兼容 Producer 发送匹配消息。

完整 handler、订阅和 Proxy 行为请参阅
[gRPC 指南](../../../src/EventHorizon.RocketMQ.Grpc/README.zh-CN.md)与
[gRPC 消费模型](../../../docs/zh-CN/grpc/consumer-model.md)。
