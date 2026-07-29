# gRPC LitePushConsumer

[English](README.md) | [简体中文](README.zh-CN.md)

`IGrpcLitePushConsumer` 会自动分发一个 LITE parent（或 bind）topic 下、由服务端管理的 LiteTopic 消息。它复用 gRPC
Push 的 handler 和长轮询 dispatcher，同时通过 `SyncLiteSubscription` 增加 Lite 订阅控制面。LiteTopic 不是普通的
RocketMQ topic，也不是 tag 过滤条件。

## 适用场景

只有 RocketMQ 部署和资源模型已经明确配置为使用 LITE 消息，并且应用希望从同一 parent topic 下的一个或多个逻辑
LiteTopic 自动分发到 handler 时，才适合使用 LitePushConsumer。

普通 topic 以及 Tag 或 SQL 过滤应使用 `IGrpcPushConsumer`。需要由应用主动接收并处理消息状态时，应使用
`IGrpcSimpleConsumer`。如果 Proxy 没有实现 `SyncLiteSubscription`，无法通过客户端配置让它支持该角色。

## SDK 工作流

注册 gRPC 传输和 LitePush 角色。`BindTopic` 指定 LITE parent topic，`LiteTopics` 是初始的完整逻辑订阅集合：

```csharp
rocketMQ.AddGrpcLitePushConsumer<OrderMessageHandler>(ServiceLifetime.Scoped, options =>
{
    options.GroupName = "orders-lite-push-consumer";
    options.BindTopic = "orders-lite";
    options.LiteTopics.Add("orders-us");
    options.LiteTopics.Add("orders-eu");
    options.MaxConcurrency = 8;
});
```

不要为该角色调用继承的 `ConsumerOptions.Subscribe`。LitePush 通过 `BindTopic` 接收消息，并用 `LiteTopics`、
`SubscribeLiteAsync` 和 `UnsubscribeLiteAsync` 管理逻辑订阅；它不接受普通 topic 过滤条件。

Generic Host 集成会启动该角色、同步已配置的集合，并随 Host 停止该角色。不使用 Host 时，应通过
`IGrpcLitePushConsumer` 管理 `StartAsync` 和 `StopAsync`。`LiteTopics` 返回当前本地集合的快照。

在运行时，新 LiteTopic 可以附带一个初始 offset 选择：

```csharp
await consumer.SubscribeLiteAsync(
    "orders-apac",
    GrpcLiteOffsetOption.Last,
    cancellationToken);
```

`GrpcLiteOffsetOption` 支持服务端定义的最近位置（`Last`）、最早或最新可用位置（`Min` / `Max`）、明确的非负 queue
offset（`AtOffset`）、最近若干条消息（`Tail`），或者该时间点或之后存储的第一条消息（`AtTimestamp`）。该选项在创建对应
LiteTopic 订阅时生效。

客户端会在启动时发送完整的已配置 LiteTopic 集合，并按 `SubscriptionSyncInterval` 定期协调。名称和配额规则仍由服务端
决定。运行时变更被拒绝时会抛出 `GrpcServiceException`，且不会更新本地集合。`SyncLiteSubscription` 只负责订阅控制；
消息投递仍然由客户端发起长轮询。

## 投递与失败语义

LitePush 与标准 Push 使用同一个 `IGrpcPushMessageHandler` 契约和依赖注入生命周期规则。
`ConsumeResult.Success` 会确认消息，`Retry` 会请求再次投递，`DeadLetter` 会将消息转入该 consumer group 的死信队列。
handler 异常会被当作重试请求，完成操作失败也可能造成重复投递，因此处理逻辑必须幂等。

损坏的消息不会进入应用 handler。客户端会重试非 FIFO 损坏消息；FIFO 损坏消息会在释放同一 message group 的下一条消息前
转入死信队列。

客户端会在 handler 运行期间续期消息的不可见时间。对于非 FIFO 工作，`ConsumeTimeout` 会取消 handler token、停止续期并
请求重试，但无法强制终止忽略取消的代码。FIFO message group 会继续保持关联以维持顺序。它还继承
`IGrpcPushConsumer` 的本地并发、有界缓存和服务端策略回退行为。

## RocketMQ 能力要求

LitePush 要求客户端、Proxy、Broker、Topic 和 consumer group 的配置彼此匹配：

- 存储 parent topic 的每个 Broker 都必须启用 `enableLmq=true` 和 `enableMultiDispatch=true`。
- parent topic 必须以 `message.type=LITE` 创建。
- consumer group 必须设置 `lite.bind.topic=<parent-topic>`，且与 `GrpcLitePushConsumerOptions.BindTopic` 一致。
- 客户端必须连接实现了 `SyncLiteSubscription` 的 cluster-mode Proxy。

在 RocketMQ 5.5.0 中，使用 `mqbroker --enable-proxy` 启动的 Broker-integrated Proxy 没有实现该 RPC；请使用
`mqproxy -pm cluster` 或兼容的新版 Proxy。独立的 cluster-mode Proxy 可以与 Broker 共用容器或 Compose service。

## 关键 SDK options

| Option | 控制内容 |
| --- | --- |
| `GrpcClientOptions.Endpoint` | RocketMQ Proxy 端点；Proxy 必须支持 `SyncLiteSubscription`。 |
| `GrpcLitePushConsumerOptions.GroupName` | 与 LITE parent topic 绑定的 consumer group。 |
| `BindTopic` | 用于分配和消息接收的单一 parent topic。 |
| `LiteTopics` | 启动时同步的完整初始逻辑 LiteTopic 集合。 |
| `SubscriptionSyncInterval` | 定期协调完整 LiteTopic 集合的间隔。 |
| `MaxConcurrency`、`BatchSize` 和缓存限制 | 从 Push 继承的本地 handler 并发、Receive 批量和有界缓冲。 |
| `InvisibleDuration` / `ConsumeTimeout` | 从 Push 继承的初始不可见时长和非 FIFO handler 超时行为。 |
| `MaxDeliveryAttempts` / `RetryDelay` | Proxy 没有提供投递策略时使用的回退值。 |
| `LongPollingTimeout` | 每次 Receive 长轮询允许服务端等待的最长时间。 |

## 运行示例

请使用专用的 [LitePush 环境](../../../test-environments/rocketmq-litepush/README.zh-CN.md)。它会启用 Broker 能力、
在 `localhost:8081` 启动 cluster-mode Proxy，并创建所需的 parent topic 和 consumer group。

```shell
docker compose -f test-environments/rocketmq-litepush/compose.yaml up -d --wait
dotnet run --project samples/grpc/LitePushConsumer
```

可运行代码绑定 `eventhorizon-test-lite-parent-topic`，并订阅 `eventhorizon-test-lite-topic`。要观察消息投递，
需要使用能在该 parent 下发送 Lite message 的 Producer；普通 Producer 示例不会写入 LiteTopic。

完整 API 和部署约束请参阅
[gRPC 指南](../../../src/EventHorizon.RocketMQ.Grpc/README.zh-CN.md)与
[gRPC 消费模型](../../../docs/zh-CN/grpc/consumer-model.md)。
