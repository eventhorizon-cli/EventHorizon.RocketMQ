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

`BindTopic` 指向唯一的 LITE parent topic。`LiteTopics` 可以提供启动时的逻辑订阅集合，但本示例会刻意以空集合启动：Web
API 用来承接应用中的会话连接、断开等事件。

```csharp
rocketMQ.AddGrpcLitePushConsumer<LitePushConsumerMessageHandler>(ServiceLifetime.Scoped, options =>
{
    options.GroupName = "eventhorizon-test-lite-push-consumer";
    options.BindTopic = "eventhorizon-test-lite-parent-topic";
    options.MaxConcurrency = 4;
});
```

SDK 的 hosted service 启动 Consumer 后，API 会调用公开的运行时订阅方法：

```csharp
await consumer.SubscribeLiteAsync(liteTopic, cancellationToken: cancellationToken);
await consumer.UnsubscribeLiteAsync(liteTopic, cancellationToken);
```

`POST /subscriptions/{liteTopic}` 新增订阅，`DELETE /subscriptions/{liteTopic}` 删除订阅，
`GET /subscriptions` 返回当前本地快照。LiteTopic 只能使用字母、数字、连字符和下划线，因此
`chat-session-123` 适合用来演示按会话管理 LiteTopic。接口成功返回前已经完成与 RocketMQ 的同步；
`chat.session-123` 这类无效名称会返回 HTTP 400，SDK 调用失败会返回 HTTP 503，且不会修改本地集合。

不要为该角色调用继承的 `ConsumerOptions.Subscribe`。LitePush 通过 `BindTopic` 接收消息，并用 `LiteTopics`、
`SubscribeLiteAsync` 和 `UnsubscribeLiteAsync` 管理逻辑订阅；它不接受普通 topic 过滤条件。同一个
`AddRocketMQGrpc` 注册可以添加多个配置彼此独立的 LitePush Consumer，并复用底层 gRPC channel。同一 group 中的 Consumer
必须使用相同 `BindTopic`，但 LiteTopic 集合可以不同；不同 group 则会独立接收消息。

Generic Host 会通过 SDK 的 hosted service 启动和停止 Consumer。API 只修改订阅，不负责 Consumer 生命周期。运行时订阅是
应用本地状态，Consumer 重启后应由应用事件重新建立。`LiteTopics` 返回当前本地集合的快照。

生产代码可以在新增 LiteTopic 时通过 `GrpcLiteOffsetOption` 选择初始 offset，例如
`GrpcLiteOffsetOption.Last`。本示例使用服务端默认值，避免把重点从会话订阅流程上移开。客户端会按
`SubscriptionSyncInterval` 定期协调完整的本地集合；`SyncLiteSubscription` 只负责订阅控制，消息仍通过客户端发起的
长轮询投递。

## 投递与失败语义

LitePush 与标准 Push 使用同一个 `IGrpcPushMessageHandler` 契约和依赖注入生命周期规则。
`ConsumeResult.Success` 会确认消息。非 FIFO `Failure` 与 `Suspend` 由服务端推进重试和死信，并忽略 Suspend 指定的时长。
FIFO `Failure` 在本地重试 handler，达到有效次数上限后转发 DLQ；结算失败按固定 1 秒间隔重试，终态失败时消息保持未
结算但会释放 FIFO 后继消息。
FIFO `ConsumeResult.Suspend(duration)` 使用 `suspend=true` 和指定时长修改不可见时间；下一次 receive 返回的投递次数由
服务端负责，且最短时长为 50 毫秒。它还会覆盖当前 receive batch 中尚未处理的同 LiteTopic 消息，并跳过这些消息的
handler。handler 异常会被当作失败结果，完成操作失败也可能造成重复投递，因此处理逻辑必须幂等。

损坏的消息不会进入应用 handler。客户端会重试非 FIFO 损坏消息；FIFO 损坏消息会在释放同一 LiteTopic 的下一条消息前
转发 DLQ。后继消息会在 completion 重试期间等待；终态结算失败后释放后继消息，失败消息保持未结算，仍可能再次投递。

LitePush 请求会设置 `AutoRenew=true`，由兼容的 Proxy 在 handler 运行期间续期 receipt；.NET 客户端不会再启动
一套续期定时器。对于非 FIFO 工作，`ConsumeTimeout` 会取消 handler token 并请求再次投递，但无法强制终止忽略取消的
代码。FIFO LiteTopic 会继续保持关联以维持顺序。它还继承
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
| `LiteTopics` | 可选的启动初始逻辑 LiteTopic 集合；本示例刻意保持为空。 |
| `SubscriptionSyncInterval` | 定期协调完整 LiteTopic 集合的间隔。 |
| `MaxConcurrency`、`BatchSize` 和缓存限制 | 从 Push 继承的本地 handler 并发、Receive 批量和有界缓冲。 |
| `InvisibleDuration` / `ConsumeTimeout` | 从 Push 继承的初始不可见时长和非 FIFO handler 超时行为。 |
| `MaxDeliveryAttempts` / `RetryDelay` | Proxy 没有提供策略时使用的 FIFO 本地尝试次数与重试延迟回退值；非 FIFO 的死信推进仍由服务端负责。 |
| `LongPollingTimeout` | 每次 Receive 长轮询允许服务端等待的最长时间。 |

## 运行示例

请使用专用的 [LitePush 环境](../../../../test-environments/rocketmq-litepush/README.zh-CN.md)。它会启用 Broker 能力、
在 `localhost:8081` 启动 cluster-mode Proxy，并创建所需的 parent topic 和 consumer group。

```shell
docker compose -f test-environments/rocketmq-litepush/compose.yaml up -d --wait
dotnet run --project samples/grpc/GenericHost/LitePushConsumer
```

Consumer Web API 运行在 `http://localhost:5233`，Swagger 地址为 `http://localhost:5233/swagger`。在另一个终端先为
会话新增订阅，再发送同一 LiteTopic 的消息：

```shell
curl http://localhost:5233/subscriptions
curl --request POST http://localhost:5233/subscriptions/chat-session-123
curl http://localhost:5233/subscriptions
```

在第三个终端启动 LiteProducer，再在第四个终端提交同一 LiteTopic 的消息：

```shell
dotnet run --project samples/grpc/GenericHost/LiteProducer
```

```shell
curl --request POST http://localhost:5232/messages \
  --header 'Content-Type: application/json' \
  --data '{"liteTopic":"chat-session-123","message":"hello Lite"}'
```

会话结束时，可调用 `curl --request DELETE http://localhost:5233/subscriptions/chat-session-123`。`appsettings.json`
只保存 Proxy 连接设置，parent topic 绑定直接写在 `Program.cs` 的注册旁。handler 会记录 parent topic 和 LiteTopic，然后返回
`Success`，由 SDK 在处理完成后确认消息。Host 会持续运行到按下 Ctrl+C。

完整 API 和部署约束请参阅
[gRPC 指南](../../../../src/EventHorizon.RocketMQ.Grpc/README.zh-CN.md)与
[gRPC 消费模型](../../../../docs/zh-CN/grpc/consumer-model.md)。
