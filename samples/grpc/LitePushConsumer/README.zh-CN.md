# gRPC LitePushConsumer 示例

[English](README.md) | [简体中文](README.zh-CN.md)

这是一个 Generic Host 示例。它通过 `AddRocketMQGrpc` 和
`AddGrpcLitePushConsumer<LitePushConsumerMessageHandler>` 注册 `IGrpcLitePushConsumer`。它自动分发属于同一个
LITE parent/bind topic 的 LiteTopic 消息，并通过 `SyncLiteSubscription` 将完整 LiteTopic 集合与服务端同步。

## 配置

| 配置键 | 默认值 | 用途 |
| --- | --- | --- |
| `RocketMQ:Client:Endpoint` | `localhost:8081` | RocketMQ Proxy gRPC 端点。 |
| `RocketMQ:Client:RequestTimeout` | `00:00:03` | 普通客户端请求的截止时间。 |
| `RocketMQ:Client:UseTLS` | `false` | 为 Proxy 连接启用 TLS。 |
| `RocketMQ:Consumer:GroupName` | `eventhorizon-test-lite-push-consumer` | 必须绑定到 LITE parent topic 的 consumer group。 |
| `RocketMQ:Consumer:BatchSize` | `16` | 每次 Receive 请求的最大消息数。 |
| `RocketMQ:Consumer:MaxConcurrency` | `4` | 最大并发 handler 执行数。 |
| `RocketMQ:Consumer:MaxDeliveryAttempts` | `16` | 进入死信前的回退投递次数上限。 |
| `RocketMQ:Consumer:InvisibleDuration` | `00:00:30` | 收到消息后的初始租约时长。 |
| `RocketMQ:Consumer:ConsumeTimeout` | `00:15:00` | 非 FIFO handler 在客户端取消其 token 并请求重新投递前允许运行的最长时间。 |
| `RocketMQ:Consumer:LongPollingTimeout` | `00:00:15` | Receive 请求允许服务端等待的最长时间。 |
| `RocketMQ:Consumer:SubscriptionSyncInterval` | `00:00:30` | 将完整 LiteTopic 集合与服务端协调的间隔。 |

本示例会固定使用 `eventhorizon-test-lite-parent-topic` 作为 LITE parent topic，并在启动时同步
`eventhorizon-test-lite-topic`。这些名称刻意写死在代码中。parent topic 不是普通订阅；生产应用可改为通过
`SubscribeLiteAsync` 和 `UnsubscribeLiteAsync` 管理 LiteTopic 集合。

## 前置条件

- `RocketMQ:Client:Endpoint` 必须指向支持 `SyncLiteSubscription` RPC 的 cluster-mode RocketMQ 5 Proxy。RocketMQ 5.5.0
  中通过 `mqbroker --enable-proxy` 启动的 Broker-integrated Proxy 不实现该服务。
- 存储 parent topic 的每个 Broker 都必须开启 `enableLmq=true` 和 `enableMultiDispatch=true`。
- 请使用 LitePush 专用环境。它会开启上述两个 Broker 配置，在 `localhost:8081` 启动 `mqproxy -pm cluster`，并在
  启动过程中创建固定的 LITE parent Topic 和 Consumer Group。
- 启动前请停止其他占用宿主机 `8081` 端口的环境。

在仓库根目录启动专用环境：

```shell
docker compose -f test-environments/rocketmq-litepush/compose.yaml up -d --wait
```

`resource-init` 成功结束后，会创建带有 `message.type=LITE` 的 `eventhorizon-test-lite-parent-topic`，并将
`eventhorizon-test-lite-push-consumer` 绑定到该 parent Topic。`eventhorizon-test-lite-topic` 是逻辑 LiteTopic，
由 Consumer 在启动时同步，不是需要单独创建的普通 Topic。

拓扑、持久化行为和本地端口限制请参阅
[LitePush 环境说明](../../../test-environments/rocketmq-litepush/README.zh-CN.md)。

## 运行

环境启动完成后，在仓库根目录执行：

```shell
dotnet run --project samples/grpc/LitePushConsumer
```

Host 会持续运行直到按下 Ctrl+C，并在启动时记录固定的 LiteTopic。要收到消息，需要使用支持 Lite message 的 Producer 向
`eventhorizon-test-lite-parent-topic` parent topic 下的 `eventhorizon-test-lite-topic` 发送 Lite 消息；标准 Producer 示例只发送普通
消息，不会写入 LiteTopic。

## 语义与常见误解

- LiteTopic 是 LITE parent topic 下由服务端管理的逻辑订阅，不是独立的普通 topic，不能用普通 tag filter 替代。
- 客户端会在启动时及每个 `SubscriptionSyncInterval` 发送完整 LiteTopic 集合；服务端可能因名称或配额规则拒绝订阅。
  被拒绝的变更会抛出 `GrpcServiceException`，且不会更新本地集合。
- 与标准 PushConsumer 一样，LitePush 是客户端发起的长轮询，而不是 Broker 网络 Push。handler 返回 `Success` 会确认，
  返回 `Retry` 会请求重新投递，返回 `DeadLetter` 会转发到该 group 的死信队列。本示例会将损坏消息转为死信。
- `BindTopic`、Broker `message.type=LITE` 属性与 group `lite.bind.topic` 属性必须指向同一个 parent topic。拼写不一致
  是配置错误，而不是订阅第二个 parent topic 的方式。
- handler 注册为 `Scoped`，每次处理尝试都会获得自己的异步 DI scope。
- `ConsumeTimeout` 与标准 Push 的非 FIFO 超时行为一致：会取消 handler token、停止客户端不可见时间续期并请求重新投递。它无法
  强制停止忽略取消的代码；FIFO handler 会继续保持关联以维护顺序。

Lite 消息生产和完整 API 请参阅 [gRPC 指南](../../../src/EventHorizon.RocketMQ.Grpc/README.zh-CN.md)。
