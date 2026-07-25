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
| `RocketMQ:Consumer:GroupName` | `rocketmq-dotnet-grpc-lite-push-sample` | 必须绑定到 LITE parent topic 的 consumer group。 |
| `RocketMQ:Consumer:BindTopic` | `rocketmq-dotnet-lite-manual` | 用于路由和分配的 LITE parent topic。 |
| `RocketMQ:Consumer:LiteTopics:0` | `rocketmq-dotnet-lite-sample` | 启动时同步的 LiteTopic。 |
| `RocketMQ:Consumer:BatchSize` | `16` | 每次 Receive 请求的最大消息数。 |
| `RocketMQ:Consumer:MaxConcurrency` | `4` | 最大并发 handler 执行数。 |
| `RocketMQ:Consumer:MaxDeliveryAttempts` | `16` | 进入死信前的回退投递次数上限。 |
| `RocketMQ:Consumer:InvisibleDuration` | `00:00:30` | 收到消息后的初始租约时长。 |
| `RocketMQ:Consumer:LongPollingTimeout` | `00:00:15` | Receive 请求允许服务端等待的最长时间。 |
| `RocketMQ:Consumer:SubscriptionSyncInterval` | `00:00:30` | 将完整 LiteTopic 集合与服务端协调的间隔。 |

`BindTopic` 不是普通订阅之一。不要为此角色增加普通 `Subscribe` 调用；应通过 `LiteTopics`、
`SubscribeLiteAsync` 或 `UnsubscribeLiteAsync` 配置 LiteTopic 名称。

## 前置条件

- `RocketMQ:Client:Endpoint` 必须指向实现 `SyncLiteSubscription` 的 cluster-mode RocketMQ 5 Proxy。RocketMQ 5.5.0
  中通过 `mqbroker --enable-proxy` 启动的 Broker-integrated Proxy 不实现该服务。
- 存储 parent topic 的每个 Broker 都必须开启 `enableLmq=true` 和 `enableMultiDispatch=true`。
- 用 `message.type=LITE` 创建 LITE parent topic，再用 `lite.bind.topic` 将 consumer group 绑定到完全相同的 parent
  topic。默认资源可按以下方式创建：

  ```shell
  docker compose -f test-environments/rocketmq/compose.yaml exec broker sh mqadmin updateTopic \
    -n nameserver:9876 -c DefaultCluster -t rocketmq-dotnet-lite-manual -a +message.type=LITE

  docker compose -f test-environments/rocketmq/compose.yaml exec broker sh mqadmin updateSubGroup \
    -n nameserver:9876 -c DefaultCluster -g rocketmq-dotnet-grpc-lite-push-sample \
    --attributes +lite.bind.topic=rocketmq-dotnet-lite-manual
  ```

- 使用仓库检查入的配置启动时，随附 RocketMQ 5.5.0 Compose 环境支持该示例：它开启了上述两个 Broker 配置，并在
  `localhost:8081` 运行 `mqproxy -pm cluster`。这与不支持的 Broker-integrated Proxy 模式不同。

在仓库根目录启动随附环境：

```shell
docker compose -f test-environments/rocketmq/compose.yaml up -d --wait
```

## 运行

准备 LITE parent topic 和 group 后，在仓库根目录执行：

```shell
dotnet run --project samples/grpc/LitePushConsumer
```

Host 会持续运行直到按下 Ctrl+C，并在启动时记录配置的 LiteTopic。要看到消息，请使用兼容 Producer 向
`rocketmq-dotnet-lite-manual` parent topic 下的 `rocketmq-dotnet-lite-sample` 发布 Lite message；标准 Producer
示例只发送普通消息，不会写入 LiteTopic。

## 语义与常见误解

- LiteTopic 是 LITE parent topic 下由服务端管理的逻辑订阅，不是独立的普通 topic，不能用普通 tag filter 替代。
- 客户端会在启动时及每个 `SubscriptionSyncInterval` 发送完整 LiteTopic 集合；服务端可能因名称或配额规则拒绝订阅。
  被拒绝的变更会抛出 `GrpcServiceException`，且不会更新本地集合。
- 与标准 PushConsumer 一样，LitePush 是客户端发起的长轮询，而不是 Broker 网络 Push。handler 返回 `Success` 会确认，
  返回 `Retry` 会请求重新投递，返回 `DeadLetter` 会转发到该 group 的死信队列。本示例会将损坏消息转为死信。
- `BindTopic`、Broker `message.type=LITE` 属性与 group `lite.bind.topic` 属性必须指向同一个 parent topic。拼写不一致
  是配置错误，而不是订阅第二个 parent topic 的方式。
- handler 注册为 `Scoped`，每次处理尝试都会获得自己的异步 DI scope。

Lite 消息生产和完整 API 请参阅 [gRPC 指南](../../../src/EventHorizon.RocketMQ.Grpc/README.zh-CN.md)。
