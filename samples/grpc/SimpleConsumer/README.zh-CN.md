# gRPC SimpleConsumer 示例

[English](README.md) | [简体中文](README.zh-CN.md)

这是一个 Generic Host 示例。它通过 `AddRocketMQGrpc` 和 `AddGrpcSimpleConsumer` 注册
`IGrpcSimpleConsumer`。Hosted worker 会显式调用 `ReceiveAsync`、处理每条消息，再调用 `AckAsync`；
这些步骤由应用控制，而不是由客户端自动控制。

## 配置

| 配置键 | 默认值 | 用途 |
| --- | --- | --- |
| `RocketMQ:Client:Endpoint` | `localhost:8081` | RocketMQ Proxy gRPC 端点。 |
| `RocketMQ:Client:RequestTimeout` | `00:00:03` | 普通客户端请求的截止时间。 |
| `RocketMQ:Client:UseTLS` | `false` | 为 Proxy 连接启用 TLS。 |
| `RocketMQ:Consumer:GroupName` | `rocketmq-dotnet-grpc-simple-sample` | 保存确认和重试状态的 consumer group。 |
| `RocketMQ:Consumer:AwaitDuration` | `00:00:05` | 服务端 Receive 长轮询时长。 |
| `RocketMQ:Consumer:InvisibleDuration` | `00:00:30` | 收到消息后的初始租约时长。 |
| `Sample:Subscriptions:0:Topic` | `rocketmq-dotnet-manual` | 要接收的标准 topic。 |
| `Sample:Subscriptions:0:Filter` | `sample` | Tag 过滤表达式。 |
| `Sample:BatchSize` | `16` | 每次 Receive 请求的最大消息数量。 |
| `Sample:MaxDeliveryAttempts` | `16` | 转发损坏消息到死信队列时传入的投递次数阈值。 |

Producer 示例的默认 tag 为 `sample`，因此其默认消息会匹配此订阅。

## 前置条件

- `RocketMQ:Client:Endpoint` 必须指向可访问的 RocketMQ 5 Proxy；客户端不会直接连接 NameServer。
- 标准 topic 和 consumer group 必须存在，或者 Broker 必须允许自动创建。禁用此策略时，请显式创建：

  ```shell
  docker compose -f test-environments/rocketmq/compose.yaml exec broker sh mqadmin updateTopic \
    -n nameserver:9876 -c DefaultCluster -t rocketmq-dotnet-manual

  docker compose -f test-environments/rocketmq/compose.yaml exec broker sh mqadmin updateSubGroup \
    -n nameserver:9876 -c DefaultCluster -g rocketmq-dotnet-grpc-simple-sample
  ```

- 本仓库提供的 RocketMQ 5.5.0 Compose 环境在 `localhost:8081` 支持 SimpleConsumer。其 Broker 开启了自动建
  topic 和 subscription group；但生产部署通常应先由管理员创建这两个资源。

在仓库根目录启动本仓库提供的环境：

```shell
docker compose -f test-environments/rocketmq/compose.yaml up -d --wait
```

## 运行

在仓库根目录执行：

```shell
dotnet run --project samples/grpc/SimpleConsumer
```

示例会持续运行直到按下 Ctrl+C。使用 Producer 示例发送消息，或通过其他客户端向
`rocketmq-dotnet-manual` 发布 tag 为 `sample` 的标准消息。

## 语义与常见误解

- `ReceiveAsync` 只租约消息，不会确认消息。示例只在成功记录日志后确认；未确认的消息会在 30 秒的不可见时长
  到期后再次可见。
- Worker 会使用 `MaxDeliveryAttempts` 将损坏消息显式转发到该 group 的死信队列。普通处理失败不会被当成成功；
  确认失败会使消息保留为可重新投递状态。
- 本仓库提供的 RocketMQ 5.5.0 Proxy 的最小长轮询间隔是五秒。使用该服务端时，请把 `AwaitDuration` 保持为五秒或更长。
- consumer group 是共享状态。以相同 group 运行多个实例会分摊消息；要独立重放或查看相同 topic，请使用不同 group。
- 这是应用主动 Receive/确认，不是 Broker Push。需要客户端自动管理长轮询和分发时，请使用 PushConsumer 示例。

运行时订阅变更和完整 API 请参阅 [gRPC 指南](../../../src/EventHorizon.RocketMQ.Grpc/README.zh-CN.md)。
