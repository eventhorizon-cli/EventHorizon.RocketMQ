# gRPC PushConsumer 示例

[English](README.md) | [简体中文](README.zh-CN.md)

这是一个 Generic Host 示例。它通过 `AddRocketMQGrpc` 和
`AddGrpcPushConsumer<PushConsumerMessageHandler>` 注册 `IGrpcPushConsumer`。客户端会轮询分配并将消息分发给
由 DI 管理的 scoped handler；它不是由 Broker 发起的网络 Push 订阅。

## 配置

| 配置键 | 默认值 | 用途 |
| --- | --- | --- |
| `RocketMQ:Client:Endpoint` | `localhost:8081` | RocketMQ Proxy gRPC 端点。 |
| `RocketMQ:Client:RequestTimeout` | `00:00:03` | 普通客户端请求的截止时间。 |
| `RocketMQ:Client:UseTLS` | `false` | 为 Proxy 连接启用 TLS。 |
| `RocketMQ:Consumer:GroupName` | `rocketmq-dotnet-grpc-push-sample` | 保存分配、确认、重试和死信状态的 consumer group。 |
| `RocketMQ:Consumer:BatchSize` | `16` | 每次 Receive 请求的最大消息数。 |
| `RocketMQ:Consumer:MaxConcurrency` | `4` | 最大并发 handler 执行数。 |
| `RocketMQ:Consumer:MaxDeliveryAttempts` | `16` | 进入死信前的回退投递次数上限。 |
| `RocketMQ:Consumer:InvisibleDuration` | `00:00:30` | 初始租约时长；处理期间客户端会续租。 |
| `RocketMQ:Consumer:ConsumeTimeout` | `00:15:00` | 非 FIFO handler 在客户端取消其 token 并请求重新投递前允许运行的最长时间。 |
| `RocketMQ:Consumer:LongPollingTimeout` | `00:00:15` | Receive 请求允许服务端等待的最长时间。 |
| `Sample:Filter` | `sample` | 固定标准 topic 的 Tag 过滤表达式。 |

本示例始终订阅写死在代码中的 `eventhorizon-test-topic`。Producer 示例固定使用 `sample` tag，因此发送的消息会匹配该过滤条件。

## 前置条件

- `RocketMQ:Client:Endpoint` 必须指向可访问的 RocketMQ 5 Proxy；gRPC consumer 连接 Proxy，不会直接连接
  NameServer。
- 标准 topic 和 consumer group 必须存在，或者 Broker 必须允许自动创建。本仓库提供的环境会在启动时创建固定 Topic。
  使用禁用自动资源创建的其他 Broker 时，请显式创建这两个资源：

  ```shell
  docker compose -f test-environments/rocketmq/compose.yaml exec broker sh mqadmin updateTopic \
    -n nameserver:9876 -c DefaultCluster -t eventhorizon-test-topic

  docker compose -f test-environments/rocketmq/compose.yaml exec broker sh mqadmin updateSubGroup \
    -n nameserver:9876 -c DefaultCluster -g rocketmq-dotnet-grpc-push-sample
  ```

- 本仓库提供的 RocketMQ 5.5.0 Compose 环境在 `localhost:8081` 支持该标准 PushConsumer 路径。一次性资源初始化服务会在
  启动命令返回前创建固定 Topic；生产部署通常仍应显式准备这两个资源。

在仓库根目录启动本仓库提供的环境：

```shell
docker compose -f test-environments/rocketmq/compose.yaml up -d --wait
```

## 运行

在仓库根目录执行：

```shell
dotnet run --project samples/grpc/PushConsumer
```

Host 会持续运行直到按下 Ctrl+C。向 `eventhorizon-test-topic` 发送 tag 为 `sample` 的标准消息，例如使用
Producer 示例。

## 语义与常见误解

- 虽然名称中有 Push，但它实际是客户端发起的长轮询：客户端查询分配、重复调用 `ReceiveMessage`，然后在本地分发。
  Broker 不会向应用建立 Push 连接。
- `PushConsumerMessageHandler` 注册为 `Scoped`；每次处理尝试会获得新的异步 DI scope。不要在 singleton handler 中
  注入 scoped 服务；如改成 singleton handler，则必须保证线程安全。
- 返回 `ConsumeResult.Success` 会确认消息；返回 `Retry` 会使消息可重新投递；`DeadLetter` 会转发到该 consumer group
  的死信队列。本示例对损坏消息返回 `DeadLetter`，对正常消息记录日志后返回 `Success`。
- `MaxConcurrency` 控制本地 handler 并发数，不控制 Broker 队列数量。即使其他消息可并发，FIFO message group 仍保持
  自身的顺序约束。
- `ConsumeTimeout` 只适用于非 FIFO 分发。到期后会取消 handler token、停止客户端不可见时间续期并请求重新投递；忽略取消的
  handler 无法被强制停止，其延迟返回的成功结果也会被忽略。为保持顺序，FIFO handler 会继续保持关联。
- 使用同一个 group 的多个实例会分摊消息并共享重试状态。要独立观察 topic，请使用新的 group。

handler 生命周期、重试行为和运行时订阅请参阅
[gRPC 指南](../../../src/EventHorizon.RocketMQ.Grpc/README.zh-CN.md)。
