# Remoting Push Consumer 示例

[English](README.md) | [简体中文](README.zh-CN.md)

此 Generic Host 通过 `AddRemotingPushConsumer<PushConsumerMessageHandler>` 注册具有 scoped 生命周期、支持批量回调的
`PushConsumerMessageHandler`，并通过 `IRemotingPushConsumer` 处理消息。这是 classic Remoting 的高级 Consumer，提供
自动分配、长轮询、批量分发、重试和位点处理。

## 公共角色和默认配置

| 键 | 默认值 | 用途 |
| --- | --- | --- |
| `RocketMQ:Remoting:NamesrvAddr` | `localhost:9876` | 用于发现 Broker 路由的 NameServer。 |
| `RocketMQ:PushConsumer:GroupName` | `remoting-sample-push-consumer` | 集群 consumer group。 |
| `RocketMQ:PushConsumer:MaxConcurrency` | `4` | 可并发运行的最大 handler 批量调用数。 |
| `RocketMQ:PushConsumer:BatchSize` | `32` | 每个长轮询从 Broker 请求的最大消息数。 |
| `RocketMQ:PushConsumer:ConsumeMessageBatchSize` | `1` | 单次 handler 调用收到的最大消息数；与 `BatchSize` 相互独立。 |
| `RocketMQ:PushConsumer:LongPollingTimeout` | `00:00:05` | 空长轮询在 Broker 上的最大等待时间。 |
| `RocketMQ:PushConsumer:RetryDelay` | `00:00:01` | 接收失败或 handler 返回失败结果后的重试等待时间。 |
| `RocketMQ:PushConsumer:ConsumeTimeout` | `00:15:00` | 并发集群、非 FIFO handler 批次在请求取消并要求 Broker 重投前允许占用的最长时间。 |
| 固定 topic | `eventhorizon-test-topic` | 共享订阅的普通 topic。 |
| `Sample:Filter` | `*` | 订阅过滤条件。 |

handler 接收 `IReadOnlyList<RemotingMessageView>`，记录列表中的每条消息，并为整个列表返回一个
`ConsumeResult`。示例保留 `ConsumeMessageBatchSize` 的默认值 `1`；可通过配置增大它，以合并来自同一个
Broker 物理队列、且不带 `MessageGroup` 的并发消息。泛型注册会在每次批量处理尝试中创建 scoped DI scope，
因此 handler 可以注入 `ILogger<PushConsumerMessageHandler>`。

## Broker 和网络前置条件

- 使用 classic Remoting NameServer 和 Broker。客户端通过 `NamesrvAddr` 发现路由，随后向公布的 Broker 建立长轮询
  连接，因此两个连接环节都必须可达。
- 使用外部 Broker 时，创建普通 topic `eventhorizon-test-topic`，并允许 group
  `remoting-sample-push-consumer` 消费。启用 ACL 的 Broker 需要路由查询和消费权限；使用重试和死信路径时，也需要
  对应 topic 的权限。
- 本仓库提供的[本地 RocketMQ 环境](../../../test-environments/rocketmq/README.zh-CN.md)适合在宿主机运行此普通 topic Remoting
  Consumer。它会在启动时自动创建共享 topic：

```shell
docker compose -f test-environments/rocketmq/compose.yaml up -d --wait
```

此环境会公布宿主机可访问的 Broker 地址。位于其他容器中的进程必须使用网络可访问的 NameServer 和 Broker 地址，
不能继续使用 `localhost:9876`。

## 运行

```shell
dotnet run --project samples/remoting/PushConsumer/EventHorizon.RocketMQ.Samples.Remoting.PushConsumer.csproj
```

应用会随 Host 启动客户端角色，并持续运行直到 Ctrl+C。启动后可使用 Remoting Producer 示例或其他 Producer 发送消息。

## 重要行为

- 虽然名称中带有 Push，但它不是 Broker 向应用进行协议级推送。客户端负责队列分配，并反复发起长轮询拉取请求，
  然后将收到的消息分发给 handler。
- 一个 handler 结果会应用于其列表中的每条消息。短暂失败时应返回 retry 结果；Broker 的重试和死信行为以 consumer
  group 为基础，因此 handler 必须幂等并能容忍重复投递。
- 带 `MessageGroup` 的 FIFO 消息和 `ConsumeOrderly` 分发始终收到只包含一条消息的列表，即使
  `ConsumeMessageBatchSize` 大于 `1` 也是如此。
- 对于并发集群、非 FIFO 投递，`ConsumeTimeout` 到期后会取消 handler token 并请求 Broker 重新投递。它不能强制
  停止忽略取消信号的应用代码；其延迟返回的结果会被忽略，并可能与重新投递的调用重叠，因此 handler 必须响应该 token，
  并保持幂等。FIFO 和顺序投递为保持顺序保证而不会应用此机制。
- 默认是集群模式。同一 group 中的实例共享队列分配。广播模式会让每个实例获得每个可读队列，并改为本地存储位点。
- 只有 `ConsumeOrderly` 为 `true` **且** `ConsumerMode` 为 `Clustering` 时才使用 Broker 队列锁。普通并发消费不会
  获取这些锁；广播模式下的顺序处理也仅限本地。
