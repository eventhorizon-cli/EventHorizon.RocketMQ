# Remoting Pull Consumer 示例

[English](README.md)

此 Generic Host 使用 `IRemotingPullConsumer` 显式进行队列发现、位点读取、长轮询和 Broker 位点更新。当应用而非
后台分发器需要选择队列和位点时，应以此为起点。

## 公共角色和默认配置

| 键 | 默认值 | 用途 |
| --- | --- | --- |
| `RocketMQ:Remoting:NamesrvAddr` | `localhost:9876` | 用于发现 Broker 路由的 NameServer。 |
| `RocketMQ:PullConsumer:GroupName` | `remoting-sample-pull-consumer` | 示例读取和写入其位点的 consumer group。 |
| `RocketMQ:PullConsumer:BatchSize` | `32` | 每次 pull 请求的最大消息数。 |
| `RocketMQ:PullConsumer:LongPollingTimeout` | `00:00:05` | 空 pull 请求在 Broker 上的最大等待时间。 |
| `Sample:Subscriptions[0]:Topic` | `rocketmq-dotnet-manual` | 示例读取的唯一 topic。 |
| `Sample:Subscriptions[0]:Filter` | `*` | 该 topic 的订阅过滤条件。 |
| `Sample:InitialOffset` | `End` | 仅在 group 没有已提交位点时使用的位置。 |
| `Sample:InitialTimestamp` | 未设置 | 当 `InitialOffset` 为 `Timestamp` 时必填。 |

示例会验证 `Sample:Subscriptions` 恰好有一项。它发现所有可读物理队列，对每一个队列以显式位点调用
`PullAsync`，并以每次响应返回的下一个位点调用 `UpdateOffsetAsync`。

## Broker 和网络前置条件

- 使用经典 Remoting NameServer 和 Broker。`NamesrvAddr` 不是 Proxy 端点：客户端从 NameServer 获取路由，进程
  还必须能够访问每个公布的 Broker 地址。
- 创建普通 topic `rocketmq-dotnet-manual`，并允许 group `remoting-sample-pull-consumer` 消费，或同时修改这两项
  设置。启用 ACL 的 Broker 还需要授予此 group 路由查询和消费权限。
- 随附的[本地 RocketMQ 环境](../../../test-environments/rocketmq/README.md)适合在宿主机上运行此普通 topic 示例。
  在仓库根目录准备默认资源：

```shell
docker compose -f test-environments/rocketmq/compose.yaml up -d --wait
docker compose -f test-environments/rocketmq/compose.yaml exec broker sh mqadmin updateTopic \
  -n nameserver:9876 -c DefaultCluster -t rocketmq-dotnet-manual
docker compose -f test-environments/rocketmq/compose.yaml exec broker sh mqadmin updateSubGroup \
  -n nameserver:9876 -c DefaultCluster -g remoting-sample-pull-consumer
```

本地栈会公布宿主机可访问的 Broker。容器化应用应配置容器可访问的 NameServer 和 Broker 公布地址，而不是保留
`localhost:9876`。

## 运行

```shell
dotnet run --project samples/remoting/PullConsumer/EventHorizon.RocketMQ.Samples.Remoting.PullConsumer.csproj
```

该 Host 会持续运行，直到按下 Ctrl+C。启动后可使用 Remoting Producer 示例或其他 Producer 发送消息。

## 重要行为

- `InitialOffset` 不是重置开关。示例总会优先使用该 group 已提交的位点；只有 group 在该队列上没有提交位点时，
  才使用 `End`、`Beginning` 或 `Timestamp`。重放消息时应使用新 group 或显式重置位点。
- 新 group 默认使用 `End`，因此首次启动会跳过队列中已存在的消息。不符合预期时，请显式选择 `Beginning` 或
  `Timestamp`。
- 这是手动队列和位点控制，并非自动 consumer group 分配。除非应用自行协调队列归属和位点写入，否则不要以同一
  group 扩展多个实例。
- 示例在记录收到的 batch 后提交位点。替换日志为业务处理时，只应在处理已完成或可安全重试后提交。
