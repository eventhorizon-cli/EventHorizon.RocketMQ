# Remoting Lite Pull Consumer 示例

[English](README.md) | [简体中文](README.zh-CN.md)

此 Generic Host 使用 `IRemotingLitePullConsumer`，实现基于订阅、由客户端管理的队列分配和由调用方发起的
`PollAsync` 操作。这是 classic Remoting Lite Pull API，不是 RocketMQ 5 gRPC LitePush 功能。

## 公共角色和默认配置

| 键 | 默认值 | 用途 |
| --- | --- | --- |
| `RocketMQ:Remoting:NamesrvAddr` | `localhost:9876` | 用于发现 Broker 路由的 NameServer。 |
| `RocketMQ:LitePullConsumer:GroupName` | `remoting-sample-lite-pull-consumer` | 用于分配和提交位点的集群 consumer group。 |
| `RocketMQ:LitePullConsumer:BatchSize` | `32` | 单次 poll 请求的最大消息数。 |
| `RocketMQ:LitePullConsumer:LongPollingTimeout` | `00:00:05` | 空 poll 请求在 Broker 上的最大等待时间。 |
| `RocketMQ:LitePullConsumer:InitialOffset` | `End` | 仅在 group 对新分配的队列没有已提交位点时使用。 |
| 固定 topic | `eventhorizon-test-topic` | 共享订阅的普通 topic。 |
| `Sample:Filter` | `*` | 订阅过滤条件。 |
| `Sample:RetryDelay` | `00:00:01` | 分配或轮询失败后的等待时间。 |

API 支持订阅模式和手动分配模式，但此示例使用订阅。它轮询当前分配给客户端的队列，记录每条返回消息，再调用
`CommitAsync` 持久化本地位点。

## Broker 和网络前置条件

- 使用 classic Remoting NameServer 和 Broker。进程必须能访问配置的 NameServer 以及其路由数据返回的 Broker 地址；
  `NamesrvAddr` 不是 Proxy 端点。
- 使用外部 Broker 时，创建普通 topic `eventhorizon-test-topic`，并允许集群 group
  `remoting-sample-lite-pull-consumer` 消费。如果启用了 ACL，请授予路由查询和消费权限。
- 本仓库提供的[本地 RocketMQ 环境](../../../test-environments/rocketmq/README.zh-CN.md)适合在宿主机运行此经典普通 topic 示例。
  它会在启动时自动创建共享 topic：

```shell
docker compose -f test-environments/rocketmq/compose.yaml up -d --wait
```

此环境的 Broker 会公布宿主机可访问的地址。在不同网络命名空间或容器中运行示例时，请使用可访问的 NameServer 和
Broker 公布地址。

## 运行

```shell
dotnet run --project samples/remoting/LitePullConsumer/EventHorizon.RocketMQ.Samples.Remoting.LitePullConsumer.csproj
```

应用会持续轮询，直到按下 Ctrl+C。应在 Consumer 获得队列分配后再启动 Producer；未分配到队列时，示例会记录调试日志并
重试。

## 重要行为

- 此实现中的 Lite Pull 由客户端驱动，且仅支持集群模式。它在客户端进行 consumer group 协调；它不是服务端 push
  API，也不使用 gRPC LitePush 协议。
- `PollAsync` 只推进**本地**队列位点。必须调用 `CommitAsync` 才会将其持久化到 Broker。示例在记录后提交，是因为
  记录日志就是其全部处理步骤；生产代码只应在业务处理成功后提交。
- `InitialOffset` 仅在 group 对已分配队列没有已提交位点时生效。复用同一个 group 会保留已提交位点；仅修改
  `InitialOffset` 不会重读历史消息。
- 订阅模式和显式 `AssignAsync` 模式互斥。保留固定的示例订阅时，不要再添加手动分配。
