# Remoting POP Consumer 示例

[English](README.md) | [简体中文](README.zh-CN.md)

此 Generic Host 使用 `IRemotingPopConsumer` 显式选择物理队列，并以 receipt 为凭据接收 POP 消息。它会轮流处理
已发现队列中的返回消息，并确认 Broker 返回的 receipt。

## 公共角色和默认配置

| 键 | 默认值 | 用途 |
| --- | --- | --- |
| `RocketMQ:Remoting:NamesrvAddr` | `localhost:9876` | 用于发现 Broker 路由的 NameServer。 |
| `RocketMQ:PopConsumer:GroupName` | `remoting-sample-pop-consumer` | POP 和 receipt 操作中携带的 consumer group。 |
| `RocketMQ:PopConsumer:BatchSize` | `32` | 每次 POP 请求的最大消息数；经典 Broker 最多接受 32 条。 |
| `RocketMQ:PopConsumer:InvisibleDuration` | `00:00:30` | 返回消息对其他 POP consumer 保持不可见的默认时长。 |
| `RocketMQ:PopConsumer:LongPollingTimeout` | `00:00:05` | 空 POP 请求在 Broker 上的最大等待时间。 |
| `Sample:Subscriptions[0]:Topic` | `rocketmq-dotnet-manual` | 示例轮转读取其队列的普通 topic。 |
| `Sample:Subscriptions[0]:Filter` | `*` | 传给 POP 操作的默认过滤条件。 |
| `Sample:RetryDelay` | `00:00:01` | 路由或 POP 失败后的重试等待时间。 |

公共 API 提供 `GetMessageQueuesAsync`、`PopAsync`、`AcknowledgeAsync` 和
`ChangeInvisibleTimeAsync`。订阅过滤条件只是 `PopAsync` 的默认值；它不会创建后台分配或接收循环。

## Broker 和网络前置条件

- 使用 RocketMQ 5 的经典 Broker，并确保其支持 `POP_MESSAGE`、确认和可见期变更命令。仅有 NameServer 并不足够，配置的
  `NamesrvAddr` 也不是 Proxy 端点。
- 进程必须能访问 NameServer 和路由发现返回的 Broker 地址。
- 创建普通 topic `rocketmq-dotnet-manual`，并允许 group `remoting-sample-pop-consumer`，或同时修改这两项设置。
  启用 ACL 时，为此 group 授予路由查询、POP、确认和可见期变更权限。
- 本仓库提供的[本地 RocketMQ 环境](../../../test-environments/rocketmq/README.zh-CN.md)运行 RocketMQ 5.5.0，适合在宿主机运行
  此普通 topic POP 示例。在仓库根目录准备默认资源：

```shell
docker compose -f test-environments/rocketmq/compose.yaml up -d --wait
docker compose -f test-environments/rocketmq/compose.yaml exec broker sh mqadmin updateTopic \
  -n nameserver:9876 -c DefaultCluster -t rocketmq-dotnet-manual
docker compose -f test-environments/rocketmq/compose.yaml exec broker sh mqadmin updateSubGroup \
  -n nameserver:9876 -c DefaultCluster -g remoting-sample-pop-consumer
```

此环境的 Broker 会公布宿主机可访问的地址。在其他容器或网络中运行应用时，请改为配置该环境可访问的 NameServer
和 Broker 公布地址。

## 运行

```shell
dotnet run --project samples/remoting/PopConsumer/EventHorizon.RocketMQ.Samples.Remoting.PopConsumer.csproj
```

应用会持续运行，直到按下 Ctrl+C。使用 Remoting Producer 示例或其他 Producer 向 topic 添加消息。

## 重要行为

- POP 由客户端发起。该示例一次手动选择一个物理队列，不提供后台队列分配、消息分发或自动确认。
- 每条返回消息都带有 receipt。必须在其可见期到期前确认；否则消息会重新具备投递资格。耗时较长的业务必须在到期前
  调用 `ChangeInvisibleTimeAsync`，并确认它新返回的 receipt。
- 示例在记录日志后立即确认，因为日志记录是它的全部处理步骤。应将该位置替换为成功的业务处理；业务逻辑应能容忍
  重复投递。
- POP 不是 PushConsumer 的变体。它直接暴露 receipt 生命周期和物理队列，因此扩展方式和队列所有权仍由应用负责。
