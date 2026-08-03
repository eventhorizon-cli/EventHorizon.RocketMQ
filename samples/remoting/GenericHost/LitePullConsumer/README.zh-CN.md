# Remoting Lite Pull Consumer

[English](README.md) | [简体中文](README.zh-CN.md)

`IRemotingLitePullConsumer` 把调用方驱动的轮询与 SDK 管理的接收循环结合起来。在订阅模式下，SDK 参与 classic
集群 consumer group 分配，在后台长轮询每个已分配队列，并把消息放入有界本地缓冲；应用决定何时调用
`PollAsync`、如何处理返回消息，以及何时提交已投递位置。

这是 classic Remoting Lite Pull 模式，与 RocketMQ 5 gRPC LitePush 协议无关，也不是协议层面的 Broker Push。

## 什么时候使用 Lite Pull

如果应用希望使用 polling API 并控制处理节奏，但不想自行实现集群队列分配，适合选择 Lite Pull。它适用于批处理
流水线、受控的摄取循环，以及需要暂停、恢复或 seek 已分配队列的应用。

如果外部系统负责物理队列分配或需要精确控制位置，应使用手工分配、`Seek` 和显式提交。如果希望 SDK 进一步负责业务
分发、重试和结算，应选择 Push Consumer。Lite Pull 当前只支持集群模式；如果每个实例都必须消费每个队列，应使用
Push Consumer 的广播模式。

订阅模式和手动分配模式互斥。普通 consumer group 负载均衡应使用订阅模式。只有应用明确选择队列，并愿意承担其归属
责任时，才使用手动模式。

## SDK 工作流

通过 `AddRemotingLitePullConsumer` 注册此角色，并以 `ConsumerOptions.Subscribe` 添加 topic 过滤条件。启动时，
SDK 会解析订阅、发送主动消费心跳，并持续维护集群分配。`Assignment` 提供当前 Consumer 所属队列的快照。当一个 group
中的 Consumer 数量多于可读队列数量时，部分 Consumer 没有分配是正常情况。

同一个 `AddRocketMQRemoting` 注册可以添加多个 Lite Pull Consumer，每个 Consumer 都有自己的 options 和生命周期。
该注册会复用 NameServer 与路由状态；同一 group 中的 Consumer 仍作为独立活跃成员参与队列分配。不同 group 可以使用
不同 topic 和 filter。

同一 group 的所有实例必须使用相同的 topic 和 filter 订阅。分配会针对每个 topic 使用完整的 group consumer-id
列表；订阅不一致可能把队列分给没有消费该 topic 的成员，filter 不一致则会让实际接收结果取决于当前队列所有者。

订阅模式的处理循环为：

1. 由 SDK 后台接收器为已分配队列填充有界本地缓冲。
2. 调用 `PollAsync` 并处理返回的 `IReadOnlyList<RemotingMessageView>`。
3. 调用 `CommitAsync()` 持久化所有当前本地位置，或以 `CommitAsync(queue)` 提交一个已分配队列。

运行时调用 `SubscribeAsync` 和 `UnsubscribeAsync` 会修改订阅并触发分配协调。使用手动模式时不要配置订阅，应通过
`GetMessageQueuesAsync` 发现队列，再把选中的集合传给 `AssignAsync`。`Pause`、`Resume`、`Seek`、
`SeekToBeginningAsync`、`SeekToEndAsync` 和 `SeekToTimestampAsync` 用于控制本地轮询位置。

`InitialPosition` 仅在已分配队列没有 group 已提交位置时使用，修改它不会重置已有位点。`PullBatchSize` 控制每个
Broker 后台请求；`MaxCachedMessages` 和 `MaxCachedMessageBytes` 限制共享本地缓冲。缓冲为空时，`PollTimeout`
只控制 `PollAsync` 的本地等待时间，与 Broker 的 `LongPollingTimeout` 相互独立。

## 位置、失败与并发

`PollAsync` 会在返回前推进已投递消息的**本地**位置，但不会更新 Broker 位点。因此业务处理失败时，应用必须保留并
重试这些消息，或者在继续轮询前以 `Seek` 回退受影响队列。仅仅不调用 `CommitAsync`，不会回退当前进程内的位置。

只应在业务处理完成后提交。进程重启或队列重新分配后，另一个 Consumer 会从 Broker 最后提交的位置继续，因此该位置
之后已经处理的工作可能再次投递。业务处理必须幂等。

并行处理需要按队列维护完成规则。同一队列中较早的结果尚未完成时，不能提交当前本地位置。`CommitAsync()` 会提交所有
已分配队列的当前位置，因此只有这些位置之前的工作都已完成时才能调用；否则应协调按队列提交，并避免轮询越过未完成空洞。

本示例关闭 `EnableAutoCommit`，因为应用循环只在记录完返回消息后提交。启用自动提交时，SDK 会按配置间隔持久化已投递
位置，但仍不会提交只完成预取、尚未由 `PollAsync` 返回的消息。两种模式下业务重试都由应用负责，失败不会自动把已返回
batch 路由到死信队列。

## 运行示例

可运行示例以 group `remoting-sample-lite-pull-consumer` 订阅 `eventhorizon-test-topic` 中的全部消息，
新分配队列从 `End` 开始。

启动[本地 RocketMQ 环境](../../../../test-environments/rocketmq/README.zh-CN.md)，然后运行：

```shell
docker compose -f test-environments/rocketmq/compose.yaml up -d --wait
dotnet run --project samples/remoting/GenericHost/LitePullConsumer/EventHorizon.RocketMQ.Samples.Remoting.LitePullConsumer.csproj
```

`appsettings.json` 只保存 NameServer 连接设置，Consumer 角色参数都直接写在 `Program.cs` 的注册旁。默认
NameServer 为 `localhost:9876`。使用外部环境时，必须保证进程可以同时访问 NameServer 及其公布的 Broker
地址、创建普通 topic，并为该 group 授予路由查询和消费权限。请先启动此 Host，再使用 Remoting Producer 示例发送消息。
它会记录分配队列中的每条消息，然后在示例处理完成后提交已推进的本地位置。按下 Ctrl+C 会停止 Host 及其 SDK 角色。

完整配置和 API 示例请参阅
[Remoting 指南](../../../../src/EventHorizon.RocketMQ.Remoting/README.zh-CN.md)。
