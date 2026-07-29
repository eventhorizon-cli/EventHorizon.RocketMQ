# Remoting Lite Pull Consumer

[English](README.md) | [简体中文](README.zh-CN.md)

`IRemotingLitePullConsumer` 把调用方驱动的轮询与客户端管理的队列分配结合起来。在订阅模式下，SDK 参与 classic
集群 consumer group 分配，并长轮询分配给当前 Consumer 的队列；应用仍然决定何时调用 `PollAsync`、如何处理每次
结果，以及何时提交。

这是 classic Remoting Lite Pull 模式，与 RocketMQ 5 gRPC LitePush 协议无关，也不是协议层面的 Broker Push。

## 什么时候使用 Lite Pull

如果应用希望使用 polling API 并控制处理节奏，但不想自行实现集群队列分配，适合选择 Lite Pull。它适用于批处理
流水线、受控的摄取循环，以及需要暂停、恢复或 seek 已分配队列的应用。

如果外部系统负责物理队列分配或需要精确控制每次请求的位点，应选择更底层的 Pull Consumer。如果希望 SDK 进一步负责
接收循环、并发分发和重试结算，应选择 Push Consumer。Lite Pull 当前只支持集群模式；如果每个实例都必须消费每个队列，
应使用 Push Consumer 的广播模式。

订阅模式和手动分配模式互斥。普通 consumer group 负载均衡应使用订阅模式。只有应用明确选择队列，并愿意承担其归属
责任时，才使用手动模式。

## SDK 工作流

通过 `AddRemotingLitePullConsumer` 注册此角色，并以 `ConsumerOptions.Subscribe` 添加 topic 过滤条件。启动时，
SDK 会解析订阅、发送主动消费心跳，并持续维护集群分配。`Assignment` 提供当前 Consumer 所属队列的快照。当一个 group
中的 Consumer 数量多于可读队列数量时，部分 Consumer 没有分配是正常情况。

订阅模式的处理循环为：

1. 调用 `PollAsync`，长轮询下一个处于活跃状态的已分配队列。
2. 处理 `RemotingPullResult.Messages`。
3. 调用 `CommitAsync()` 持久化所有当前本地位置，或以 `CommitAsync(queue)` 提交一个已分配队列。

运行时调用 `SubscribeAsync` 和 `UnsubscribeAsync` 会修改订阅并触发分配协调。使用手动模式时不要配置订阅，应通过
`GetMessageQueuesAsync` 发现队列，再把选中的集合传给 `AssignAsync`。`Pause`、`Resume`、`Seek`、
`SeekToBeginningAsync` 和 `SeekToEndAsync` 用于控制本地轮询位置。

`InitialOffset` 仅在已分配队列没有 group 已提交位置时使用。修改它不会重置已有的已提交位点。

## 位置、失败与并发

`PollAsync` 会在返回前把所选队列的**本地**位置推进到 `NextOffset`，但不会更新 Broker 位点。因此业务处理失败时，
应用必须保留并重试此次结果，或者在继续轮询前以 `Seek` 回退该队列。仅仅不调用 `CommitAsync`，不会回退当前进程内的
位置。

只应在业务处理完成后提交。进程重启或队列重新分配后，另一个 Consumer 会从 Broker 最后提交的位置继续，因此该位置
之后已经处理的工作可能再次投递。业务处理必须幂等。

并行处理需要按队列维护完成规则。同一队列中较早的结果尚未完成时，不能提交当前本地位置。`CommitAsync()` 会提交所有
已分配队列的当前位置，因此只有这些位置之前的工作都已完成时才能调用；否则应协调按队列提交，并避免轮询越过未完成空洞。

SDK 负责分配和长轮询，但业务重试以及是否提交仍由应用决定。轮询或分配失败不会自动重试已经返回的业务 batch，也不会
自动把它路由到死信队列。

## 运行示例

可运行示例以 group `remoting-sample-lite-pull-consumer` 订阅 `eventhorizon-test-topic` 中的全部消息，
新分配队列从 `End` 开始。

启动[本地 RocketMQ 环境](../../../test-environments/rocketmq/README.zh-CN.md)，然后运行：

```shell
docker compose -f test-environments/rocketmq/compose.yaml up -d --wait
dotnet run --project samples/remoting/LitePullConsumer/EventHorizon.RocketMQ.Samples.Remoting.LitePullConsumer.csproj
```

默认 NameServer 为 `localhost:9876`。使用外部环境时，必须保证进程可以同时访问 NameServer 及其公布的 Broker
地址、创建普通 topic，并为该 group 授予路由查询和消费权限。可使用 Remoting Producer 示例发送消息。

完整配置和 API 示例请参阅
[Remoting 指南](../../../src/EventHorizon.RocketMQ.Remoting/README.zh-CN.md)。
