# Remoting Pull Consumer

[English](README.md) | [简体中文](README.zh-CN.md)

`IRemotingPullConsumer` 是 classic Remoting 中控制层级最低的消费模式。它直接暴露 Broker 物理队列和队列位点：
应用选择读取哪个队列、从哪个位点发起请求、何时算处理完成，以及提交哪个位点。SDK 负责路由发现、协议请求、消息解码和
Broker 位点操作，但不会在应用实例之间分配队列，也不会把消息分发给 handler。

## 什么时候使用 Pull Consumer

当应用需要精确控制队列和位置时，适合使用 Pull Consumer，例如：

- 从开头、末尾或某个时间点开始的重放、回填、检查或迁移任务；
- 已有外部调度器负责队列分配；
- 消息处理需要把 RocketMQ 位点与其他 checkpoint 或事务协调起来；
- 轮询顺序、节奏和每队列并发度都由应用决定。

如果希望由调用方发起 poll，但让 SDK 参与集群队列分配，应选择 Lite Pull。如果需要自动分配、后台长轮询、handler
分发和 Broker 重试，应选择 Push Consumer。如果工作通过逐消息 receipt 和不可见时间结算，而不是通过队列位点结算，
应选择 POP。

如果没有外部队列归属协议，Pull Consumer 不适合把同一个 group 的多个相同实例直接横向扩展。这些实例可能发现相同的
物理队列，并互相覆盖同一个 group 的位点。

## SDK 工作流

通过 `AddRemotingPullConsumer` 注册此角色。`ConsumerOptions.Subscribe` 将 topic 与 tag 或 SQL92 过滤条件关联；
使用 SQL92 过滤还要求目标 Broker 允许属性过滤。

标准读取流程是显式的：

1. `GetMessageQueuesAsync` 返回已订阅 topic 的可读 `RemotingPullMessageQueue`。
2. `GetOffsetAsync` 读取某个队列的 group 已提交位点；不存在时返回 `-1`。
3. `QueryOffsetAsync` 通过 `Beginning`、`End` 或 `Timestamp` 解析起始位点。
4. `PullAsync` 从一个队列的一个位点开始请求，并返回 `RemotingPullResult`，其中包含 `Status`、`Messages`、
   `NextOffset` 和 Broker 当前的位点边界。
5. 处理成功后，以 `UpdateOffsetAsync` 为该 group 持久化此队列的下一位点。

`PullAsync` 可以使用 Broker 长轮询，但绝不会自动提交。`NextOffset` 只是下一次请求的位置，并不表示返回的消息已经
处理完成。

## 位点、失败与并发

位点属于 Broker 物理队列。必须为每个队列分别维护位置和完成状态；全局位点没有意义。如果同一队列内并行处理，只能提交
连续完成的最高位点。后面的 batch 即使先完成，也不能让已提交位点越过前面尚未完成的 batch。

应先处理、后提交。在 `UpdateOffsetAsync` 前发生进程故障可能再次读到相同消息；如果在业务处理可靠完成前提交，崩溃后
则可能跳过工作。因此处理逻辑必须能容忍重复；需要更强原子性的应用必须把 RocketMQ checkpoint 与自己的持久化状态
协调起来。

pull 错误退避、业务处理重试、死信策略和队列重新分配也由应用负责。`PullAsync` 或业务操作失败不会自动进入 handler
重试流水线。只有在队列归属和位点更新都按队列隔离时，才适合并行运行多个队列循环。

## 运行示例

可运行示例以 group `remoting-sample-pull-consumer` 消费 `eventhorizon-test-topic` 中的全部消息。该 group
没有已提交位点时，从 `End` 开始。

启动[本地 RocketMQ 环境](../../../test-environments/rocketmq/README.zh-CN.md)，然后运行：

```shell
docker compose -f test-environments/rocketmq/compose.yaml up -d --wait
dotnet run --project samples/remoting/PullConsumer/EventHorizon.RocketMQ.Samples.Remoting.PullConsumer.csproj
```

默认 NameServer 为 `localhost:9876`。使用外部环境时，进程必须同时能访问 NameServer 和路由数据中公布的每个
Broker 地址、创建普通 topic，并为该 group 授予路由查询和消费权限。Consumer 启动后，可用 Remoting Producer
示例发送消息。

完整配置和 API 示例请参阅
[Remoting 指南](../../../src/EventHorizon.RocketMQ.Remoting/README.zh-CN.md)。
