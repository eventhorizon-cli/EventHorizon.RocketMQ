# Remoting Push Consumer

[English](README.md) | [简体中文](README.zh-CN.md)

`IRemotingPushConsumer` 是 classic Remoting 的高级消费模式。SDK 维护 consumer group 分配、发起 Broker 长轮询、
缓存收到的消息、分发 handler、结算重试或死信结果，并持久化位点。应用代码提供消息 handler，而不是自行驱动 poll
循环。

虽然名称中带有 Push，但消息并不是由 Broker 直接推送到应用代码。客户端执行长轮询，同时处理重平衡和位点重置等 classic
Broker 回调。

## 什么时候使用 Push Consumer

如果持续运行的服务希望由 SDK 负责队列分配、接收调度、handler 并发、重试和位点推进，适合选择 Push Consumer。只要
业务逻辑可以表达为异步回调，并能容忍至少一次投递，它通常就是默认选择。

如果希望 SDK 分配队列，但由应用控制何时 poll 和提交，应选择 Lite Pull。如果需要精确控制物理队列和每次请求的位点，
应选择 Pull Consumer。如果需要逐消息 receipt 和不可见时间语义，应选择 POP。

Push Consumer 不是“每队列固定一个线程”的执行模型。如果应用要求线程亲和性，或者必须由外部调度器拥有每个队列，应使用
由调用方驱动的 Consumer。

## 注册与 handler 合约

通过 `AddRemotingPushConsumer<TMessageHandler>(ServiceLifetime, Action<RemotingPushConsumerOptions>)`
注册类型化 handler。handler 实现 `IRemotingPushMessageHandler.HandleAsync`，并接收：

- 来自同一个 Broker 物理队列、保持投递顺序的 `IReadOnlyList<RemotingMessageView>`；
- 控制部分确认和重试延迟的 `RemotingPushConsumeContext`；
- 用于停止，以及符合条件的消费超时的 cancellation token。

一个 Remoting 客户端注册可以包含多个分别配置的 Push Consumer：

```csharp
const string Topic = "eventhorizon-test-topic";

builder.Services
    .AddRocketMQRemoting(remotingSection.Bind)
    .AddRemotingPushConsumer<PushConsumerMessageHandler>(ServiceLifetime.Scoped, options =>
    {
        options.GroupName = "remoting-sample-push-consumer";
        options.MaxConcurrency = 4;
        options.BatchSize = 32;
        options.ConsumeMessageBatchSize = 4;
        options.Subscribe(Topic, new FilterExpression("sample"));
    })
    .AddRemotingPushConsumer<AllMessagesMessageHandler>(ServiceLifetime.Scoped, options =>
    {
        options.GroupName = "remoting-all-messages-push-consumer";
        options.MaxConcurrency = 2;
        options.BatchSize = 8;
        options.ConsumeMessageBatchSize = 1;
        options.Subscribe(Topic);
    });
```

每次调用分别拥有自己的 group、订阅、handler、调度、位点状态和 Host 生命周期。同一个
`AddRocketMQRemoting` 注册下的 Consumer 会复用 NameServer 发现、路由状态以及兼容的底层 Broker 连接。连接和凭据仍放在
`appsettings.json`；Consumer 的选择则直接展示在 `Program.cs` 中。

`Singleton` handler 可能被并发调用，因此必须线程安全。`Scoped` 和 `Transient` handler 会在每次 batch 尝试时
从新的异步 DI scope 中解析。自动分发始终使用由应用容器解析的 `IRemotingPushMessageHandler` typed handler。handler
需要数据库上下文等 scoped 应用服务时应选择 `Scoped`，并通过构造函数注入这些依赖。

启动前可通过 `ConsumerOptions.Subscribe` 配置订阅。运行时调用 `SubscribeAsync` 和 `UnsubscribeAsync` 会更新
classic 心跳并协调活跃 receiver。

## 队列分配与并发分发

在默认 `Clustering` 模式下，同一 group 的存活 Consumer 会共同分配 topic 在各 Broker 上的物理队列。该 group
中的一个队列只会分配给一个 Consumer，因此增加 Consumer 只有在数量不超过所有 Broker 上可读队列总数时才能增加有效
并行度；多出的 Consumer 可能保持空闲。不同 group 的 Consumer 会各自独立消费此 topic。

同一 group 的所有集群实例必须使用相同的 topic 和 filter 订阅。分配会针对每个 topic 使用完整的 group
consumer-id 列表；订阅不一致可能把队列分给没有消费该 topic 的成员，filter 不一致则会让实际接收结果取决于当前
队列所有者。注册会在本地验证这个约束。配置兼容的同组注册会获得不同的活跃 Consumer 身份并共同分配队列，而不是形成
各自独立的 fan-out 订阅。

客户端会为每个已分配的 Broker `MessageQueue` 维护一个本地 `ProcessQueue`。`ProcessQueue` 保存已拉取消息及其
消费状态；它不是新建的 Broker 队列，也不会创建服务端资源。这个名称和职责概念沿用了 RocketMQ 官方 Java 客户端。

`BatchSize` 限制一次 Broker pull 请求的消息数。`ConsumeMessageBatchSize` 则独立限制一次 handler 调用收到的
消息数。并发 batch 只包含来自同一个物理队列的消息。带 `MessageGroup` 的消息和所有 `ConsumeOrderly` 投递仍然是
单消息 batch。

就绪的 `ProcessQueue` 共享异步消费循环。每轮从一个就绪队列取一个 batch，仍有工作的队列会放回队尾，从而让不同
Broker 上的活跃队列公平推进。`MaxConcurrency` 限制整个 Consumer 的 handler 分发数，而不是每个队列的分发数。
这些循环是 .NET ThreadPool 任务，不是独占线程，也不会永久绑定某个队列。这个公平调度器是 .NET 的设计，并非逐项复制
Java 的并发分发路径。

`MaxCachedMessages` 限制等待首次分发的新拉取消息准入。在途或被结算阻塞的消息不会继续占用这部分准入容量；被阻塞的
队列也会暂停新准入，直到能够继续推进。因此，该值不是进程内所有驻留消息数量的严格上限。

## 确认、重试与位点

handler 为一个 batch 返回一个 `ConsumeResult`：

- 默认返回 `Success` 会确认整个 batch。
- 对于并发、非 FIFO batch，可把 `context.AckIndex` 设为最后一条成功消息的从零开始索引，再返回 `Success`。
  客户端会接受连续前缀，只把尾部交给 Broker 重试。默认值 `int.MaxValue` 接受全部消息；`-1` 表示一条也不接受。
- `Retry` 和 `DeadLetter` 会忽略 `AckIndex`，并应用到整个 batch。

对于集群并发、非 FIFO batch，`context.DelayLevelWhenNextConsume` 初始为由 `RetryDelay` 映射得到的 Broker
延迟级别。设为 `0` 时由 Broker 选择；设为正的 RocketMQ 延迟级别时请求该级别；设为负值时请求直接进入死信队列。
`MessageGroup` FIFO、`ConsumeOrderly` 和广播路径会忽略此 context 设置。在集群模式下，
`MaxDeliveryAttempts` 限制重试投递次数；重试消息达到上限后会进入死信队列。

每个 `ProcessQueue` 都独立维护连续完成水位。后面的 batch，或者来自其他队列、其他 Broker 的 batch，即使先
完成，也不能跳过前面尚未解决的队列位点。拉取可以先于 handler 完成继续前进。在集群模式下，Broker 已提交位点不能
越过成功结算；广播模式则会把不可用的重试和死信结果视为本地已解决并丢弃，具体见后文。

在集群并发 `ProcessQueue` 路径中，如果重试或死信结算失败，客户端会在 `RetryDelay` 后仅在本地重试结算，
不会再次调用应用 handler；完成水位仍保持阻塞。位点持久化失败也会重试。如果分配被撤销，旧分配上延迟返回的
handler 结果会被忽略，不能推进替代它的新分配。

`ConsumeTimeout` 适用于并发集群、非 FIFO batch。超时后，客户端会取消 handler token，并为整个 batch 请求 Broker
重新投递。忽略取消的代码无法被强制停止；其延迟结果会被忽略，但其外部工作可能与重新投递的调用重叠。handler 必须响应
取消并保持幂等。带 `MessageGroup` 的 FIFO 投递和 `ConsumeOrderly` 投递不会应用此机制，避免超时恢复破坏顺序。

## 顺序与广播

`MessageGroup` 提供生产端队列亲和性和单消息 FIFO 分发。`ConsumeOrderly` 则串行处理每个已分配的物理队列。
在集群模式下，顺序消费会获取并续约 classic Broker 队列锁；`MaxConcurrency` 仍可并发处理不同的已锁队列。重试会
阻塞当前队列，使后面的消息不能超越它。队列锁保持顺序，但不会改变至少一次投递，因此顺序 handler 也必须幂等。

`Broadcasting` 会把每个可读队列分配给每个 Consumer 实例，并在本地存储位点。它没有 group 所属的 Broker 重试或
死信流程：`Retry`、`DeadLetter` 和未确认的 batch 尾部都会被丢弃，同时推进本地位点。当客户端标识不稳定时，应配置
稳定且每实例独立的 `LocalOffsetStorePath`。

对于普通订阅队列，`InitialPosition` 只在当前模式使用的位点存储没有值时提供起点：集群模式读取 Broker group
位点，广播模式读取当前实例的本地位点文件。它不会重置已有位置。重试队列会保留自己的恢复边界，不使用这一普通队列
起始策略。

## 运行示例

可运行示例在 `eventhorizon-test-topic` 上注册两个 scoped 类型化 handler。group
`remoting-sample-push-consumer` 只接收 `sample` tag，`MaxConcurrency` 为 4；group
`remoting-all-messages-push-consumer` 接收全部 tag，`MaxConcurrency` 为 2。因为两个 group 独立消费，一条
`sample` 消息会投递给两个 handler。

启动[本地 RocketMQ 环境](../../../test-environments/rocketmq/README.zh-CN.md)，然后运行：

```shell
docker compose -f test-environments/rocketmq/compose.yaml up -d --wait
dotnet run --project samples/remoting/PushConsumer/EventHorizon.RocketMQ.Samples.Remoting.PushConsumer.csproj
```

默认 NameServer 为 `localhost:9876`。使用外部环境时，必须保证进程能访问 NameServer 和每个公布的 Broker 地址，
创建普通 topic，并授予路由查询和消费权限，包括其消费结果会使用的重试和死信 topic。

可使用 [Remoting Producer 示例](../Producer/README.zh-CN.md)发送匹配的 `sample` 消息。

完整配置和 API 示例请参阅
[Remoting 指南](../../../src/EventHorizon.RocketMQ.Remoting/README.zh-CN.md)。
