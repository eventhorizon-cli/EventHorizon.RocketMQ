# Remoting Push Consumer

[English](README.md) | [简体中文](README.zh-CN.md)

`IRemotingPushConsumer` 是 classic Remoting 的高级消费模式。SDK 维护 consumer group 分配、发起 Broker 长轮询、
缓存收到的消息、分发 handler，并结算重试或死信结果。应用代码提供消息 handler，而不是自行驱动 poll
循环。

虽然名称中带有 Push，但消息并不是由 Broker 直接推送到应用代码。客户端执行长轮询，同时处理重平衡和位点重置等 classic
Broker 回调。

## 什么时候使用 Push Consumer

如果持续运行的服务希望由 SDK 负责队列分配、接收调度、handler 并发、重试和位点推进，适合选择 Push Consumer。只要
业务逻辑可以表达为异步回调，并能容忍至少一次投递，它通常就是默认选择。

如果需要由应用控制 poll、手工物理队列分配、seek 和提交，应选择 Lite Pull。Broker 侧 POP 是 Push 内部的接收
模式。应用必须自行管理逐消息 receipt 和不可见期时，应使用 gRPC `IGrpcSimpleConsumer`。

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
var allMessagesQueueAssignmentMode = builder.Configuration.GetValue(
    "Sample:AllMessagesQueueAssignmentMode",
    RemotingPushQueueAssignmentMode.Client);

builder.Services
    .AddRocketMQRemoting(remotingSection.Bind)
    .AddRemotingPushConsumer<PushConsumerMessageHandler>(ServiceLifetime.Scoped, options =>
    {
        options.GroupName = "remoting-sample-push-consumer";
        options.QueueAssignmentMode = RemotingPushQueueAssignmentMode.Client;
        options.MaxConcurrency = 4;
        options.PullBatchSize = 32;
        options.ConsumeMessageBatchSize = 4;
        options.Subscribe(Topic, new FilterExpression("sample"));
    })
    .AddRemotingPushConsumer<AllMessagesMessageHandler>(ServiceLifetime.Scoped, options =>
    {
        options.GroupName = "remoting-all-messages-push-consumer";
        options.QueueAssignmentMode = allMessagesQueueAssignmentMode;
        options.MaxConcurrency = 2;
        options.PullBatchSize = 8;
        options.ConsumeMessageBatchSize = 1;
        options.Subscribe(Topic);
    });
```

每次调用分别拥有自己的 group、订阅、handler、调度、位点状态和 Host 生命周期。同一个
`AddRocketMQRemoting` 注册下的 Consumer 会复用 NameServer 发现、路由状态以及兼容的底层 Broker 连接。连接和凭据，以及
可选的 all-messages assignment mode 开关由 `appsettings.json` 读取；两个 Consumer 注册及其余角色选择仍直接展示在
`Program.cs` 中。

该可执行项目运行在 Generic Host 中。`RunAsync` 会调用 `AddRemotingPushConsumer` 注册的 hosted service，由它启动和停止
Consumer 角色。此应用中不要调用 `IRemotingPushConsumer.StartAsync` 或 `StopAsync`。未运行 Generic Host 的进程应改用完整的
[非 Host 示例](../../NonHost/PushConsumer/README.zh-CN.md)。

`Singleton` handler 可能被并发调用，因此必须线程安全。`Scoped` 和 `Transient` handler 会在每次 batch 尝试时
从新的异步 DI scope 中解析。自动分发始终使用由应用容器解析的 `IRemotingPushMessageHandler` typed handler。handler
需要数据库上下文等 scoped 应用服务时应选择 `Scoped`，并通过构造函数注入这些依赖。

启动前可通过 `ConsumerOptions.Subscribe` 配置订阅。运行时调用 `SubscribeAsync` 和 `UnsubscribeAsync` 会更新
classic 心跳并协调活跃 receiver。

## 分配、接收与并发分发

`QueueAssignmentMode.Client` 是默认值，执行经典客户端平均分配并创建 PULL receiver。在 `Clustering` 模式下，同一
group 的存活 Consumer 共同分配 topic 的物理队列，不同 group 则独立消费。`Broadcasting` 和
`ConsumeOrderly` 也使用客户端分配。

`QueueAssignmentMode.Broker` 为集群并发消费发送 `QUERY_ASSIGNMENT`，并根据 Broker 返回的每条 assignment 创建 PULL
或 POP receiver。即使请求 `Broker`，广播与 `ConsumeOrderly` 也会强制使用有效的客户端分配和 PULL。运行时
Consumer 从不发送 `SET_MESSAGE_REQUEST_MODE`；运维人员在 Broker 上为 topic/group 配置 PULL 或 POP。满足条件的
查询失败或 Broker 不支持时，本轮协调会失败，不会回退到客户端分配，以免产生重叠归属。
因此 Broker assignment 不等于 POP；官方
[5.5.0 Broker processor](https://github.com/apache/rocketmq/blob/rocketmq-all-5.5.0/broker/src/main/java/org/apache/rocketmq/broker/processor/QueryAssignmentProcessor.java#L92-L132)
会把选定的 request mode 放到每条返回的 assignment 中。
所有集群模式还会声明并对账 `%RETRY%{consumerGroup}`。Broker assignment 会强制该 retry topic 使用 PULL，确保
PULL send-back 的消息仍可消费。

同一 group 的所有集群实例必须使用相同的 topic 和 filter 订阅。分配会针对每个 topic 使用完整的 group
consumer-id 列表；订阅不一致可能把队列分给没有消费该 topic 的成员，filter 不一致则会让实际接收结果取决于当前
队列所有者。注册会在本地验证这个约束。配置兼容的同组注册会获得不同的活跃 Consumer 身份并共同分配队列，而不是形成
各自独立的 fan-out 订阅。

客户端会为每个 PULL assignment 维护一个本地 `PullProcessQueue`。`PullProcessQueue` 保存已拉取消息及其
消费状态；它不是新建的 Broker 队列，也不会创建服务端资源。这个名称和职责概念沿用了 RocketMQ 官方 Java 客户端。

`PullBatchSize` 限制一次 Broker PULL 请求的消息数；`PullMaxCachedMessages` 和
`PullMaxCachedMessageBytes` 限制 PULL 准入。`ConsumeMessageBatchSize` 则独立限制一次 handler 调用收到的
消息数。并发 PULL batch 只包含来自同一个物理队列的消息。带 `MessageGroup` 的消息和所有 `ConsumeOrderly`
投递仍然是单消息 batch。

就绪的 `PullProcessQueue` 共享异步消费循环。每轮从一个就绪队列取一个 batch，仍有工作的队列会放回队尾，从而让不同
Broker 上的活跃队列公平推进。`MaxConcurrency` 限制整个 Consumer 的 handler 分发数，而不是每个队列的分发数。
这些循环是 .NET ThreadPool 任务，不是独占线程，也不会永久绑定某个队列。这个公平调度器是 .NET 的设计，并非逐项复制
Java 的并发分发路径。

`PullMaxCachedMessages` 限制等待首次分发的新拉取消息准入。在途或被结算阻塞的消息不会继续占用这部分准入容量；
被阻塞的 PULL 队列也会暂停新准入，直到能够继续推进。因此，该值不是进程内所有驻留消息数量的严格上限。

Broker-assigned POP receiver 使用独立的 receipt 状态，而不是 `PullProcessQueue` 位点。`PopBatchSize` 控制每次 POP
请求，`PopInvisibleDuration` 设置固定的不可见时间，`PopMaxInflightMessagesPerAssignment` 限制一条 assignment 上等待
结算的消息数。handler 执行期间客户端不会续租；超过 Broker 返回的 deadline 后，延迟结果会被忽略。

## 确认、重试与位点

handler 为一个 batch 返回一个 `ConsumeResult`：

- 默认返回 `Success` 会结算整个 batch：PULL 推进连续位点水位，POP 则确认 Broker receipt。
- 对于并发、非 FIFO batch，可把 `context.AckIndex` 设为最后一条成功消息的从零开始索引，再返回 `Success`。
  客户端会结算连续前缀，只把尾部交给 Broker 重试。默认值 `int.MaxValue` 接受全部消息；`-1` 表示一条也不接受。
- `Retry` 和 `DeadLetter` 会忽略 `AckIndex`，并应用到整个 batch。

对于集群并发、非 FIFO batch，`context.DelayLevelWhenNextConsume` 默认为 `0`，由 Broker 选择重试间隔；设为正的
RocketMQ 延迟级别时请求该级别，设为负值时请求直接进入死信队列。PULL 重试使用经典 send-back；POP 重试按
classic POP 重试表将级别换算为不可见时间，并修改 receipt 不可见期。POP 死信处理则先转发、再确认 receipt。
`MessageGroup` FIFO、`ConsumeOrderly` 和广播路径会忽略此 context 设置。在集群模式下，
`MaxDeliveryAttempts` 会启动终态重试处理：PULL 把重试消息送入死信；POP 则遵循官方 Java 的消息年龄策略，继续按年龄
选择 POP 延迟并修改不可见时间，只有消息年龄严格超过最后一级 POP 延迟的两倍时才 ACK，不会隐式转入死信。

每个 PULL `PullProcessQueue` 都独立维护连续完成水位。后面的 batch，或者来自其他队列、其他 Broker 的 batch，即使先
完成，也不能跳过前面尚未解决的队列位点。拉取可以先于 handler 完成继续前进。在集群模式下，Broker 已提交位点不能
越过成功结算；广播模式则会把不可用的重试和死信结果视为本地已解决并丢弃，具体见后文。

在集群并发 `PullProcessQueue` 路径中，如果重试或死信结算失败，客户端会在 `RetryDelay` 后仅在本地重试结算，
不会再次调用应用 handler；完成水位仍保持阻塞。位点持久化失败也会重试。如果分配被撤销，旧分配上延迟返回的
handler 结果会被忽略，不能推进替代它的新分配。

POP 结算只在 Broker 返回的固定不可见 deadline 之前有效。超过 deadline 后，延迟返回的 handler 结果会被忽略，由
Broker 重新投递。assignment 被移除或切换模式时，旧 generation 上延迟到达的结算同样失效。

`ConsumeTimeout` 适用于并发集群、非 FIFO batch。超时后，客户端会取消 handler token，并为整个 batch 请求 Broker
重新投递。忽略取消的代码无法被强制停止；其延迟结果会被忽略，但其外部工作可能与重新投递的调用重叠。handler 必须响应
取消并保持幂等。带 `MessageGroup` 的 FIFO 投递和 `ConsumeOrderly` 投递不会应用此机制，避免超时恢复破坏顺序。

## 顺序与广播

这些模式始终使用有效的客户端分配和 PULL；配置的 `QueueAssignmentMode.Broker` 请求不会用于 assignment 选择。
`MessageGroup` 提供生产端队列亲和性和单消息 FIFO 分发。`ConsumeOrderly` 则串行处理每个已分配的物理队列。
在集群模式下，顺序消费会获取并续约 classic Broker 队列锁；`MaxConcurrency` 仍可并发处理不同的已锁队列。重试会
阻塞当前队列，使后面的消息不能超越它。队列锁保持顺序，但不会改变至少一次投递，因此顺序 handler 也必须幂等。

`Broadcasting` 会把每个可读队列分配给每个 Consumer 实例，并在本地存储位点。它没有 group 所属的 Broker 重试或
死信流程：`Retry`、`DeadLetter` 和未确认的 batch 尾部都会被丢弃，同时推进本地位点。当客户端标识不稳定时，应配置
稳定且每实例独立的 `LocalOffsetStorePath`。

对于普通 PULL 队列，`InitialPosition` 只在当前模式使用的位点存储没有值时提供起点：集群模式读取 Broker group
位点，广播模式读取当前实例的本地位点文件。它不会重置已有位置。重试队列会保留自己的恢复边界，不使用这一普通队列
起始策略。

对于 Broker-assigned POP，`Beginning` 映射为 Broker init mode MIN；`End` 和 `Timestamp` 映射为 MAX，与
classic Java 客户端一致。POP 不提供有序消费或跨成员 `MessageGroup` FIFO 保证。

本示例将 `InitialPosition` 显式设为新 group 的默认值 `End`。`StartAsync` 并不会把初始位点查询变成部署切换边界：
消息若在新分配接收器解析首个位点前写入，可能会被当作已有积压而跳过。需要有意回放时应选择 `Beginning` 或
`Timestamp`；切换必须包含每条消息时，应使用已提交的 group 位点或应用就绪握手。

## 运行示例

可运行示例在 `eventhorizon-test-topic` 上注册两个 scoped 类型化 handler。group
`remoting-sample-push-consumer` 固定使用客户端分配并只接收 `sample` tag；group
`remoting-all-messages-push-consumer` 接收全部 tag，其 assignment mode 来自示例配置。默认值为 `Client`，所以普通
工作流不要求服务端 assignment 或 POP 配置。只有目标 Broker 已配置服务端 assignment 时，才设置
`Sample__AllMessagesQueueAssignmentMode=Broker`；该 Broker 可为每个 topic/group assignment 返回 PULL 或 POP。因为两个
group 独立消费，一条 `sample` 消息会投递给两个 handler。

启动[本地 RocketMQ 环境](../../../../test-environments/rocketmq/README.zh-CN.md)，然后运行：

```shell
docker compose -f test-environments/rocketmq/compose.yaml up -d --wait
dotnet run --project samples/remoting/GenericHost/PushConsumer/EventHorizon.RocketMQ.Samples.Remoting.PushConsumer.csproj
```

默认 NameServer 为 `localhost:9876`。使用外部环境时，必须保证进程能访问 NameServer 和每个公布的 Broker 地址，
创建普通 topic，并授予路由查询和消费权限，包括其消费结果会使用的重试和死信 topic。
Broker 分配还要求支持 `QUERY_ASSIGNMENT`。要验证 POP，运维人员必须另行把 Broker-assigned group 的请求模式配置为
POP；这不是客户端 option。

请先启动此 Host，再使用 [Remoting Producer 示例](../Producer/README.zh-CN.md)发送匹配的 `sample` 消息。两个类型化
handler 都会记录各自独立收到的副本并返回 `Success`，因此 SDK 会结算其 batch 并推进源队列位点。按下 Ctrl+C 会停止
Host 及其 Consumer 角色。

完整配置和 API 示例请参阅
[Remoting 指南](../../../../src/EventHorizon.RocketMQ.Remoting/README.zh-CN.md)。
