# Classic Remoting Consumer 模型

[English](../../en-US/remoting/consumer-model.md) | [简体中文](consumer-model.md)

## 状态

已接受。本文定义 Remoting Consumer API 修订，以及最终公开面的决策依据。

## 背景

本次修订前，Remoting Project 曾公开四种 Consumer：

| 历史角色 | 历史模型 |
| --- | --- |
| `IRemotingPullConsumer` | 调用方为每次 Broker Pull 选择物理队列和 offset。 |
| `IRemotingLitePullConsumer` | 调用方轮询已分配队列，并显式提交本地 position。 |
| `IRemotingPopConsumer` | 调用方选择物理队列并直接管理 POP receipt。 |
| `IRemotingPushConsumer` | 客户端分配队列、长轮询、分发 handler 并结算消息。 |

这组旧 API 中有两个重复的低层角色。Direct Pull 暴露了 Java 已废弃的 Consumer 模型；direct POP 则把官方
classic 客户端隐藏在 Push 内部的 Broker 命令直接交给应用。应用因而不得不处理队列选择、路由变化、
多成员协调、retry topic、receipt 流控和结算竞态。

审计还发现两个相关的实现问题：

- LitePull 在 `PollAsync` 内直接执行一次 Broker 长轮询。被选中的空队列会阻塞另一个已有消息的分配队列。
  官方 LitePull 会按 assignment 在后台接收，再由 `poll` 读取本地缓存。
- `RemotingPullMessageQueue` 同时保存 endpoint 快照和可变的首选 Broker ID。队列 identity 的生命周期长于
  route 快照，因此路由变化后，活跃 Consumer 仍可能持续使用旧 endpoint。

## 官方基线

本决策以 Apache RocketMQ 主仓库中的 Java 客户端作为 classic Remoting 基线。RocketMQ 5.x 官网的 Java SDK
示例描述的是 `apache/rocketmq-clients` 中另一套 gRPC SDK，不能把其中的 PushConsumer 负载均衡模型直接套用到
classic Remoting 客户端。本文参考的 Remoting API 来自 4.x PushConsumer 指南和 `apache/rocketmq` 5.5.0 的
`client` 模块。

Java classic 已将 `DefaultMQPullConsumer` 标记为废弃，并推荐使用
`DefaultLitePullConsumer` 完成应用驱动的轮询。Java LitePull 为每个 assignment 维护接收任务和本地
consume-request 缓存。

Java classic 没有公开高层手动 POP Consumer。`DefaultMQPushConsumer` 公开 `clientRebalance`，默认值为 `true`。
当它为 `false` 时，满足条件的 Push Consumer 发送 `QUERY_ASSIGNMENT`，Broker 为每条 assignment 分别返回
`PULL` 或 `POP`。POP 可以分配 `queueId=-1`，表示同一 Broker 上该 topic 的全部可读队列，也可以让多个
Consumer 成员共享物理队列。

即使 `clientRebalance=false`，Java `RebalancePushImpl` 也会为广播和顺序 listener 强制使用客户端 rebalance，
因此这些路径仍使用 PULL。对于集群并发消费，`RebalanceImpl` 把每条 Broker assignment 的 mode 当成内部 receiver
选择，分别创建 PullRequest 或 PopRequest；应用 listener 及其消费结果不会改变。

Broker 按 topic 和 consumer group 保存 request mode，默认值是 PULL。POP 通过 Broker 配置或管理操作
`SET_MESSAGE_REQUEST_MODE` 选择；运行时 Push Consumer 只查询该决策，不会写入它。无论有效 assignment 或
receive mode 是什么，Push heartbeat 都继续声明被动消费。

每个集群 Java Push Consumer 还会订阅 classic `%RETRY%{consumerGroup}` topic。该订阅与业务 topic 一样参与 client
或 Broker assignment。即使业务 topic 使用 POP，Broker assignment processor 也会强制 retry topic 使用 PULL，
从而让 PULL receiver 的 send-back 结果仍可在 Broker assignment 下重新投递。

## 决策

Remoting 对外只保留两种应用模型：

| 公开角色 | 职责 |
| --- | --- |
| `IRemotingLitePullConsumer` | 应用驱动轮询；SDK 管理 assignment、接收循环、本地 position，并自动或显式提交。 |
| `IRemotingPushConsumer` | SDK 管理 assignment、接收、分发、重试、确认和关闭。 |

直接移除 `IRemotingPullConsumer`、`IRemotingPopConsumer`、对应注册方法、角色 options、hosted service、示例和
公开的低层操作结果类型，不保留兼容期。LitePull 与 Push 仍需要内部 Pull/POP wire operation，因此协议实现
继续保留。

需要显式物理队列工作流的应用关闭自动提交，并使用 LitePull 手动 assignment、`Seek` 和显式提交。只读 offset
检查继续属于 `IRemotingAdmin`。需要由调用方掌控 receipt 时机的应用使用 gRPC `IGrpcSimpleConsumer`；本次不
新增 Remoting SimpleConsumer 门面。

被删除的 direct Pull 能力分别由以下 API 承接：

| Direct Pull 能力 | 替代入口 |
| --- | --- |
| 发现可读队列 | `IRemotingLitePullConsumer.GetMessageQueuesAsync` |
| 从精确物理 offset 开始 | 手动 `AssignAsync`，随后调用 `Seek` |
| 接收并推进调用方控制的 position | `PollAsync` 与 `Position` |
| 读取或持久化消费组 position | `GetCommittedOffsetAsync` 与 `CommitAsync` |
| 检查 min、max、timestamp 或其他消费组 offset | `IRemotingAdmin` |

Broker Pull 的低层 status 和 min/max response 字段属于内部协议细节。LitePull 在接收循环内处理 offset 校正，
对外只暴露消息、position 和 commit。

### 队列 identity 与路由归属

`RemotingPullMessageQueue` 替换为不可变且按值比较的 `RemotingConsumerQueue`。它是 LitePull 与只读 Admin
offset 操作共用的协议专属可读队列 identity，只包含逻辑 topic、Broker 名称和非负 queue ID。
`IRemotingAdmin.GetMessageQueuesAsync` 发现可读队列并返回同一类型，因此结果可以直接传给 min、max、timestamp
和消费组 offset 查询。Producer 发布队列继续使用独立的 `RemotingMessageQueue` 类型。

这里的 Admin 返回类型是有意的 .NET API 适配，并不表示所有官方 Admin API 都负责发现可读队列。官方 Java
客户端共用 `MessageQueue` identity，但将可写队列发现放在
[`MQProducer`](https://github.com/apache/rocketmq/blob/rocketmq-all-5.5.0/client/src/main/java/org/apache/rocketmq/client/producer/MQProducer.java#L30-L35)，
将订阅可读队列发现放在
[`MQConsumer`](https://github.com/apache/rocketmq/blob/rocketmq-all-5.5.0/client/src/main/java/org/apache/rocketmq/client/consumer/MQConsumer.java#L48-L53)。
本客户端则为 Producer 与 Consumer 使用不同的 CLR 类型，因此只读 Admin offset 流程返回这些 offset 方法直接
接收的 Consumer queue identity。

Broker endpoint 与建议 Broker ID 由内部路由解析器保存。topic 快照仍按 topic 隔离；最后一份有效地址则
按 Broker 名称保存，这与官方 Java 客户端的 Broker 地址表一致。因此，即使暂时无法从 NameServer 查询到 retry topic，
客户端仍可复用从应用 topic 发现的同一 Broker 地址，完成 retry topic 的 offset 初始化。PULL response
返回的建议 Broker ID 仍按 Consumer queue identity 保存，因为副本偏好只属于对应 queue。Broker-assigned POP
使用单独的内部 assignment 类型，因为其逻辑 queue ID 可以为 `-1`。

每次 Pull、offset、send-back、POP、ACK 和不可见时间请求都从最新有效 route 解析 endpoint。NameServer 短暂
故障时可以使用 route cache 中最后一份有效路由，但应用传入的 queue value 永远不是 endpoint 的权威来源。

`RemotingConsumerQueue` 的所有构造路径都要求 queue ID 非负，也不会保存 endpoint 快照或可变的 Broker hint。
因此，无论 queue 来自 Admin、LitePull 发现接口还是应用直接构造，其能力都完全相同。Broker-assigned POP 中的
`queueId=-1` 只存在于内部 assignment，不会被包装成公开 queue。

每个 Consumer 角色独占一个 `RemotingConsumerRouteResolver`。route 刷新成功时，解析器以原子方式替换该 topic 的队列，
并刷新当前角色按 Broker 名称保存的地址表。topic 刷新失败时，可以回退到该 topic 的上一份有效快照，也可以使用
已知的同名 Broker 地址；不同 Broker 名称之间不会互相借用地址。PULL response 返回的建议 Broker ID 也属于解析器
状态，并限制在当前 Consumer 实例内。LitePull 的手工 assignment 在开始接收和发送 group heartbeat 前，同样通过
解析器解析 topic 与 Broker；queue 的来源不能改变这条路径。

### Consumer 共享内部边界

`Consumer` 根目录只保留公开基础模型和少量协议组合类型。运行行为按职责组织，不引入通用 Consumer 基类：

| 位置 | 职责 |
| --- | --- |
| `Consumer` 根目录 | 公开的 `ConsumerOptions`、filter、position、result、逻辑 queue identity 以及 message view。 |
| `Route` | 每个角色的 route 快照、可读队列发现、endpoint 解析、最后有效 route 回退以及建议 Broker ID。 |
| `Coordination` | Heartbeat 与成员查询 wire 操作、heartbeat 编码、consumer-list 解码，以及每次启动独享的 group session。 |
| `Coordination/Rebalance` | 共享的客户端 rebalance 调度、注册、唤醒以及确定性的平均队列分配策略。 |
| `Pull` | 供 LitePull 与 Push PULL receiver 共用、且不保存订阅状态的内部 PULL request/response 契约和 wire 操作；每次 Pull 都从当前 receive target 获取 filter。 |
| `Offset` | 内部 queue position 查询与 consumer-group offset 持久化。 |
| `Settlement` | 供 Push PULL 重试和 POP 显式死信转发共用的 classic `CONSUMER_SEND_MSG_BACK`。 |
| `LitePull` 与 `Push` | 两个公开角色的门面，以及各自的 assignment、receive、dispatch 和运行态。 |

Remoting 组合根仍会为每个已注册角色创建一个 `IRemotingConsumerEngine`。Engine 只管理角色生命周期，并将
`PullWireClient`、`RemotingConsumerOffsetClient`、`RemotingSettlementClient` 与当前角色的 route resolver 组合起来。
它不负责 assignment 策略、接收循环或 group membership，也不再维护第二套公开生命周期。角色内部协作者只依赖
实际需要的 route、PULL、offset 或 settlement 接口，因此 POP 结算路径不依赖 PULL 与 offset 行为。

`RemotingConsumerGroupClient` 是无状态的 heartbeat、成员查询和 unregister wire client。
`RemotingConsumerGroupSession` 则属于一次启动周期，负责已知 Broker 集合、subscription version、rebalance
registration 和 unregister 边界。Push 与 LitePull 各自管理 queue reconciliation，不继承共享 Consumer 基类。
QUERY_ASSIGNMENT JSON 位于 `Push/Assignment`；heartbeat JSON 与 consumer-list 解码位于 `Coordination`；平均分配
算法以纯 coordination policy 形式存放，不再混入 protocol helper。

使用同一 namespaced group 的本地 Consumer 必须呈现一致的 Broker 可见行为。除 subscription、角色、message
model、顺序模式和初始位置必须一致外，同组 Push 成员还必须使用相同的 `QueueAssignmentMode`。混用 Client 与 Broker
assignment 可能使两个成员计算出重叠的 ownership，因此 options validation 会直接拒绝这种注册。

### LitePull 接收模型

订阅重平衡或手动 assignment 后，LitePull 为每个期望的 queue/filter receive target 启动一个可取消的接收循环：

```text
订阅或手动 assignment
        -> assignment 对账
        -> queue/filter receive target
        -> 每个 target 的 Broker 长轮询
        -> 有界共享本地缓存
        -> 应用 PollAsync
        -> 已交付本地 position
        -> 自动或显式 CommitAsync
```

fetch position 与 delivered position 分开维护。Broker 结果进入本地缓存后，接收循环推进 fetch position；只有
`PollAsync` 将消息交给应用时，才推进可提交 position。因此，已预取但尚未交付的消息不会被一次 commit 跳过。

调整后的 LitePull 契约如下：

- `PollAsync` 返回 `IReadOnlyList<RemotingMessageView>`，不再泄漏低层 `RemotingPullResult`。
- `PollTimeout` 与 Broker `LongPollingTimeout` 相互独立。`PollAsync(null)` 使用 `PollTimeout`，零表示非阻塞
  drain；本地超时返回空列表，只有调用方取消或 Consumer 关闭才抛取消异常。
- `MaxCachedMessages` 和 `MaxCachedMessageBytes` 分别按条数与字节数限制共享预取缓存。
- `Position` 返回已分配队列的下一个本地 position，且不会执行 Broker I/O。
- `Pause` 停止队列的新接收；已经缓存的消息仍可排空。`Resume` 重新启动接收。
- `Seek` 和移除 assignment 会取消受影响的接收 generation，并在新 generation 可见前丢弃旧缓存消息。route
  替换只取消 in-flight request；逻辑队列 identity 没有改变，因此已经缓存的消息仍有效。Seek 和 position receiver
  替换会保留当前 target filter。
- `SeekToBeginningAsync`、`SeekToEndAsync` 和 `SeekToTimestampAsync` 覆盖应用定位需求。
- `GetCommittedOffsetAsync` 返回持久化的 group position；min/max 检查继续属于 Admin。
- 手动 `AssignAsync` 接收队列以及可选的按 topic 配置的只读 filter dictionary；缺少 topic filter 时匹配全部
  消息。订阅模式和手动 assignment 模式仍互斥。手工 assignment 及其 filter 会一直作为期望状态保留，直到下一次
  assignment 替换；手工模式下的每次 heartbeat 与 queue/filter receive target 都必须使用对应的 filter。
- `EnableAutoCommit` 默认 `true`，`AutoCommitInterval` 默认为五秒。自动与显式 commit 都只持久化 delivered
  position，绝不提交 fetch position。启用自动提交后，assignment 对账会保留被撤销队列的状态快照，在 receiver
  排空后持久化 delivered position，再完成本轮对账。这与 Java LitePull 的
  [`removeUnnecessaryMessageQueue`](https://github.com/apache/rocketmq/blob/rocketmq-all-5.5.0/client/src/main/java/org/apache/rocketmq/client/impl/consumer/RebalanceLitePullImpl.java#L60-L65)
  一致。
- route 刷新、已知 Broker heartbeat 与 assignment 对账彼此隔离失败。NameServer 故障不会阻止客户端向仍
  可达的 Broker 发送 heartbeat。

### LitePull 内部职责布局

LitePull 是顶层应用模型，不是内部 PULL wire 操作的子层级。公开 namespace 与源码根目录分别调整为
`EventHorizon.RocketMQ.Remoting.Consumer.LitePull` 和 `Consumer/LitePull`，`Consumer/Pull` 只保留内部实现。与 Push
一致，每次启动都会创建一组独享协作者，公开门面不再继续累积运行态职责：

| 位置与类型 | 负责 | 不负责 |
| --- | --- | --- |
| 根目录 `RemotingLitePullConsumer` | 串行执行公开生命周期操作，并校验 subscribe、assign、poll、seek、pause、resume、position 与 commit 调用。 | 每次运行的协作者组合、接收循环、route 状态、heartbeat 目标或可变 queue position。 |
| 根目录 `LitePullConsumerRunLifecycle` | 组合每次启动独享的协作者，执行首次协调，并按顺序完成关闭、排空、最终提交、注销和 engine 停止。 | 公开 API 状态或可复用的单例服务。 |
| 根目录 `RemotingLitePullConsumerRun` | 一次启动的 generation、操作准入、取消，以及这一 generation 所拥有的协作者引用。 | 公开配置或跨 generation 的可变状态。 |
| `Assignment/LitePullSubscriptionState` | 将订阅或手工 assignment 意图、按 topic 配置的 filter 与 subscription version 维护为一个串行化快照。 | route 查询、队列分配或 receiver 启动。 |
| `Assignment/LitePullAssignmentCoordinator` | Route 刷新、group membership、客户端分配、heartbeat 准备和期望的 queue/filter receive target。 | receiver generation、本地缓存或 offset 推进。 |
| `Assignment/LitePullAssignmentReconciler` | 比较期望与当前的 queue/filter target，停止并排空已移除的 generation；启用自动提交时持久化已撤销队列的 delivered position；初始化 offset，并使用 target filter 启动缺失的 generation。 | 分配策略或 heartbeat 内容。 |
| `Receive/LitePullReceiverRegistry` | 为每个已分配的 queue/filter target 提供 generation-aware receiver，负责保留 target filter 的停止、排空和替换，以及观察异常结束。 | 队列分配或应用 position。 |
| `Receive/LitePullBuffer` | 共享 FIFO 投递缓存，并对等待 `PollAsync` 的已解码 batch 执行条数和字节数的原子准入。 | 进行中的 Broker 请求、Broker offset 或 assignment 策略。 |
| `Receive/LitePullDeliveryState` | 在同一把锁的保护下维护 assignment generation 及其 queue/filter target、共享 buffer、fetch/delivered position、seek/pause 状态和可提交快照。 | Broker I/O 或定时调度。 |
| `Offset/LitePullCommitLoop` | 负责周期提交、显式提交串行化，以及关闭时的最后一次自动提交。 | 消息接收或 handler dispatch。 |

订阅内容与 heartbeat 中的 subscription version 必须在同一次 coordination transition 中更新。同一份不可变的
subscription/filter 快照同时生成 heartbeat 和期望的 queue/filter receive target，因此并发执行的周期 rebalance
不能观察到“新 version 加旧 filter”的组合。手工 assignment 也遵循同一规则：run 保留期望队列和已解析的按 topic
filter，首次和后续 heartbeat 以及 receive target 都必须携带它们，与 Java LitePull 的 assignment-mode heartbeat 保持一致。
替换 target 会按请求的 filter 创建新的 receiver generation；如果是 filter 替换，旧 generation 的缓存消息会被
移除。in-flight 的旧 request 仍可能用旧 filter 完成，但之后的 request 不会再使用它。`Seek` 和 position receiver
替换保留当前 target filter。移除订阅会移除对应 receive target；缺少 topic/filter entry 时绝不能回退到
`FilterExpression.All`。route、heartbeat 或 offset 初始化的暂时性故障不会清除这份意图；周期协调会持续重试，直到
期望的 receiver generation 全部建立。参见 Java
[`updateAssignPullTask`](https://github.com/apache/rocketmq/blob/rocketmq-all-5.5.0/client/src/main/java/org/apache/rocketmq/client/impl/consumer/DefaultLitePullConsumerImpl.java#L1032-L1038)。

Receiver registry 会监视每个 receiver task。若 receiver 异常退出且对应 queue generation 仍是当前 generation，
registry 会唤醒协调，再由 reconciler 启动替代 receiver；已经撤销或替换的旧 generation 即使稍后结束，也不能重新
创建自身。这个恢复职责属于 assignment 对账，receive loop 仍只负责一个队列的 Broker 轮询和 batch 准入。

每个 queue receiver 最多保留一个进行中的 PULL 或一份已返回的 batch。请求条数与字节上限同时受 `PullBatchSize`、
`MaxCachedMessages`、`MaxMessageBytes` 和 `MaxCachedMessageBytes` 约束，但等待消息的空长轮询不占用共享投递缓存。
这是接收隔离的必要条件：一条暂时没有消息的队列，不能阻止另一条已分配队列接收已有消息。

返回非空 response 后，已解码 batch 通过一次原子化的条数和字节检查进入共享缓存。如果共享容量正被其他队列占用，
receiver 只保留这份 response，等待 `PollAsync`、seek、移除 assignment 或 shutdown 改变状态；等待期间既不再发起
PULL，也不推进 fetch position。单条消息超过字节上限时，只有共享缓存为空才允许进入。这样既能严格限制可供
`PollAsync` 排空的聚合缓存，又不会让空长轮询造成跨队列队头阻塞。移除 assignment、seek 或取消会丢弃等待准入的
response，不推进该 generation 的 fetch position。

Java LitePull 会在 PULL 前检查全局 request cache，以及每队列的条数、大小和 span；batch 返回后才写入
`PullProcessQueue`，因此并发 receive task 仍可能在短时间内越过 threshold。本客户端保留官方的逐 assignment 后台接收模型，
但对可供 poll 的共享缓存采用更严格的原子准入。等待准入的 response 每个 assignment 最多一份，在成功准入前不算
作可供 poll 的缓存。参见 Java
[`PullTaskImpl`](https://github.com/apache/rocketmq/blob/rocketmq-all-5.5.0/client/src/main/java/org/apache/rocketmq/client/impl/consumer/DefaultLitePullConsumerImpl.java#L909-L998)
与
[`ProcessQueue.putMessage`](https://github.com/apache/rocketmq/blob/rocketmq-all-5.5.0/client/src/main/java/org/apache/rocketmq/client/impl/consumer/ProcessQueue.java#L129-L168)。

像 `PollAsync` 排空缓存、`Seek`、pause、resume 和显式提交这样的短暂状态变更，都要经过 run 的 operation gate；
`GetCommittedOffsetAsync` 这类需要调用 engine 的查询也使用同一入口和 run cancellation，不能比它所依赖的 engine
generation 的生命周期更长。assignment 与订阅变更则继续由外层 lifecycle gate 串行化。关闭时先封闭两个操作入口，取消
Broker receive 与仍在等待的 poll，再等已经进入的操作退出。这样，最终快照生成后不会再出现新的 delivered
position 或 receiver generation。调用方取消后，`StopAsync` 会立即停止等待并返回取消，但不会遗弃清理工作；同一个
shutdown task 会在后台继续排空已准入操作、提交 offset、注销、停止 engine 并释放 run。后续 `StartAsync` 或
`DisposeAsync` 必须先观察并等待这项清理，不能并行复用或释放该角色独占的 engine。

仅当启用 `EnableAutoCommit` 时，关闭流程才会在注销 group membership 前，对最终 delivered-position 快照执行一次
best-effort 提交。手动提交模式不会因为 Consumer 停止就推进 Broker offset；只有已经交给 `CommitAsync` 的位置才会
持久化。这与 Java LitePull 一致：它的
[`shutdown`](https://github.com/apache/rocketmq/blob/rocketmq-all-5.5.0/client/src/main/java/org/apache/rocketmq/client/impl/consumer/DefaultLitePullConsumerImpl.java#L239-L255)
也会先持久化 offset store，再注销 Consumer。最后才释放当前角色独占的 engine。

### Push 队列 assignment 模式

`RemotingPushConsumerOptions` 新增类型为 `RemotingPushQueueAssignmentMode` 的 `QueueAssignmentMode`。队列
assignment 归属与 receive mode 是两个独立维度：

| 值 | 谁计算队列 assignment | 可用的内部接收方式 | 应用 API |
| --- | --- | --- | --- |
| `Client` | 客户端根据 topic route、group 成员和分配策略计算；这是默认值。 | 仅 PULL。 | 消息交给 Push handler。 |
| `Broker` | Broker 为集群并发消费返回 assignment。 | PULL 或 POP，由每条返回的 assignment 决定。 | 消息仍交给同一个 Push handler。 |

`QueueAssignmentMode` 只选择由谁计算队列 assignment，不是公开的 PULL/POP 开关。与 Java Remoting 一致，即使请求
`Broker`，广播或 `ConsumeOrderly` 也在内部使用客户端 assignment 和 PULL。满足条件的集群并发 Consumer 查询
Broker，并服从每条返回的 request mode。Consumer 不会调用 `SET_MESSAGE_REQUEST_MODE`；运维人员应在 Broker
上配置 topic/group 的 request mode。assignment 缺少 mode 时按 `PULL` 解码；assignment 集合为 `null` 时保留
旧状态，为空集合时清空旧状态。Broker 不支持 assignment 查询或查询失败时，本次对账失败，绝不静默退回
client assignment；否则可能形成重叠 ownership。

这个边界遵循官方 classic
[PushConsumer 指南](https://rocketmq.apache.org/docs/4.x/consumer/02push/)：应用 handler 不负责 rebalance 与
pull 逻辑。在 5.5.0 源码中，
[`RebalancePushImpl`](https://github.com/apache/rocketmq/blob/rocketmq-all-5.5.0/client/src/main/java/org/apache/rocketmq/client/impl/consumer/RebalancePushImpl.java#L125-L128)
决定何时仍使用客户端 assignment；
[`QueryAssignmentProcessor`](https://github.com/apache/rocketmq/blob/rocketmq-all-5.5.0/broker/src/main/java/org/apache/rocketmq/broker/processor/QueryAssignmentProcessor.java#L92-L132)
则把配置的 PULL 或 POP request mode 写入每条 Broker assignment。

所有集群模式都必须在 heartbeat 中声明并对账 classic retry topic。使用 Broker assignment 时，Consumer 也会
查询该 topic 的 assignment，并接受 Broker 强制返回的 PULL mode。排除它会导致成功执行的 PULL
`CONSUMER_SEND_MSG_BACK` 无法被同一个 Push Consumer 再次接收。

Broker 可以为一个 topic 返回 PULL、为另一个返回 POP，也可以动态切换同一 topic。对账时 mode 是 assignment
identity 的组成部分：旧 receiver 完成停止与排空后，才启动替代 receiver。

Broker-assigned POP 为 Push 增加以下 options：

| Option | 默认值 | 作用 |
| --- | ---: | --- |
| `PopBatchSize` | `32` | 单次 POP 最大消息数；有效范围为 1 到 32。 |
| `PopInvisibleDuration` | 60 秒 | 初始不可见 lease；有效范围为 5 秒到 5 分钟。 |
| `PopMaxInflightMessagesPerAssignment` | `96` | 单个 assignment 中已接收但尚未结算的 POP 消息上限。 |

PULL options 使用明确的 `PullBatchSize`、`PullMaxCachedMessages` 和 `PullMaxCachedMessageBytes`，避免在混合 Broker
assignment 中和 POP 混淆。LitePull 的相同 wire operation 也使用 `PullBatchSize`。共享的 `InitialPosition` 与可选
`ConsumeTimestamp` 属于 `ConsumerOptions`；原公开 `QueryOffsetPolicy` 类型删除。

`InitialPosition.Beginning` 映射为 POP init mode MIN；End 和 Timestamp 映射为 MAX，与 Java classic 行为一致。
POP assignment 不提供顺序消费或跨成员 `MessageGroup` FIFO 保证。

### PULL 与 POP 状态必须分离

PULL 和 POP 共享订阅、消费组成员、heartbeat、Broker assignment 查询、handler 并发、telemetry 和生命周期，
但保留独立的接收与结算状态：

| 关注点 | PULL receiver | POP receiver |
| --- | --- | --- |
| 进度 | 队列 offset 与连续 commit watermark | Broker receipt 与不可见 deadline |
| 流控 | 等待分发的缓存消息 | 已接收但尚未结算的消息 |
| 成功 | 推进并持久化 offset | ACK 最新 receipt |
| 重试 | `CONSUMER_SEND_MSG_BACK` | 一次 `suspend=false` 的 `CHANGE_MESSAGE_INVISIBLETIME` |
| 移除 assignment | drop process queue，并拒绝迟到 commit | drop POP queue，并拒绝迟到 receipt 结算 |

POP 不会进入基于 offset 的 `PullProcessQueue`。其 decoder 支持 `queueId=-1`、retry-v1/retry-v2 receipt marker，并使用
每条 receipt 编码的真实物理队列发送 ACK 和不可见时间变更。

receipt 的 retry marker 还决定 ACK 与不可见时间请求使用的真实 wire topic：marker `0` 使用普通 topic，`1`
使用 retry-v1 topic，`2` 使用 retry-v2 topic。重试不可见时间变更返回的 receipt 必须保留 marker，结算请求不能
一律按逻辑普通 topic 重建。

### Push 内部职责布局

公开层仍只有一个 Push Consumer，内部则由每次启动独享的一组协作者组成。这些类型只是 Consumer 的构造细节，
不会单独注册到应用 DI。

| 位置与类型 | 负责 | 不负责 |
| --- | --- | --- |
| 根目录 `RemotingPushConsumer` | 串行执行公开生命周期操作、维护订阅、注册 reset-offset 请求处理器，并提供只读 assignment 快照。 | route 发现、接收循环、handler 调度或结算。 |
| 根目录 `RemotingPushConsumerRun` | 一次启动周期、对应的取消源、协作者组合和有序关闭。 | 公开 API 状态或可复用的单例服务。 |
| `Assignment/PushAssignmentCoordinator` | 在 coordination gate 内以原子方式生成 subscription/version 快照，执行 route 刷新、已知 Broker heartbeat、客户端或 Broker assignment、队列锁续期，并生成携带快照 filter 的 PULL/POP receive target。 | 消息处理或协议结算。 |
| `Assignment/PushReceiverRegistry` 与 `PushReceiverFactory` | 保存带有 receive mode 的活动 receiver，负责创建 receiver、保留 target filter，以及切换时的停止、排空与释放。 | 队列分配决策。 |
| `Processing/PushMessageHandlerInvoker` | 作为应用 handler 的唯一入口，执行 Consumer 全局 `MaxConcurrency` 限制、消费上下文与结果映射、超时处理、process telemetry 和迟到结果拒绝。 | PULL offset/send-back 或 POP receipt 结算。 |
| `Pull/Dispatch/PullDeliveryCoordinator` | 整个 Consumer 范围内的 PULL 缓存准入、ready queue 轮转、消费分批、固定数量的消费循环、`MessageGroup` 前驱链与 queue drop 清理。 | 队列内消息状态、handler 结果或协议 I/O。 |
| `Pull/Dispatch/PullProcessQueue` | 一个物理 PULL queue 的缓存消息、拉取/提交位点、连续完成水位、settlement retry 状态和 FIFO barrier，并由同一个同步边界保护。 | Consumer 范围调度或 wire I/O。 |
| `Pull/Receive/ConcurrentPullReceiveLoop` | 单个并发 PULL assignment 使用自身 target filter 的长轮询、缓存准入和串行 offset 持久化循环。 | 应用 handler 结果或 send-back。 |
| `Pull/Dispatch/PullConsumeRequestProcessor` | 并发 handler 结果映射、队列内完成状态，以及激活已保存的本地 settlement retry。 | 接收轮询、retry topic 准备、send-back 执行或顺序消费的 Broker lock 处理。 |
| `Pull/Settlement/PullSendBackSettlement` | retry topic 准备、send-back 执行与 retry queue offset 初始化。 | 本地 settlement retry 状态或调度、接收轮询、handler 结果映射或队列内完成状态。 |
| `Pull/Receive/OrderlyPullReceiveLoop` | 顺序消费的 Broker lock 校验、使用 target filter 的长轮询、串行 handler 重试、send-back 和内联 offset 持久化。 | 并发 `PullProcessQueue` 调度。 |
| `Pull/Offset/PullOffsetManager` | PULL 初始与已提交 offset、广播存储、reset boundary 和 retry-topic 初始化 boundary。 | POP 进度。 |
| `Pull/Receive/PullAssignmentReceiver` | 单个 PULL assignment 的 identity、取消、观察到的 Broker 队列锁 lease，以及覆盖接收循环和活动消费请求的完成边界。 | POP wire 操作或 handler 结果策略。 |
| `Pop/PopAssignmentReceiver` | 单个 POP assignment 使用自身 target filter 的长轮询、接收分批、取消和接收故障隔离。 | handler 调用、receipt 生命周期或结算。 |
| `Pop/PopDeliveryProcessor` | POP handler 调用、固定不可见 deadline 检查、一次性重试不可见时间变更、最大投递次数下的消息年龄处理，以及原始 deadline 内的 ACK/显式死信结算。 | assignment reconciliation、接收轮询、`PullProcessQueue` 或 offset commit。 |
| `Pull/Orderly/OrderlyPullQueueLockClient` 与 `Pop/PopWireClient` | 分别执行 lock/unlock 与 POP wire command；queue-lock client 只接受物理 PULL lock target。 | assignment 策略或应用 handler 调用。 |

控制链路与投递链路刻意保持不同：

```text
RemotingPushConsumer
  -> RemotingPushConsumerRun
     -> PushAssignmentCoordinator
        -> 不可变 subscription/version 快照
        -> heartbeat 与携带 filter 的 PULL/POP receive target
        -> PushReceiverRegistry -> PULL 或 POP receiver

PULL receiver -> ConcurrentPullReceiveLoop -> PullDeliveryCoordinator -> PullProcessQueue
                                                        \-> ready-queue dispatch
                                                            -> PullConsumeRequestProcessor
                                                               -> PushMessageHandlerInvoker
                                                               -> PULL send-back
                                                        \-> offset commit

顺序 PULL receiver -> OrderlyPullReceiveLoop -> PushMessageHandlerInvoker
                                             \-> PULL send-back / offset commit

POP receiver  -> PopDeliveryProcessor -> receipt 与 lease 状态
                                    \-> PushMessageHandlerInvoker
                                    \-> POP ACK / 修改不可见时间 / 显式死信
```

以下规则属于行为契约，而不只是文件布局约定：

- 订阅变更、reset-offset 命令、周期 rebalance 与 assignment 唤醒都通过同一个 coordination gate 修改 receiver。
  Coordinator 在其中原子化地快照订阅及其 version，再用同一份快照生成 heartbeat 与携带 filter 的 PULL/POP receive
  target；接收循环不能直接改动 assignment 集合。
- receive target 携带其 topic filter。替换 target 会创建新的 receiver generation；in-flight 的旧 request 仍可能
  用旧 filter 完成，但之后的 request 不会再使用它。移除订阅会移除对应 target；topic/filter entry 缺失时绝不能
  回退到 `FilterExpression.All`。
- receive mode 是 assignment identity 的一部分。PULL 切换到 POP 或 POP 切换到 PULL 时，必须先把旧状态标为
  dropped，停止 receiver，并等待接收循环和活动消费请求退出该 receiver 拥有的协议工作；释放队列锁或 receipt
  ownership 后才能启动新 receiver。应用 handler 若忽略取消，仍可能稍后才退出，但它的结果会在 dropped
  `PullProcessQueue` 边界被拒绝，不能再触发 retry offset 初始化、send-back 或 offset 推进。
- 并发 PULL 的 handler 结果只能通过一个原子的队列内接受操作进入结算。缓存预留数量为零可能是正常接受结果，
  不能与队列已 dropped 或 entry 已不再归属该队列时的拒绝混为一谈。
- 所有 assignment 的 PULL 缓存消息数与 body byte 预留必须以原子方式执行。准入策略需要保持可工作性：较大的 batch 暂时
  无法进入时，不能独占 waiter 路径并挡住后面本可进入的 batch；同时必须限制绕过次数，让较大或暂时受阻的
  batch 最终成为准入屏障，避免持续被小 batch 饿死。
- PULL、POP、并发、顺序及混合模式的每次投递都进入 `PushMessageHandlerInvoker`。因此 `MaxConcurrency` 限制的是
  整个 Push Consumer，而不是分别为两套接收引擎提供一份并发额度。
- handler 结果返回各自 receiver 的处理器。只有 PULL 可以推进 offset 和 send-back；只有 POP 可以 ACK 或修改
  receipt 不可见时间。两条路径不能把自己的状态转换成另一条路径的表示。
- 关闭时先阻止新的 reconciliation，再取消 receiver、排空 dispatch、释放 Broker 队列锁、注销消费组成员、刷新
  广播 offset，最后停止 Consumer engine。若调用方取消导致不配合取消的 handler 脱离等待，其迟到结果必须被忽略；
  只有观察任务结束后才能释放本次启动所拥有的资源。
- 重构不能改变现有 OpenTelemetry 拓扑：PULL/POP wire client 负责 receive operation，
  `PushMessageHandlerInvoker` 负责 process operation，PULL 与 POP 各自负责并且只完成一次对应的 settlement
  operation。参见 [OpenTelemetry 埋点](../architecture/opentelemetry-instrumentation.md)。

这个拆分参考官方 Java 5.5.0 的职责边界，但不照搬类结构。Java
[`RebalanceImpl`](https://github.com/apache/rocketmq/blob/rocketmq-all-5.5.0/client/src/main/java/org/apache/rocketmq/client/impl/consumer/RebalanceImpl.java)
分别维护 PULL `PullProcessQueue` 与 Java POP `PopProcessQueue`；
[`ConsumeMessageConcurrentlyService`](https://github.com/apache/rocketmq/blob/rocketmq-all-5.5.0/client/src/main/java/org/apache/rocketmq/client/impl/consumer/ConsumeMessageConcurrentlyService.java)
和
[`ConsumeMessagePopConcurrentlyService`](https://github.com/apache/rocketmq/blob/rocketmq-all-5.5.0/client/src/main/java/org/apache/rocketmq/client/impl/consumer/ConsumeMessagePopConcurrentlyService.java)
则分开处理 PULL 与 POP 结算。classic
[Go Push Consumer](https://github.com/apache/rocketmq-client-go/blob/master/consumer/push_consumer.go) 也把 Push 描述为
PULL 之上的 callback 门面，并把队列与 offset 状态放在门面以下；它只用于校验 PULL 生命周期，不作为 POP
语义来源。

### Handler 结果与 POP 固定 deadline

现有 Push handler 契约继续供两类 receiver 共用：

- `Success` 根据 `AckIndex` 结算已确认前缀。
- `Retry` 将 PULL 尾部 send-back，或对 POP 尾部使用一次 `suspend=false` 的
  `CHANGE_MESSAGE_INVISIBLETIME`，并使用 `DelayLevelWhenNextConsume`。
- `DeadLetter` 或负 delay level 会让 PULL 直接转入死信。本客户端也为 POP 保留这项显式请求：先执行 classic
  dead-letter send-back，只有转发成功后才 ACK receipt。ACK，包括显式死信转发后的 ACK，只能在原始不可见
  deadline 内重试；deadline 过期后不再发送结算请求。
- PULL 的 `Retry` 达到 `MaxDeliveryAttempts` 后继续按 classic send-back 进入死信。POP 的 `Retry` 达到同一上限时，
  则遵循 Java 客户端的 `checkNeedAckOrDelay`，不会隐式执行死信 send-back。消息年龄不超过 POP 最后一级延迟的两倍时，
  客户端按年龄选择下一个 POP 延迟档位，并只发送一次 `CHANGE_MESSAGE_INVISIBLETIME`；只有消息年龄严格超过该阈值才
  ACK receipt。官方最后一级延迟为 7,200 秒，因此阈值为四小时。这个最大投递次数分支会忽略 handler 指定的 delay level。

`DelayLevelWhenNextConsume` 默认为 `0`，与 Java 的
[`ConsumeConcurrentlyContext`](https://github.com/apache/rocketmq/blob/rocketmq-all-5.5.0/client/src/main/java/org/apache/rocketmq/client/consumer/listener/ConsumeConcurrentlyContext.java)
一致。PULL 把该值传给 classic send-back；POP 则把正值映射到 Java 的 POP 专用重试表。值为 `0` 时，根据从零开始
的重试次数（`DeliveryAttempt - 1`）选择表项。该表从 10 秒开始，并不是普通延迟消息的 level 表。负值是本客户端提供的
显式死信扩展；官方 Java POP 达到重试上限时不会使用它。

POP handler 执行期间不会自动续租 receipt。客户端会在 handler 处理前和结算前检查固定不可见 deadline；如果已
过期，则忽略迟到的 handler 结果，不创建结算 operation，并允许 Broker 重新投递。`Retry` 结果只发起一次带
`suspend=false` 的 `CHANGE_MESSAGE_INVISIBLETIME` 请求；对于不确定的失败，不会使用旧 receipt 重试。这遵循官方 Java 的
结果处理与关键保护：
[`processConsumeResult`](https://github.com/apache/rocketmq/blob/rocketmq-all-5.5.0/client/src/main/java/org/apache/rocketmq/client/impl/consumer/ConsumeMessagePopConcurrentlyService.java#L245-L310)
会读取 context 中的 delay level，并在最大投递次数分支按消息年龄处理；POP consume request 会在处理前检查
[`isPopTimeout`](https://github.com/apache/rocketmq/blob/rocketmq-all-5.5.0/client/src/main/java/org/apache/rocketmq/client/impl/consumer/ConsumeMessagePopConcurrentlyService.java#L335-L360)，
并在调用 `processConsumeResult` 前再次检查；receipt 过期后不再执行结算。

固定 deadline 是 classic Remoting Push 的协议规则，不能从 gRPC Push 反推。gRPC receive contract 可以通过
`AutoRenew=true` 请求 Proxy 托管自动续约；Remoting 没有 Proxy 持有的客户端 channel 和 receipt registry。
Retry 之后的 `CHANGE_MESSAGE_INVISIBLETIME` 也不等同于 gRPC handler 活跃期间的续约。

### gRPC Consumer 审计

gRPC 角色保持不变：

- `IGrpcSimpleConsumer` 是受支持的调用方驱动 receipt 模型。
- `IGrpcPushConsumer` 负责普通自动分发。
- `IGrpcLitePushConsumer` 负责 LiteTopic 订阅同步与自动分发。
- 不新增公开 gRPC PullConsumer，因为受支持的 Proxy 路径没有实现完整 classic queue-offset 工作流。

这些结论都属于各自协议，不会引入共享 Consumer interface 或跨 Project 模型。

## 组合与所有权

只有 LitePull 和 Push 注册活跃 Remoting Consumer 角色。每个角色独立拥有 group-member client、route resolver、
consumer engine、每次启动的 run、group session、rebalance participant、接收状态和 hosted 生命周期。同组的
多个本地成员继续使用不同 channel，避免 Broker membership 相互覆盖。

Push 与 LitePull orchestrator 会把 assignment、接收、handler 分发或缓存、position 和结算交给各自运行态中的协作者。
Route、PULL、offset 与 send-back 组件在 Remoting 组合根中随角色的 engine 一起构造，不会注册成应用可独立解析的
服务。内部 Consumer 协议操作也不会通过低层公开门面暴露。

Solution、单元测试、集成测试、benchmark、Generic Host 与非 Host sample、协议指南、根 README 和测试环境指南
都只暴露本文接受的 API。被删除的 Pull 与 POP sample Project 直接移除，不保留历史示例。Push sample 同时说明
两种 assignment mode，但默认工作流不依赖 Broker 侧 POP 配置。

## 测试契约

单元测试必须覆盖：

- assignment request JSON，以及 `null` 与空 response 的不同处理；
- PULL/POP 增加、移除和 mode 切换；
- 新 receive mode 生效前，旧 receiver 已经完成排空；
- 混合 PULL/POP assignment 共用同一份 Consumer 全局 handler 并发上限；
- reset-offset 串行化、迟到 handler 结果拒绝和确定性的关闭所有权；
- `queueId=-1`、retry receipt marker 和真实物理 ACK 目标；
- POP in-flight 流控、无 handler 续租的固定不可见 deadline、handler 前后过期检查、一次性 Retry 不可见时间变更、
  最大投递次数下按消息年龄延期或终态 ACK 且不隐式 send-back、仅在原始 deadline 内重试 ACK/显式死信结算、
  显式死信顺序和迟到结算；
- 职责迁移后，receive、process、commit、send-back、ACK 与不可见时间 telemetry 的结果保持不变；
- 无论 queue identity 来自公开构造、Admin 返回还是 LitePull 发现，都应具有相同的 route 解析和 heartbeat 行为；
- route 替换或刷新失败时，只使用 resolver 管理的最新或最后有效 endpoint；测试还需覆盖 suggested Broker 选择，以及
  应用 topic 到 retry topic 的同名 Broker 地址回退；queue 本身不保存快照；
- 同组本地 Push 成员使用不同 `QueueAssignmentMode` 时拒绝注册；
- Broker-assigned POP 的 `queueId=-1` 只保留在内部 assignment，不会成为公开 Consumer queue；
- LitePull 后台接收不发生队头阻塞，跨多队列的消息条数与字节数遵守统一的缓存准入和容量上限，并覆盖
  delivered-position commit、seek、pause/resume、heartbeat 故障隔离、seek/stop 操作准入，以及仅在开启自动提交时
  于关闭阶段提交最终 delivered position。

Remoting 集成测试通过测试管理操作配置 Broker request mode，并在真实 Broker 上验证 Push POP 投递与 ACK，覆盖
POP 不可见期重试与 Broker-assigned PULL send-back 重投，同时保留普通 PULL Push 覆盖。LitePull 还会分配多个物理
队列，验证一个空队列仍在 long poll 时，定向发送到另一队列的消息不会被延迟。PULL 和 POP 场景分别创建独立
topic 和 consumer group，避免 request-mode 状态、offset、retry topic 和测试清理相互干扰。Broker 实际以 topic
加 consumer group 作为键，因此只更换 group 在协议上也足够；独立 topic 是更严格、确定性更高的测试边界。集成测试
不能因此把 `SET_MESSAGE_REQUEST_MODE` 暴露成生产 Consumer API。

## 取舍与非目标

- 移除 direct Pull 与 POP，是以低层便利换取两种一致的应用模型。
- Broker assignment 要求 Broker 支持 `QUERY_ASSIGNMENT`；客户端分配继续作为默认值，与官方 Java Remoting
  客户端一致。
- Broker POP request mode 是运维控制的服务端配置。
- 首版 Broker POP 不支持广播、顺序消费、batch ACK 和协议级 POP orderly 语义。
- LitePull 后台接收会比当前单次 Poll 实现占用更多并发 Broker 请求，但能消除跨队列队头阻塞，并与官方
  Consumer 模型一致。
- 将 LitePull 移到与 Push 同层的 `Consumer.LitePull` namespace 是有意的命名空间破坏性变更。公开 LitePull
  因此与 Push 处于同一层级，`Consumer.Pull` 只保留内部 wire 关注点，不提供兼容转发类型。
- LitePull 的原子缓存准入比 Java 的接收后 threshold 统计更严格。已返回 batch 可能要等待可供 poll 的缓存腾出容量，
  但空长轮询不会占用其他队列的缓存额度。
- 本次不增加共享生产 Project、传输无关 Consumer API 或 gRPC/Remoting 抽象。

## 延伸阅读

- [Classic Remoting 传输与客户端角色](transport-and-client-roles.md)
- [gRPC Consumer 模型](../grpc/consumer-model.md)
- [依赖注入注册与生命周期](../architecture/dependency-injection-and-lifetimes.md)
- [本地与集成测试](../testing/local-and-integration-testing.md)
- [官方 SDK 概览：Remoting 与 gRPC](https://rocketmq.apache.org/docs/sdk/01overview/)
- [官方 classic PushConsumer 指南](https://rocketmq.apache.org/docs/4.x/consumer/02push/)
- [Java Remoting `DefaultMQPushConsumer`](https://github.com/apache/rocketmq/blob/rocketmq-all-5.5.0/client/src/main/java/org/apache/rocketmq/client/consumer/DefaultMQPushConsumer.java)
- [Java Push 订阅构建](https://github.com/apache/rocketmq/blob/rocketmq-all-5.5.0/client/src/main/java/org/apache/rocketmq/client/impl/consumer/DefaultMQPushConsumerImpl.java)
- [Java Push 有效 rebalance 选择](https://github.com/apache/rocketmq/blob/rocketmq-all-5.5.0/client/src/main/java/org/apache/rocketmq/client/impl/consumer/RebalancePushImpl.java)
- [Java Broker-assigned Push 对账](https://github.com/apache/rocketmq/blob/rocketmq-all-5.5.0/client/src/main/java/org/apache/rocketmq/client/impl/consumer/RebalanceImpl.java)
- [Java `DefaultMQPullConsumer` 废弃声明](https://github.com/apache/rocketmq/blob/rocketmq-all-5.5.0/client/src/main/java/org/apache/rocketmq/client/consumer/DefaultMQPullConsumer.java)
- [Java LitePull 接收任务](https://github.com/apache/rocketmq/blob/rocketmq-all-5.5.0/client/src/main/java/org/apache/rocketmq/client/impl/consumer/DefaultLitePullConsumerImpl.java)
- [Broker assignment 与 request-mode processor](https://github.com/apache/rocketmq/blob/rocketmq-all-5.5.0/broker/src/main/java/org/apache/rocketmq/broker/processor/QueryAssignmentProcessor.java)
- [Broker request-mode 默认配置](https://github.com/apache/rocketmq/blob/rocketmq-all-5.5.0/common/src/main/java/org/apache/rocketmq/common/BrokerConfig.java)
