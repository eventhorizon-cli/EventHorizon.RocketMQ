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

Broker endpoint 与建议 Broker ID 由内部路由解析器按 Consumer queue identity 保存。Broker-assigned POP 使用
单独的内部 assignment 类型，因为其逻辑 queue ID 可以为 `-1`。

每次 Pull、offset、send-back、POP、ACK 和不可见时间请求都从最新有效 route 解析 endpoint。NameServer 短暂
故障时可以使用 route cache 中最后一份有效路由，但应用传入的 queue value 永远不是 endpoint 的权威来源。

### LitePull 接收模型

订阅重平衡或手动 assignment 后，LitePull 为每个已分配队列启动一个可取消的接收循环：

```text
订阅或手动 assignment
        -> assignment 对账
        -> 每队列 Broker 长轮询
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
  替换只取消 in-flight request；逻辑队列 identity 没有改变，因此已经缓存的消息仍有效。
- `SeekToBeginningAsync`、`SeekToEndAsync` 和 `SeekToTimestampAsync` 覆盖应用定位需求。
- `GetCommittedOffsetAsync` 返回持久化的 group position；min/max 检查继续属于 Admin。
- 手动 `AssignAsync` 接收队列以及可选的按 topic 配置的只读 filter dictionary；缺少 topic filter 时匹配全部
  消息。订阅模式和手动 assignment 模式仍互斥。
- `EnableAutoCommit` 默认 `true`，`AutoCommitInterval` 默认为五秒。自动与显式 commit 都只持久化 delivered
  position，绝不提交 fetch position。
- route 刷新、已知 Broker heartbeat 与 assignment 对账彼此隔离失败。NameServer 故障不会阻止客户端向仍
  可达的 Broker 发送 heartbeat。

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
| 重试 | `CONSUMER_SEND_MSG_BACK` | `CHANGE_MESSAGE_INVISIBLETIME` |
| 移除 assignment | drop process queue，并拒绝迟到 commit | drop POP queue，并拒绝迟到 receipt 结算 |

POP 不会进入基于 offset 的 `ProcessQueue`。其 decoder 支持 `queueId=-1`、retry-v1/retry-v2 receipt marker，并使用
每条 receipt 编码的真实物理队列发送 ACK 和不可见时间变更。

receipt 的 retry marker 还决定 ACK 与不可见时间请求使用的真实 wire topic：marker `0` 使用普通 topic，`1`
使用 retry-v1 topic，`2` 使用 retry-v2 topic。续租后的 receipt 必须保留 marker，结算请求不能一律按逻辑普通
topic 重建。

### Push 内部职责布局

`Consumer/Push` 源码目录按内聚职责组织，公开 Push 契约与生命周期 facade 保留在根目录：

| 目录 | 职责 |
| --- | --- |
| `Assignment` | Broker/client assignment 值与接收模式决策。 |
| `Dispatch` | PULL process queue、消费请求与并发 handler 调度。 |
| `Offset` | 广播 offset 持久化与 Broker reset-offset 解码。 |
| `Orderly` | Broker 队列锁命令与锁续期所有权。 |
| `Pull` | PULL receiver 适配。 |
| `Pop` | POP wire 操作、receipt、解码、lease 与结算。 |

该布局不会引入新的公开抽象，也不表示 PULL 与 POP 的状态可以互换。本次仍由 `RemotingPushConsumer` 统一负责
生命周期与 reconciliation；进一步拆分内部编排属于独立改动，需要先补充有针对性的 characterization tests。

### Handler 结果与 POP lease

现有 Push handler 契约继续供两类 receiver 共用：

- `Success` 根据 `AckIndex` 结算已确认前缀。
- `Retry` 将 PULL 尾部 send-back，或使用 `DelayLevelWhenNextConsume` 修改 POP 尾部的不可见时间。
- `DeadLetter`、负 delay level 或达到最大投递次数时，PULL 直接转入死信。POP 先执行 classic dead-letter
  send-back，只有转发成功后才 ACK receipt。如果之后 ACK 失败，仍可能重复转发死信；这属于至少一次语义。

POP handler 执行期间，客户端会在当前不可见 deadline 前续租 receipt。每次成功续租都会替换后续操作必须使用
的 receipt，因此续租与最终结算必须串行。lease 无法继续续租时，迟到的 handler 结果会被忽略，并允许 Broker
重新投递。结算失败只在 receipt 仍可能有效时重试；超过 deadline 后释放本地 in-flight 容量，以 Broker 重投为准。

handler 仍在执行时的续租使用 `CHANGE_MESSAGE_INVISIBLETIME suspend=true`，避免 lease 最终过期时把续租本身算作
新的失败投递。真正的 `Retry` 结算使用 `suspend=false`，由 Broker 推进 retry attempt。

### gRPC Consumer 审计

gRPC 角色保持不变：

- `IGrpcSimpleConsumer` 是受支持的调用方驱动 receipt 模型。
- `IGrpcPushConsumer` 负责普通自动分发。
- `IGrpcLitePushConsumer` 负责 LiteTopic 订阅同步与自动分发。
- 不新增公开 gRPC PullConsumer，因为受支持的 Proxy 路径没有实现完整 classic queue-offset 工作流。

这些结论都属于各自协议，不会引入共享 Consumer interface 或跨 Project 模型。

## 组合与所有权

只有 LitePull 和 Push 注册活跃 Remoting Consumer 角色。每个角色继续独立拥有 group-member client、
`RemotingConsumerGroupSession`、rebalance participant、接收状态和 hosted 生命周期。同组的重复本地成员继续使用
不同 channel，避免 Broker membership 相互覆盖。

Push orchestrator 会把 Broker assignment、PULL 接收、POP 接收、handler 分发和结算交给职责明确的内部协作者。
内部 Consumer 协议操作不会注册成应用服务，也不会再通过低层公开门面暴露。

Solution、单元测试、集成测试、benchmark、Generic Host 与非 Host sample、协议指南、根 README 和测试环境指南
都只暴露本文接受的 API。被删除的 Pull 与 POP sample Project 直接移除，不保留历史示例。Push sample 同时说明
两种 assignment mode，但默认工作流不依赖 Broker 侧 POP 配置。

## 测试契约

单元测试必须覆盖：

- assignment request JSON，以及 `null` 与空 response 的不同处理；
- PULL/POP 增加、移除和 mode 切换；
- `queueId=-1`、retry receipt marker 和真实物理 ACK 目标；
- POP in-flight 流控、续租 receipt 替换、部分 ACK、重试、死信顺序和迟到结算；
- route 替换后不复用旧 endpoint；
- LitePull 后台接收不发生队头阻塞、有界缓存、delivered-position commit、seek、pause/resume，以及 heartbeat
  故障隔离。

Remoting 集成测试通过测试管理操作配置 Broker request mode，在真实 Broker 上验证 Push POP 投递与 ACK，覆盖
POP 不可见期重试与 Broker-assigned PULL send-back 重投，并保留普通 PULL Push 覆盖。PULL 和 POP 场景分别创建
独立 topic 和 consumer group，避免 request-mode 状态、offset、retry topic 和测试清理相互干扰。Broker 的键实际
是 topic 加 consumer group，因此只换 group 在协议上也足够；独立 topic 是更严格、确定性更高的测试边界。
集成测试不能因此把
`SET_MESSAGE_REQUEST_MODE` 暴露成生产 Consumer API。

## 取舍与非目标

- 移除 direct Pull 与 POP，是以低层便利换取两种一致的应用模型。
- Broker assignment 要求 Broker 支持 `QUERY_ASSIGNMENT`；客户端分配继续作为默认值，与官方 Java Remoting
  客户端一致。
- Broker POP request mode 是运维控制的服务端配置。
- 首版 Broker POP 不支持广播、顺序消费、batch ACK 和协议级 POP orderly 语义。
- LitePull 后台接收会比当前单次 Poll 实现占用更多并发 Broker 请求，但能消除跨队列队头阻塞，并与官方
  Consumer 模型一致。
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
