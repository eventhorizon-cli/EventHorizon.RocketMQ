# classic Remoting：传输、路由与客户端角色

[English](../../en-US/remoting/transport-and-client-roles.md) | [简体中文](transport-and-client-roles.md)

classic Remoting 不是“gRPC 的低级实现”。它保留 RocketMQ NameServer/Broker 协议的 route、回调、队列和
消费模型，因此需要独立的传输实现与角色边界。应用选择它时，也是在选择直接可达的 NameServer 和 Broker
网络拓扑。

## 背景

Remoting 客户端先连接 NameServer 获取 topic route，然后对 route 中的 Broker 发起请求。一次应用调用可能
涉及多个端点：NameServer、master/slave Broker，以及在请求-响应场景中由 Broker 发起的经典回调。

过去实现依赖 Bedrock Framework 的 Socket transport。当前实现改为内置的 `System.IO.Pipelines` 和
`Socket`，把协议 framing、连接复用、取消、故障处理和 TLS 控制保留在本仓库中。这样减少了生产依赖，同时
要求客户端实现明确处理经典 wire protocol 的细节。

## 决策

`EventHorizon.RocketMQ.Remoting` 通过 `AddRocketMQRemoting` 创建默认客户端注册，并公开经典协议自己的
角色：

| 角色 | 主要职责 | 是否有后台生命周期 |
| --- | --- | :---: |
| `IRemotingAdmin` | 只读路由、位点、时间和物理消息查询 | — |
| `IRemotingProducer` | 发送、队列选择、批量、事务、撤回与请求-响应 | ✅ |
| `IRemotingPullConsumer` | 应用显式选择队列和位点来拉取、确认/提交 | ✅ |
| `IRemotingLitePullConsumer` | 订阅后的自动分配或 `AssignAsync` 手工分配、客户端位点、pause/resume/seek | ✅ |
| `IRemotingPopConsumer` | POP receipt、确认和可见期续租 | ✅ |
| `IRemotingPushConsumer` | 自动批量消费、重试、死信、重平衡与经典 Broker 兼容行为 | ✅ |

“后台生命周期”表示通过 Generic Host 注册时会有 hosted service 启动/停止该角色。Admin 是按需查询 API，
不启动独立循环。

## 如何工作

### 1. NameServer 路由是发送和消费的起点

`NamesrvAddr` 可以包含多个以分号分隔的地址。内部
[`TopicRouteService`](../../../src/EventHorizon.RocketMQ.Remoting/Protocol/Route/TopicRouteService.cs)
按 topic 缓存 route，并在缓存失效时向 NameServer 请求新 route。并发的同一 topic 刷新会合并到一个门控操作；
在刷新失败而旧缓存尚有效时，客户端可以继续使用旧 route。

这解释了一个重要部署事实：连接 NameServer 成功不表示消息路径一定可用。NameServer 返回的 Broker 地址
必须从应用网络可达，且地址必须与 Broker 实际监听和 TLS 配置相符。

### 2. 内置传输管理连接与命令关联

[`RemotingClient`](../../../src/EventHorizon.RocketMQ.Remoting/Protocol/RemotingClient.cs) 以 endpoint 为键缓存
连接。每个请求获得 opaque ID，未完成请求存放在并发映射中；接收循环根据响应 opaque ID 完成对应任务。
连接失效时，客户端取消相关等待、移除不可用连接，并让上层的路由/重试策略决定下一步。

它还维护入站请求 handler 注册。这是经典 Broker 回调和 Producer 请求-响应等能力所需的基础设施，而不需要
把 Producer 和 Consumer 合并成一个公共接口。

底层
[`RemotingConnection`](../../../src/EventHorizon.RocketMQ.Remoting/Protocol/RemotingConnection.cs) 使用
`Socket` 建立 TCP 连接，以 `PipeReader`/`PipeWriter` 读取和写入完整帧，并分别串行化读、写操作。序列化器在
写入前验证完整帧大小，接收侧也拒绝超出 `MaxRemotingFrameSize` 的帧；这既避免畸形输入造成无界缓冲，也让
错误能以明确的 I/O 异常暴露给上层。

### 3. TLS 与 ACL 在协议边界处理

启用 `UseTLS` 时，连接在 TCP 之上建立 `SslStream`。`ConfigureLegacySslOptions` 可配置私有根、mTLS、
SNI、吊销检查或协议选择；该回调可能并发执行，且每条连接都会得到新的配置对象。

同时设置 `AccessKey` 与 `AccessSecret` 后，
[`LegacyAclSigner`](../../../src/EventHorizon.RocketMQ.Remoting/Protocol/LegacyAclSigner.cs) 会对 NameServer 和
Broker 请求签名。注册阶段强制两个值成对出现，防止“只有一半凭据”的失败拖到网络请求时才暴露。

### 4. 不同 Consumer 代表不同确认模型

这些角色名称都包含“消费”，但确认与所有权完全不同：

- **Pull**：调用方决定 `RemotingPullMessageQueue`、起始 offset、批量与处理节奏。适合需要明确 queue/offset
  语义的工作流；成功处理后才应提交 offset。
- **LitePull**：订阅模式使用自动分配；也可以使用 `AssignAsync` 手工指定队列。两种模式互斥，客户端维护
  offset，并提供 pause、resume 与 seek。当前实现仅支持集群消费，不能把它当作广播 Consumer。
- **POP**：Broker 返回 receipt；应用使用 receipt 确认，并在长时间处理时请求更改不可见期。receipt 不是
  可跨消息、跨队列复用的 token。
- **Push**：客户端仍以拉取/长轮询为基础，但加上经典 group、心跳、Broker 回调、重平衡和可选队列锁的
  兼容流程。它支持 cluster/broadcast、并发批量或 FIFO 分发，以及队列有序消费所需的经典锁定行为。
  唯一的 `IRemotingPushMessageHandler.HandleAsync` API 接收 `IReadOnlyList<RemotingMessageView>` 和
  `RemotingPushConsumeContext`。`BatchSize` 限制单次长轮询向 Broker 请求的消息数；`ConsumeMessageBatchSize`
  独立限制单次 handler 调用收到的消息数，默认值为 `1`。来自同一个 Broker 物理队列、且不带 `MessageGroup`
  的并发消息可以合并为一批，多个批次可在 `MaxConcurrency` 范围内并行处理；带 `MessageGroup` 的 FIFO 投递和
  `ConsumeOrderly` 投递保持单消息、串行处理。

  `Success` 默认确认整个批次。并发、非 FIFO handler 可以在返回 `Success` 前把 `AckIndex` 设为最后一条已接受
  消息的从零开始索引；客户端会确认这个连续前缀，并只重试尾部消息。无论 `AckIndex` 为何，`Retry` 和
  `DeadLetter` 都会应用到批次中的每条消息。context 还提供 `DelayLevelWhenNextConsume`，用于未确认的尾部：
  `0` 交由 Broker 决定重试时间，正值选择 RocketMQ 延迟级别，负值请求直接进入死信队列。广播模式没有 Broker
  重试或死信路径，因此未确认的尾部消息会被跳过。

`ConsumeTimeout` 默认值为 15 分钟，仅适用于并发集群、非 FIFO 批次。超时后，客户端会取消 handler token，并为
整批消息请求 Broker 重新投递。它不能强制终止忽略取消信号的应用代码，因此 handler 必须响应该 token，并保持幂等。
延迟返回的结果会被忽略，并可能与重新投递重叠。FIFO 和顺序投递为保持顺序保证而不会进入此恢复路径。

#### Push group membership、Rebalance 与连接所有权

一个 `AddRocketMQRemoting` 注册可以添加多个 Push Consumer。每个实例分别拥有自己的 options、逻辑 client
identity、Consumer Engine、typed handler 绑定、分配状态和 hosted 生命周期。不同 group 的 Consumer 可以共享
NameServer 发现、route 和物理 `RemotingClient`；同一 group 的重复配置成员则使用不同物理 Client：经典 Broker
的消费组表以连接 channel 作为成员键，同一 channel 上另一 client ID 的 heartbeat 会替换已有条目。

分配给活跃 Push 或 LitePull 成员的每个物理 Client 拥有一个 `RemotingRebalanceService`，因此也只注册一个
`NotifyConsumerIdsChanged` handler：

```text
Broker 成员变化
    -> RemotingClient 入站请求循环
    -> RemotingRebalanceService 按 group 查找
    -> participant 写入可合并的唤醒信号
    -> route 刷新 / heartbeat / Consumer ID 查询
    -> 队列分配与 assignment 收敛
```

入站 handler 只记录唤醒，不会在接收循环上执行 route 或 Broker I/O。每个 group 都有独立、单飞的 participant，
因此一个 group 阻塞或失败不会串行阻塞其他 group。失败会在 1 秒后重试，最长 20 秒的周期兜底会修复丢失的通知。
共享 heartbeat、成员查询和 unregister 操作归 `RemotingConsumerGroupSession` 所有；顺序消费的队列 lock/unlock
命令归 `RemotingPushQueueLockManager` 所有。

物理 `RemotingClient` 只负责传输，本身没有 group identity。每个活跃的 Push 或 LitePull session 都会在初始和周期
协调时，经由分配给自己的 Client 发送 heartbeat；LitePull 在拥有订阅或手工分配队列后成为活跃成员。因此共享 Client
可以保持多个不同 group 的活跃状态，而同组的隔离成员会分别维护自己的 membership。直接 Pull 和 POP 是显式 API，
有意不创建后台消费组成员关系或 heartbeat 循环；Producer 维护自己的 producer heartbeat 生命周期。

通过 `AssignAsync([])` 清除 LitePull 手工分配会立即结束该活跃成员关系：session 会向已知 Broker 注销，同时保留这些
端点，以便 shutdown 时可重试一次失败的注销。

Consumer 生命周期就绪与初始位点就绪是两件事。`StartAsync` 会创建生命周期并启动 assignment 收敛，但并发队列接收器的
初始位点仍会异步解析。对于使用默认末尾位置的新 group，若消息在接收器解析首个 `maxOffset` 前写入，该消息可能位于该
位置之前，因此有意不属于该 group 的消费范围。这是经典 Java `CONSUME_FROM_LAST_OFFSET` 的行为，不是 Rebalance 或
typed handler 的故障。需要精确切换边界的应用应选择起始或时间戳位置、保留已提交的 group 位点，或建立显式的就绪握手。
集成测试不能依赖默认末尾位置来断言跨越此边界的消息送达。

对于顺序 Push，初始 lock 被拒绝或 lock 续期失败都会使本次收敛保持未完成状态，participant 会重试。只有 Broker
授予队列锁后，Consumer 才会开始分发该队列。

同一 Push group 的成员必须使用完全相同的 topic/filter 订阅、集群或广播模式、顺序消费模式和初始位点；并发度、
超时与 typed handler 仍属于实例本地配置。这些是 group 范围的规则，但注册层只能对同一进程、同一客户端注册中
可见的成员做 fail-fast。经典 heartbeat 不携带顺序消费标志，因此其他进程必须由部署配置保证一致。这与官方 Java
和 Go 客户端的协议行为一致：group membership 对 Broker 可见，而顺序锁定是客户端本地的 Rebalance 决策。

一旦同组的另一成员已在本地注册，单个 Remoting Push 或 LitePull 成员就不能通过 `SubscribeAsync` 或
`UnsubscribeAsync` 变更订阅。应更新完全一致的 group 范围订阅集合后一起重启本地成员；跨进程成员仍需由部署协调。

Docker IT 同时覆盖一个注册中的两个成员和两个独立操作系统进程。跨进程 case 会分别运行并发与顺序模式，验证
assignment 互斥且并集完整，再正常停止一个成员，并确认另一个成员在周期兜底运行前由通知触发接管全部队列。另有
一个并发模式 case 会强制终止成员。顺序模式的强制终止不使用短超时断言，因为恢复边界由 Broker 队列锁 lease 过期
决定，而不是客户端通知。

#### 并发 Push 分发与位点安全

并发模式下，每个已分配的 Broker 物理队列都有一个客户端侧 `ProcessQueue`。`ProcessQueue` 保存本地已拉取的
消息及其消费状态；它不是另一个 Broker 队列，也不会创建任何服务端资源。拉取位点可以独立于 handler 完成而
继续推进。`MaxCachedMessages` 限制等待首次分发的新拉取消息准入，已经交给 handler 的批次不会继续占用该容量。
处置尚未解决，或 FIFO 消息存在未完成的前驱（包括位于另一个物理队列的前驱）时，该 `ProcessQueue` 中已经缓存
的消息会释放准入配额，并暂停该队列的新准入，直到可以继续推进。因此被阻塞的队列不会独占健康队列所需的容量，
该设置也不是进程内所有驻留消息的严格总数上限。

每个已就绪的 `ProcessQueue` 最多以一个合并后的通知进入 ready queue。共享的逻辑消费循环每次从一个就绪队列
取得一个 handler 批次；若仍有工作，就把该队列放回队尾。这样会在活跃物理队列之间进行公平轮询，避免一个热点
队列占满整个分发路径。`MaxConcurrency` 是所有已分配队列共享的总并发度。这些循环是由 .NET ThreadPool 调度的
异步任务，并非独占或绑定固定线程的 Consumer 线程。

每个 `ProcessQueue` 还维护自己的连续完成水位。后面的批次即使先完成，也不能让持久化位点越过前面尚未解决的
消息；处理较快的队列也不能推进另一个队列的位点。retry/dead-letter 处置失败时，该消息保持未解决，并在
`RetryDelay` 后仅重试处置而不再次调用应用 handler；位点持久化失败也会重试，且不会跨过缺口。分配被撤销时，
对应 `ProcessQueue` 会被标记为 dropped，因此延迟返回的 handler 结果不能修改新的分配状态或提交其位点。

`ProcessQueue` 的名称以及每个物理队列一份状态的职责沿用了 RocketMQ 官方 Java 客户端的概念。Java 的并发路径
会从刚拉取的消息列表创建 consume request；当前实现使用合并 ready queue，并在每轮从 `ProcessQueue` 取一个批次，
这是 .NET 的调度设计，并非对 Java 数据路径的逐项复制。

无论选择哪种角色，至少一次交付仍要求业务处理幂等。发送重试、连接中断、可见期到期或确认失败都可能导致
重复投递。

### 5. Producer 与 Admin 也保持经典语义

`IRemotingProducer` 支持经典的指定队列、队列 selector、批量、单向发送、事务、撤回和请求-响应。普通
`SendAsync` 的 `RemotingSendResult.Status` 必须由调用方检查：Broker 可能返回磁盘或 slave 相关状态而不抛出
异常。`SendOnewayAsync` 返回只表示请求已写入连接，并不表示 Broker 已持久化消息。

`IRemotingAdmin` 保持只读边界：它用于发现队列、查询 offset/时间，并按 `OffsetMessageId` 查看物理消息。
这是一项适合运维、诊断和审计工具的 API；它不是 topic、group 或 ACL 的写管理面，也不应与业务 Producer
职责混在一起。可执行的配置与查询示例见
[Remoting Admin sample](../../../samples/remoting/Admin/README.zh-CN.md)。

## 取舍与约束

**网络拓扑比 Proxy 路径更直接。** Remoting 可以使用经典能力，但应用必须能够抵达 NameServer 和 route 中
的 Broker。这在容器、NAT、跨 VPC、Broker 广告地址或 TLS SNI 配置不一致时尤其重要。

**内置传输减少依赖，但不是抽象掉协议。** `System.IO.Pipelines` 负责高效缓冲，协议帧、opaque 关联、
ACL、重连和 route 仍由本项目维护。修改这些代码时应配合 framing、并发和 Docker 集成测试，而不是仅看
单个发送测试。

**经典类型应留在 Remoting Project。** `RemotingMessageQueue`、`RemotingSendStatus`、POP receipt 和
route 数据都有经典协议语义。把它们放进第三个通用 Project，会让 gRPC 用户错误依赖无法实现的能力。

**服务端特性仍需验证。** SQL 过滤、POP、Lite、优先级、定时撤回、事务、请求-响应回调和队列锁均可能依赖
Broker 版本、Broker 配置与网络策略。请使用[Remoting Project 指南](../../../src/EventHorizon.RocketMQ.Remoting/README.zh-CN.md)
和[可运行样例索引](../../../samples/README.zh-CN.md)逐项确认。

## 延伸阅读

- [协议边界](../architecture/protocol-boundaries.md)
- [依赖注入与生命周期](../architecture/dependency-injection-and-lifetimes.md)
- [gRPC 消费模型](../grpc/consumer-model.md)
- [本地与集成测试](../testing/local-and-integration-testing.md)
