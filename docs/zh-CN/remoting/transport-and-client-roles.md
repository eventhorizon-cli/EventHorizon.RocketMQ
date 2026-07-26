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
  唯一的 `IRemotingPushMessageHandler.HandleAsync` API 接收 `IReadOnlyList<RemotingMessageView>`。
  `BatchSize` 限制单次长轮询向 Broker 请求的消息数；`ConsumeMessageBatchSize` 独立限制单次 handler
  调用收到的消息数，默认值为 `1`。来自同一个 Broker 物理队列、且不带 `MessageGroup` 的并发消息可以合并为
一批，多个批次可在 `MaxConcurrency` 范围内并行处理；带 `MessageGroup` 的 FIFO 投递和 `ConsumeOrderly`
投递保持单消息、串行处理。一项 `ConsumeResult` 会应用于该批中的每条消息。

`ConsumeTimeout` 默认值为 15 分钟，仅适用于并发集群、非 FIFO 批次。超时后，客户端会取消 handler token，并为
整批消息请求 Broker 重新投递。它不能强制终止忽略取消信号的应用代码，因此 handler 必须响应该 token，并保持幂等。
延迟返回的结果会被忽略，并可能与重新投递重叠。FIFO 和顺序投递为保持顺序保证而不会进入此恢复路径。

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
