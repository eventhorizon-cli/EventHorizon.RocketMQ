# gRPC 消费模型：显式接收、自动分发与 LitePush

[English](../../en-US/grpc/consumer-model.md) | [简体中文](consumer-model.md)

gRPC 客户端连接的是 RocketMQ Proxy。它提供的 Consumer API 应反映 Proxy 实际支持的工作流，而不是照搬
经典客户端的全部名称。本项目因此将 gRPC 消费建模为三种明确的客户端角色：Simple、Push 和 LitePush。

## 背景

“PushConsumer”这个名字容易让人以为 Broker 会主动建立到应用的连接。实际不是这样：客户端先查询
分配，再自行发起 `ReceiveMessage` 长轮询，然后在本地将收到的消息分发给 handler。网络方向和失败边界
依然由客户端控制。

另一类常见误解是把显式队列/位点 Pull 视作 gRPC 的必备替代品。这样的流程依赖 Proxy 提供成套 Pull 与
位点服务端 RPC；当目标 Proxy 没有可用实现时，保留一个看似对等的公开 API 只会让应用在部署后失败。

## 决策

gRPC Project 公开以下 Consumer 角色：

| 角色 | 由谁驱动接收 | 适用场景 |
| --- | --- | --- |
| `IGrpcSimpleConsumer` | 应用调用 `ReceiveAsync` | 应用需要自行决定批量、确认、不可见时间和死信时机。 |
| `IGrpcPushConsumer` | 客户端后台循环 | 应用希望自动接收、并发/FIFO 分发、重试和死信处理。 |
| `IGrpcLitePushConsumer` | 客户端后台循环与 Lite 订阅同步 | 使用 LiteTopic，且部署已配置 bind topic 和 Lite 能力。 |

不提供 `IGrpcPullConsumer`。这不是尚未完成的示例，也不是配置开关；该公开客户端角色已从仓库移除。
需要显式队列和位点 Pull 语义时，使用经典
[`IRemotingPullConsumer`](../../../src/EventHorizon.RocketMQ.Remoting/Consumer/Pull/IRemotingPullConsumer.cs)。
如果连接目标必须是 Proxy，则应根据工作流选择 Simple 或 Push，而不是假定 gRPC Pull 可以由客户端单独
补齐。

已签入的
[`service.proto`](../../../src/EventHorizon.RocketMQ.Grpc/Protocol/Protos/apache/rocketmq/v2/service.proto) 仍保留
`PullMessage`、`GetOffset`、`UpdateOffset` 和 `QueryOffset`，因为它是上游 wire 定义的一部分；这不等于本 Project
支持 gRPC Pull。内部
[`IRocketMQGrpcClient`](../../../src/EventHorizon.RocketMQ.Grpc/Protocol/IRocketMQGrpcClient.cs) 不调用这些 RPC，
也不存在围绕它们的公开角色。随附 RocketMQ 5.5.0 环境的 Apache Proxy 没有提供实现经典 Pull 所需的完整
Pull/位点行为。

## 如何工作

### 共享的内部接收引擎统一协议操作，角色保留各自的调度策略

内部
[`GrpcReceiveConsumerEngine`](../../../src/EventHorizon.RocketMQ.Grpc/Consumer/GrpcReceiveConsumerEngine.cs)
负责与 gRPC route、telemetry session、订阅集合和 `ReceiveMessage` 协作。每个 Consumer 角色从协议的
composition root 创建自己的 Engine 实例；它不是应用可共享的服务。

这样，Simple、Push 和 LitePush 可以复用接收、确认、不可见时间和订阅的底层协议操作，但不会共享
group、长轮询参数、handler 或停止逻辑。

### SimpleConsumer：应用控制确认边界

SimpleConsumer 的调用流程是：

1. 使用 `SubscribeAsync` 建立主题订阅和过滤表达式。
2. 调用 `ReceiveAsync` 获取一批 `GrpcMessageView`。
3. 在消息持久化或业务副作用完成后调用 `AckAsync`。
4. 工作时间超过不可见期时调用 `ChangeInvisibleDurationAsync`；不能继续处理时可调用
   `ForwardToDeadLetterQueueAsync`。

这种方式适合需要将确认与本地事务、批处理或自定义并发调度绑定的应用。它不替调用方处理异常、重复
投递或幂等性。收到的 `GrpcMessageView` 还可能标记为损坏消息，应用应先决定是否跳过或转入死信，而不是
盲目反序列化。

### PushConsumer：客户端发起接收，应用提供处理结果

PushConsumer 启动后大致执行如下循环：

```text
订阅与路由变更
        │
        ▼
查询分配 ──> 对已分配队列发起 ReceiveMessage 长轮询
                         │
                         ▼
                  有界消息缓存与 worker
                         │
                         ▼
              handler 返回 Success / Retry / DeadLetter
                         │
        ┌────────────────┼────────────────┐
        ▼                ▼                ▼
       确认             重试             转发死信
```

`ConsumeResult.Success` 表示可以确认；`Retry` 表示应交给重试策略；`DeadLetter` 表示转发到死信队列。
只有业务处理真正完成后才应返回 `Success`。网络超时、进程终止或确认失败仍可能造成重复投递，因此 handler
必须具备幂等性。

`MaxConcurrency` 控制 worker 并发度，`MaxCachedMessages` 与 `MaxCachedMessageBytes` 限制本地缓冲。开启
FIFO 分发时，客户端为同一消息组维持顺序尾链并阻塞同组后续消息；这是应用处理顺序的约束，不应被理解为
跨消费组、跨队列或跨消费者的全局顺序保证。

通过 `AddGrpcPushConsumer<THandler>(ServiceLifetime.Scoped, ...)` 注册 typed handler 时，每次处理都会创建
DI scope。详情见[依赖注入与生命周期](../architecture/dependency-injection-and-lifetimes.md)。

### LitePush：Push 调度器加上 Lite 订阅控制面

LitePush 复用 Push 的接收和分发机制，但消息来自 LiteTopic。它在普通 dispatcher 外增加
[`GrpcLiteSubscriptionManager`](../../../src/EventHorizon.RocketMQ.Grpc/Consumer/Lite/GrpcLiteSubscriptionManager.cs)，
用来同步 LiteTopic 订阅与起始位点。

部署端至少需要满足以下条件：

- 存储 LITE parent topic 的每个 Broker 都支持 Lite 消息，并已启用 `enableLmq=true` 和
  `enableMultiDispatch=true`；
- parent topic 以 `message.type=LITE` 创建，consumer group 配置 `lite.bind.topic=<parent-topic>`；
- 使用支持 `SyncLiteSubscription` RPC 的 Proxy；
- 应用使用 `SubscribeLiteAsync`/`UnsubscribeLiteAsync` 管理 LiteTopic，而不是将它当作普通 topic。

`SyncLiteSubscription` 是订阅控制 RPC：客户端借此向 Proxy 同步 consumer group、bind topic、LiteTopic 集合和可选
起始 offset 策略，它不传输消息。LitePush 不是 gRPC Pull 的替代名称，仍是客户端发起长轮询的自动分发模型。
RocketMQ 5.5.0 应使用独立运行的 cluster-mode Proxy（`mqproxy -pm cluster`）；Broker-integrated Proxy
（`mqbroker --enable-proxy`）没有实现该 RPC。独立 Proxy 可以与 Broker 共用一个容器或 Compose service，
不需要另建一套 Compose 环境。

### 生命周期与订阅变更

当通过 Generic Host 注册角色时，对应 hosted service 会启动和停止 Consumer。SimpleConsumer 和 PushConsumer
使用 `SubscribeAsync`、`UnsubscribeAsync`；LitePush 使用 `SubscribeLiteAsync`、`UnsubscribeLiteAsync`。这些
订阅变更都会通知接收循环重新协调，而不是重新注册一个 DI 服务。

对于没有 Generic Host 的程序，调用方必须在解析 Consumer 后显式调用 `StartAsync` 和 `StopAsync`。不要在
业务代码里反复创建 Consumer：重复的 client identity、长轮询和订阅状态会使问题难以诊断。

## 取舍与约束

**Push 不等于服务端网络 Push。** 它保留了熟悉的 handler 体验，但依赖客户端发起的长轮询。因此网络、
超时、取消和本地 backpressure 仍是调用方的重要运维参数。

**Simple 提供控制，不自动提供调度。** 它适合需要精确确认边界的应用，但调用方要自己管理循环、并发和
错误策略。

**不存在无条件可用的 gRPC Pull 开关。** 缺失的是服务端 Proxy 路径，不是一个可以通过 SDK 重试、超时或
序列化配置修复的客户端细节。对显式 queue/offset 工作流，classic Remoting Pull 是当前明确的协议选择。

**Server capability 是部署前提。** Lite、死信、过滤、FIFO、事务和最小长轮询参数均可能依赖 Broker/Proxy
版本与配置。请以部署环境和[gRPC Project 指南](../../../src/EventHorizon.RocketMQ.Grpc/README.zh-CN.md)为准，并从
[可运行样例索引](../../../samples/README.zh-CN.md)开始验证。

## 延伸阅读

- [协议边界](../architecture/protocol-boundaries.md)
- [依赖注入与生命周期](../architecture/dependency-injection-and-lifetimes.md)
- [Remoting 传输与客户端角色](../remoting/transport-and-client-roles.md)
- [本地与集成测试](../testing/local-and-integration-testing.md)
