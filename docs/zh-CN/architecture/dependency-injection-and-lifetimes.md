# 依赖注入与生命周期：客户端注册是明确的客户端边界

[English](../../en-US/architecture/dependency-injection-and-lifetimes.md) | [简体中文](dependency-injection-and-lifetimes.md)

RocketMQ 客户端不是一次 HTTP 调用：它会维护路由、连接、心跳、长轮询和消息处理循环。把这些对象交给
应用随意 `new` 出来，通常会导致重复连接、无法优雅停止，或把 scoped 数据库上下文错误地捕获进
singleton Consumer。本项目因此把“如何构造”和“何时运行”放进协议专用的 DI 注册层。

## 背景

一个进程可能同时连接多个 RocketMQ 集群，也可能在同一集群中同时运行 Producer、PushConsumer 和
Admin。它们可以共享部分配置，却不能共用含糊的“客户端生命周期”：不同角色有各自的 options、后台任务
和身份标识。

此外，Push 的 handler 往往依赖 scoped 服务，例如数据库上下文、事务或租户上下文。长寿命 Consumer
不能直接持有这样的 handler 实例。

## 决策

每个协议 Project 在根命名空间提供自己的注册入口：

```csharp
services
    .AddRocketMQGrpc("orders", options => options.Endpoint = "proxy:8081")
    .AddGrpcProducer()
    .AddGrpcPushConsumer<OrderHandler>(ServiceLifetime.Scoped, ConfigureConsumer);

services
    .AddRocketMQRemoting("analytics", options => options.NamesrvAddr = "nameserver:9876")
    .AddRemotingAdmin()
    .AddRemotingLitePullConsumer(ConfigureLitePullConsumer);
```

这个 fluent builder 不是共享的 transport selector。`AddRocketMQGrpc` 和 `AddRocketMQRemoting` 分别建立
独立的配置空间；`"orders"` 与 `"analytics"` 分别是客户端注册的 `registrationName`。该
`registrationName` 同时作为 named options 的名称和 keyed service 的 key。

核心规则如下：

- 每个客户端注册最多添加一个 Producer，Remoting 还最多添加一个 Admin；同一个 Consumer 扩展方法则可在
  同一 builder 上重复调用，且不改变现有方法签名。
- 对外角色接口按协议注册为 singleton；keyed 客户端注册使用 keyed singleton，默认客户端注册使用普通
  singleton。
- 每次 Consumer 注册都会获得独立的 options、逻辑 client ID、Engine、handler 绑定和 hosted 生命周期；
  Engine 不注册为可跨 Consumer 共享的
  全局服务。
- 所有已注册的 Producer 和 Consumer 角色都由配套 hosted service 随 Generic Host 启停。Admin 等没有后台循环的
  角色不需要单独的 hosted service。
- 框架不会在注册时构造 `ServiceProvider`。所有依赖都由最终应用容器创建和验证。

## 如何工作

### 1. 注册把配置和角色固定下来

`AddRocketMQGrpc`/`AddRocketMQRemoting` 先注册 named options，并验证 endpoint、ACL 成对配置及时间参数。
随后角色扩展方法写入角色元数据、角色专用 options 和协议基础设施。

gRPC 的实现可从
[`GrpcRocketMQServiceCollectionExtensions`](../../../src/EventHorizon.RocketMQ.Grpc/GrpcRocketMQServiceCollectionExtensions.cs)
和
[`GrpcRocketMQBuilderExtensions`](../../../src/EventHorizon.RocketMQ.Grpc/GrpcRocketMQBuilderExtensions.cs)
查看；Remoting 对应实现位于
[`RemotingRocketMQServiceCollectionExtensions`](../../../src/EventHorizon.RocketMQ.Remoting/RemotingRocketMQServiceCollectionExtensions.cs)
与
[`RemotingRocketMQBuilderExtensions`](../../../src/EventHorizon.RocketMQ.Remoting/RemotingRocketMQBuilderExtensions.cs)。

注册阶段会为每个角色实例生成一个逻辑 client 名称。这样同一客户端注册中的 Producer 与多个 Consumer 即使
连接同一服务端，也有可区分的客户端身份、配置和生命周期。

同一客户端注册不等于每个 Consumer 都创建一套物理连接。gRPC Consumer 会共享 HTTP/2 channel，但仍使用
不同的逻辑 client ID 和请求元数据。Remoting 会在经典协议身份允许时共享 NameServer 发现、路由状态和
Broker 连接；不同消费组可共享连接，但同一 Push 或 LitePull 消费组下重复配置的成员必须使用不同 Broker
连接：经典 Broker 的消费组表以连接 channel 作为成员键，同一 channel 上另一 client ID 的 heartbeat 会替换
已有条目。

分配给活跃 Push 或 LitePull 成员的每个物理 Remoting Client 都拥有一个内部 `RemotingRebalanceService`。共享该
Client 的不同 group 会共用唯一的 `NotifyConsumerIdsChanged` 请求处理器和周期兜底，但每个 group 都有独立、单飞且
可合并信号的 participant，因此一个 group 阻塞或失败不会拖住其他 group。Broker 回调只发送唤醒信号；路由刷新、
成员查询和队列收敛都在入站请求循环之外执行。同一 group 的重复成员使用隔离的物理 Client，因此也分别拥有独立的
Rebalance service。

heartbeat 属于逻辑 group session，而不属于物理 `RemotingClient`：活跃的 Push 或 LitePull 会在初始和周期协调时，
经由分配给自己的 Client 发送心跳。LitePull 在拥有订阅或手工分配队列后开始这项维护。因此共享 Client 可以维护多个
不同 group，而同组的隔离成员会分别维护自己的 Broker membership。低层 PULL 与 POP 操作只由 LitePull 和 Push
内部使用，不会形成额外的公开角色或 group session；Producer 则维护自己的 producer heartbeat 生命周期。

Push 与 LitePull 各自在每次启动的 run 中保留队列分配决策。两者共享的经典消费组协议 wire 操作由无状态的
`RemotingConsumerGroupClient` 负责；由 run 独占的 `RemotingConsumerGroupSession` 保存已知 Broker、subscription
version、Rebalance registration 以及 unregister 边界。顺序 Push 的队列锁 wire 操作仍由
`OrderlyPullQueueLockClient` 负责。这样，transport I/O、Rebalance 触发、membership 生命周期和角色专属的
队列收敛各自都有明确的所有者。

### 2. 角色拥有自己的 Engine

gRPC 的 Push、Simple 与 LitePush 角色会构造内部
[`IGrpcReceiveConsumerEngine`](../../../src/EventHorizon.RocketMQ.Grpc/Consumer/IGrpcReceiveConsumerEngine.cs)；
Remoting 的 LitePull 与 Push 角色会分别构造内部
[`IRemotingConsumerEngine`](../../../src/EventHorizon.RocketMQ.Remoting/Consumer/IRemotingConsumerEngine.cs)。

Engine 是角色内部的协议协作对象，不会作为应用可注入的公共服务，也不会在多个 Consumer 之间共享。gRPC
Engine 负责其协议接收生命周期；Remoting Engine 只负责组合当前角色的 route、PULL、offset 与 send-back 能力，
group membership、assignment、接收循环和有序关闭则由该角色在每次启动的 run 中负责。释放角色时，这两部分也会
一并释放。这样既避免 Remoting wire engine 继续维护一套空的生命周期，也不会让一个角色停止另一个角色的循环。

### 3. Host 负责启动和停止

已注册的 Producer 和 Consumer 角色会注册对应的 hosted service。Host 启动时调用角色的 `StartAsync`，停止时调用
`StopAsync`，随后由 DI 容器负责释放资源。

因此，通常应在应用服务构造函数中直接注入协议接口，而不是手工调用 `new`：

```csharp
public sealed class OrderPublisher(IGrpcProducer producer)
{
    public Task PublishAsync(Message message, CancellationToken cancellationToken) =>
        producer.SendAsync(message, cancellationToken);
}
```

在不使用 Generic Host 的独立 `ServiceProvider` 中解析角色时，调用方必须显式调用 `StartAsync` 和
`StopAsync`；容器本身不会替应用启动后台工作。已经启动的 Generic Host 中，应用代码不应再次调用角色的这些
生命周期方法。

### 4. 多 Consumer 与 keyed service

默认客户端注册中的多个同类型 Consumer 可通过 `IEnumerable<TConsumer>` 解析；keyed 客户端注册则使用
`GetKeyedServices<TConsumer>(registrationName)` 取得全部实例。Microsoft DI 的普通单实例解析会返回最后注册的
Consumer，因此除非明确需要该行为，注册多个 Consumer 的代码应依赖集合。

普通 PushConsumer 的同一消费组必须为每个 topic 配置完全一致的订阅和过滤表达式；不一致的注册会在 Host
启动前被拒绝。组合语义如下：

| 注册组合 | 行为 |
| --- | --- |
| 相同 group、相同订阅 | 合法的集群副本，队列分配会在实例间负载均衡。 |
| 不同 group、相同 topic | 扇出消费，每个 group 都收到该 topic 的完整消息流。 |
| 不同 group、不同 topic | 相互隔离地消费。 |
| 相同 group、不同 topic 或 filter | 普通 PushConsumer 的非法组合；应统一订阅或拆分 group。 |

经典 Remoting 还要求同组 Push 实例使用相同的 assignment mode、集群/广播模式、顺序消费模式和初始位点，同组
LitePull 实例使用相同的初始位点。Push 与 LitePull 会向经典 Broker 上报不同的消费类型，因此不能共用 group。
并发度、超时和 handler
等实例本地配置仍可不同。这些是 group 范围的配置规则，但注册层只能对同一进程、同一客户端注册中声明的成员做
fail-fast。其他进程中的成员仍需由部署配置保证一致；经典 Broker heartbeat 不会携带包括顺序消费模式在内的所有
客户端本地设置。

该普通 topic 一致性规则不适用于 gRPC LitePush 的 LiteTopic 集合。同一 Lite 消费组的成员可在该组的 bind
topic 下持有不同 LiteTopic 集合，但必须使用相同的 bind topic；最终仍以 Proxy 对消费组和 bind topic 的校验为准。

当同一客户端注册中已有同组普通 gRPC Push 成员时，单个成员不能通过 `SubscribeAsync` 或 `UnsubscribeAsync`
变更 group 范围的订阅。Remoting Push 和 LitePull 也有相同的本地保护。应配置新的、完全一致的订阅集合后一起
重启本地成员；其他进程中的成员仍需由部署协调。gRPC LitePush 的 LiteTopic 变更不属于这条普通 topic 规则。

现有方法签名可以分别配置每个实例：

```csharp
var rocketMQ = services.AddRocketMQGrpc(options => options.Endpoint = "proxy:8081");

rocketMQ.AddGrpcPushConsumer<OrderHandler>(ServiceLifetime.Scoped, options =>
{
    options.GroupName = "orders-consumer";
    options.Subscribe("orders");
});

rocketMQ.AddGrpcPushConsumer<AuditHandler>(ServiceLifetime.Scoped, options =>
{
    options.GroupName = "audit-consumer";
    options.Subscribe("audit");
});
```

同一 Host 需要不同连接配置或多个 Producer/Admin 实例时，使用 keyed 客户端注册，并在使用方按
`registrationName` 选择对应的 keyed service：

```csharp
public sealed class AuditPublisher(
    [FromKeyedServices("audit")] IRemotingProducer producer)
{
}
```

`registrationName` 不是 Broker 的功能，也不是消息路由标签；该 `registrationName` 同时作为本地 DI 容器中
keyed service 的 key，用于选择已配置的客户端角色。默认客户端注册使用普通构造函数注入，不需要
`FromKeyedServices`。

### 5. typed handler 的 scope 以“单次处理”为边界

Push 和 LitePush 的自动分发必须注册实现 `IGrpcPushMessageHandler` 或
`IRemotingPushMessageHandler` 的 typed handler。handler 生命周期由调用方选择：

| handler 生命周期 | 实例与 scope 行为 |
| --- | --- |
| `Singleton` | 直接复用一个 handler；它只能依赖 singleton 或安全的工厂/抽象。 |
| `Scoped` | 每次 handler 调用创建一个异步 DI scope，并从该 scope 解析 handler。 |
| `Transient` | 同样为每次 handler 调用创建一个异步 DI scope，再取得新的 handler。 |

因此，scoped handler 可以通过构造函数安全地依赖数据库上下文等 scoped 服务；但 singleton handler 不能捕获
scoped 依赖。启用容器 scope 校验时，后者会在容器构建或解析阶段暴露为生命周期错误，而不是在长轮询运行后才随机失败。

gRPC Push 和 LitePush 会为每条消息调用 handler。Remoting Push 则通过唯一的
`IRemotingPushMessageHandler` API，在每次批量回调中传入 `IReadOnlyList<RemotingMessageView>` 和
`RemotingPushConsumeContext`；其 `ConsumeMessageBatchSize` 默认值为 `1`，因此除非显式配置，否则仍是
单消息投递。对于并发、非 FIFO 批次，handler 可以返回 `Success` 并设置 `AckIndex`，以确认连续前缀并重试
尾部；`Retry` 和 `DeadLetter` 仍是整批结果。

对于非 FIFO 的 gRPC Push 和 LitePush 消息，`ConsumeTimeout` 到期后会取消 handler token 并请求重新投递。handler
执行期间的 receipt 续期由 `AutoRenew` 请求的 Proxy 负责，不属于 handler scope，也不由客户端定时器承担。
dispatcher 会忽略延迟返回的结果并释放消费循环的并发槽位，但无法终止应用代码。超时调用可能与重新投递重叠，因此必须保持幂等。
对应 typed handler 的异步 scope 会一直存活到该调用返回，因此 handler 代码仍在运行时不会提前释放 scoped 依赖。FIFO message group
为了保持顺序而不会应用此机制。

对于并发集群、非 FIFO 的 Remoting 批次，`ConsumeTimeout` 到期后会取消 handler token，并请求 Broker 重新投递。
对于 POP 投递，handler 执行期间不会续租 receipt；客户端会在 handler 处理前和结算前检查固定不可见 deadline。
过期结果会被忽略且不发送结算。`Retry` 结果只发起一次带 `suspend=false` 的
`CHANGE_MESSAGE_INVISIBLETIME` 请求；对于不确定的失败，不会使用旧 receipt 重试。ACK，包括死信转发后的 ACK，
只在原始 deadline 内重试。延迟返回的结果会被忽略，并可能与重新投递重叠，因此 handler 仍需自行配合取消并保持
幂等；运行时不能强制停止应用代码。FIFO 和顺序投递为了保持顺序保证，不应用此机制。

对应的 handler scope 逻辑位于
[`GrpcPushMessageHandlerFactory`](../../../src/EventHorizon.RocketMQ.Grpc/Consumer/Push/GrpcPushMessageHandlerFactory.cs)
和
[`RemotingPushMessageHandlerFactory`](../../../src/EventHorizon.RocketMQ.Remoting/Consumer/Push/RemotingPushMessageHandlerFactory.cs)。

## 取舍与约束

**单例角色并不等于单线程。** Consumer 本身是长寿命对象，但可以按 `MaxConcurrency` 并发调用 handler。
handler 及其依赖必须满足所选生命周期和并发模型。

**一个客户端注册可以包含多个 Consumer。** Producer 仍只能有一个，Remoting Admin 也只能有一个。需要另一套
连接配置或另一个 Producer/Admin 时，应创建不同的 keyed 客户端注册。

**不要把 ServiceProvider 当作业务依赖。** 注册层使用 provider 只是 composition root 的一部分；业务
代码应接受接口和普通依赖。运行时再从 `IServiceProvider` 查找 RocketMQ 客户端会隐藏连接目标和生命周期。

**启动不代表资源一定已完全就绪。** Host 已启动的含义是客户端循环已开始；topic、consumer group、ACL、
Proxy/Broker 能力和外部网络仍应由部署与健康检查保证。

## 延伸阅读

- [协议边界](protocol-boundaries.md)
- [gRPC 消费模型](../grpc/consumer-model.md)
- [classic Remoting Consumer 模型](../remoting/consumer-model.md)
- [Remoting 传输与客户端角色](../remoting/transport-and-client-roles.md)
- [本地与集成测试](../testing/local-and-integration-testing.md)
