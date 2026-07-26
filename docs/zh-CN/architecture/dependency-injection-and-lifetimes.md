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
    .AddRemotingPullConsumer(ConfigurePullConsumer);
```

这个 fluent builder 不是共享的 transport selector。`AddRocketMQGrpc` 和 `AddRocketMQRemoting` 分别建立
独立的配置空间；`"orders"` 与 `"analytics"` 分别是客户端注册的 `registrationName`。该
`registrationName` 同时作为 named options 的名称和 keyed service 的 key。

核心规则如下：

- 每个客户端注册中的同一角色只能注册一次，避免同一身份重复启动后台客户端。
- 对外角色接口按协议注册为 singleton；keyed 客户端注册使用 keyed singleton，默认客户端注册使用普通
  singleton。
- 每个 Consumer 角色在对应 builder 的组合根中创建一个私有 Engine；Engine 不注册为可跨角色共享的
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

注册阶段会为每个角色生成一个逻辑 client 名称。这样同一客户端注册中的 Producer 与 Consumer 即使连接同一
服务端，也有可区分的客户端身份、配置和生命周期。

### 2. 角色拥有自己的 Engine

gRPC 的 Push、Simple 与 LitePush 角色会构造内部
[`IGrpcReceiveConsumerEngine`](../../../src/EventHorizon.RocketMQ.Grpc/Consumer/IGrpcReceiveConsumerEngine.cs)；
Remoting 的 Pull、LitePull、POP 与 Push 角色会构造内部
[`IRemotingConsumerEngine`](../../../src/EventHorizon.RocketMQ.Remoting/Consumer/IRemotingConsumerEngine.cs)。

Engine 是角色内部的协作对象，负责协议收发、订阅或必要的后台协调。它不应作为应用可注入的公共服务，也
不应在多个 Consumer 之间共享。这样停止某个角色不会意外停止另一个角色的循环，配置选项、消费组和处理器
也不会相互覆盖。

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
`StopAsync`；容器本身不会替应用启动后台工作。

### 4. 多客户端配置与 keyed service

同一 Host 需要多个同类型角色时，使用 keyed 客户端注册，并在使用方按 `registrationName` 选择对应的 keyed service：

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

Push 和 LitePush 可接收委托，也可注册实现 `IGrpcPushMessageHandler` 或
`IRemotingPushMessageHandler` 的 typed handler。handler 生命周期由调用方选择：

| handler 生命周期 | 实例与 scope 行为 |
| --- | --- |
| `Singleton` | 直接复用一个 handler；它只能依赖 singleton 或安全的工厂/抽象。 |
| `Scoped` | 每次 handler 调用创建一个异步 DI scope，并从该 scope 解析 handler。 |
| `Transient` | 同样为每次 handler 调用创建一个异步 DI scope，再取得新的 handler。 |

因此，scoped handler 可以安全地依赖 scoped 服务；但 singleton handler 不能捕获 scoped 依赖。启用
容器 scope 校验时，后者会在容器构建或解析阶段暴露为生命周期错误，而不是在长轮询运行后才随机失败。

gRPC Push 和 LitePush 会为每条消息调用 handler。Remoting Push 则通过唯一的
`IRemotingPushMessageHandler` API，在每次批量回调中传入 `IReadOnlyList<RemotingMessageView>`；其
`ConsumeMessageBatchSize` 默认值为 `1`，因此除非显式配置，否则仍是单消息投递。
直接配置的 Remoting `MessageHandler` 委托也使用相同的列表合约。

对于非 FIFO 的 gRPC Push 和 LitePush 消息，`ConsumeTimeout` 到期后会取消 handler token、停止客户端不可见时间续期，并请求
重新投递。dispatcher 会忽略延迟返回的结果并释放 worker，但无法终止应用代码。超时调用可能与重新投递重叠，因此必须保持幂等。
对应 typed handler 的异步 scope 会一直存活到该调用返回，因此 handler 代码仍在运行时不会提前释放 scoped 依赖。FIFO message group
为了保持顺序而不会应用此机制。

对于并发集群、非 FIFO 的 Remoting 批次，`ConsumeTimeout` 到期后会取消 handler token，并请求 Broker 重新投递。
延迟返回的结果会被忽略，并可能与重新投递重叠，因此 handler 仍需自行配合取消并保持幂等；运行时不能强制停止应用代码。
FIFO 和顺序投递为保持顺序保证而不会应用此机制。

对应的 handler scope 逻辑位于
[`GrpcPushMessageHandlerFactory`](../../../src/EventHorizon.RocketMQ.Grpc/Consumer/Push/GrpcPushMessageHandlerFactory.cs)
和
[`RemotingPushMessageHandlerFactory`](../../../src/EventHorizon.RocketMQ.Remoting/Consumer/Push/RemotingPushMessageHandlerFactory.cs)。

## 取舍与约束

**单例角色并不等于单线程。** Consumer 本身是长寿命对象，但可以按 `MaxConcurrency` 并发调用 handler。
handler 及其依赖必须满足所选生命周期和并发模型。

**一个客户端注册中的一个角色只能有一个实例。** 需要同协议的多个 Producer 或 Consumer 时，应创建不同的
keyed 客户端注册，而不是重复对一个 builder 调用相同的 `Add...` 方法。

**不要把 ServiceProvider 当作业务依赖。** 注册层使用 provider 只是 composition root 的一部分；业务
代码应接受接口和普通依赖。运行时再从 `IServiceProvider` 查找 RocketMQ 客户端会隐藏连接目标和生命周期。

**启动不代表资源一定已完全就绪。** Host 已启动的含义是客户端循环已开始；topic、consumer group、ACL、
Proxy/Broker 能力和外部网络仍应由部署与健康检查保证。

## 延伸阅读

- [协议边界](protocol-boundaries.md)
- [gRPC 消费模型](../grpc/consumer-model.md)
- [Remoting 传输与客户端角色](../remoting/transport-and-client-roles.md)
- [本地与集成测试](../testing/local-and-integration-testing.md)
