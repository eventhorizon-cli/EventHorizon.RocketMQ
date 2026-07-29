# Remoting Producer

[English](README.md) | [简体中文](README.zh-CN.md)

`IRemotingProducer` 通过 RocketMQ classic Remoting 协议发布消息。它先从 NameServer 发现路由，再直连路由中的
Broker 端点。使用 classic RocketMQ 部署，或需要选择物理队列、批量发送、单向发送、事务、延迟消息撤回以及
请求-响应等 Producer 操作时，适合选择该角色。

它不适用于只暴露 Proxy 的部署：仅能访问 `NamesrvAddr` 并不够，应用还必须访问每个 Broker 公布地址。如果部署
暴露的是 RocketMQ 5 Proxy，且不需要 classic 专属操作，应改用 [gRPC Producer](../../grpc/Producer/README.zh-CN.md)。

## 注册与生命周期

先注册 Remoting 客户端，再添加 Producer 角色：

```csharp
builder.Services
    .AddRocketMQRemoting(remotingSection.Bind)
    .AddRemotingProducer(producerSection.Bind);
```

该角色通过 `IRemotingProducer` 对外提供。使用依赖注入注册后，.NET Host 负责 Producer 的生命周期；应用服务通常
只需注入它，由 Host 调用 `StartAsync` 和 `StopAsync`。

`RemotingProducerOptions.GroupName` 配置的 Producer group 是 classic Producer 的客户端身份，不是 consumer group。
同一进程需要独立的集群、凭据或 Producer 身份时，可以创建带名称的客户端注册，并以 keyed service 方式解析
`IRemotingProducer`：

```csharp
builder.Services
    .AddRocketMQRemoting("audit", auditRemotingSection.Bind)
    .AddRemotingProducer(auditProducerSection.Bind);

static Task<RemotingSendResult> PublishAsync(
    Message message,
    [FromKeyedServices("audit")] IRemotingProducer producer,
    CancellationToken cancellationToken) =>
    producer.SendAsync(message, cancellationToken);
```

注册名只是应用组合信息，不是 Broker 侧的审计功能。带名称的注册和默认注册使用独立配置，彼此不会回退。

## 发送与结果

创建 Remoting `Message`，再通过 `IRemotingProducer` 发送：

```csharp
var message = new Message("orders", Encoding.UTF8.GetBytes(orderId))
{
    Tag = "created"
};
message.Keys.Add(orderId);

RemotingSendResult result = await producer.SendAsync(message, cancellationToken);
```

`RemotingSendResult` 标识实际选择的 Broker 队列和位点，并包含 Broker 返回的发送 `Status`。必须检查该状态：
`FlushDiskTimeout`、`FlushSlaveTimeout` 和 `SlaveNotAvailable` 可能作为结果值返回，而不是抛出异常。发送完成也不代表
Consumer 已经处理消息。

配置发送重试后可能产生重复消息，包括上一次发送已经到达 Broker、但响应丢失的情况，因此 Consumer 应实现幂等
处理。`SendOnewayAsync` 不等待 Broker 响应，方法完成不代表消息已经存储。

## Producer 操作

| 需求 | SDK API | 重要边界 |
| --- | --- | --- |
| 普通发送 | `SendAsync(Message, ...)` | 检查 `RemotingSendResult.Status`。 |
| 显式选择队列 | `GetPublishMessageQueuesAsync`，以及接收队列或 selector 的 `SendAsync` 重载 | 目标队列必须是消息 Topic 的可写队列。 |
| 批量发送 | 接收集合的 `SendAsync` 重载 | 每个批次只能使用一个 Topic，且不能包含延迟、重试或事务消息。 |
| FIFO、延迟、优先级或 Lite 消息 | `MessageGroup`、`DeliveryTimestamp`、`Priority` 或 `LiteTopic` | 这些专用属性互斥，并且需要 Broker 支持对应能力。 |
| 事务发送 | `SendTransactionAsync` | 同时配置 `LocalTransactionExecutor` 和 `TransactionChecker`；Broker 以后可能回查未决结果。 |
| 撤回延迟消息 | `RecallAsync` | 原样保留不透明的 `RecallHandle`，并在投递前与同一逻辑 Topic 一起使用。 |
| 请求-响应 | `RequestAsync` 和 `SendReplyAsync` | 需要配套的响应端；超时时间覆盖发送请求和接收响应。 |
| 单向发送 | `SendOnewayAsync` | 不返回 Broker 确认或存储确认。 |

专用消息模式能否使用取决于服务端版本和 Broker 配置。队列 selector、事务、撤回、请求-响应以及兼容性细节请参阅
[Remoting 协议指南](../../../src/EventHorizon.RocketMQ.Remoting/README.zh-CN.md)。

## 前置条件与运行

默认配置要求 `localhost:9876` 上存在 NameServer、宿主机能够访问 Broker 公布端点，并准备好可写的普通 Topic
`eventhorizon-test-topic`。仓库提供的环境会创建该 Topic：

```shell
docker compose -f test-environments/rocketmq/compose.yaml up -d --wait
dotnet run --project samples/remoting/Producer/EventHorizon.RocketMQ.Samples.Remoting.Producer.csproj
```

连接外部集群时，配置 `RocketMQ:Remoting:NamesrvAddr`，确保每个 Broker 公布端点都可访问，并在禁用自动创建资源时
预先创建 Topic。可运行项目通过 Swagger 和 HTTP 暴露普通发送工作流；传输语义来自 `IRemotingProducer`，而不是
HTTP。
