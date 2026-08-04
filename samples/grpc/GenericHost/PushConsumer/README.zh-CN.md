# gRPC PushConsumer

[English](README.md) | [简体中文](README.zh-CN.md)

`IGrpcPushConsumer` 是自动分发的 gRPC 消费模型。客户端查询分配、向 RocketMQ Proxy 发起长轮询、在本地缓冲收到的消息，
再调用应用提供的 handler。“Push”描述的是应用 API；Broker 不会向应用建立一个由服务端发起的连接。

## 适用场景

当消息处理适合表示成 handler，并希望 SDK 负责接收循环、背压、并发、请求 Proxy 续期及完成操作时，适合使用
PushConsumer。对于持续运行、每条消息都能由 handler 返回一个处理结果的服务，它通常是更简单的选择。

如果应用必须自行决定何时接收、直接控制批量，或者将确认交给自己的调度器，请改用 `IGrpcSimpleConsumer`。如果应用必须选择
Broker queue 并管理数字 offset，请使用经典 Remoting Pull。

## SDK 工作流

只注册一次 gRPC 传输，再为每个 Consumer 分别添加类型化 handler 和 options：

```csharp
const string Topic = "eventhorizon-test-topic";

builder.Services
    .AddRocketMQGrpc(clientSection.Bind)
    .AddGrpcPushConsumer<PushConsumerMessageHandler>(ServiceLifetime.Scoped, options =>
    {
        options.GroupName = "rocketmq-dotnet-grpc-push-sample";
        options.MaxConcurrency = 4;
        options.BatchSize = 16;
        options.Subscribe(Topic, new FilterExpression("sample"));
    })
    .AddGrpcPushConsumer<AllMessagesMessageHandler>(ServiceLifetime.Scoped, options =>
    {
        options.GroupName = "rocketmq-dotnet-grpc-push-all-messages";
        options.MaxConcurrency = 2;
        options.BatchSize = 8;
        options.Subscribe(Topic);
    });
```

每次调用 `AddGrpcPushConsumer` 都会创建独立的逻辑 Consumer，分别拥有自己的 group、订阅、handler、本地缓冲、并发配置和
Host 生命周期。两个角色仍属于同一个 `AddRocketMQGrpc` 注册，并复用连接到已配置 Proxy 的底层传输 channel pool。
连接和凭据放在 `appsettings.json`；订阅和处理策略则直接写在 `Program.cs`，便于比较。

`Scoped` 和 `Transient` handler 会在每次处理尝试中新建异步 scope，再从中解析。`Singleton` handler 会被并发调用共享，
因此必须线程安全。自动分发始终使用由应用容器解析的 `IGrpcPushMessageHandler` typed handler。handler 需要数据库上下文等
scoped 应用服务时应选择 `Scoped`，并通过构造函数注入这些依赖。

Generic Host 集成会为已注册角色调用 `StartAsync` 和 `StopAsync`。生命周期归 Host 所有，应用代码不能再次调用这些方法。
不使用 Host 时，应用必须通过 `IGrpcPushConsumer` 管理生命周期；请参阅完整的[非 Host 示例](../../NonHost/PushConsumer/README.zh-CN.md)。
`SubscribeAsync` 和 `UnsubscribeAsync` 用于在启动后修改当前 topic 和过滤条件；Options 上的 `Subscribe` 用于定义初始订阅。

## 投递、完成与失败

每个 handler 结果都会要求 Consumer 执行一种明确的完成操作：

| 结果 | SDK 行为 |
| --- | --- |
| `ConsumeResult.Success` | 确认消息。只有业务副作用已经持久化后才能返回该结果。 |
| `ConsumeResult.Failure` | 由当前生效的重试策略安排再次投递。 |

未处理的 handler 异常会被当作 `Failure`。如果确认或重试调度失败，消息可能再次投递。因此，即使 handler 返回
`Success`，处理逻辑仍必须幂等。

损坏的消息不会进入 `IGrpcPushMessageHandler`。非 FIFO 损坏消息会重新变为可投递状态；FIFO 损坏消息会在释放同一
message group 的下一条消息前转入死信队列。

Push 请求会设置 `AutoRenew=true`，由兼容的 Proxy 在 handler 运行期间续期 receipt；.NET 客户端不会再启动一套续期
定时器。对于非 FIFO 消息，`ConsumeTimeout` 限制 dispatcher 等待 handler 的时长：超时后会取消 handler token、
请求再次投递，并忽略延迟返回的成功结果。SDK 无法强制停止忽略取消的代码，
因此旧调用可能与重新投递重叠。FIFO message group 不使用这种超时后脱离 handler 的行为，因为释放下一条消息可能破坏顺序。

`MaxConcurrency` 控制本地 handler 并发，不代表 Broker queue 数量。FIFO 顺序只在一个 message group 内成立，不是跨
queue、consumer group 或 Consumer 实例的全局顺序。使用相同 consumer group 的实例会共享分配和重试状态。

不同 group 会分别独立消费 topic。同一 group 的 Consumer 则是集群副本：它们必须配置完全相同的 topic 和 filter
订阅，并共同分配消息，而不是每个实例都收到一份。同一个 `AddRocketMQGrpc` 注册中的普通 Push Consumer 如果复用 group
却配置了不同订阅，注册会在本地失败。

## 关键 SDK options

| Option | 控制内容 |
| --- | --- |
| `GrpcClientOptions.Endpoint` | RocketMQ Proxy 端点；这里不能填写 NameServer 地址。 |
| `GrpcPushConsumerOptions.GroupName` | 用于分配和投递状态的 consumer group。 |
| `ConsumerOptions.Subscribe` | 初始 topic 以及 Tag 或 SQL 过滤订阅。 |
| `MaxConcurrency` | 最大 handler 调度并发数。已经超时且忽略取消的 handler 仍可能留在进程中。 |
| `BatchSize` | 单次 Receive 请求的最大消息数。 |
| `MaxCachedMessages` / `MaxCachedMessageBytes` | 有界本地消息缓冲的数量和消息体字节数限制。 |
| `InvisibleDuration` | 初始消息不可见时长；处理期间由兼容的 Proxy 续期 receipt。 |
| `ConsumeTimeout` | 非 FIFO handler 在请求取消和重试前允许运行的最长时间。 |
| `MaxDeliveryAttempts` / `RetryDelay` | Proxy 没有提供策略时使用的 FIFO 本地尝试次数与重试延迟回退值；非 FIFO 的死信推进仍由服务端负责。 |
| `LongPollingTimeout` | 每次 Receive 长轮询允许服务端等待的最长时间。 |

## 运行示例

本仓库提供的 [RocketMQ 环境](../../../../test-environments/rocketmq/README.zh-CN.md) 会在 `localhost:8081` 提供兼容
Proxy，并创建示例 Topic。使用外部部署且禁用了自动创建时，需要预先创建 Topic 和 consumer group。

```shell
docker compose -f test-environments/rocketmq/compose.yaml up -d --wait
dotnet run --project samples/grpc/GenericHost/PushConsumer
```

可运行代码在本地环境创建的同一个普通 topic 上注册两个 Consumer。group
`rocketmq-dotnet-grpc-push-sample` 只接收 `sample` tag，group
`rocketmq-dotnet-grpc-push-all-messages` 接收全部 tag。因为 group 不同，一条 `sample` 消息会独立投递给两个类型化
handler。请先启动此 Host，再通过 [Producer 示例](../Producer/README.zh-CN.md)发送这种消息。两个 handler 都会记录各自
收到的消息并返回 `Success`，因此 SDK 会确认它们。Host 会持续运行到按下 Ctrl+C，并由 Host 负责停止 Consumer。

完整 handler、订阅和 Proxy 行为请参阅
[gRPC 指南](../../../../src/EventHorizon.RocketMQ.Grpc/README.zh-CN.md)与
[gRPC 消费模型](../../../../docs/zh-CN/grpc/consumer-model.md)。
