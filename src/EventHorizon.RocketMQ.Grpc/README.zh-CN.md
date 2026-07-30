# EventHorizon.RocketMQ.Grpc

[English](https://github.com/eventhorizon-cli/EventHorizon.RocketMQ/blob/main/src/EventHorizon.RocketMQ.Grpc/README.md) |
[简体中文](https://github.com/eventhorizon-cli/EventHorizon.RocketMQ/blob/main/src/EventHorizon.RocketMQ.Grpc/README.zh-CN.md)

`EventHorizon.RocketMQ.Grpc` 是本仓库提供 `net8.0` 和 `net10.0` 目标的非官方 Apache RocketMQ 5
protobuf/gRPC Project 和 NuGet Package。它只通过 `Endpoint` 连接 RocketMQ Proxy，不会直接连接 NameServer 或
Broker。

客户端行为由高覆盖率单元测试保障，并通过 Docker 驱动的集成测试验证真实的 NameServer、Broker 和 Proxy
链路。

> 请先通过[仓库总览](https://github.com/eventhorizon-cli/EventHorizon.RocketMQ/blob/main/README.zh-CN.md)
> 了解两种协议各自支持的功能以及如何选择 Package，完整应用请参考
> [可运行的 gRPC 示例](https://github.com/eventhorizon-cli/EventHorizon.RocketMQ/blob/main/samples/README.zh-CN.md#grpc)。

## 快速开始

先配置应用所使用的 NuGet 源，再安装 gRPC Package。该 Package 在客户端 API 层面完全自包含：所有公开
模型都在自己的程序集内，不依赖其他 EventHorizon.RocketMQ Package。

```shell
dotnet add package EventHorizon.RocketMQ.Grpc
```

所有公开客户端模型都位于 `EventHorizon.RocketMQ.Grpc` 命名空间树下。gRPC 与 Remoting Package 可以在
同一应用中使用，但两套协议命名空间中的同名模型仍是不同的 CLR 类型，不能直接互换。如果同一个文件
导入了两边的协议命名空间，应为重复的短类型名设置 alias：

```csharp
using GrpcMessage = EventHorizon.RocketMQ.Grpc.Producer.Message;
using RemotingMessage = EventHorizon.RocketMQ.Remoting.Producer.Message;
```

假设 `localhost:8081` 上已有可访问的 Proxy，并且已创建 `orders` topic，下面的 ASP.NET Core
Minimal API 会注册默认 Producer，并将其注入发送消息的 endpoint：

```csharp
using System.Text;
using EventHorizon.RocketMQ.Grpc;
using EventHorizon.RocketMQ.Grpc.Producer;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;

var builder = WebApplication.CreateBuilder(args);

var rocketMQ = builder.Services.AddRocketMQGrpc(
    options => options.Endpoint = "localhost:8081");
rocketMQ.AddGrpcProducer();

var app = builder.Build();

app.MapPost("/messages", async (
    IGrpcProducer producer,
    CancellationToken cancellationToken) =>
{
    var receipt = await producer.SendAsync(
        new Message("orders", Encoding.UTF8.GetBytes("created")),
        cancellationToken);

    return Results.Ok(new { receipt.MessageId });
});

await app.RunAsync();
```

ASP.NET Core Host 会随应用启动和停止已注册的 Producer。完整应用可参考
[Producer](https://github.com/eventhorizon-cli/EventHorizon.RocketMQ/blob/main/samples/grpc/Producer/README.zh-CN.md)、
[SimpleConsumer](https://github.com/eventhorizon-cli/EventHorizon.RocketMQ/blob/main/samples/grpc/SimpleConsumer/README.zh-CN.md)、
[PushConsumer](https://github.com/eventhorizon-cli/EventHorizon.RocketMQ/blob/main/samples/grpc/PushConsumer/README.zh-CN.md) 和
[LitePushConsumer](https://github.com/eventhorizon-cli/EventHorizon.RocketMQ/blob/main/samples/grpc/LitePushConsumer/README.zh-CN.md)
示例目录。

## 支持的功能

`✅` 表示该 Project 已实现客户端 API。目标 RocketMQ Proxy 和 Broker 仍必须支持并启用相应的服务端功能。
`—` 表示 gRPC Project 不提供该 API。

| 功能 | 状态 | 条件和说明 |
| --- | :---: | --- |
| 普通 Producer 消息 | ✅ | 通过 `IGrpcProducer.SendAsync` 发送。 |
| FIFO Producer 消息 | ✅ | 设置 `MessageGroup`；目标服务端必须支持 FIFO 投递。 |
| 定时或延迟 Producer 消息 | ✅ | 设置 `DeliveryTimestamp`；目标服务端必须支持定时投递。 |
| 撤回定时或延迟消息 | ✅ | 在消息投递前使用返回的 `RecallHandle`。 |
| 优先级 Producer 消息 | ✅ | 设置 `Priority`；目标服务端必须支持优先级消息。 |
| Lite Producer 消息 | ✅ | 设置 `LiteTopic`；需要使用支持 Lite 消息的 Proxy 和 Broker。 |
| 事务 Producer 消息 | ✅ | 在启动前声明所有事务主题，并提供事务检查器。 |
| 指定队列、队列选择器、批量或单向发送 | — | 这些 classic Remoting Producer API 不属于 `IGrpcProducer`。 |
| Producer 请求-响应 | — | 该 classic Remoting Producer API 不属于 `IGrpcProducer`。 |
| `IGrpcSimpleConsumer` | ✅ | 应用控制接收、确认、不可见时间、死信转发和订阅。RocketMQ 5.5.0 Proxy 的最小长轮询间隔为五秒。 |
| `IGrpcPushConsumer` | ✅ | 支持并发或 FIFO 分发、确认、不可见时间续期、重试、死信处理和运行时订阅。它通过客户端发起的分配查询和 `ReceiveMessage` 长轮询实现。 |
| `IGrpcLitePushConsumer` | ✅ | 在一个 bind topic 下从 LiteTopic 进行并发或 FIFO 分发。需要使用支持 `SyncLiteSubscription` RPC 的 Proxy；它同样使用客户端发起的长轮询。 |
| 协议层面的 Broker Push | — | Push 和 LitePush 都不是由服务端主动发起的 Broker Push。 |
| 直接连接 NameServer 或 Broker | — | gRPC 通过 `Endpoint` 连接 RocketMQ Proxy。 |
| 依赖注入、Options、日志、Generic Host 生命周期以及默认客户端注册和 keyed 客户端注册 | ✅ | 使用 `AddRocketMQGrpc` 创建客户端注册，再添加所需角色。 |
| OpenTelemetry tracing 和 metrics | ✅ | 在应用的 OpenTelemetry SDK 中订阅 `GrpcRocketMQInstrumentation` 提供的 source 和 meter 名称。 |

## 客户端注册和连接

`Endpoint` 接受一个或多个以分号分隔的 Proxy 地址。发往这些地址的请求会在请求截止时间内故障转移。
`RequestTimeout`、`RouteCacheDuration` 和 `HeartbeatInterval` 可分别控制请求、路由刷新和心跳时序。
启用 `UseTLS` 后使用 TLS；通过 `AccessKey`、`AccessSecret` 和 `SecurityToken` 配置 ACL 凭据。配置
`SecurityToken` 时必须同时提供 access key 和 secret。`Namespace` 用于限定 topic 和 consumer group。
`AddRocketMQGrpc` 的返回值用于继续添加 Producer 和 Consumer 角色。

当一个 Host 需要连接多个集群、使用独立连接配置或添加另一个 Producer 时，请使用 keyed 客户端注册。注册名称即
`registrationName`；该 `registrationName` 同时作为 keyed service 的 key。

```csharp
using Microsoft.Extensions.DependencyInjection;
using EventHorizon.RocketMQ.Grpc;
using EventHorizon.RocketMQ.Grpc.Producer;

builder.Services
    .AddRocketMQGrpc("orders", options => options.Endpoint = "orders-proxy:8081")
    .AddGrpcProducer();

public sealed class OrderPublisher(
    [FromKeyedServices("orders")] IGrpcProducer producer)
{
}
```

每个客户端注册最多添加一个 Producer。同一个 builder 可重复调用 Consumer 注册方法；每次调用都会创建
options、逻辑 client ID、接收 Engine、handler 绑定和 hosted 生命周期相互隔离的实例。同一客户端注册内的
这些实例会共享 HTTP/2 channel，但不会因此合并逻辑身份或订阅。

### 在一个客户端注册中添加多个 Consumer

现有方法签名可以分别配置每个 Consumer。以下两个 PushConsumer 使用不同 group 和 topic，同时共享同一个
`AddRocketMQGrpc` 连接配置：

```csharp
var rocketMQ = builder.Services.AddRocketMQGrpc(options =>
{
    options.Endpoint = "proxy:8081";
});

rocketMQ.AddGrpcPushConsumer<OrderHandler>(ServiceLifetime.Scoped, options =>
{
    options.GroupName = "orders-consumer";
    options.Subscribe("orders", new FilterExpression("created"));
});

rocketMQ.AddGrpcPushConsumer<AuditHandler>(ServiceLifetime.Scoped, options =>
{
    options.GroupName = "audit-consumer";
    options.Subscribe("audit");
});
```

默认客户端注册可注入 `IEnumerable<IGrpcPushConsumer>` 取得两个实例；keyed 客户端注册则调用
`GetKeyedServices<IGrpcPushConsumer>(registrationName)`。按照 Microsoft DI 的注册顺序，单实例解析会返回
最后注册的 Consumer。

普通 Push 订阅组合具有以下语义：

| Consumer 注册组合 | 行为 |
| --- | --- |
| 相同 group、相同 topic/filter 订阅 | 合法的集群副本，分配会在实例间负载均衡。 |
| 不同 group、相同 topic | 扇出消费，每个 group 独立收到该 topic 的消息流。 |
| 不同 group、不同 topic | 相互隔离地消费。 |
| 相同 group、不同 topic 或 filter | 非法组合；注册会拒绝不一致的普通 Push 订阅。 |

默认客户端注册使用常规构造函数注入。当从独立的 `ServiceProvider` 而非 Generic Host 解析客户端时，请显式
调用其 `StartAsync` 和 `StopAsync`。

## OpenTelemetry

该 Package 会发出 OpenTelemetry Activity 和 metrics，不需要安装单独的 instrumentation Package。在应用的
OpenTelemetry SDK 中订阅公开名称：

```csharp
builder.Services.AddOpenTelemetry()
    .WithTracing(tracing => tracing
        .AddRocketMQGrpcInstrumentation())
    .WithMetrics(metrics => metrics
        .AddRocketMQGrpcInstrumentation());
```

资源属性、采样器、processor 和 exporter 由应用负责。客户端会为 Producer send、非空 receive、自动 Push/LitePush
process 以及 Consumer 完结操作发出 telemetry；它会把分布式上下文注入出站消息属性，且不会覆盖已有传播字段。
trace 模型和 metrics 请参阅 [OpenTelemetry 埋点设计](../../docs/zh-CN/architecture/opentelemetry-instrumentation.md)，
通过 OTLP 接入 Grafana 的方式请参阅 [OpenTelemetry Web 示例](../../samples/opentelemetry/README.zh-CN.md)。

## Producer

快速开始已经通过 `AddGrpcProducer` 注册了 `IGrpcProducer`。将其注入应用服务后，可以发送带有更丰富
元数据的消息：

```csharp
using System.Text;
using EventHorizon.RocketMQ.Grpc.Producer;

public sealed class OrderPublisher(IGrpcProducer producer)
{
    public Task<GrpcSendReceipt> PublishAsync(
        string orderId,
        CancellationToken cancellationToken)
    {
        var message = new Message("orders", Encoding.UTF8.GetBytes(orderId))
        {
            Tag = "created",
            MessageGroup = orderId
        };
        message.Keys.Add(orderId);
        message.Properties["source"] = "checkout";

        return producer.SendAsync(message, cancellationToken);
    }
}
```

`SendAsync` 返回不可变的 `GrpcSendReceipt`，其中包含消息 ID、位点、可选事务 ID、可选的
`RecallHandle` 和关联的 Broker 端点列表。发送失败会抛出异常，不会通过回执状态值表示。

需要特殊消息类型时，只能设置以下属性中的一个：

- `MessageGroup` 选择 FIFO 投递和稳定的队列亲和性。
- `DeliveryTimestamp` 安排延迟投递。投递前可将返回的 `RecallHandle` 传给 `RecallAsync` 撤回消息。
- `Priority` 选择非负的消息优先级。
- `LiteTopic` 选择 lite 消息。

### 事务

如需启用事务，请将快速开始中的 `AddGrpcProducer()` 替换为带配置的注册。在 Producer 启动前声明所有
事务 topic，并配置一个检查器，以便 Broker 请求时恢复持久化的本地结果。

```csharp
using EventHorizon.RocketMQ.Grpc.Producer;
using EventHorizon.RocketMQ.Grpc.Producer.Transactions;

rocketMQ.AddGrpcProducer(options =>
{
    options.Topics.Add("order-transactions");
    options.TransactionChecker = static (message, cancellationToken) =>
    {
        // Query durable local state by message.TransactionId or message.MessageId.
        return ValueTask.FromResult(TransactionResolution.Unknown);
    };
});

public sealed class TransactionalOrderWriter(IGrpcProducer producer)
{
    public async Task WriteAsync(byte[] body, CancellationToken cancellationToken)
    {
        var transaction = await producer.SendTransactionAsync(
            new Message("order-transactions", body),
            cancellationToken);

        try
        {
            await PersistLocalTransactionAsync(
                transaction.TransactionId,
                cancellationToken);
        }
        catch
        {
            await transaction.RollbackAsync(CancellationToken.None);
            throw;
        }

        await transaction.CommitAsync(cancellationToken);
    }

    private static Task PersistLocalTransactionAsync(
        string transactionId,
        CancellationToken cancellationToken) => Task.CompletedTask;
}
```

检查器在结果已知时返回 `Commit` 或 `Rollback`；返回 `Unknown` 会要求 Broker 稍后再次检查。

## SimpleConsumer

当应用需要控制接收和确认时机时，请使用 `IGrpcSimpleConsumer`。初始订阅是可选的，也可以在运行时
修改。

```csharp
using EventHorizon.RocketMQ.Grpc.Consumer;
using EventHorizon.RocketMQ.Grpc.Consumer.Simple;

rocketMQ.AddGrpcSimpleConsumer(options =>
{
    options.GroupName = "orders-simple-consumer";
    options.AwaitDuration = TimeSpan.FromSeconds(5);
    options.InvisibleDuration = TimeSpan.FromSeconds(30);
    options.Subscribe("orders", new FilterExpression("created"));
});

public sealed class OrderReceiver(IGrpcSimpleConsumer consumer)
{
    public async Task ReceiveOnceAsync(CancellationToken cancellationToken)
    {
        var messages = await consumer.ReceiveAsync(
            maxMessages: 16,
            cancellationToken: cancellationToken);

        foreach (var message in messages)
        {
            if (message.IsCorrupted)
            {
                continue;
            }

            await ProcessAsync(message.Body, cancellationToken);
            await consumer.AckAsync(message, cancellationToken);
        }
    }

    private static Task ProcessAsync(
        byte[] body,
        CancellationToken cancellationToken) => Task.CompletedTask;
}
```

`SubscribeAsync` 和 `UnsubscribeAsync` 会更新当前生效的订阅集合。Consumer 还提供
`ChangeInvisibleDurationAsync` 和 `ForwardToDeadLetterQueueAsync`。RocketMQ 5.5.0 Proxy 默认的
最小长轮询间隔为五秒，因此使用该服务端时，请将 `AwaitDuration` 保持在五秒或更长。

## PushConsumer

当客户端需要自动接收和分发消息时，请使用 `IGrpcPushConsumer`。

```csharp
using System.Text;
using Microsoft.Extensions.DependencyInjection;
using EventHorizon.RocketMQ.Grpc.Consumer;
using EventHorizon.RocketMQ.Grpc.Consumer.Push;

rocketMQ.AddGrpcPushConsumer<OrderMessageHandler>(ServiceLifetime.Scoped, options =>
{
    options.GroupName = "orders-push-consumer";
    options.MaxConcurrency = 8;
    options.MaxDeliveryAttempts = 16;
    options.InvisibleDuration = TimeSpan.FromSeconds(30);
    options.ConsumeTimeout = TimeSpan.FromMinutes(15);
    options.Subscribe("orders", new FilterExpression("created"));
});

public sealed class OrderMessageHandler : IGrpcPushMessageHandler
{
    public ValueTask<ConsumeResult> HandleAsync(GrpcMessageView message, CancellationToken cancellationToken)
    {
        Console.WriteLine(Encoding.UTF8.GetString(message.Body));
        return ValueTask.FromResult(ConsumeResult.Success);
    }
}
```

返回 `Success` 会确认消息，返回 `Retry` 会使消息重新变为可投递状态，返回 `DeadLetter` 会将消息
转发到死信队列。损坏的消息不会进入 handler：普通消息会重新变为可投递状态，损坏的 FIFO 消息会
在释放同组下一条消息前进入死信队列。运行时订阅变更会立即更新当前接收器。泛型注册会为当前 Consumer 实例
选择 handler 生命周期：`Singleton` 会为每个 Consumer 实例创建一个 handler，且必须线程安全；`Scoped` 和
`Transient` 会在每次处理尝试中创建新的异步 DI scope，再从其中解析 handler。自动分发必须使用由应用容器解析的
`IGrpcPushMessageHandler` typed handler；handler 需要数据库上下文等 scoped 依赖时应选择 `Scoped`。
对于非 FIFO 分发，`ConsumeTimeout` 默认值为 15 分钟。到期后会取消 handler token、停止客户端的不可见时间续期，并请求消息
重新投递；延迟返回的成功结果会被忽略。重新投递可能与仍在执行、但忽略取消的代码重叠，因此 handler 必须具备幂等性。
客户端无法强制停止这段代码。FIFO message group 会刻意排除在该机制之外，以维持其顺序。

## LitePushConsumer

当需要自动分发绑定到同一 LITE parent topic 的多个 LiteTopic 消息时，使用
`IGrpcLitePushConsumer`。它复用 `IGrpcPushConsumer` 的长轮询、确认、重试、死信、并发和 FIFO
选项，但订阅对象是 LiteTopic，而不是普通 topic 过滤器。

```csharp
using Microsoft.Extensions.DependencyInjection;
using EventHorizon.RocketMQ.Grpc.Consumer;
using EventHorizon.RocketMQ.Grpc.Consumer.Lite;
using EventHorizon.RocketMQ.Grpc.Consumer.Push;

rocketMQ.AddGrpcLitePushConsumer<OrderMessageHandler>(ServiceLifetime.Scoped, options =>
{
    options.GroupName = "orders-lite-push-consumer";
    options.BindTopic = "orders-lite";
    options.LiteTopics.Add("orders-us");
    options.LiteTopics.Add("orders-eu");
    options.MaxConcurrency = 8;
});
```

不要为该角色调用继承的 `Subscribe` 方法，也不要配置普通 topic 订阅。客户端会在启动时同步完整的
LiteTopic 集合，并按配置的间隔再次同步；也可通过 `SubscribeLiteAsync` 和 `UnsubscribeLiteAsync` 在运行时
更新订阅。Lite Push 与标准 Push 使用相同的 `IGrpcPushMessageHandler` 和生命周期语义：

与普通 Push 订阅不同，同一消费组中的 LitePush 实例不要求 LiteTopic 集合完全一致，但客户端要求它们使用
相同的 bind topic。它们可以分别持有不同的 LiteTopic 集合，最终仍以 Proxy 对该消费组和 bind topic 的配置
校验为准。

```csharp
await consumer.SubscribeLiteAsync(
    "orders-apac",
    GrpcLiteOffsetOption.Last,
    cancellationToken);
```

启动 Lite Push consumer 前，需要先准备 RocketMQ 部署：

- 在存储该 LITE parent topic 的每个 Broker 上启用 `enableLmq=true` 和 `enableMultiDispatch=true`。
- 使用 `message.type=LITE` 创建 parent topic。
- 创建 consumer group 时设置 `lite.bind.topic=<parent-topic>`。
- 连接支持 `SyncLiteSubscription` RPC 的 cluster-mode Proxy。在 RocketMQ 5.5.0 中，
  `mqbroker --enable-proxy` 启动的 Broker-integrated Proxy 不支持该 RPC；请改用 `mqproxy -pm cluster` 或兼容的新版 Proxy。

`SyncLiteSubscription` 是 LitePush 的订阅控制 RPC。客户端通过它向 Proxy 同步 consumer group、bind topic 和
LiteTopic 集合；它不负责传输消息。独立的 cluster-mode Proxy 可以与 Broker 共用一个容器或 Compose service，
不需要因此另建一套 Compose 环境。

LiteTopic 名称和配额限制仍由服务端决定；订阅被拒绝时会抛出 `GrpcServiceException`，且不会改变本地订阅集合。

## 错误处理

已完成的 gRPC 调用如果返回不成功的 RocketMQ 状态，会抛出 `GrpcServiceException`。其
`ResponseCode` 保留 RocketMQ 数字服务码。该异常派生自当前 Package 中的
`RocketMQClientException` 基类。

## 本地测试

仓库的 [Docker Compose 环境](https://github.com/eventhorizon-cli/EventHorizon.RocketMQ/tree/main/test-environments/rocketmq)
运行 Apache RocketMQ 5.5.0，并在
`localhost:8081` 提供 gRPC Proxy。从仓库根目录执行：

```shell
docker compose -f test-environments/rocketmq/compose.yaml config --quiet
docker compose -f test-environments/rocketmq/compose.yaml up -d --wait
```

请使用[环境说明](https://github.com/eventhorizon-cli/EventHorizon.RocketMQ/blob/main/test-environments/rocketmq/README.zh-CN.md)
中的命令创建主题和消费组。停止并删除
环境：

```shell
docker compose -f test-environments/rocketmq/compose.yaml down -v --remove-orphans
```

可通过以下命令运行相互隔离、由 Docker 支持的 gRPC 集成测试：

```shell
dotnet test tests/it/EventHorizon.RocketMQ.Grpc.IntegrationTests/EventHorizon.RocketMQ.Grpc.IntegrationTests.csproj
```

## 许可证

本 Project 基于 [Apache License 2.0](https://github.com/eventhorizon-cli/EventHorizon.RocketMQ/blob/main/LICENSE) 授权。
