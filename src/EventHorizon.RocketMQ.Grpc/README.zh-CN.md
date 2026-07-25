# EventHorizon.RocketMQ.Grpc

[English](README.md) |
[简体中文](README.zh-CN.md)

> **[请先阅读仓库总览和可运行示例](../../README.zh-CN.md)。** 其中包含跨协议功能矩阵和包选择说明。

一个符合 .NET 习惯的 Apache RocketMQ 5 protobuf/gRPC 客户端，支持 .NET 8 及以上版本。客户端
连接 RocketMQ Proxy，不会直接连接 NameServer。

## 支持的功能

`✅` 表示该包已实现客户端 API。目标 RocketMQ Proxy 和 Broker 仍必须支持并启用相应的服务端功能。
`—` 表示 gRPC 包不提供该 API。

| 功能 | 状态 | 条件和说明 |
| --- | :---: | --- |
| 普通 Producer 消息 | ✅ | 通过 `IGrpcProducer.SendAsync` 发送。 |
| FIFO Producer 消息 | ✅ | 设置 `MessageGroup`；目标服务端必须支持 FIFO 投递。 |
| 定时或延迟 Producer 消息 | ✅ | 设置 `DeliveryTimestamp`；目标服务端必须支持定时投递。 |
| 撤回定时或延迟消息 | ✅ | 在消息投递前使用返回的 recall handle。 |
| 优先级 Producer 消息 | ✅ | 设置 `Priority`；目标服务端必须支持优先级消息。 |
| Lite Producer 消息 | ✅ | 设置 `LiteTopic`；需要支持 Lite 消息的 Proxy 和 Broker。 |
| 事务 Producer 消息 | ✅ | 在启动前声明所有事务主题，并提供事务检查器。 |
| 指定队列、队列选择器、批量或单向发送 | — | 这些经典 Remoting Producer API 不属于 `IGrpcProducer`。 |
| Producer 请求-响应 | — | 该经典 Remoting Producer API 不属于 `IGrpcProducer`。 |
| `IGrpcSimpleConsumer` | ✅ | 应用控制接收、确认、不可见时间、死信转发和订阅。RocketMQ 5.5.0 Proxy 的最小长轮询间隔为五秒。 |
| `IGrpcPushConsumer` | ✅ | 支持并发或 FIFO 分发、确认、不可见时间续期、重试、死信处理和运行时订阅。它通过客户端发起的分配查询和 `ReceiveMessage` 长轮询实现。 |
| `IGrpcLitePushConsumer` | ✅ | 在一个 bind topic 下从 LiteTopic 进行并发或 FIFO 分发。Proxy 必须实现 `SyncLiteSubscription`；它同样使用客户端发起的长轮询。 |
| 协议层面的 Broker Push | — | Push 和 LitePush 都不是由服务端主动发起的 Broker Push。 |
| 直接连接 NameServer 或 Broker | — | gRPC 通过 `Endpoint` 连接 RocketMQ Proxy。 |
| 依赖注入、Options、日志、Generic Host 生命周期以及命名/键控 profile | ✅ | 使用 `AddRocketMQGrpc` 注册 profile，再添加所需角色。 |

## 安装

先配置应用所使用的包源，再安装 gRPC 包。Shared 包会作为传递依赖引入。

```shell
dotnet add package EventHorizon.RocketMQ.Grpc --version 0.1.0-alpha.1
```

## Host 注册和连接

使用 `AddRocketMQGrpc` 注册 gRPC 配置，再添加应用所需的角色。Generic Host 会随应用启动和停止
已注册的角色。

```csharp
using Microsoft.Extensions.Hosting;
using EventHorizon.RocketMQ.Grpc;

var builder = Host.CreateApplicationBuilder(args);

var rocketMQ = builder.Services.AddRocketMQGrpc(options =>
{
    options.Endpoint = "proxy-a:8081;proxy-b:8081";
    options.RequestTimeout = TimeSpan.FromSeconds(3);
    options.RouteCacheDuration = TimeSpan.FromSeconds(30);
    options.HeartbeatInterval = TimeSpan.FromSeconds(30);
});

// Add Producer or Consumer roles through rocketMQ.

await builder.Build().RunAsync();
```

`Endpoint` 接受一个或多个以分号分隔的 Proxy 地址。访问点请求会在请求截止时间内故障转移。启用
`UseTLS` 后使用 TLS；通过 `AccessKey`、`AccessSecret` 和 `SecurityToken` 配置 ACL 凭据；
`Namespace` 用于限定主题和消费组。

当一个 Host 需要连接多个集群或注册同一角色的多个实例时，请使用命名配置。配置名称也是键控服务
的键。

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

每个配置中的同一角色只能注册一次。未命名配置使用常规构造函数注入。当从独立的
`ServiceProvider` 而非 Generic Host 解析客户端时，请显式调用其 `StartAsync` 和 `StopAsync`。

## Producer

使用 `AddGrpcProducer` 注册 `IGrpcProducer`，然后将其注入应用服务。

```csharp
using System.Text;
using EventHorizon.RocketMQ.Grpc.Producer;
using EventHorizon.RocketMQ.Producer;

rocketMQ.AddGrpcProducer();

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

`SendAsync` 返回不可变的 `GrpcSendReceipt`，其中包含消息 ID、位点、可选事务 ID、可选撤回句柄和
Broker 端点。发送失败通过异常报告，而不是通过回执状态值报告。

需要特殊消息类型时，只能设置以下属性中的一个：

- `MessageGroup` 选择 FIFO 投递和稳定的队列亲和性。
- `DeliveryTimestamp` 安排延迟投递。投递前可将返回的 `RecallHandle` 传给 `RecallAsync` 撤回消息。
- `Priority` 选择非负的消息优先级。
- `LiteTopic` 选择 lite 消息。

### 事务

在 Producer 启动前声明所有事务主题，并配置一个检查器，以便 Broker 请求时恢复持久化的本地结果。

```csharp
using EventHorizon.RocketMQ.Grpc.Producer;
using EventHorizon.RocketMQ.Grpc.Producer.Transactions;
using EventHorizon.RocketMQ.Producer;

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
using EventHorizon.RocketMQ.Consumer;
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

`SubscribeAsync` 和 `UnsubscribeAsync` 会更新实时订阅集合。Consumer 还提供
`ChangeInvisibleDurationAsync` 和 `ForwardToDeadLetterQueueAsync`。RocketMQ 5.5.0 Proxy 默认的
最小长轮询间隔为五秒，因此使用该服务端时，请将 `AwaitDuration` 保持在五秒或更长。

## PushConsumer

当客户端需要自动接收和分发消息时，请使用 `IGrpcPushConsumer`。

```csharp
using System.Text;
using Microsoft.Extensions.DependencyInjection;
using EventHorizon.RocketMQ.Consumer;
using EventHorizon.RocketMQ.Grpc.Consumer;
using EventHorizon.RocketMQ.Grpc.Consumer.Push;

rocketMQ.AddGrpcPushConsumer<OrderMessageHandler>(ServiceLifetime.Scoped, options =>
{
    options.GroupName = "orders-push-consumer";
    options.MaxConcurrency = 8;
    options.MaxDeliveryAttempts = 16;
    options.InvisibleDuration = TimeSpan.FromSeconds(30);
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
转发到死信队列。损坏的消息不会进入处理程序：普通消息会重新变为可投递状态，损坏的 FIFO 消息会
在释放同组下一条消息前进入死信队列。运行时订阅变更会立即协调接收器。泛型注册会为当前 client profile
选择处理程序生命周期：`Singleton` 会为每个 profile/role 创建一个实例，且必须线程安全；`Scoped` 和
`Transient` 会为每次处理尝试在新的异步服务 scope 中解析处理程序。`MessageHandler` 委托仍适合简单的
无状态回调，但不能与 typed handler 同时配置。

## LitePushConsumer

当需要从共享同一个 LITE parent/bind topic 的多个 LiteTopic 自动分发消息时，使用
`IGrpcLitePushConsumer`。它复用 `IGrpcPushConsumer` 的长轮询、确认、重试、死信、并发和 FIFO
选项，但订阅对象是 LiteTopic，而不是普通主题过滤器。

```csharp
using Microsoft.Extensions.DependencyInjection;
using EventHorizon.RocketMQ.Consumer;
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

不要为该角色调用继承的 `Subscribe` 方法，也不要配置普通主题订阅。客户端启动时会发送完整的
LiteTopic 集合、定期协调该集合，并可通过 `SubscribeLiteAsync` 和 `UnsubscribeLiteAsync` 在运行时
更新订阅。Lite Push 与标准 Push 使用相同的 `IGrpcPushMessageHandler` 和生命周期语义：

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
- 连接实现了 `SyncLiteSubscription` 的 cluster-mode Proxy。在 RocketMQ 5.5.0 中，
  `mqbroker --enable-proxy` 启动的 local Proxy 没有实现该服务；请改用 `mqproxy -pm cluster` 或兼容的新版 Proxy。

LiteTopic 名称和配额限制仍由服务端决定；订阅被拒绝时会抛出 `GrpcServiceException`，且不会改变本地订阅集合。

## 错误处理

已完成的 gRPC 调用如果返回不成功的 RocketMQ 状态，会抛出 `GrpcServiceException`。其
`ResponseCode` 保留 RocketMQ 数字服务码。该异常派生自 Shared 中的
`RocketMQClientException`，因此调用方可以捕获协议专用异常，也可以捕获公共基类。

## 本地测试

仓库的 [Docker Compose 环境](https://github.com/eventhorizon-cli/EventHorizon.RocketMQ/tree/main/test-environments/rocketmq)
运行 Apache RocketMQ 5.5.0，并在
`localhost:8081` 提供 gRPC Proxy。从仓库根目录执行：

```shell
docker compose -f test-environments/rocketmq/compose.yaml config --quiet
docker compose -f test-environments/rocketmq/compose.yaml up -d --wait
```

请使用[环境 README](https://github.com/eventhorizon-cli/EventHorizon.RocketMQ/blob/main/test-environments/rocketmq/README.md)
中的命令创建主题和消费组。停止并删除
环境：

```shell
docker compose -f test-environments/rocketmq/compose.yaml down -v --remove-orphans
```

可通过以下命令运行相互隔离、由 Docker 支持的 gRPC 集成测试：

```shell
dotnet test tests/EventHorizon.RocketMQ.Grpc.IntegrationTests/EventHorizon.RocketMQ.Grpc.IntegrationTests.csproj
```

## 许可证

本项目基于 [Apache License 2.0](https://github.com/eventhorizon-cli/EventHorizon.RocketMQ/blob/main/LICENSE) 授权。
