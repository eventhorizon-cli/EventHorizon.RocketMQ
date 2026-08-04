# EventHorizon.RocketMQ.Remoting

[English](https://github.com/eventhorizon-cli/EventHorizon.RocketMQ/blob/main/src/EventHorizon.RocketMQ.Remoting/README.md) |
[简体中文](https://github.com/eventhorizon-cli/EventHorizon.RocketMQ/blob/main/src/EventHorizon.RocketMQ.Remoting/README.zh-CN.md)

`EventHorizon.RocketMQ.Remoting` 是面向经典 Apache RocketMQ Remoting 协议的非官方 .NET 客户端。它支持
`net8.0` 和 `net10.0`，直接连接 NameServer 与 Broker。

经典 NameServer/Broker 部署请选择此包。应用通过 gRPC 连接 RocketMQ 5 Proxy 时，请使用
[`EventHorizon.RocketMQ.Grpc`](https://www.nuget.org/packages/EventHorizon.RocketMQ.Grpc)。

如需强类型应用事件，请参阅
[`EventHorizon.RocketMQ.Remoting.EventBus`](https://github.com/eventhorizon-cli/EventHorizon.RocketMQ.EventBus)。

## 快速开始

安装此包：

```shell
dotnet add package EventHorizon.RocketMQ.Remoting
```

在 NameServer 可达且已存在 `orders` 主题的前提下，注册 Producer 并发送消息：

```csharp
using EventHorizon.RocketMQ.Remoting;
using EventHorizon.RocketMQ.Remoting.Producer;
using Microsoft.AspNetCore.Builder;

var builder = WebApplication.CreateBuilder(args);

var rocketMQ = builder.Services.AddRocketMQRemoting(options =>
{
    options.NamesrvAddr = "nameserver-a:9876;nameserver-b:9876";
});

rocketMQ.AddRemotingProducer(options =>
{
    options.GroupName = "orders-producer";
});

var app = builder.Build();

app.MapPost("/messages", async (
    IRemotingProducer producer,
    CancellationToken cancellationToken) =>
{
    var result = await producer.SendAsync(
        new Message("orders", "hello"u8.ToArray()),
        cancellationToken);

    return Results.Ok(new { result.MessageId, result.Status });
});

await app.RunAsync();
```

Generic Host 会自动启动和停止已注册的角色。完整应用请参阅
[Remoting 示例](https://github.com/eventhorizon-cli/EventHorizon.RocketMQ/blob/main/samples/README.zh-CN.md)。

## 功能

目标 RocketMQ 部署也必须启用相应的服务端功能。

| 公共 API | 用途 |
| --- | --- |
| `IRemotingProducer` | 普通、单向、指定队列、批量、FIFO、延迟、优先级、Lite、事务、撤回和请求-响应消息。 |
| `IRemotingAdmin` | 只读队列、位点、时间戳和物理消息查询。 |
| `IRemotingLitePullConsumer` | 由应用驱动的轮询、位点控制和位点提交。 |
| `IRemotingPushConsumer` | 自动并发或有序处理，并支持客户端或 Broker 分配队列。 |

应用需要显式轮询时使用 LitePull；需要由客户端将消息分发给处理器时使用 Push。RocketMQ 5 Proxy 的 gRPC 和
SimpleConsumer API 请使用 `EventHorizon.RocketMQ.Grpc`。

## 连接与安全

`AddRocketMQRemoting` 配置一个逻辑客户端注册项：

```csharp
builder.Services.AddRocketMQRemoting(options =>
{
    options.NamesrvAddr = "nameserver-a:9876;nameserver-b:9876";
    options.Namespace = "tenant-a";
    options.RequestTimeout = TimeSpan.FromSeconds(3);
});
```

常用连接选项：

| 选项 | 用途 |
| --- | --- |
| `NamesrvAddr` | 一个或多个以分号分隔的 NameServer 地址。 |
| `Namespace` | 为一个租户或环境限定主题和消费组。 |
| `RequestTimeout` | 限制普通 NameServer 和 Broker 请求的耗时。 |
| `AccessKey`、`AccessSecret`、`SecurityToken` | 配置 ACL 凭据。AccessKey 和 AccessSecret 必须同时提供。 |
| `UseTLS` | 为 NameServer 和 Broker 连接启用 TLS。 |
| `ConfigureLegacySslOptions` | 自定义信任设置、客户端证书、吊销检查、协议或 SNI。 |

需要连接多个集群或使用相互独立的凭据时，请使用命名注册。注册名也会作为 .NET keyed service 的键：

```csharp
using Microsoft.Extensions.DependencyInjection;
using EventHorizon.RocketMQ.Remoting.Producer;

builder.Services
    .AddRocketMQRemoting("audit", options =>
    {
        options.NamesrvAddr = "audit-nameserver:9876";
    })
    .AddRemotingProducer(options => options.GroupName = "audit-producer");

public sealed class AuditPublisher(
    [FromKeyedServices("audit")] IRemotingProducer producer)
{
}
```

协议和连接内部机制请参阅[经典 Remoting 传输与角色](https://github.com/eventhorizon-cli/EventHorizon.RocketMQ/blob/main/docs/zh-CN/remoting/transport-and-client-roles.md)；
高级注册行为请参阅[依赖注入注册与生命周期](https://github.com/eventhorizon-cli/EventHorizon.RocketMQ/blob/main/docs/zh-CN/architecture/dependency-injection-and-lifetimes.md)。

## Producer

使用 `AddRemotingProducer` 注册 `IRemotingProducer`：

```csharp
using System.Text;
using EventHorizon.RocketMQ.Remoting.Producer;

rocketMQ.AddRemotingProducer(options =>
{
    options.GroupName = "orders-producer";
    options.RetryTimesWhenSendFailed = 2;
});

public sealed class OrderPublisher(IRemotingProducer producer)
{
    public Task<RemotingSendResult> PublishAsync(
        string orderId,
        CancellationToken cancellationToken)
    {
        var message = new Message("orders", Encoding.UTF8.GetBytes(orderId))
        {
            Tag = "created",
            MessageGroup = orderId
        };
        message.Keys.Add(orderId);

        return producer.SendAsync(message, cancellationToken);
    }
}
```

始终检查 `RemotingSendResult.Status`。经典 Broker 可能返回 `FlushDiskTimeout`、`FlushSlaveTimeout` 或
`SlaveNotAvailable`，而不会抛出异常。`SendOnewayAsync` 不等待 Broker 响应，因此任务完成并不代表消息已存储。

常用 Producer API 包括：

- `GetPublishMessageQueuesAsync` 及指定队列或 selector 重载。
- 针对同一主题消息的批量 `SendAsync`。
- 搭配 `LocalTransactionExecutor` 和 `TransactionChecker` 的 `SendTransactionAsync`。
- 对符合条件的延迟消息调用 `RecallAsync`，并使用返回的 `RecallHandle`。
- 用于请求-响应流程的 `RequestAsync` 和 `SendReplyAsync`。

`MessageGroup`、`DeliveryTimestamp`、`Priority` 和 `LiteTopic` 用于选择专用消息行为，彼此互斥。目标 Broker
必须支持所选功能。发送重试可能产生重复消息，因此 Consumer 应保证幂等。

可运行示例请参阅 [Producer 示例](https://github.com/eventhorizon-cli/EventHorizon.RocketMQ/blob/main/samples/remoting/GenericHost/Producer/README.zh-CN.md)。

## Admin

`IRemotingAdmin` 提供只读的队列、位点、时间戳和已存储消息查询：

```csharp
using EventHorizon.RocketMQ.Remoting.Admin;

rocketMQ.AddRemotingAdmin();

public sealed class OrderOffsets(IRemotingAdmin admin)
{
    public async Task<long> GetMaximumAsync(CancellationToken cancellationToken)
    {
        var queue = (await admin.GetMessageQueuesAsync("orders", cancellationToken))[0];
        return await admin.GetMaxOffsetAsync(queue, cancellationToken: cancellationToken);
    }
}
```

`GetMessageQueuesAsync` 返回可读的 `RemotingConsumerQueue`，可以直接传给各项 offset 查询方法。

`ViewMessageAsync` 接受经典发送返回的物理 `OffsetMessageId`。应用所在网络必须能够访问该 ID 中编码的 Broker 地址。

可运行示例请参阅 [Admin 示例](https://github.com/eventhorizon-cli/EventHorizon.RocketMQ/blob/main/samples/remoting/GenericHost/Admin/README.zh-CN.md)。

## LitePullConsumer

应用代码需要轮询消息并自行决定提交时机时，请使用 `IRemotingLitePullConsumer`：

```csharp
using EventHorizon.RocketMQ.Remoting;
using EventHorizon.RocketMQ.Remoting.Consumer;
using EventHorizon.RocketMQ.Remoting.Consumer.LitePull;

rocketMQ.AddRemotingLitePullConsumer(options =>
{
    options.GroupName = "orders-lite-pull-consumer";
    options.InitialPosition = ConsumeFromPosition.Beginning;
    options.EnableAutoCommit = false;
    options.Subscribe("orders", new FilterExpression("created"));
});

public sealed class OrderLitePuller(IRemotingLitePullConsumer consumer)
{
    public async Task PollOnceAsync(CancellationToken cancellationToken)
    {
        var messages = await consumer.PollAsync(cancellationToken: cancellationToken);
        foreach (var message in messages)
        {
            await ProcessAsync(message.Body, cancellationToken);
        }

        await consumer.CommitAsync(cancellationToken);
    }

    private static Task ProcessAsync(
        byte[] body,
        CancellationToken cancellationToken) => Task.CompletedTask;
}
```

订阅模式与手工分配互斥。手工模式使用 `GetMessageQueuesAsync` 和 `AssignAsync`。位点控制 API 包括 `Pause`、
`Resume`、`Seek`、`SeekToBeginningAsync`、`SeekToEndAsync` 和 `SeekToTimestampAsync`。

`EnableAutoCommit` 默认为 `true`。业务处理必须在 `CommitAsync` 之前完成时，请将其禁用。LitePull 使用集群模式，
并提供至少一次投递语义；发生故障或重新分配后要处理重复消息。

可运行示例请参阅 [LitePullConsumer 示例](https://github.com/eventhorizon-cli/EventHorizon.RocketMQ/blob/main/samples/remoting/GenericHost/LitePullConsumer/README.zh-CN.md)。

## PushConsumer

客户端需要接收消息并自动调用应用处理器时，请使用 `IRemotingPushConsumer`：

```csharp
using System.Text;
using EventHorizon.RocketMQ.Remoting;
using EventHorizon.RocketMQ.Remoting.Consumer;
using EventHorizon.RocketMQ.Remoting.Consumer.Push;
using Microsoft.Extensions.DependencyInjection;

rocketMQ.AddRemotingPushConsumer<OrderMessageHandler>(ServiceLifetime.Scoped, options =>
{
    options.GroupName = "orders-push-consumer";
    options.QueueAssignmentMode = RemotingPushQueueAssignmentMode.Client;
    options.MaxConcurrency = 8;
    options.ConsumeMessageBatchSize = 16;
    options.MaxDeliveryAttempts = 16;
    options.Subscribe("orders", new FilterExpression("created"));
});

public sealed class OrderMessageHandler : IRemotingPushMessageHandler
{
    public ValueTask<ConsumeResult> HandleAsync(
        IReadOnlyList<RemotingMessageView> messages,
        RemotingPushConsumeContext context,
        CancellationToken cancellationToken)
    {
        foreach (var message in messages)
        {
            Console.WriteLine(Encoding.UTF8.GetString(message.Body));
        }

        return ValueTask.FromResult(ConsumeResult.Success);
    }
}
```

通过 `QueueAssignmentMode` 选择队列分配策略。接收模式变化时，处理器 API 不变：

| `QueueAssignmentMode` | 适用场景 | 队列分配与接收行为 |
| --- | --- | --- |
| `Client` | 集群、广播和有序消费，默认值。 | 客户端分配队列并使用 PULL 接收。 |
| `Broker` | 并发集群消费。 | Broker 分配队列；主题和消费组的服务端请求模式决定使用 PULL 还是 POP。 |

应用不直接选择 POP，也不需要为它编写不同的处理器。即使请求 `Broker`，广播和有序消费也会使用客户端分配和
PULL。Broker 分配查询失败时，Consumer 会等待下一轮协调，不会切换到客户端分配。

对于 Broker-assigned POP，`PopInvisibleDuration` 是固定的处理 deadline。classic Remoting Push 不会在 handler
执行期间续租 receipt；可能超过 deadline 的处理必须保持幂等，并接受消息被重新投递。这与 gRPC Push 自动续约不同。

该行为遵循 Apache RocketMQ 官方的 classic
[PushConsumer 指南](https://rocketmq.apache.org/docs/4.x/consumer/02push/)。源码层面的 Java 与 Broker 依据记录在
[Remoting Consumer 设计](https://github.com/eventhorizon-cli/EventHorizon.RocketMQ/blob/main/docs/zh-CN/remoting/consumer-model.md)中。

`ConsumeMessageBatchSize` 控制一次处理器调用接收的消息列表上限。处理器返回 `Success`、`Retry` 或 `DeadLetter`。
对于并发的非 FIFO 批次，返回 `Success` 前设置 `RemotingPushConsumeContext.AckIndex`，只确认连续前缀内的消息。
`DelayLevelWhenNextConsume` 控制下一次重试的延迟，或决定是否直接进入死信；默认值为 `0`，由 Broker 决定重试时间。

`ServiceLifetime.Scoped` 或 `Transient` 会为每次处理尝试创建新的异步作用域。Singleton 处理器必须支持并发访问。
由于投递语义是至少一次，处理器必须响应取消并保证幂等。

运行时可通过 `SubscribeAsync` 和 `UnsubscribeAsync` 更新订阅。新消费组默认从 `ConsumeFromPosition.End` 开始；
首次部署需要包含较早消息时，请选择 `Beginning` 或 `Timestamp`。已有提交位点优先。

可运行示例请参阅 [PushConsumer 示例](https://github.com/eventhorizon-cli/EventHorizon.RocketMQ/blob/main/samples/remoting/GenericHost/PushConsumer/README.zh-CN.md)；有关分配、
PULL/POP、重试、有序处理和位点内部机制，请参阅[经典 Remoting Consumer 模型](https://github.com/eventhorizon-cli/EventHorizon.RocketMQ/blob/main/docs/zh-CN/remoting/consumer-model.md)。

## OpenTelemetry

该包会发出 tracing 和 metrics。请在应用的 OpenTelemetry SDK 中订阅这些信号：

```csharp
builder.Services.AddOpenTelemetry()
    .WithTracing(tracing => tracing
        .AddRocketMQRemotingInstrumentation())
    .WithMetrics(metrics => metrics
        .AddRocketMQRemotingInstrumentation());
```

资源属性、采样、processor 和 exporter 由应用负责。请参阅
[OpenTelemetry 埋点设计](https://github.com/eventhorizon-cli/EventHorizon.RocketMQ/blob/main/docs/zh-CN/architecture/opentelemetry-instrumentation.md)和
[OpenTelemetry 示例](https://github.com/eventhorizon-cli/EventHorizon.RocketMQ/blob/main/samples/opentelemetry/README.zh-CN.md)。

## 兼容性与错误

- 此包连接经典 NameServer 和 Broker 端点，不连接 RocketMQ Proxy。
- Broker 分配的 POP 需要兼容的 RocketMQ 5 Broker 及匹配的服务端配置。
- SQL 过滤器、TLS、ACL、优先级、Lite 消息、撤回及其他可选能力取决于目标部署。
- `RemotingCommandException` 表示 NameServer 或 Broker 拒绝请求，并保留数字 `ResponseCode`。
- 仓库本地环境基于 Apache RocketMQ 5.5.0。要在本地运行示例，请参阅[环境指南](https://github.com/eventhorizon-cli/EventHorizon.RocketMQ/blob/main/test-environments/rocketmq/README.zh-CN.md)。

## 延伸阅读

- [仓库总览](https://github.com/eventhorizon-cli/EventHorizon.RocketMQ/blob/main/README.zh-CN.md)
- [可运行示例](https://github.com/eventhorizon-cli/EventHorizon.RocketMQ/blob/main/samples/README.zh-CN.md)
- [经典 Remoting Consumer 模型](https://github.com/eventhorizon-cli/EventHorizon.RocketMQ/blob/main/docs/zh-CN/remoting/consumer-model.md)
- [经典 Remoting 传输与角色](https://github.com/eventhorizon-cli/EventHorizon.RocketMQ/blob/main/docs/zh-CN/remoting/transport-and-client-roles.md)
- [依赖注入注册与生命周期](https://github.com/eventhorizon-cli/EventHorizon.RocketMQ/blob/main/docs/zh-CN/architecture/dependency-injection-and-lifetimes.md)
- [OpenTelemetry 埋点设计](https://github.com/eventhorizon-cli/EventHorizon.RocketMQ/blob/main/docs/zh-CN/architecture/opentelemetry-instrumentation.md)
- [本地与集成测试](https://github.com/eventhorizon-cli/EventHorizon.RocketMQ/blob/main/docs/zh-CN/testing/local-and-integration-testing.md)

## 许可证

本项目使用 [Apache License 2.0](https://github.com/eventhorizon-cli/EventHorizon.RocketMQ/blob/main/LICENSE) 授权。
