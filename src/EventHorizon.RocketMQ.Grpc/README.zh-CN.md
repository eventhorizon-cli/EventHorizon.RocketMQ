# EventHorizon.RocketMQ.Grpc

[English](https://github.com/eventhorizon-cli/EventHorizon.RocketMQ/blob/main/src/EventHorizon.RocketMQ.Grpc/README.md) |
[简体中文](https://github.com/eventhorizon-cli/EventHorizon.RocketMQ/blob/main/src/EventHorizon.RocketMQ.Grpc/README.zh-CN.md)

`EventHorizon.RocketMQ.Grpc` 是面向 .NET 的非官方 Apache RocketMQ 5 protobuf/gRPC 客户端，支持 `net8.0`
和 `net10.0`，通过 `GrpcClientOptions.Endpoint` 连接 RocketMQ Proxy，不会直接连接 NameServer 或 Broker。

当部署环境提供 RocketMQ 5 Proxy 时，请选择此包。应用必须使用经典 NameServer/Broker 协议，或需要队列选择、
批量或单向发送、请求-响应、由应用控制 LitePull 轮询与位点等 classic-only API 时，请选择
[`EventHorizon.RocketMQ.Remoting`](https://www.nuget.org/packages/EventHorizon.RocketMQ.Remoting)。

如需强类型集成事件，请参阅
[EventHorizon.RocketMQ.EventBus](https://github.com/eventhorizon-cli/EventHorizon.RocketMQ.EventBus)。

## 安装

```shell
dotnet add package EventHorizon.RocketMQ.Grpc
```

公开类型位于 `EventHorizon.RocketMQ.Grpc` 命名空间树下。如果同时使用两个协议包，请为同名模型设置 alias：

```csharp
using GrpcMessage = EventHorizon.RocketMQ.Grpc.Producer.Message;
using RemotingMessage = EventHorizon.RocketMQ.Remoting.Producer.Message;
```

## 支持的 API

使用 `AddRocketMQGrpc` 注册一个客户端，再向返回的 builder 添加一个或多个角色。

| 角色或功能 | 公开 API | 说明 |
| --- | --- | --- |
| 普通消息 | `IGrpcProducer.SendAsync` | 通过 Proxy 发送。 |
| FIFO 消息 | `Message.MessageGroup` | 目标端必须支持 FIFO 投递。 |
| 延迟消息 | `Message.DeliveryTimestamp` | 服务端支持时，可在投递前使用回执中的 recall handle 调用 `RecallAsync`。 |
| 优先级消息 | `Message.Priority` | 目标端必须支持优先级消息。 |
| Lite 消息 | `Message.LiteTopic` | 需要 Proxy 和 Broker 提供兼容支持。 |
| 事务消息 | `IGrpcProducer.SendTransactionAsync` | 在启动前声明事务 topic，并配置事务检查器。 |
| 显式接收和结算 | `IGrpcSimpleConsumer` | 应用调用 `ReceiveAsync`、`AckAsync`、不可见时间和死信 API。 |
| 自动分发 | `IGrpcPushConsumer` | typed handler 返回 `Success`、`Retry` 或 `DeadLetter`。 |
| Lite 自动分发 | `IGrpcLitePushConsumer` | 在一个已配置的 bind topic 下分发 `LiteTopics`。 |
| Tag 和 SQL 过滤器 | `FilterExpression` | SQL 过滤需要匹配的服务端配置。 |
| 运行时订阅 | `SubscribeAsync` / `UnsubscribeAsync` 及 Lite 等效 API | 由适用的 Consumer 角色支持。 |

## 快速开始

假设 Proxy 位于 `localhost:8081`，且已经存在 `orders` topic，下面的最小 ASP.NET Core host 可以发布一条消息：

```csharp
using System.Text;
using EventHorizon.RocketMQ.Grpc;
using EventHorizon.RocketMQ.Grpc.Producer;

var builder = WebApplication.CreateBuilder(args);
builder.Services
    .AddRocketMQGrpc(options => options.Endpoint = "localhost:8081")
    .AddGrpcProducer();

var app = builder.Build();
app.MapPost("/messages", async (IGrpcProducer producer, CancellationToken cancellationToken) =>
{
    var receipt = await producer.SendAsync(
        new Message("orders", Encoding.UTF8.GetBytes("created")),
        cancellationToken);

    return Results.Ok(new { receipt.MessageId });
});

await app.RunAsync();
```

## 连接与安全

`Endpoint` 接受一个或多个以分号分隔的 Proxy 地址。可以使用 `host:port` 值，或绝对的 `http`/`https`
URI。设置 `UseTLS = true` 可启用 TLS；显式指定的 endpoint scheme 必须与该设置一致。默认 endpoint 为
`localhost:8081`。

```csharp
builder.Services.AddRocketMQGrpc(options =>
{
    options.Endpoint = "proxy-a:8081;proxy-b:8081";
    options.UseTLS = true;
    options.AccessKey = builder.Configuration["RocketMQ:AccessKey"];
    options.AccessSecret = builder.Configuration["RocketMQ:AccessSecret"];
    options.SecurityToken = builder.Configuration["RocketMQ:SecurityToken"];
    options.Namespace = "orders";
    options.RequestTimeout = TimeSpan.FromSeconds(5);
    options.RouteCacheDuration = TimeSpan.FromSeconds(30);
    options.HeartbeatInterval = TimeSpan.FromSeconds(30);
});
```

`AccessKey` 和 `AccessSecret` 必须同时提供。`SecurityToken` 也要求同时提供这两个 ACL 凭据。请将凭据存放在应用配置或 secret store 中，不要写入源代码。`Namespace` 会限定 topic 和 consumer group。应用必须能够访问 Proxy，且请求所需操作对应的 Broker 广播端点也必须可访问。

### Keyed 注册

注册名称会创建独立连接，同时也是 .NET keyed service 的 key：

```csharp
using Microsoft.Extensions.DependencyInjection;
using EventHorizon.RocketMQ.Grpc.Producer;

builder.Services
    .AddRocketMQGrpc("orders", options => options.Endpoint = "orders-proxy:8081")
    .AddGrpcProducer();

public sealed class OrderPublisher(
    [FromKeyedServices("orders")] IGrpcProducer producer)
{
    public Task<GrpcSendReceipt> PublishAsync(
        Message message,
        CancellationToken cancellationToken) =>
        producer.SendAsync(message, cancellationToken);
}
```

使用 `GetRequiredKeyedService<T>(registrationName)` 或 `GetKeyedServices<T>(registrationName)` 解析 keyed Consumer。未命名注册使用普通构造函数注入。

## Producer

创建协议专用的 `Message`，再通过 `IGrpcProducer` 发送：

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
        return producer.SendAsync(message, cancellationToken);
    }
}
```

`GrpcSendReceipt` 包含服务端分配的消息 ID、存储位点、Broker 端点，以及可选的事务或撤回数据。发送完成表示 RocketMQ 服务已接受消息，并不表示 Consumer 已处理消息。发送失败会抛出异常。`DeliveryTimestamp`、`Priority` 和 `LiteTopic` 分别选择对应的消息模式。使用事务时，将 topic 加入 `GrpcProducerOptions.Topics`，配置 `TransactionChecker`，然后提交或回滚 `SendTransactionAsync` 返回的事务。

## SimpleConsumer

当接收时机和结算由应用代码控制时，请使用 `IGrpcSimpleConsumer`：

```csharp
using EventHorizon.RocketMQ.Grpc.Consumer;
using EventHorizon.RocketMQ.Grpc.Consumer.Simple;

rocketMQ.AddGrpcSimpleConsumer(options =>
{
    options.GroupName = "orders-simple";
    options.AwaitDuration = TimeSpan.FromSeconds(5);
    options.InvisibleDuration = TimeSpan.FromSeconds(30);
    options.Subscribe("orders", new FilterExpression("created"));
});

public sealed class OrderReceiver(IGrpcSimpleConsumer consumer)
{
    public async Task ReceiveOnceAsync(CancellationToken cancellationToken)
    {
        var messages = await consumer.ReceiveAsync(16, cancellationToken: cancellationToken);
        foreach (var message in messages)
        {
            await ProcessAsync(message.Body, cancellationToken);
            await consumer.AckAsync(message, cancellationToken);
        }
    }

    private static Task ProcessAsync(byte[] body, CancellationToken cancellationToken) => Task.CompletedTask;
}
```

调用 `SubscribeAsync` 或 `UnsubscribeAsync` 可在启动后修改订阅。`ReceiveAsync` 既不会确认消息，也不会启用自动续约；
只有在消息完成持久化处理后才确认。对于长时间运行的处理，需要显式延长不可见时间；应用决定将消息转入死信时，
调用 `ForwardToDeadLetterQueueAsync`。RocketMQ 5.5.0 Proxy 要求接收长轮询间隔至少为五秒。

## PushConsumer

需要自动接收、分发和结算时，请使用 `IGrpcPushConsumer`。注册 typed
`IGrpcPushMessageHandler`；如果 handler 使用 scoped 应用服务，适合选择 `Scoped`。
Push 还会在 handler 执行期间管理消息的不可见时间，应用无需自行续租 receipt。

```csharp
using System.Text;
using EventHorizon.RocketMQ.Grpc.Consumer;
using EventHorizon.RocketMQ.Grpc.Consumer.Push;
using Microsoft.Extensions.DependencyInjection;

rocketMQ.AddGrpcPushConsumer<OrderHandler>(ServiceLifetime.Scoped, options =>
{
    options.GroupName = "orders-push";
    options.MaxConcurrency = 8;
    options.Subscribe("orders", new FilterExpression("created"));
});

public sealed class OrderHandler : IGrpcPushMessageHandler
{
    public ValueTask<ConsumeResult> HandleAsync(
        GrpcMessageView message,
        CancellationToken cancellationToken)
    {
        Console.WriteLine(Encoding.UTF8.GetString(message.Body));
        return ValueTask.FromResult(ConsumeResult.Success);
    }
}
```

返回 `ConsumeResult.Success` 表示确认消息，返回 `Retry` 请求重新投递，返回 `DeadLetter` 将消息转入消费组的
死信队列。由于可能发生重试和重复投递，handler 应保持幂等。`SubscribeAsync` 和 `UnsubscribeAsync` 可在运行时
更新普通 Push 订阅。

## LitePushConsumer

需要从一个 LITE parent（bind）topic 下的 LiteTopic 自动分发消息时，请使用 `IGrpcLitePushConsumer`：

```csharp
using EventHorizon.RocketMQ.Grpc.Consumer.Lite;
using EventHorizon.RocketMQ.Grpc.Consumer.Push;
using Microsoft.Extensions.DependencyInjection;

rocketMQ.AddGrpcLitePushConsumer<OrderHandler>(ServiceLifetime.Scoped, options =>
{
    options.GroupName = "orders-lite-push";
    options.BindTopic = "orders-lite-parent";
    options.LiteTopics.Add("orders-us");
    options.LiteTopics.Add("orders-eu");
});
```

LitePush 不要调用 `Subscribe`。启动时使用 `LiteTopics`，启动后使用 `SubscribeLiteAsync` 和
`UnsubscribeLiteAsync`。`IGrpcLitePushConsumer` 使用与 PushConsumer 相同的 handler 结果。

LitePush 部署要求：

- 存储 parent topic 的 Broker 必须启用 `enableLmq=true` 和 `enableMultiDispatch=true`。
- 使用 `message.type=LITE` 创建 parent topic，并使用 `lite.bind.topic=<parent-topic>` 配置消费组。
- 连接实现 `SyncLiteSubscription` 的 cluster-mode Proxy。
- 在 RocketMQ 5.5.0 中，`mqbroker --enable-proxy` 启动的 Proxy 不实现该 RPC；请使用 `mqproxy -pm cluster` 或兼容的新版 Proxy。

## OpenTelemetry

该包会发出 OpenTelemetry Activity 和 metrics，但不会选择 exporter。请将包的 source 和 meter 加入应用的 OpenTelemetry SDK：

```csharp
using EventHorizon.RocketMQ.Grpc;
using OpenTelemetry.Metrics;
using OpenTelemetry.Trace;

builder.Services.AddOpenTelemetry()
    .WithTracing(tracing => tracing
        .AddRocketMQGrpcInstrumentation())
    .WithMetrics(metrics => metrics
        .AddRocketMQGrpcInstrumentation());
```

资源属性、采样、processor 和 exporter 由应用负责。详见
[OpenTelemetry 埋点设计](https://github.com/eventhorizon-cli/EventHorizon.RocketMQ/blob/main/docs/zh-CN/architecture/opentelemetry-instrumentation.md)
和 [OpenTelemetry 示例](https://github.com/eventhorizon-cli/EventHorizon.RocketMQ/tree/main/samples/opentelemetry)。

## 部署与错误

如果部署环境禁用了自动创建，请在启动前准备所需的 topic 和 consumer group。确认 Proxy 版本和 Broker 配置支持所选功能。NameServer 地址不是有效的 gRPC `Endpoint`；如果 Proxy 缺少所需 RPC，单靠客户端配置无法补足。

已完成调用返回不成功的 RocketMQ 状态时，会抛出 `GrpcServiceException`；请检查其 `ResponseCode` 获取 RocketMQ 数字服务码。客户端侧故障派生自 `RocketMQClientException`。关于完整的 Consumer 模型、Proxy 兼容性说明和生命周期细节，请参阅
[gRPC Consumer 模型](https://github.com/eventhorizon-cli/EventHorizon.RocketMQ/blob/main/docs/zh-CN/grpc/consumer-model.md)、
[DI 注册与生命周期](https://github.com/eventhorizon-cli/EventHorizon.RocketMQ/blob/main/docs/zh-CN/architecture/dependency-injection-and-lifetimes.md)
以及[协议边界](https://github.com/eventhorizon-cli/EventHorizon.RocketMQ/blob/main/docs/zh-CN/architecture/protocol-boundaries.md)。

## 示例

- [gRPC 示例索引](https://github.com/eventhorizon-cli/EventHorizon.RocketMQ/tree/main/samples/grpc)
- [Producer](https://github.com/eventhorizon-cli/EventHorizon.RocketMQ/tree/main/samples/grpc/GenericHost/Producer)
- [SimpleConsumer](https://github.com/eventhorizon-cli/EventHorizon.RocketMQ/tree/main/samples/grpc/GenericHost/SimpleConsumer)
- [PushConsumer](https://github.com/eventhorizon-cli/EventHorizon.RocketMQ/tree/main/samples/grpc/GenericHost/PushConsumer)
- [LitePushConsumer](https://github.com/eventhorizon-cli/EventHorizon.RocketMQ/tree/main/samples/grpc/GenericHost/LitePushConsumer)
- [LiteProducer](https://github.com/eventhorizon-cli/EventHorizon.RocketMQ/tree/main/samples/grpc/GenericHost/LiteProducer)
- [仓库总览与包矩阵](https://github.com/eventhorizon-cli/EventHorizon.RocketMQ#packages)

## 许可证

本包以 [Apache License 2.0](https://github.com/eventhorizon-cli/EventHorizon.RocketMQ/blob/main/LICENSE) 授权。
