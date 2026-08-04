# EventHorizon.RocketMQ.Grpc

[English](https://github.com/eventhorizon-cli/EventHorizon.RocketMQ/blob/main/src/EventHorizon.RocketMQ.Grpc/README.md) |
[Simplified Chinese](https://github.com/eventhorizon-cli/EventHorizon.RocketMQ/blob/main/src/EventHorizon.RocketMQ.Grpc/README.zh-CN.md)

`EventHorizon.RocketMQ.Grpc` is an unofficial Apache RocketMQ 5 protobuf/gRPC client for .NET. It targets `net8.0`
and `net10.0`, and connects to a RocketMQ Proxy through `GrpcClientOptions.Endpoint`. It does not connect directly to
a NameServer or Broker.

Choose this package when the deployment exposes a RocketMQ 5 Proxy. Choose
[`EventHorizon.RocketMQ.Remoting`](https://www.nuget.org/packages/EventHorizon.RocketMQ.Remoting)
when the application must use the classic NameServer/Broker protocol or classic-only APIs such as queue selection,
batch or one-way sends, request-reply, or application-controlled LitePull polling and offset positioning.

For strongly typed integration events, see
[EventHorizon.RocketMQ.EventBus](https://github.com/eventhorizon-cli/EventHorizon.RocketMQ.EventBus).

## Install

```shell
dotnet add package EventHorizon.RocketMQ.Grpc
```

Public types are under the `EventHorizon.RocketMQ.Grpc` namespace tree. If both protocol packages are used, alias
duplicate model names:

```csharp
using GrpcMessage = EventHorizon.RocketMQ.Grpc.Producer.Message;
using RemotingMessage = EventHorizon.RocketMQ.Remoting.Producer.Message;
```

## Supported API

Register one client with `AddRocketMQGrpc`, then add one or more roles to the returned builder.

| Role or feature | Public API | Notes |
| --- | --- | --- |
| Standard messages | `IGrpcProducer.SendAsync` | Sends through the Proxy. |
| FIFO messages | `Message.MessageGroup` | The destination must support FIFO delivery. |
| Delayed messages | `Message.DeliveryTimestamp` | Use the receipt's recall handle with `RecallAsync` before delivery when supported. |
| Priority messages | `Message.Priority` | The destination must support priority messages. |
| Lite messages | `Message.LiteTopic` | Requires compatible Proxy and Broker support. |
| Transactional messages | `IGrpcProducer.SendTransactionAsync` | Declare transactional topics and configure a transaction checker before startup. |
| Explicit receive and settlement | `IGrpcSimpleConsumer` | The application calls `ReceiveAsync`, `AckAsync`, invisibility, and dead-letter APIs. |
| Automatic dispatch | `IGrpcPushConsumer` | Typed handlers return `Success`, `Retry`, or `DeadLetter`. |
| Lite automatic dispatch | `IGrpcLitePushConsumer` | Dispatches LiteTopics under one configured bind topic. |
| Tag and SQL filters | `FilterExpression` | SQL filtering requires matching server configuration. |
| Runtime subscriptions | `SubscribeAsync` / `UnsubscribeAsync` and Lite equivalents | Supported by the applicable Consumer role. |

## Quick start

With a Proxy on `localhost:8081` and an existing `orders` topic, a minimal ASP.NET Core host can publish a message:

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

## Connection and security

`Endpoint` accepts one or more semicolon-separated Proxy addresses. Use `host:port` values or absolute `http`/`https`
URIs. Set `UseTLS = true` for TLS; an explicit endpoint scheme must agree with that setting. The default endpoint is
`localhost:8081`.

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

`AccessKey` and `AccessSecret` must be supplied together. `SecurityToken` also requires both ACL credentials. Keep
credentials in application configuration or a secret store rather than source code. `Namespace` qualifies topics and
consumer groups. The Proxy must be reachable from the application, and its advertised Broker endpoints must also be
reachable for the requested operation.

### Keyed registrations

A registration name creates an independent connection and is also the .NET keyed-service key:

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

Resolve keyed Consumers with `GetRequiredKeyedService<T>(registrationName)` or
`GetKeyedServices<T>(registrationName)`. An unkeyed registration uses ordinary constructor injection.

## Producer

Create a protocol-specific `Message`, then send it through `IGrpcProducer`:

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

`GrpcSendReceipt` contains the server-assigned message ID, storage offset, Broker endpoints, and optional transaction
or recall data. A completed send means the RocketMQ service accepted the message; it does not mean a Consumer finished
processing it. Send failures are raised as exceptions. `DeliveryTimestamp`, `Priority`, and `LiteTopic` select the
corresponding message modes. For transactions, add the topic to `GrpcProducerOptions.Topics`, configure
`TransactionChecker`, then commit or roll back the transaction returned by `SendTransactionAsync`.

## SimpleConsumer

Use `IGrpcSimpleConsumer` when application code owns receive timing and settlement:

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

Call `SubscribeAsync` or `UnsubscribeAsync` to change subscriptions after startup. `ReceiveAsync` does not acknowledge
messages and does not enable automatic renewal; acknowledge only after durable processing, extend invisibility for
long-running work, or call
`ForwardToDeadLetterQueueAsync` when the application chooses to dead-letter a message. RocketMQ 5.5.0 Proxy requires a
five-second minimum receive long-poll interval.

## PushConsumer

Use `IGrpcPushConsumer` for automatic receive, handler dispatch, and settlement. Register a typed
`IGrpcPushMessageHandler`; `Scoped` is appropriate when the handler uses scoped application services.
Push also manages message invisibility while the handler is active; applications do not renew receipts themselves.

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

Return `ConsumeResult.Success` to acknowledge, `Retry` for redelivery, or `DeadLetter` to forward to the consumer
group's dead-letter queue. Handlers should be idempotent because retries and duplicate delivery are possible.
`SubscribeAsync` and `UnsubscribeAsync` update ordinary Push subscriptions at runtime.

## LitePushConsumer

Use `IGrpcLitePushConsumer` for automatic dispatch from LiteTopics under one LITE parent (bind) topic:

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

Do not call `Subscribe` for LitePush. Use `LiteTopics` at startup or `SubscribeLiteAsync` and
`UnsubscribeLiteAsync` after startup. `IGrpcLitePushConsumer` uses the same handler outcomes as PushConsumer.

LitePush deployment requirements:

- Brokers storing the parent topic must enable `enableLmq=true` and `enableMultiDispatch=true`.
- Create the parent topic with `message.type=LITE` and configure the group with `lite.bind.topic=<parent-topic>`.
- Connect to a cluster-mode Proxy that implements `SyncLiteSubscription`.
- In RocketMQ 5.5.0, the Proxy started by `mqbroker --enable-proxy` does not implement that RPC; use
  `mqproxy -pm cluster` or a compatible newer Proxy.

## OpenTelemetry

The package emits OpenTelemetry Activities and metrics but does not select an exporter. Add the package's source and
meter to the application's OpenTelemetry SDK:

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

The application owns resource attributes, sampling, processors, and exporters. See the
[OpenTelemetry instrumentation design](https://github.com/eventhorizon-cli/EventHorizon.RocketMQ/blob/main/docs/en-US/architecture/opentelemetry-instrumentation.md)
and [OpenTelemetry sample](https://github.com/eventhorizon-cli/EventHorizon.RocketMQ/tree/main/samples/opentelemetry).

## Deployment and errors

Provision the requested topics and consumer groups before startup when the deployment disables automatic creation.
Verify that the Proxy version and Broker configuration support each selected feature. A NameServer address is not a
valid gRPC `Endpoint`, and a Proxy that lacks a required RPC cannot be fixed by client configuration alone.

Unsuccessful RocketMQ statuses from completed calls throw `GrpcServiceException`; inspect its `ResponseCode` for the
numeric RocketMQ service code. Client-side failures derive from `RocketMQClientException`. For the full consumer
model, Proxy compatibility notes, and lifecycle details, see the
[gRPC consumer model](https://github.com/eventhorizon-cli/EventHorizon.RocketMQ/blob/main/docs/en-US/grpc/consumer-model.md),
[DI registrations and lifetimes](https://github.com/eventhorizon-cli/EventHorizon.RocketMQ/blob/main/docs/en-US/architecture/dependency-injection-and-lifetimes.md),
and [protocol boundaries](https://github.com/eventhorizon-cli/EventHorizon.RocketMQ/blob/main/docs/en-US/architecture/protocol-boundaries.md).

## Samples

- [gRPC samples index](https://github.com/eventhorizon-cli/EventHorizon.RocketMQ/tree/main/samples/grpc)
- [Producer](https://github.com/eventhorizon-cli/EventHorizon.RocketMQ/tree/main/samples/grpc/GenericHost/Producer)
- [SimpleConsumer](https://github.com/eventhorizon-cli/EventHorizon.RocketMQ/tree/main/samples/grpc/GenericHost/SimpleConsumer)
- [PushConsumer](https://github.com/eventhorizon-cli/EventHorizon.RocketMQ/tree/main/samples/grpc/GenericHost/PushConsumer)
- [LitePushConsumer](https://github.com/eventhorizon-cli/EventHorizon.RocketMQ/tree/main/samples/grpc/GenericHost/LitePushConsumer)
- [LiteProducer](https://github.com/eventhorizon-cli/EventHorizon.RocketMQ/tree/main/samples/grpc/GenericHost/LiteProducer)
- [Repository overview and package matrix](https://github.com/eventhorizon-cli/EventHorizon.RocketMQ#packages)

## License

Licensed under the [Apache License 2.0](https://github.com/eventhorizon-cli/EventHorizon.RocketMQ/blob/main/LICENSE).
