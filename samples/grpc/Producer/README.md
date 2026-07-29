# gRPC Producer

[English](README.md) | [简体中文](README.zh-CN.md)

`IGrpcProducer` publishes messages through a RocketMQ 5 Proxy. Choose this role when the application reaches
RocketMQ through a gRPC `Endpoint` and wants the Proxy-based message model, lifecycle, and telemetry. It is not a
direct Broker or NameServer client.

Use the [Remoting Producer](../../remoting/Producer/README.md) instead when the application must use classic-only
operations such as selecting a physical publish queue, batch or one-way sends, or request-reply, or when the
deployment exposes a NameServer and Broker endpoints rather than a Proxy.

## Registration and lifecycle

Register the protocol client first, then add the Producer role to that registration:

```csharp
builder.Services
    .AddRocketMQGrpc(clientSection.Bind)
    .AddGrpcProducer(producerSection.Bind);
```

The role is exposed as `IGrpcProducer`. The .NET host starts and stops a DI-registered Producer with the application,
so application services normally inject and use it rather than calling `StartAsync` or `StopAsync` themselves. A
gRPC Producer does not use a consumer group.

A registration name creates an independent client registration and also becomes its .NET keyed-service key. Use
this when one process must publish through different Proxy endpoints, credentials, or Producer settings:

```csharp
builder.Services
    .AddRocketMQGrpc("audit", auditClientSection.Bind)
    .AddGrpcProducer(auditProducerSection.Bind);

static Task<GrpcSendReceipt> PublishAsync(
    Message message,
    [FromKeyedServices("audit")] IGrpcProducer producer,
    CancellationToken cancellationToken) =>
    producer.SendAsync(message, cancellationToken);
```

The registration name is application composition metadata. It does not create a Broker-side Producer type or an
audit feature, and a keyed registration does not fall back to the default registration.

## Sending and receipts

Create a protocol-specific `Message`, then call `SendAsync`:

```csharp
var message = new Message("orders", Encoding.UTF8.GetBytes(orderId))
{
    Tag = "created"
};
message.Keys.Add(orderId);

GrpcSendReceipt receipt = await producer.SendAsync(message, cancellationToken);
```

`GrpcSendReceipt` contains the server-assigned `MessageId`, storage `Offset`, Broker endpoints, and optional
transaction or recall data. Completion means the RocketMQ service returned a send receipt; it does not mean a
consumer processed the message. Send failures are raised as exceptions, and the Producer is not a durable local
outbox. Propagate cancellation and decide retries at the application boundary where duplicate-send consequences can
be handled.

## Message modes

`SendAsync` sends a standard message unless one specialized property is set:

| Requirement | SDK API | Constraint |
| --- | --- | --- |
| FIFO delivery | `Message.MessageGroup` | Provides stable queue affinity; the destination must support FIFO. |
| Delayed delivery | `Message.DeliveryTimestamp` | The destination must support scheduled delivery. Use the receipt's `RecallHandle` with `RecallAsync` before delivery when recall is supported. |
| Priority delivery | `Message.Priority` | The priority is non-negative and requires server support. |
| Lite message | `Message.LiteTopic` | Requires a Proxy and Broker that support Lite messages. |
| Transactional message | `SendTransactionAsync` | Declare transactional topics and configure a transaction checker before startup; commit or roll back the returned transaction. |

These specialized modes depend on Proxy and Broker capabilities. Do not infer support from the client API alone.
The [gRPC protocol guide](../../../src/EventHorizon.RocketMQ.Grpc/README.md) covers their complete configuration and
transaction recovery semantics.

## Prerequisites and run

The default configuration expects a Proxy at `localhost:8081` and a writable normal topic named
`eventhorizon-test-topic`. The repository environment prepares that topic:

```shell
docker compose -f test-environments/rocketmq/compose.yaml up -d --wait
dotnet run --project samples/grpc/Producer
```

For an external cluster, point `RocketMQ:Client:Endpoint` at its Proxy and create the topic when automatic resource
creation is disabled. The runnable project exposes its send operation through Swagger and HTTP; those endpoints are
only an application shell around the SDK workflow above.
