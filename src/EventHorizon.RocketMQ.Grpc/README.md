# EventHorizon.RocketMQ.Grpc

[English](https://github.com/eventhorizon-cli/EventHorizon.RocketMQ/blob/main/src/EventHorizon.RocketMQ.Grpc/README.md) |
[简体中文](https://github.com/eventhorizon-cli/EventHorizon.RocketMQ/blob/main/src/EventHorizon.RocketMQ.Grpc/README.zh-CN.md)

`EventHorizon.RocketMQ.Grpc` is the repository's unofficial Apache RocketMQ 5 protobuf/gRPC project and
NuGet package for .NET 8 or later. It connects only to a RocketMQ Proxy through `Endpoint`; it does not
connect directly to a NameServer or Broker.

> See the [repository overview](https://github.com/eventhorizon-cli/EventHorizon.RocketMQ/blob/main/README.md)
> for the cross-protocol matrix and package-selection guidance, then use the
> [runnable gRPC samples](https://github.com/eventhorizon-cli/EventHorizon.RocketMQ/blob/main/samples/README.md#grpc)
> for complete applications.

## Quick start

Configure the package source used by your application, then install the gRPC package. The shared package is
referenced transitively.

```shell
dotnet add package EventHorizon.RocketMQ.Grpc
```

With a reachable Proxy on `localhost:8081` and an existing `orders` topic, this ASP.NET Core Minimal API
registers the default Producer and injects it into an endpoint that sends one message:

```csharp
using System.Text;
using EventHorizon.RocketMQ.Grpc;
using EventHorizon.RocketMQ.Grpc.Producer;
using EventHorizon.RocketMQ.Producer;
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

The ASP.NET Core host starts and stops the registered Producer with the application. Complete applications are
available in the [Producer](https://github.com/eventhorizon-cli/EventHorizon.RocketMQ/blob/main/samples/grpc/Producer/README.md),
[SimpleConsumer](https://github.com/eventhorizon-cli/EventHorizon.RocketMQ/blob/main/samples/grpc/SimpleConsumer/README.md),
[PushConsumer](https://github.com/eventhorizon-cli/EventHorizon.RocketMQ/blob/main/samples/grpc/PushConsumer/README.md), and
[LitePushConsumer](https://github.com/eventhorizon-cli/EventHorizon.RocketMQ/blob/main/samples/grpc/LitePushConsumer/README.md)
sample directories.

## Supported features

`✅` means this Project implements the client API. A target RocketMQ Proxy and Broker must still
support and enable the applicable server-side feature. `—` means the gRPC Project does not expose
that API.

| Feature | Status | Conditions and notes |
| --- | :---: | --- |
| Standard Producer messages | ✅ | Send with `IGrpcProducer.SendAsync`. |
| FIFO Producer messages | ✅ | Set `MessageGroup`; the destination must support FIFO delivery. |
| Timed or delayed Producer messages | ✅ | Set `DeliveryTimestamp`; the destination must support scheduled delivery. |
| Recall timed or delayed messages | ✅ | Use the returned recall handle before the message is delivered. |
| Priority Producer messages | ✅ | Set `Priority`; the destination must support priority messages. |
| Lite Producer messages | ✅ | Set `LiteTopic`; use a Proxy and Broker that support Lite messages. |
| Transactional Producer messages | ✅ | Declare every transactional topic before startup and provide a transaction checker. |
| Selected queues, queue selectors, batch sends, or one-way sends | — | These classic Remoting Producer APIs are not part of `IGrpcProducer`. |
| Producer request-reply | — | This classic Remoting Producer API is not part of `IGrpcProducer`. |
| `IGrpcSimpleConsumer` | ✅ | Application controls receive, acknowledgement, invisible duration, dead-letter forwarding, and subscriptions. RocketMQ 5.5.0 Proxy uses a five-second minimum long-poll interval. |
| `IGrpcPushConsumer` | ✅ | Supports concurrent or FIFO dispatch, acknowledgement, invisibility renewal, retry, dead-letter handling, and runtime subscriptions. It uses client-initiated assignment queries and `ReceiveMessage` long polling. |
| `IGrpcLitePushConsumer` | ✅ | Supports concurrent or FIFO dispatch from LiteTopics under one bind topic. The Proxy must implement `SyncLiteSubscription`; it also uses client-initiated long polling. |
| Protocol-level Broker push | — | Push and LitePush are not server-initiated Broker push. |
| Direct NameServer or Broker connection | — | gRPC connects to a RocketMQ Proxy through `Endpoint`. |
| Dependency injection, options, logging, Generic Host lifecycle, and default/keyed client registrations | ✅ | Add a client registration with `AddRocketMQGrpc`, then add the required roles. |

## Client registration and connection

`Endpoint` accepts one or more semicolon-separated Proxy addresses. Access-point requests fail over
within their request deadline. `RequestTimeout`, `RouteCacheDuration`, and `HeartbeatInterval` control
request, route refresh, and heartbeat timing. Set `UseTLS` to use TLS. `AccessKey`, `AccessSecret`, and
`SecurityToken` configure ACL credentials; `Namespace` qualifies topics and consumer groups. The
`AddRocketMQGrpc` return value is the builder used to add Producer and Consumer roles.

When one host needs multiple clusters or multiple instances of the same role, add a keyed client registration.
Its `registrationName` is also used as the .NET keyed-service key.

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

A role can be registered only once per client registration. The default client registration uses ordinary constructor
injection. When clients are resolved from a standalone `ServiceProvider` instead of a Generic Host,
call their `StartAsync` and `StopAsync` methods explicitly.

## Producer

The quick start registers `IGrpcProducer` with `AddGrpcProducer`. Inject it into application services to send
messages with richer metadata:

```csharp
using System.Text;
using EventHorizon.RocketMQ.Grpc.Producer;
using EventHorizon.RocketMQ.Producer;

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

`SendAsync` returns an immutable `GrpcSendReceipt` containing the message ID, offset, optional
transaction ID, optional recall handle, and Broker endpoints. Send failures are reported as
exceptions rather than receipt status values.

Set exactly one specialized message property when needed:

- `MessageGroup` selects FIFO delivery and stable queue affinity.
- `DeliveryTimestamp` schedules delayed delivery. Use the returned `RecallHandle` with
  `RecallAsync` to recall the message before delivery.
- `Priority` selects a non-negative message priority.
- `LiteTopic` selects a lite message.

### Transactions

To enable transactions, replace the quick-start `AddGrpcProducer()` call with a configured registration.
Declare every transactional topic before the Producer starts and configure a checker that can recover the
durable local outcome when requested by the Broker.

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

The checker returns `Commit` or `Rollback` when the outcome is known. `Unknown` asks the Broker to
check again later.

## SimpleConsumer

Use `IGrpcSimpleConsumer` when the application should control receive and acknowledgement timing.
Initial subscriptions are optional and can also be changed at runtime.

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

`SubscribeAsync` and `UnsubscribeAsync` update the live subscription set. The consumer also exposes
`ChangeInvisibleDurationAsync` and `ForwardToDeadLetterQueueAsync`. RocketMQ 5.5.0 Proxy defaults to
a five-second minimum long-poll interval, so keep `AwaitDuration` at five seconds or longer with
that server.

## PushConsumer

Use `IGrpcPushConsumer` when the client should receive and dispatch messages automatically.

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

Return `Success` to acknowledge a message, `Retry` to make it available for redelivery, or
`DeadLetter` to forward it to the dead-letter queue. Corrupted messages do not reach the handler:
ordinary messages become available for redelivery, while corrupted FIFO messages are dead-lettered
before the next group member is released. Runtime subscription changes reconcile receivers
immediately. The generic registration selects the handler lifetime for the current client registration:
`Singleton` creates one handler per client registration/role and must be thread-safe; `Scoped` and `Transient`
resolve a handler in a new async service scope for each handling attempt. The `MessageHandler`
delegate remains available for small stateless callbacks, but cannot be combined with a typed handler.

## LitePushConsumer

Use `IGrpcLitePushConsumer` for automatic dispatch from LiteTopics that share a LITE parent/bind topic. It uses
the same long-poll, acknowledgement, retry, dead-letter, concurrency, and FIFO options as
`IGrpcPushConsumer`, but its subscriptions are LiteTopics rather than standard topic filters.

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

Do not call the inherited `Subscribe` method or configure normal topic subscriptions for this role. The client
sends the complete configured LiteTopic set when it starts, reconciles that set periodically, and supports
runtime changes through `SubscribeLiteAsync` and `UnsubscribeLiteAsync`. Lite Push uses the same
`IGrpcPushMessageHandler` and lifetime semantics as standard Push:

```csharp
await consumer.SubscribeLiteAsync(
    "orders-apac",
    GrpcLiteOffsetOption.Last,
    cancellationToken);
```

Prepare the RocketMQ deployment before starting a Lite Push consumer:

- Enable `enableLmq=true` and `enableMultiDispatch=true` on every Broker that stores the LITE parent topic.
- Create the parent topic with `message.type=LITE`.
- Create the consumer group with `lite.bind.topic=<parent-topic>`.
- Connect to a cluster-mode Proxy that implements `SyncLiteSubscription`. In RocketMQ 5.5.0, the local Proxy
  started by `mqbroker --enable-proxy` does not implement this service; use `mqproxy -pm cluster` or a compatible
  newer Proxy.

`SyncLiteSubscription` is LitePush's subscription-control RPC. It synchronizes the consumer group, bind topic, and
LiteTopic set with the Proxy; it does not transfer messages. A separate cluster-mode Proxy can share a container or
Compose service with a Broker, so it does not require a separate Compose environment.

The service remains authoritative for LiteTopic name and quota constraints; a rejected subscription is reported
through `GrpcServiceException` and does not change the local subscription set.

## Errors

An unsuccessful RocketMQ status returned by a completed gRPC call throws `GrpcServiceException`.
Its `ResponseCode` retains the numeric RocketMQ service code. The exception derives from the shared
`RocketMQClientException`, so callers can catch either the protocol-specific exception or the common
base.

## Local testing

The repository's [Docker Compose environment](https://github.com/eventhorizon-cli/EventHorizon.RocketMQ/tree/main/test-environments/rocketmq)
runs Apache RocketMQ
5.5.0 with a gRPC Proxy on `localhost:8081`. From the repository root:

```shell
docker compose -f test-environments/rocketmq/compose.yaml config --quiet
docker compose -f test-environments/rocketmq/compose.yaml up -d --wait
```

Create topics and consumer groups with the commands in the
[environment README](https://github.com/eventhorizon-cli/EventHorizon.RocketMQ/blob/main/test-environments/rocketmq/README.md).
Stop and delete the environment
with:

```shell
docker compose -f test-environments/rocketmq/compose.yaml down -v --remove-orphans
```

The isolated Docker-backed gRPC integration tests can be run with:

```shell
dotnet test tests/EventHorizon.RocketMQ.Grpc.IntegrationTests/EventHorizon.RocketMQ.Grpc.IntegrationTests.csproj
```

## License

Licensed under the [Apache License 2.0](https://github.com/eventhorizon-cli/EventHorizon.RocketMQ/blob/main/LICENSE).
