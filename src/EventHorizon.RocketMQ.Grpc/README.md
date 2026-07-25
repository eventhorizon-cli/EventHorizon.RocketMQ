# EventHorizon.RocketMQ.Grpc

[English](README.md) |
[简体中文](README.zh-CN.md)

> **[Read the repository overview and runnable samples first](../../README.md).** It contains the
> cross-protocol feature matrix and package-selection guidance.

An idiomatic .NET client for the Apache RocketMQ 5 protobuf/gRPC protocol. It targets .NET 8 or
later and connects to a RocketMQ Proxy, not directly to a NameServer.

## Supported features

`✅` means this package implements the client API. A target RocketMQ Proxy and Broker must still
support and enable the applicable server-side feature. `—` means the gRPC package does not expose
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
| Dependency injection, options, logging, Generic Host lifecycle, and named/keyed profiles | ✅ | Register a profile with `AddRocketMQGrpc`, then add the required roles. |

## Installation

Configure the package source used by your application, then install the gRPC package. The shared
package is referenced transitively.

```shell
dotnet add package EventHorizon.RocketMQ.Grpc --version 0.1.0-alpha.1
```

## Host registration and connection

Register a gRPC profile with `AddRocketMQGrpc`, then add each role needed by the application. A
Generic Host starts and stops registered roles with the application.

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

`Endpoint` accepts one or more semicolon-separated Proxy addresses. Access-point requests fail over
within their request deadline. Set `UseTLS` to use TLS. `AccessKey`, `AccessSecret`, and
`SecurityToken` configure ACL credentials; `Namespace` qualifies topics and consumer groups.

When one host needs multiple clusters or multiple instances of the same role, use named profiles.
The profile name is also the keyed-service key.

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

A role can be registered only once in a profile. Unnamed profiles use ordinary constructor
injection. When clients are resolved from a standalone `ServiceProvider` instead of a Generic Host,
call their `StartAsync` and `StopAsync` methods explicitly.

## Producer

Register `IGrpcProducer` with `AddGrpcProducer` and inject it into application services.

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

Declare every transactional topic before the Producer starts and configure a checker that can
recover the durable local outcome when requested by the Broker.

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
immediately. The generic registration selects the handler lifetime for the current client profile:
`Singleton` creates one handler per profile/role and must be thread-safe; `Scoped` and `Transient`
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
