# EventHorizon.RocketMQ.Remoting

[English](https://github.com/eventhorizon-cli/EventHorizon.RocketMQ/blob/main/src/EventHorizon.RocketMQ.Remoting/README.md) |
[简体中文](https://github.com/eventhorizon-cli/EventHorizon.RocketMQ/blob/main/src/EventHorizon.RocketMQ.Remoting/README.zh-CN.md)

`EventHorizon.RocketMQ.Remoting` is an unofficial .NET client for the classic Apache RocketMQ Remoting protocol. It
targets `net8.0` and `net10.0` and connects to NameServers and Brokers directly.

Choose this package for classic NameServer/Broker deployments. Use
[`EventHorizon.RocketMQ.Grpc`](https://www.nuget.org/packages/EventHorizon.RocketMQ.Grpc) when the application connects to a RocketMQ 5
Proxy through gRPC.

For strongly typed application events, see
[`EventHorizon.RocketMQ.Remoting.EventBus`](https://github.com/eventhorizon-cli/EventHorizon.RocketMQ.EventBus).

## Quick start

Install the package:

```shell
dotnet add package EventHorizon.RocketMQ.Remoting
```

With a reachable NameServer and an existing `orders` topic, register a Producer and send a message:

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

The Generic Host starts and stops registered roles automatically. See the
[Remoting samples](https://github.com/eventhorizon-cli/EventHorizon.RocketMQ/blob/main/samples/README.md) for complete applications.

## Features

Server-side features must also be enabled by the target RocketMQ deployment.

| Public API | Use it for |
| --- | --- |
| `IRemotingProducer` | Standard, one-way, selected-queue, batch, FIFO, delayed, priority, Lite, transactional, recall, and request-reply messages. |
| `IRemotingAdmin` | Read-only queue, offset, timestamp, and physical-message queries. |
| `IRemotingLitePullConsumer` | Application-driven polling, position control, and offset commits. |
| `IRemotingPushConsumer` | Automatic concurrent or orderly handling, with client or Broker queue assignment. |

Use LitePull when the application must poll explicitly, or Push when the client should dispatch messages to a handler.
Use `EventHorizon.RocketMQ.Grpc` for RocketMQ 5 Proxy gRPC and SimpleConsumer APIs.

## Connection and security

`AddRocketMQRemoting` configures one logical client registration:

```csharp
builder.Services.AddRocketMQRemoting(options =>
{
    options.NamesrvAddr = "nameserver-a:9876;nameserver-b:9876";
    options.Namespace = "tenant-a";
    options.RequestTimeout = TimeSpan.FromSeconds(3);
});
```

Common connection options:

| Option | Purpose |
| --- | --- |
| `NamesrvAddr` | One or more semicolon-separated NameServer addresses. |
| `Namespace` | Qualifies topics and consumer groups for one tenant or environment. |
| `RequestTimeout` | Limits normal NameServer and Broker requests. |
| `AccessKey`, `AccessSecret`, `SecurityToken` | Configures ACL credentials. The key and secret must be supplied together. |
| `UseTLS` | Enables TLS for NameServer and Broker connections. |
| `ConfigureLegacySslOptions` | Customizes trust, client certificates, revocation, protocols, or SNI. |

For multiple clusters or independent credentials, use a named registration. The registration name is also the
.NET keyed-service key:

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

See [classic Remoting transport and roles](https://github.com/eventhorizon-cli/EventHorizon.RocketMQ/blob/main/docs/en-US/remoting/transport-and-client-roles.md) for protocol and
connection internals, and [dependency-injection registrations and lifetimes](https://github.com/eventhorizon-cli/EventHorizon.RocketMQ/blob/main/docs/en-US/architecture/dependency-injection-and-lifetimes.md)
for advanced registration behavior.

## Producer

Register `IRemotingProducer` with `AddRemotingProducer`:

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

Always inspect `RemotingSendResult.Status`. A classic Broker can report `FlushDiskTimeout`,
`FlushSlaveTimeout`, or `SlaveNotAvailable` without throwing an exception. `SendOnewayAsync` does not wait for a
Broker response, so completion does not confirm storage.

Useful Producer APIs include:

- `GetPublishMessageQueuesAsync` and selected-queue or selector overloads.
- Batch `SendAsync` for messages that use one topic.
- `SendTransactionAsync` with `LocalTransactionExecutor` and `TransactionChecker`.
- `RecallAsync` for an eligible delayed message with a returned `RecallHandle`.
- `RequestAsync` and `SendReplyAsync` for request-reply workflows.

`MessageGroup`, `DeliveryTimestamp`, `Priority`, and `LiteTopic` select specialized message behavior and are
mutually exclusive. The target Broker must support the selected feature. Send retries can produce duplicates, so
consumers should be idempotent.

See the runnable [Producer sample](https://github.com/eventhorizon-cli/EventHorizon.RocketMQ/blob/main/samples/remoting/GenericHost/Producer/README.md).

## Admin

`IRemotingAdmin` provides read-only queue, offset, timestamp, and stored-message queries:

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

`GetMessageQueuesAsync` returns readable `RemotingConsumerQueue` values that can be passed directly to the offset
query methods.

`ViewMessageAsync` accepts the physical `OffsetMessageId` returned by a classic send. The application network must
be able to reach the Broker address encoded in that ID.

See the runnable [Admin sample](https://github.com/eventhorizon-cli/EventHorizon.RocketMQ/blob/main/samples/remoting/GenericHost/Admin/README.md).

## LitePullConsumer

Use `IRemotingLitePullConsumer` when application code should poll messages and decide when to commit:

```csharp
using EventHorizon.RocketMQ.Remoting;
using EventHorizon.RocketMQ.Remoting.Consumer;
using EventHorizon.RocketMQ.Remoting.Consumer.Pull.Lite;

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

Subscription mode and manual assignment are mutually exclusive. Manual mode uses
`GetMessageQueuesAsync` and `AssignAsync`. Position-control APIs include `Pause`, `Resume`, `Seek`,
`SeekToBeginningAsync`, `SeekToEndAsync`, and `SeekToTimestampAsync`.

`EnableAutoCommit` defaults to `true`. Disable it when business processing must complete before
`CommitAsync`. LitePull is clustered and at least once; handle duplicates after failures or reassignment.

See the runnable [LitePullConsumer sample](https://github.com/eventhorizon-cli/EventHorizon.RocketMQ/blob/main/samples/remoting/GenericHost/LitePullConsumer/README.md).

## PushConsumer

Use `IRemotingPushConsumer` when the client should receive messages and invoke an application handler automatically:

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

Choose the queue-assignment policy through `QueueAssignmentMode`. The handler API does not change when the receive mode
changes:

| `QueueAssignmentMode` | Applies to | Queue assignment and receive behavior |
| --- | --- | --- |
| `Client` | Clustered, broadcasting, and orderly consumption. This is the default. | The client allocates queues and receives with PULL. |
| `Broker` | Concurrent clustered consumption. | The Broker allocates queues. The topic and consumer group's server-side request mode selects PULL or POP. |

Applications do not select POP directly and do not need a different handler for it. Broadcasting and orderly
consumption use client assignment and PULL even when `Broker` is requested. If a Broker assignment query fails, the
consumer waits for the next reconciliation instead of switching to client allocation.

This distinction follows Apache RocketMQ's classic
[PushConsumer guide](https://rocketmq.apache.org/docs/4.x/consumer/02push/). The
[Remoting consumer design](https://github.com/eventhorizon-cli/EventHorizon.RocketMQ/blob/main/docs/en-US/remoting/consumer-model.md)
records the source-level Java and Broker references.

`ConsumeMessageBatchSize` controls the largest list passed to one handler call. Return `Success`, `Retry`, or
`DeadLetter`. For a concurrent non-FIFO batch, set `RemotingPushConsumeContext.AckIndex` before returning
`Success` to acknowledge only a contiguous prefix. `DelayLevelWhenNextConsume` controls the next retry delay or
direct dead-letter behavior.

`ServiceLifetime.Scoped` or `Transient` creates a fresh async scope for each handling attempt. A singleton handler
must be thread-safe. Handlers must honor cancellation and remain idempotent because delivery is at least once.

Runtime `SubscribeAsync` and `UnsubscribeAsync` update subscriptions. A new group starts at
`ConsumeFromPosition.End` by default; choose `Beginning` or `Timestamp` when the initial deployment must include
older messages. Existing committed offsets take precedence.

See the runnable [PushConsumer sample](https://github.com/eventhorizon-cli/EventHorizon.RocketMQ/blob/main/samples/remoting/GenericHost/PushConsumer/README.md) and the
[classic Remoting consumer model](https://github.com/eventhorizon-cli/EventHorizon.RocketMQ/blob/main/docs/en-US/remoting/consumer-model.md) for assignment, PULL/POP, retry,
ordering, and offset internals.

## OpenTelemetry

The package emits tracing and metrics. Configure the application's OpenTelemetry SDK to subscribe:

```csharp
builder.Services.AddOpenTelemetry()
    .WithTracing(tracing => tracing
        .AddRocketMQRemotingInstrumentation())
    .WithMetrics(metrics => metrics
        .AddRocketMQRemotingInstrumentation());
```

The application owns resource attributes, sampling, processors, and exporters. See the
[OpenTelemetry instrumentation design](https://github.com/eventhorizon-cli/EventHorizon.RocketMQ/blob/main/docs/en-US/architecture/opentelemetry-instrumentation.md) and the
[OpenTelemetry sample](https://github.com/eventhorizon-cli/EventHorizon.RocketMQ/blob/main/samples/opentelemetry/README.md).

## Compatibility and errors

- This package connects to classic NameServer and Broker endpoints, not a RocketMQ Proxy.
- Broker-assigned POP requires a compatible RocketMQ 5 Broker and matching server-side configuration.
- SQL filters, TLS, ACL, priority, Lite messages, recall, and other optional capabilities depend on the target
  deployment.
- `RemotingCommandException` reports a NameServer or Broker rejection and retains the numeric `ResponseCode`.
- The repository's local environment is based on Apache RocketMQ 5.5.0. See the
  [environment guide](https://github.com/eventhorizon-cli/EventHorizon.RocketMQ/blob/main/test-environments/rocketmq/README.md) to run the samples locally.

## Further reading

- [Repository overview](https://github.com/eventhorizon-cli/EventHorizon.RocketMQ/blob/main/README.md)
- [Runnable samples](https://github.com/eventhorizon-cli/EventHorizon.RocketMQ/blob/main/samples/README.md)
- [Classic Remoting consumer model](https://github.com/eventhorizon-cli/EventHorizon.RocketMQ/blob/main/docs/en-US/remoting/consumer-model.md)
- [Classic Remoting transport and roles](https://github.com/eventhorizon-cli/EventHorizon.RocketMQ/blob/main/docs/en-US/remoting/transport-and-client-roles.md)
- [Dependency-injection registrations and lifetimes](https://github.com/eventhorizon-cli/EventHorizon.RocketMQ/blob/main/docs/en-US/architecture/dependency-injection-and-lifetimes.md)
- [OpenTelemetry instrumentation](https://github.com/eventhorizon-cli/EventHorizon.RocketMQ/blob/main/docs/en-US/architecture/opentelemetry-instrumentation.md)
- [Local and integration testing](https://github.com/eventhorizon-cli/EventHorizon.RocketMQ/blob/main/docs/en-US/testing/local-and-integration-testing.md)

## License

Licensed under the [Apache License 2.0](https://github.com/eventhorizon-cli/EventHorizon.RocketMQ/blob/main/LICENSE).
