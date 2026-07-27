# EventHorizon.RocketMQ.Remoting

[English](https://github.com/eventhorizon-cli/EventHorizon.RocketMQ/blob/main/src/EventHorizon.RocketMQ.Remoting/README.md) |
[简体中文](https://github.com/eventhorizon-cli/EventHorizon.RocketMQ/blob/main/src/EventHorizon.RocketMQ.Remoting/README.zh-CN.md)

`EventHorizon.RocketMQ.Remoting` is an unofficial Apache RocketMQ classic Remoting Project and NuGet Package targeting
`net8.0` and `net10.0`. It integrates with Microsoft dependency injection, options, logging, and Generic Host lifecycle
management.

The client connects to RocketMQ NameServer, then uses route data to discover Broker endpoints and connect directly to
the selected Brokers. Use this package when an application needs the classic protocol. For the RocketMQ 5
protobuf/gRPC API through a Proxy, use `EventHorizon.RocketMQ.Grpc` instead.

Client behavior is covered by high-coverage unit tests and Docker-backed integration tests against real
NameServer and Broker paths.

> **[Read the repository overview](https://github.com/eventhorizon-cli/EventHorizon.RocketMQ/blob/main/README.md) and
> [runnable samples](https://github.com/eventhorizon-cli/EventHorizon.RocketMQ/blob/main/samples/README.md) first.**
> The overview has the cross-protocol feature matrix and package-selection guidance; the samples demonstrate the
> client roles in runnable applications.

## Quick start

### Install

Configure the package source used by your environment, then install the Remoting package:

```shell
dotnet add package EventHorizon.RocketMQ.Remoting
```

The package is self-contained as a client package: all public client models live in its own assembly, with no
dependency on another EventHorizon.RocketMQ package.

All public client models use the `EventHorizon.RocketMQ.Remoting` namespace tree. The Remoting and gRPC packages can
be installed together, but similarly named models in their protocol namespaces are distinct CLR types and are not
interchangeable. When one file imports both protocol namespaces, use aliases for the duplicate short names:

```csharp
using GrpcMessage = EventHorizon.RocketMQ.Grpc.Producer.Message;
using RemotingMessage = EventHorizon.RocketMQ.Remoting.Producer.Message;
```

### Minimal registration and use

Create a client registration through `AddRocketMQRemoting`, add a role with `AddRemotingProducer`, then inject its
protocol-specific interface into an endpoint. The following ASP.NET Core Minimal API sends one message for each
`POST /messages` request.

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

app.MapPost("/messages", async (IRemotingProducer producer, CancellationToken cancellationToken) =>
{
    return await producer.SendAsync(
        new Message("orders", "hello"u8.ToArray()),
        cancellationToken);
});

await app.RunAsync();
```

The default client registration resolves through normal parameter or constructor injection. Register a given role
only once per client registration; a duplicate role registration for the same client registration fails during service registration.

## Supported features

`✅` means this Project implements the client API. A target RocketMQ NameServer and Broker must still support and
enable the applicable server-side feature.

| Feature | Status | Conditions and notes |
| --- | :---: | --- |
| Standard Producer sends and retries | ✅ | `IRemotingProducer.SendAsync` returns `RemotingSendResult`; inspect its status because some Broker send results are not exceptions. |
| One-way Producer sends | ✅ | Completion does not confirm Broker storage because `SendOnewayAsync` receives no response. |
| Selected queues and queue selectors | ✅ | Route discovery must return the requested writable Broker queue. |
| Batch Producer sends | ✅ | All messages must use one topic and cannot be delayed, retry, or transactional messages. |
| FIFO Producer messages | ✅ | Set `MessageGroup` to select stable queue affinity. |
| Timed or delayed Producer messages and recall | ✅ | Set `DeliveryTimestamp`; recall requires the returned handle before delivery and a Broker with the corresponding feature enabled. |
| Priority and Lite Producer messages | ✅ | Set `Priority` or `LiteTopic`; these specialized message types are mutually exclusive and require Broker support. |
| Transactional Producer messages | ✅ | Configure `LocalTransactionExecutor` and `TransactionChecker`; the Broker can later check unresolved outcomes. |
| Producer request-reply | ✅ | The application's network must allow the Broker callback needed for the response. |
| Read-only `IRemotingAdmin` | ✅ | Supports queue discovery, physical-message lookup by `OffsetMessageId`, and offset/time queries. `ViewMessageAsync` connects directly to the Broker endpoint encoded in the ID. |
| `IRemotingPullConsumer` | ✅ | Supports explicit queues and offsets plus tag or SQL filtering. SQL filtering requires the target Broker's filter configuration. |
| `IRemotingLitePullConsumer` | ✅ | Supports clustered automatic or manual assignment, client-maintained positions, commits, pause, resume, and seek. It currently supports clustered consumption only. |
| `IRemotingPopConsumer` | ✅ | Supports manually selected physical queues, receipt acknowledgement, and invisibility renewal for normal-topic POP messages; the Broker must support POP. |
| `IRemotingPushConsumer` | ✅ | Supports clustering or broadcasting, runtime subscriptions, configurable initial offsets, concurrent batch callbacks with partial prefix acknowledgement or singleton FIFO dispatch, retries, dead-letter handling, and Broker-triggered rebalance or offset reset. |
| Queue-orderly Push consumption | ✅ | Uses optional Broker queue locks for ordered consumption; configure it only where the destination Broker supports the classic locking flow. |
| Built-in socket transport | ✅ | Uses `System.IO.Pipelines` with optional TLS, multiple NameServer addresses, Broker failover, ACL signing, namespaces, and configurable frame limits. It does not depend on Bedrock Framework. |
| Dependency injection, options, logging, Generic Host lifecycle, and default/keyed client registrations | ✅ | Add a client registration with `AddRocketMQRemoting`, then add the required roles. |
| OpenTelemetry tracing and metrics | ✅ | Subscribe to `RemotingRocketMQInstrumentation` source and meter names from the application's OpenTelemetry SDK. |
| RocketMQ 5 Proxy gRPC and `SimpleConsumer` APIs | — | Use `EventHorizon.RocketMQ.Grpc` for the Proxy-backed protobuf/gRPC API. |

## Connection and security

`RemotingClientOptions` configures the classic connection:

- `NamesrvAddr` accepts one or more semicolon-separated NameServer addresses and defaults to
  `localhost:9876`.
- `RequestTimeout`, `NameServerRequestTimeout`, `PollNameServerInterval`, and
  `HeartbeatBrokerInterval` control request and maintenance timing.
- `ClientIP`, `InstanceName`, `UnitMode`, and `UnitName` control the classic client identity.
- `Namespace` transparently prefixes topics and groups on the wire while public results retain
  logical resource names.
- `MaxRemotingFrameSize` limits the complete accepted frame and defaults to 16 MiB. Configure the
  Broker with the same limit before increasing it.

### ACL

Set both `AccessKey` and `AccessSecret` to sign NameServer and Broker requests. `SecurityToken` is
optional and is included with signed requests; configuring it requires both the access key and secret.

```csharp
builder.Services.AddRocketMQRemoting(options =>
{
    options.NamesrvAddr = "nameserver:9876";
    options.AccessKey = configuration["RocketMQ:AccessKey"];
    options.AccessSecret = configuration["RocketMQ:AccessSecret"];
    options.SecurityToken = configuration["RocketMQ:SecurityToken"];
    options.Namespace = "tenant-a";
});
```

Registration validation requires the access key and secret to be supplied together.

### TLS

Set `UseTLS`, then customize each connection with `ConfigureLegacySslOptions` when private trust
roots, mTLS, certificate revocation, protocol selection, or a different SNI name is required.

```csharp
using System.Security.Cryptography.X509Certificates;

builder.Services.AddRocketMQRemoting(options =>
{
    options.NamesrvAddr = "nameserver.internal:9876";
    options.UseTLS = true;
    options.ConfigureLegacySslOptions = (_, tls) =>
    {
        tls.TargetHost = "broker.internal";
        tls.ClientCertificates = new X509CertificateCollection { clientCertificate };
        tls.CertificateChainPolicy = new X509ChainPolicy
        {
            TrustMode = X509ChainTrustMode.CustomRootTrust,
            RevocationMode = X509RevocationMode.Online
        };
        tls.CertificateChainPolicy.CustomTrustStore.Add(rootCertificate);
    };
});
```

The callback can be invoked concurrently and receives a fresh `SslClientAuthenticationOptions`
instance for each connection.

## OpenTelemetry

The Package emits OpenTelemetry Activities and metrics without requiring a separate instrumentation Package. Configure
the application's OpenTelemetry SDK to subscribe to the public names:

```csharp
builder.Services.AddOpenTelemetry()
    .WithTracing(tracing => tracing
        .AddRocketMQRemotingInstrumentation())
    .WithMetrics(metrics => metrics
        .AddRocketMQRemotingInstrumentation());
```

The application owns its resource attributes, sampler, processors, and exporter. The client emits Producer send,
non-empty receive, automatic Push process, and consumer-settlement telemetry. It injects distributed context into
outbound message properties and preserves existing propagation fields. See the
[OpenTelemetry instrumentation design](../../docs/en-US/architecture/opentelemetry-instrumentation.md) for the trace
model and metrics, and the [OpenTelemetry web sample](../../samples/opentelemetry/README.md) for an OTLP/Grafana
setup.

## Producer

Register and inject `IRemotingProducer`; there is no transport-independent Producer interface.

```csharp
using System.Text;
using EventHorizon.RocketMQ.Remoting;
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
        message.Properties["source"] = "checkout";

        return producer.SendAsync(message, cancellationToken);
    }
}
```

`SendAsync` returns a `RemotingSendResult`. Always inspect `Status`: classic Brokers may report
`FlushDiskTimeout`, `FlushSlaveTimeout`, or `SlaveNotAvailable` without raising an exception.
`SendOnewayAsync` does not wait for a Broker response, so completion does not confirm storage.

`MessageGroup` selects a stable queue for FIFO delivery. `DeliveryTimestamp`, `Priority`, and
`LiteTopic` map to their classic reserved properties. These four specialized message types are
mutually exclusive, and the corresponding Broker feature must be enabled and supported. Send
retries can produce duplicates, so consumers should process messages idempotently.

### Queues and batches

Use `GetPublishMessageQueuesAsync` when an application needs a known target queue. The queue and
selector overloads reuse the same route validation and retry behavior as an ordinary send.

```csharp
var queues = await producer.GetPublishMessageQueuesAsync("orders", cancellationToken);
var queue = queues.First(static value => value.BrokerName == "broker-a" && value.QueueId == 0);
await producer.SendAsync(new Message("orders", "first"u8.ToArray()), queue, cancellationToken);

await producer.SendAsync(
    [
        new Message("orders", "second"u8.ToArray()),
        new Message("orders", "third"u8.ToArray())
    ],
    cancellationToken);
```

Batch messages must use one topic and cannot be delayed, retry, or transactional messages. The client
uses the classic batch mini-record format and preserves a unique identifier for every message.

### Transactions, recall, and request-reply

`SendTransactionAsync` follows the classic Broker transaction flow: it sends a half message, invokes
`LocalTransactionExecutor`, and automatically reports commit, rollback, or unknown to the Broker.
Configure `TransactionChecker` as well, because the Broker can later check an unresolved local outcome.

```csharp
using EventHorizon.RocketMQ.Remoting.Producer.Transactions;

rocketMQ.AddRemotingProducer(options =>
{
    options.GroupName = "orders-producer";
    options.LocalTransactionExecutor = static (message, state, cancellationToken) =>
        ValueTask.FromResult(RemotingTransactionResolution.Commit);
    options.TransactionChecker = static (message, cancellationToken) =>
        ValueTask.FromResult(RemotingTransactionResolution.Unknown);
});
```

For an eligible delayed message, `RemotingSendResult.RecallHandle` contains the opaque Broker handle
needed by `RecallAsync`. Keep it unchanged and recall it with the same logical topic.

```csharp
var sent = await producer.SendAsync(delayedMessage, cancellationToken);
if (sent.RecallHandle is { } recallHandle)
{
    await producer.RecallAsync(delayedMessage.Topic, recallHandle, cancellationToken);
}
```

`RequestAsync` sends normal classic message properties for the correlation, requester client ID, and
time-to-live. The producer registers the required Broker callback, maintains producer heartbeats for
the participating Brokers, and removes the pending request on completion, cancellation, timeout, or
shutdown.

```csharp
var reply = await producer.RequestAsync(
    new Message("orders", "quote"u8.ToArray()),
    TimeSpan.FromSeconds(10),
    cancellationToken);
```

An application that receives the request creates a `RemotingReply` from the request properties and
sends it through `SendReplyAsync`. This sends the classic `SEND_REPLY_MESSAGE` command and does not
couple Producer APIs to the Consumer implementation.

```csharp
using EventHorizon.RocketMQ.Remoting.Consumer;
using EventHorizon.RocketMQ.Remoting.Producer;

public sealed class OrderResponder(IRemotingProducer producer)
{
    public async ValueTask<ConsumeResult> HandleAsync(
        RemotingMessageView request,
        CancellationToken cancellationToken)
    {
        var reply = RemotingReply.FromRequestProperties(request.Properties, "quoted"u8.ToArray());
        await producer.SendReplyAsync(reply, cancellationToken);
        return ConsumeResult.Success;
    }
}
```

Only pass actual request messages to `RemotingReply.FromRequestProperties`; it validates the required
`CLUSTER`, `CORRELATION_ID`, `REPLY_TO_CLIENT`, and `TTL` properties.

## Admin

`IRemotingAdmin` is an independent, read-only role. It does not start a hosted background service.

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

`GetConsumerOffsetAsync` returns `null` when the group has no committed offset. `GetEarliestMessageStoreTimeAsync`
returns `null` when the Broker reports no available message. The remaining methods query the queue's earliest
offset, current maximum offset, or an offset at a timestamp with a lower or upper boundary.

`ViewMessageAsync` reads one stored message from the physical `RemotingSendResult.OffsetMessageId` returned by a
classic send. This is not the producer-assigned `MessageId`: it encodes the Broker endpoint and commit-log offset.
The Admin client decodes that endpoint and connects to it directly, so the application's network must be able to
reach the Broker address and port embedded in the ID; NameServer route discovery cannot substitute for that
connection.

```csharp
var sent = await producer.SendAsync(new Message("orders", "receipt"u8.ToArray()), cancellationToken);
var stored = await admin.ViewMessageAsync("orders", sent.OffsetMessageId, cancellationToken);
```

For a runnable HTTP and Swagger interface, see the
[Remoting Admin sample](https://github.com/eventhorizon-cli/EventHorizon.RocketMQ/blob/main/samples/remoting/Admin/README.md).

## PullConsumer

`IRemotingPullConsumer` leaves queue assignment, processing, and commit timing to the application.

```csharp
using EventHorizon.RocketMQ.Remoting;
using EventHorizon.RocketMQ.Remoting.Consumer;
using EventHorizon.RocketMQ.Remoting.Consumer.Pull;

rocketMQ.AddRemotingPullConsumer(options =>
{
    options.GroupName = "orders-pull-consumer";
    options.BatchSize = 32;
    options.Subscribe("orders", new FilterExpression("created"));
});

public sealed class OrderPuller(IRemotingPullConsumer consumer)
{
    public async Task PullOnceAsync(CancellationToken cancellationToken)
    {
        var queues = await consumer.GetMessageQueuesAsync("orders", cancellationToken);
        foreach (var queue in queues)
        {
            var offset = await consumer.GetOffsetAsync(queue, cancellationToken);
            if (offset < 0)
            {
                offset = await consumer.QueryOffsetAsync(
                    queue,
                    QueryOffsetPolicy.Beginning,
                    cancellationToken: cancellationToken);
            }

            var result = await consumer.PullAsync(
                queue,
                offset,
                cancellationToken: cancellationToken);

            foreach (var message in result.Messages)
            {
                await ProcessAsync(message.Body, cancellationToken);
            }

            await consumer.UpdateOffsetAsync(queue, result.NextOffset, cancellationToken);
        }
    }

    private static Task ProcessAsync(byte[] body, CancellationToken cancellationToken) =>
        Task.CompletedTask;
}
```

`GetOffsetAsync` returns `-1` when the group has no committed offset. `PullAsync` returns a
`RemotingPullResult` with a `RemotingPullStatus`, the next offset, and `RemotingMessageView`
instances. Queue handles are `RemotingPullMessageQueue` values.

Tag expressions are the default. Use `new FilterExpression("region = 'west'",
FilterExpressionType.Sql)` for SQL92 filtering; the target Broker must allow property filtering.

## LitePullConsumer

`IRemotingLitePullConsumer` is the classic assignment-oriented pull model. In subscription mode, it sends
`CONSUME_ACTIVELY` heartbeats, participates in clustered consumer-group allocation, and long-polls assigned
queues. `PollAsync` advances only the local queue position; `CommitAsync` is the explicit operation that
writes that position to the Broker.

```csharp
using EventHorizon.RocketMQ.Remoting;
using EventHorizon.RocketMQ.Remoting.Consumer;
using EventHorizon.RocketMQ.Remoting.Consumer.Pull.Lite;

rocketMQ.AddRemotingLitePullConsumer(options =>
{
    options.GroupName = "orders-lite-pull-consumer";
    options.BatchSize = 32;
    options.InitialOffset = QueryOffsetPolicy.Beginning;
    options.Subscribe("orders", new FilterExpression("created"));
});

public sealed class OrderLitePuller(IRemotingLitePullConsumer consumer)
{
    public async Task PollOnceAsync(CancellationToken cancellationToken)
    {
        var result = await consumer.PollAsync(cancellationToken: cancellationToken);
        foreach (var message in result.Messages)
        {
            await ProcessAsync(message.Body, cancellationToken);
        }

        await consumer.CommitAsync(cancellationToken);
    }

    private static Task ProcessAsync(byte[] body, CancellationToken cancellationToken) =>
        Task.CompletedTask;
}
```

Subscriptions and manual assignment are mutually exclusive. For manual mode, configure no subscriptions,
discover queues through `GetMessageQueuesAsync`, and pass the selected queues to `AssignAsync`. Use
`Pause`, `Resume`, `Seek`, `SeekToBeginningAsync`, or `SeekToEndAsync` to control local polling positions.

Lite Pull currently supports clustered consumption only. It is still client-initiated Broker long polling,
not protocol-level Broker push; broadcast local-offset storage is intentionally not provided.

## POPConsumer

`IRemotingPopConsumer` exposes classic Broker POP as an explicit receipt-based operation. The application
selects a physical queue, processes returned messages, and acknowledges each broker-issued receipt. It does
not run background assignment or dispatch, and a missing acknowledgement lets the Broker redeliver after the
receipt's invisible interval.

Classic Brokers accept at most 32 messages in one POP request. Both `BatchSize` and the per-call
`maxMessages` argument enforce that limit.

```csharp
using EventHorizon.RocketMQ.Remoting;
using EventHorizon.RocketMQ.Remoting.Consumer;
using EventHorizon.RocketMQ.Remoting.Consumer.Push.Pop;

rocketMQ.AddRemotingPopConsumer(options =>
{
    options.GroupName = "orders-pop-consumer";
    options.BatchSize = 32;
    options.InvisibleDuration = TimeSpan.FromSeconds(30);
    options.LongPollingTimeout = TimeSpan.FromSeconds(10);
});

public sealed class OrderPopper(IRemotingPopConsumer consumer)
{
    public async Task PopOnceAsync(CancellationToken cancellationToken)
    {
        var queue = (await consumer.GetMessageQueuesAsync("orders", cancellationToken))[0];
        var result = await consumer.PopAsync(
            queue,
            filter: new FilterExpression("created"),
            cancellationToken: cancellationToken);

        foreach (var received in result.Messages)
        {
            await ProcessAsync(received.Message.Body, cancellationToken);
            await consumer.AcknowledgeAsync(received.Receipt, cancellationToken);
        }
    }

    private static Task ProcessAsync(byte[] body, CancellationToken cancellationToken) =>
        Task.CompletedTask;
}
```

Call `ChangeInvisibleTimeAsync` before the current invisible interval expires when processing needs more time;
it returns a replacement receipt, and subsequent acknowledgement must use that replacement. This initial API
supports only direct normal physical-topic checkpoints. It intentionally does not provide automatic
assignment, broadcast mode, orderly POP, batch acknowledgement, or retry/logical-topic checkpoint handling.

POP command codes exceed the signed 16-bit code field retained by RocketMQ's optional binary command-header
format. The registered default serializer uses JSON and is required for POP; do not replace it with the
`RocketMQ` binary header serializer for this role.

## PushConsumer

Classic `IRemotingPushConsumer` is implemented with client-initiated pulls and long polling. It is
not protocol-level Broker push, although it also handles classic Broker callbacks for rebalance and
offset reset.

There is one Remoting PushConsumer role and one list-based message-handler contract. Configuring a larger
`ConsumeMessageBatchSize` changes the maximum messages delivered to that contract; it does not select a separate
consumer implementation.

```csharp
using System.Text;
using Microsoft.Extensions.DependencyInjection;
using EventHorizon.RocketMQ.Remoting;
using EventHorizon.RocketMQ.Remoting.Consumer;
using EventHorizon.RocketMQ.Remoting.Consumer.Push;

rocketMQ.AddRemotingPushConsumer<OrderMessageHandler>(ServiceLifetime.Scoped, options =>
{
    options.GroupName = "orders-push-consumer";
    options.MaxConcurrency = 8;
    options.BatchSize = 32;
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

`BatchSize` is the maximum number of messages requested from the Broker by one long poll and defaults to 32.
`ConsumeMessageBatchSize` is the maximum number passed to one handler invocation and defaults to 1. For concurrent
dispatch, only non-`MessageGroup` messages received from the same Broker physical queue are combined. `MessageGroup`
FIFO messages and all `ConsumeOrderly` deliveries are passed as singleton lists to preserve ordering and retry
semantics. Eligible concurrent batches can run in parallel up to `MaxConcurrency`.

`ConsumeTimeout` defaults to 15 minutes. For a concurrent clustered non-FIFO batch that exceeds the timeout, the
consumer cancels the handler token and requests Broker redelivery for the whole batch. It cannot forcibly terminate
application code that ignores cancellation; its late result is ignored and can overlap a redelivered invocation, so
handlers must honor the token and remain idempotent. FIFO and orderly delivery are excluded to preserve their ordering
guarantees.

The handler also receives a `RemotingPushConsumeContext`. Returning `Success` commits the complete batch by default.
For a concurrent non-FIFO batch, set `context.AckIndex` to the zero-based index of the last successfully processed
message, then return `Success`: the client commits that contiguous prefix and sends only the remaining tail for
Broker retry. `AckIndex` defaults to `int.MaxValue` (all messages) and accepts `-1` when no message was processed.
Returning `Retry` or `DeadLetter` ignores `AckIndex` and applies to every message in the batch.

`context.DelayLevelWhenNextConsume` controls messages that will be retried. It starts with the Broker delay level
mapped from `RetryDelay`; set it to `0` to let the Broker choose its retry policy, a positive RocketMQ delay level to
request that level, or a negative value to send those messages directly to the dead-letter queue. Broadcasting has no
Broker retry or dead-letter flow, so an unacknowledged batch tail is skipped while the local offset advances.
`MessageGroup` FIFO and `ConsumeOrderly` paths always receive singleton lists and do not use partial batch
confirmation.

Runtime `SubscribeAsync` and `UnsubscribeAsync` calls update classic heartbeats and reconcile active queue receivers.
The generic registration selects the handler lifetime for the current client registration: `Singleton` creates one
handler per client registration/role and must be thread-safe; `Scoped` and `Transient` resolve a handler in a new
async service scope for each batch handling attempt. The `MessageHandler` delegate has the same list-and-context
signature and remains available for small stateless callbacks, but cannot be combined with a typed handler.

### Queue-orderly consumption

Set `ConsumeOrderly` when a classic PushConsumer must process each assigned physical queue one message
at a time. It delivers singleton lists regardless of `ConsumeMessageBatchSize`:

```csharp
rocketMQ.AddRemotingPushConsumer(options =>
{
    options.GroupName = "orders-orderly-consumer";
    options.ConsumeOrderly = true;
    options.MaxConcurrency = 8; // Limits concurrent queues, not a single queue's order.
    options.Subscribe("orders");
    options.MessageHandler = ProcessOrderAsync;
});
```

In clustering mode, the client acquires and renews classic Broker queue locks before it pulls or commits
an assigned queue, and releases them after a rebalance, unsubscribe, or graceful shutdown. `Retry`
suspends the current queue locally, so later messages on that queue do not overtake it; `DeadLetter`
sends the current message back before the queue advances. Broadcasting mode keeps per-queue local
serialization but does not acquire Broker locks.

`ConsumeOrderly` is distinct from producer `MessageGroup`. `MessageGroup` provides stable producer
queue affinity and local FIFO dispatch; it does not acquire a classic Broker queue lock.

Queue locks retain at-least-once delivery semantics. A handler that continues after a lost lease can
still overlap a new owner through external side effects, so ordered handlers must remain idempotent.

A new group starts at the end by default. Configure another starting point when the group has no
committed offset:

```csharp
options.InitialPosition = ConsumeFromPosition.Beginning;

// Or start at the first offset at or after a timestamp.
options.InitialPosition = ConsumeFromPosition.Timestamp;
options.ConsumeTimestamp = DateTimeOffset.UtcNow.AddHours(-1);
```

The initial position does not reset an existing group offset, and retry queues retain their recovery
position.

Broadcasting assigns every readable queue to every consumer instance and stores offsets locally:

```csharp
options.ConsumerMode = ConsumerMode.Broadcasting;
options.InitialPosition = ConsumeFromPosition.Beginning;
options.LocalOffsetStorePath = Path.Combine(
    Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData),
    "orders-broadcast-offsets.json");
```

Use a stable, instance-specific path when the default client identity changes between launches.
Broadcast mode has no group-owned retry or dead-letter queue, so `Retry` and `DeadLetter` outcomes
are dropped and the local offset advances.

## Keyed client registrations and lifecycle

Use keyed client registrations for multiple clusters or multiple instances of the same role. The registration name
(`registrationName`) is also used as the .NET keyed-service key.

```csharp
using Microsoft.Extensions.DependencyInjection;
using EventHorizon.RocketMQ.Remoting;
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

Each client registration and role owns an independent transport and lifecycle. When resolving clients from a
standalone `ServiceProvider` instead of a Generic Host, call `StartAsync` and `StopAsync` explicitly.

## Compatibility and errors

- This project talks directly to classic NameServer and Broker endpoints; it does not connect to a
  RocketMQ Proxy.
- `IRemotingProducer` implements classic half-message transactions, delayed-message recall, and
  request-reply independently from the gRPC Producer APIs. Their wire contracts and server compatibility
  are protocol-specific.
- PullConsumer, LitePullConsumer, POPConsumer, and PushConsumer use Broker long polling where applicable.
  LitePullConsumer adds active classic group coordination and local positions with explicit commits;
  POPConsumer uses per-message invisible receipts; PushConsumer additionally performs automatic dispatch and
  supports classic Broker callback compatibility.
- POP uses classic `POP_MESSAGE`, acknowledgement, and visibility-change commands introduced for RocketMQ 5
  Brokers. It requires JSON command serialization because those request codes exceed the optional binary
  command-header format's signed 16-bit code field.
- The repository's local Docker Compose environment uses Apache RocketMQ 5.5.0. Capabilities such as SQL filters,
  TLS, ACL, priority, and lite messages still depend on the target cluster configuration and server
  support.
- `RemotingCommandException` reports a NameServer or Broker rejection and retains the raw
  `ResponseCode`. It derives from this package's `RocketMQClientException` base type.

## Local testing

The repository includes a Docker Compose environment under
[`test-environments/rocketmq`](https://github.com/eventhorizon-cli/EventHorizon.RocketMQ/tree/main/test-environments/rocketmq),
pinned to Apache RocketMQ 5.5.0. From
the repository root:

```shell
docker compose -f test-environments/rocketmq/compose.yaml config --quiet
docker compose -f test-environments/rocketmq/compose.yaml up -d --wait
```

The Remoting client uses the NameServer at `localhost:9876`. Create topics and consumer groups with
the commands in the [environment README](https://github.com/eventhorizon-cli/EventHorizon.RocketMQ/blob/main/test-environments/rocketmq/README.md),
then remove the
environment with:

```shell
docker compose -f test-environments/rocketmq/compose.yaml down -v --remove-orphans
```

The Remoting integration tests use isolated Testcontainers fixtures and do not require this manual
stack to be running:

```shell
dotnet test tests/it/EventHorizon.RocketMQ.Remoting.IntegrationTests/EventHorizon.RocketMQ.Remoting.IntegrationTests.csproj
```

## License

Licensed under the [Apache License 2.0](https://github.com/eventhorizon-cli/EventHorizon.RocketMQ/blob/main/LICENSE).
