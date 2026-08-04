# gRPC PushConsumer

[English](README.md) | [简体中文](README.zh-CN.md)

`IGrpcPushConsumer` is the automatic-dispatch gRPC consumption model. The client queries assignments, long-polls a
RocketMQ Proxy, buffers received messages locally, and invokes an application handler. "Push" describes the
application API: the Broker does not open a server-initiated connection to the application.

## When to use it

Use PushConsumer when message processing naturally fits a handler and the SDK should own the receive loops,
backpressure, concurrency, Proxy renewal requests, and settlement calls. It is usually the simpler choice for continuously
running services whose handler can return one delivery outcome per message.

Use `IGrpcSimpleConsumer` when the application must choose when to receive, control batches directly, or coordinate
acknowledgement with its own scheduler. Use classic Remoting Pull when the application must select Broker queues and
manage numeric offsets.

## SDK workflow

Register the gRPC transport once, then add each Consumer with its own typed handler and options:

```csharp
const string Topic = "eventhorizon-test-topic";

builder.Services
    .AddRocketMQGrpc(clientSection.Bind)
    .AddGrpcPushConsumer<PushConsumerMessageHandler>(ServiceLifetime.Scoped, options =>
    {
        options.GroupName = "rocketmq-dotnet-grpc-push-sample";
        options.MaxConcurrency = 4;
        options.BatchSize = 16;
        options.Subscribe(Topic, new FilterExpression("sample"));
    })
    .AddGrpcPushConsumer<AllMessagesMessageHandler>(ServiceLifetime.Scoped, options =>
    {
        options.GroupName = "rocketmq-dotnet-grpc-push-all-messages";
        options.MaxConcurrency = 2;
        options.BatchSize = 8;
        options.Subscribe(Topic);
    });
```

Each `AddGrpcPushConsumer` call creates an independent logical Consumer, including its group, subscriptions, handler,
buffering, concurrency, and hosted lifecycle. The two roles remain under the same `AddRocketMQGrpc` registration and
reuse its transport channel pool to the configured Proxy. Connection and credential settings belong in
`appsettings.json`; subscription and processing choices are shown directly in `Program.cs`.

`Scoped` and `Transient` handlers are resolved in a new async scope for each handling attempt. A `Singleton` handler is
shared by concurrent calls and must be thread-safe. Automatic dispatch always uses a typed
`IGrpcPushMessageHandler` resolved from the application container. Choose `Scoped` when the handler needs scoped
application services such as a database context; those dependencies are available through constructor injection.

The Generic Host integration calls `StartAsync` and `StopAsync` for the registered role. The Host owns that lifecycle,
so application code must not call those methods again. Outside a host, the application must manage it through
`IGrpcPushConsumer`; see the complete [non-Host sample](../../NonHost/PushConsumer/README.md). `SubscribeAsync` and `UnsubscribeAsync`
change the live topic and filter set after startup; `Subscribe` on the options object defines an initial subscription.

## Delivery, completion, and failure

Each handler result asks the consumer to perform a specific settlement operation:

| Result | SDK action |
| --- | --- |
| `ConsumeResult.Success` | Acknowledge the message. Return it only after the business effect is durable. |
| `ConsumeResult.Failure` | Let the effective retry policy schedule another delivery. |

An unhandled handler exception is treated as `Failure`. If acknowledgement or retry scheduling fails, the
message may be delivered again. Handlers must therefore be idempotent even when they return `Success`.

Corrupted messages do not reach `IGrpcPushMessageHandler`. A non-FIFO corrupted message is made eligible for retry;
a corrupted FIFO message is dead-lettered before the next member of its message group is released.

Push requests set `AutoRenew=true`, so a compatible Proxy renews the receipt while the handler is active; the .NET
client does not run a second renewal timer. For non-FIFO messages, `ConsumeTimeout` bounds how long the dispatcher
waits: expiry cancels the handler token, requests another delivery, and ignores a late success.
The SDK cannot forcibly stop code that ignores cancellation, so the old invocation can overlap a redelivery. FIFO
message groups do not use this timeout-detach behavior because releasing the next message could break ordering.

`MaxConcurrency` is local handler concurrency, not a Broker queue count. FIFO ordering applies within a message group,
not globally across queues, groups, or consumer instances. Instances using the same consumer group share assignments
and retry state.

Consumers in different groups receive the topic independently. Consumers in the same group are clustered replicas:
they must configure identical topic and filter subscriptions, and they share assignments instead of each receiving a
copy. Registration fails locally when ordinary Push Consumers in one `AddRocketMQGrpc` registration reuse a group with
different subscriptions.

## Key SDK options

| Option | What it controls |
| --- | --- |
| `GrpcClientOptions.Endpoint` | The RocketMQ Proxy endpoint. A NameServer address is not valid here. |
| `GrpcPushConsumerOptions.GroupName` | The consumer group used for assignment and delivery state. |
| `ConsumerOptions.Subscribe` | Initial topic and tag or SQL filter subscriptions. |
| `MaxConcurrency` | Maximum scheduled handler dispatches. A timed-out handler that ignores cancellation can still remain in process. |
| `BatchSize` | Maximum messages requested by one receive operation. |
| `MaxCachedMessages` / `MaxCachedMessageBytes` | Count and body-byte limits for the bounded local message buffer. |
| `InvisibleDuration` | Initial message invisibility; a compatible Proxy renews the receipt during active processing. |
| `ConsumeTimeout` | Maximum non-FIFO handler time before cancellation and retry are requested. |
| `MaxDeliveryAttempts` / `RetryDelay` | Fallback FIFO local-attempt limit and retry delay when the Proxy does not supply a policy. Non-FIFO dead-letter progression remains service-owned. |
| `LongPollingTimeout` | Maximum server wait for each receive long poll. |

## Run the sample

The supplied [RocketMQ environment](../../../../test-environments/rocketmq/README.md) exposes a compatible Proxy at
`localhost:8081` and creates the sample Topic. External deployments must provision the Topic and consumer group when
automatic creation is disabled.

```shell
docker compose -f test-environments/rocketmq/compose.yaml up -d --wait
dotnet run --project samples/grpc/GenericHost/PushConsumer
```

The runnable code registers two Consumers over the one normal topic created by the local environment. Group
`rocketmq-dotnet-grpc-push-sample` accepts only the `sample` tag, while group
`rocketmq-dotnet-grpc-push-all-messages` accepts every tag. Because the groups are different, a `sample` message is
delivered independently to both typed handlers. Start this host before using the
[Producer sample](../Producer/README.md) to publish such a message. The two handlers log their separate deliveries and
return `Success`, so the SDK acknowledges them. The host continues running until Ctrl+C, which owns Consumer shutdown.

For the complete handler, subscription, and Proxy behavior, see the
[gRPC guide](../../../../src/EventHorizon.RocketMQ.Grpc/README.md) and
[gRPC consumer model](../../../../docs/en-US/grpc/consumer-model.md).
