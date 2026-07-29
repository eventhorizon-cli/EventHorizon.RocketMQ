# gRPC PushConsumer

[English](README.md) | [简体中文](README.zh-CN.md)

`IGrpcPushConsumer` is the automatic-dispatch gRPC consumption model. The client queries assignments, long-polls a
RocketMQ Proxy, buffers received messages locally, and invokes an application handler. "Push" describes the
application API: the Broker does not open a server-initiated connection to the application.

## When to use it

Use PushConsumer when message processing naturally fits a handler and the SDK should own the receive loops,
backpressure, concurrency, invisibility renewal, and settlement calls. It is usually the simpler choice for continuously
running services whose handler can return one delivery outcome per message.

Use `IGrpcSimpleConsumer` when the application must choose when to receive, control batches directly, or coordinate
acknowledgement with its own scheduler. Use classic Remoting Pull when the application must select Broker queues and
manage numeric offsets.

## SDK workflow

Register the gRPC transport and a typed handler with an explicit dependency-injection lifetime:

```csharp
rocketMQ.AddGrpcPushConsumer<OrderMessageHandler>(ServiceLifetime.Scoped, options =>
{
    options.GroupName = "orders-push-consumer";
    options.MaxConcurrency = 8;
    options.Subscribe("orders", new FilterExpression("created"));
});

public sealed class OrderMessageHandler : IGrpcPushMessageHandler
{
    public async ValueTask<ConsumeResult> HandleAsync(
        GrpcMessageView message,
        CancellationToken cancellationToken)
    {
        await ProcessAsync(message.Body, cancellationToken);
        return ConsumeResult.Success;
    }

    private static Task ProcessAsync(byte[] body, CancellationToken cancellationToken) =>
        Task.CompletedTask;
}
```

`Scoped` and `Transient` handlers are resolved in a new async scope for each handling attempt. A `Singleton` handler is
shared by concurrent calls and must be thread-safe. For a small stateless callback, the non-generic registration can
set `GrpcPushConsumerOptions.MessageHandler`; a delegate and a typed handler cannot be configured together.

The Generic Host integration calls `StartAsync` and `StopAsync` for the registered role. Outside a host, the application
must manage that lifecycle through `IGrpcPushConsumer`. `SubscribeAsync` and `UnsubscribeAsync` change the live topic
and filter set after startup; `Subscribe` on the options object defines an initial subscription.

## Delivery, completion, and failure

Each handler result asks the consumer to perform a specific settlement operation:

| Result | SDK action |
| --- | --- |
| `ConsumeResult.Success` | Acknowledge the message. Return it only after the business effect is durable. |
| `ConsumeResult.Retry` | Make the message eligible for another attempt according to the effective retry policy. |
| `ConsumeResult.DeadLetter` | Forward the message to the consumer group's dead-letter queue. |

An unhandled handler exception is treated as `Retry`. If acknowledgement, retry, or dead-letter completion fails, the
message may be delivered again. Handlers must therefore be idempotent even when they return `Success`.

Corrupted messages do not reach `IGrpcPushMessageHandler`. A non-FIFO corrupted message is made eligible for retry;
a corrupted FIFO message is dead-lettered before the next member of its message group is released.

While a handler is active, the client renews message invisibility. For non-FIFO messages, `ConsumeTimeout` bounds how
long the dispatcher waits: expiry cancels the handler token, stops renewal, requests retry, and ignores a late success.
The SDK cannot forcibly stop code that ignores cancellation, so the old invocation can overlap a redelivery. FIFO
message groups do not use this timeout-detach behavior because releasing the next message could break ordering.

`MaxConcurrency` is local handler concurrency, not a Broker queue count. FIFO ordering applies within a message group,
not globally across queues, groups, or consumer instances. Instances using the same consumer group share assignments
and retry state.

## Key SDK options

| Option | What it controls |
| --- | --- |
| `GrpcClientOptions.Endpoint` | The RocketMQ Proxy endpoint. A NameServer address is not valid here. |
| `GrpcPushConsumerOptions.GroupName` | The consumer group used for assignment and delivery state. |
| `ConsumerOptions.Subscribe` | Initial topic and tag or SQL filter subscriptions. |
| `MaxConcurrency` | Maximum scheduled handler dispatches. A timed-out handler that ignores cancellation can still remain in process. |
| `BatchSize` | Maximum messages requested by one receive operation. |
| `MaxCachedMessages` / `MaxCachedMessageBytes` | Count and body-byte limits for the bounded local message buffer. |
| `InvisibleDuration` | Initial message invisibility; the client renews it during active processing. |
| `ConsumeTimeout` | Maximum non-FIFO handler time before cancellation and retry are requested. |
| `MaxDeliveryAttempts` / `RetryDelay` | Fallback dead-letter threshold and retry delay when the Proxy does not supply a policy. |
| `LongPollingTimeout` | Maximum server wait for each receive long poll. |

## Run the sample

The supplied [RocketMQ environment](../../../test-environments/rocketmq/README.md) exposes a compatible Proxy at
`localhost:8081` and creates the sample Topic. External deployments must provision the Topic and consumer group when
automatic creation is disabled.

```shell
docker compose -f test-environments/rocketmq/compose.yaml up -d --wait
dotnet run --project samples/grpc/PushConsumer
```

The runnable code subscribes to `eventhorizon-test-topic` with the `sample` tag. Use the
[Producer sample](../Producer/README.md) or another compatible producer to publish matching messages.

For the complete handler, subscription, and Proxy behavior, see the
[gRPC guide](../../../src/EventHorizon.RocketMQ.Grpc/README.md) and
[gRPC consumer model](../../../docs/en-US/grpc/consumer-model.md).
