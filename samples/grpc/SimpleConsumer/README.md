# gRPC SimpleConsumer

[English](README.md) | [简体中文](README.zh-CN.md)

`IGrpcSimpleConsumer` is the application-driven gRPC consumption model. The application asks a RocketMQ Proxy for a
batch of messages, decides when processing is complete, and settles every message explicitly. It is not classic
queue-and-offset Pull, and the Broker does not push messages over a connection to the application.

## When to use it

Use SimpleConsumer when the acknowledgement boundary belongs in application code. Typical cases include coupling an
acknowledgement to a durable write, choosing the batch size for each receive, extending invisibility around long work,
or applying a custom concurrency and backpressure policy.

Use `IGrpcPushConsumer` instead when a handler-oriented API with automatic receive, dispatch, and settlement is a
better fit. Use classic Remoting Pull when the application must select Broker queues and manage numeric offsets
directly.

## SDK workflow

Register the gRPC transport with `AddRocketMQGrpc`, then add one SimpleConsumer role to that registration:

```csharp
rocketMQ.AddGrpcSimpleConsumer(options =>
{
    options.GroupName = "orders-simple-consumer";
    options.AwaitDuration = TimeSpan.FromSeconds(5);
    options.InvisibleDuration = TimeSpan.FromSeconds(30);
    options.Subscribe("orders", new FilterExpression("created"));
});
```

The Generic Host integration starts and stops the registered role. Without a host, resolve `IGrpcSimpleConsumer` and
call `StartAsync` and `StopAsync` explicitly.

The public API keeps reception and settlement separate:

| API | Responsibility |
| --- | --- |
| `SubscribeAsync` / `UnsubscribeAsync` | Change the live topic and filter set after startup. `Subscribe` on the options object defines an initial subscription. |
| `ReceiveAsync` | Long-poll the Proxy and return up to the requested number of `GrpcMessageView` values. An empty result is valid. |
| `AckAsync` | Confirm one message after its business effect is durable. |
| `ChangeInvisibleDurationAsync` | Replace the current invisibility duration when processing needs more time. |
| `ForwardToDeadLetterQueueAsync` | Explicitly move a message to the consumer group's dead-letter queue using the supplied delivery-attempt threshold. |

Check `GrpcMessageView.IsCorrupted` before reading the body. A corrupted payload still requires an explicit settlement
decision; it is not acknowledged automatically by SimpleConsumer.

## Completion and failure semantics

`ReceiveAsync` makes a message temporarily invisible but does not acknowledge it. If processing fails, the process
stops, or `AckAsync` does not complete successfully, the message can become visible again after its invisibility
duration. Applications must therefore make handlers idempotent and acknowledge only after durable processing.

If work can outlive the current invisibility window, call `ChangeInvisibleDurationAsync` before the window expires.
Forwarding to the dead-letter queue is also an explicit application decision; an ordinary processing exception does
not invoke `ForwardToDeadLetterQueueAsync` on the application's behalf.

Consumer groups own acknowledgement and retry state. Multiple instances using the same group share the work; a
different group observes the topic independently.

## Key SDK options

| Option | What it controls |
| --- | --- |
| `GrpcClientOptions.Endpoint` | The RocketMQ Proxy endpoint. A NameServer address is not valid here. |
| `GrpcClientOptions.RequestTimeout` / `UseTLS` | Deadlines for ordinary gRPC requests and transport security. |
| `GrpcSimpleConsumerOptions.GroupName` | The consumer group that owns delivery and acknowledgement state. |
| `GrpcSimpleConsumerOptions.AwaitDuration` | How long the Proxy may wait in a receive long poll. The supplied RocketMQ 5.5.0 Proxy requires at least five seconds. |
| `GrpcSimpleConsumerOptions.InvisibleDuration` | The default invisibility requested for received messages; `ReceiveAsync` can override it per call. |
| `ConsumerOptions.Subscribe` | Initial topic subscription and `FilterExpression`; omitting the expression matches every message. |

## Run the sample

The supplied [RocketMQ environment](../../../test-environments/rocketmq/README.md) exposes a Proxy at
`localhost:8081` and creates the sample Topic. External deployments must provide a compatible RocketMQ 5 Proxy and
provision the Topic and consumer group when automatic creation is disabled.

```shell
docker compose -f test-environments/rocketmq/compose.yaml up -d --wait
dotnet run --project samples/grpc/SimpleConsumer
```

The runnable code subscribes to `eventhorizon-test-topic` with the `sample` tag. Use the
[Producer sample](../Producer/README.md) or another compatible producer to publish matching messages.

For the complete consumer API and Proxy constraints, see the
[gRPC guide](../../../src/EventHorizon.RocketMQ.Grpc/README.md) and
[gRPC consumer model](../../../docs/en-US/grpc/consumer-model.md).
