# gRPC SimpleConsumer without Generic Host

[English](README.md) | [简体中文](README.zh-CN.md)

This console sample drives gRPC `IGrpcSimpleConsumer` directly without constructing a Generic Host. It builds an
ordinary `ServiceProvider`, explicitly starts the Consumer, long-polls the Proxy, acknowledges normal messages,
forwards corrupted messages to the dead-letter queue, explicitly stops the Consumer, and asynchronously disposes the
service provider.

## When to use this shape

Use this form only when the process owns a non-Generic-Host lifetime. In a normal .NET Generic Host,
`AddGrpcSimpleConsumer` registers the SDK lifecycle service and the Host starts and stops the role. Application code
must not call `IGrpcSimpleConsumer.StartAsync` or `StopAsync` again; see the
[Generic Host SimpleConsumer sample](../../GenericHost/SimpleConsumer/README.md).

`BuildServiceProvider` only constructs registered services. It does not run its `IHostedService` registrations, so the
manual lifecycle calls in this sample are required.

## Receive and settlement

`ReceiveAsync` long-polls a RocketMQ 5 Proxy and returns temporarily invisible `GrpcMessageView` values. It does not
acknowledge them. The sample logs each valid message and calls `AckAsync`; real applications must acknowledge only
after the business effect is durable. It checks `IsCorrupted` before reading the body and calls
`ForwardToDeadLetterQueueAsync` for corrupted payloads. A failed or omitted settlement lets the message become visible
again after its invisibility window, so processing must be idempotent.

SimpleConsumer is application-driven: the process controls receive batch size, receive concurrency, and settlement.
Choose `IGrpcPushConsumer` when handler-oriented automatic dispatch and settlement are the better fit.

## Run the sample

Start the [local RocketMQ environment](../../../../test-environments/rocketmq/README.md), run the Consumer, then send
a message with the `sample` tag using the non-Host [Producer](../Producer/README.md):

```shell
docker compose -f test-environments/rocketmq/compose.yaml up -d --wait
dotnet run --project samples/grpc/NonHost/SimpleConsumer
```

Press Ctrl+C to cancel the receive loop. The sample calls `StopAsync` with a live token because the Ctrl+C token has
already been cancelled. The configured Proxy endpoint can be overridden with normal .NET configuration, for example
`RocketMQ__Client__Endpoint=proxy.example:8081`.

See the [gRPC protocol guide](../../../../src/EventHorizon.RocketMQ.Grpc/README.md) for live subscription changes,
invisibility renewal, and the complete SimpleConsumer contract.
