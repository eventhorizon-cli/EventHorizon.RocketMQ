# gRPC Push Consumer without Generic Host

[English](README.md) | [简体中文](README.zh-CN.md)

This runnable console sample is the non-Host counterpart to the
[Generic Host Push Consumer sample](../../GenericHost/PushConsumer/README.md). It uses `IGrpcPushConsumer` directly, but it deliberately does not
construct an `IHost`.

`AddGrpcPushConsumer` still registers an `IHostedService` for normal Generic Host applications. Building a plain
`ServiceProvider` does not start that service. The main program therefore resolves the protocol-specific consumer,
calls `StartAsync`, waits for Ctrl+C, calls `StopAsync` with a live cancellation token, and asynchronously disposes the
container.

## When to use this shape

Use this pattern only when the application owns a non-Generic-Host lifetime, for example an embedded runtime or an
existing process host. A normal .NET Generic Host should use the [companion Host sample](../../GenericHost/PushConsumer/README.md): its `RunAsync`
invokes the SDK's registered lifecycle service, so application code must not call `StartAsync` or `StopAsync` again.

The consumer itself still owns assignment polling, long-poll receives, handler concurrency, acknowledgement, retry,
and dead-letter settlement. The scoped handler returns `Success` only after its sample processing completes.

## Run the sample

Start the [local RocketMQ environment](../../../../test-environments/rocketmq/README.md), then run the consumer before
publishing a matching `sample` tag message:

```shell
docker compose -f test-environments/rocketmq/compose.yaml up -d --wait
dotnet run --project samples/grpc/NonHost/PushConsumer
```

Use the [non-Host gRPC Producer sample](../Producer/README.md) to send the message. Press Ctrl+C once to begin graceful
shutdown; the sample calls `StopAsync` even when startup is cancelled or fails part-way through.

The default Proxy endpoint is `localhost:8081`. External deployments must expose a RocketMQ 5 Proxy, create
`eventhorizon-test-topic`, and grant route-query and consume permissions, including retry and dead-letter access when
those outcomes are enabled. Standard .NET configuration overrides apply, such as
`RocketMQ__Client__Endpoint=proxy.example:8081`.

See the [gRPC protocol guide](../../../../src/EventHorizon.RocketMQ.Grpc/README.md) for the complete API and delivery
contract.
