# gRPC Producer without Generic Host

[English](README.md) | [简体中文](README.zh-CN.md)

This console sample publishes one ordinary message through `IGrpcProducer` without constructing a Generic Host. It
uses an ordinary `ServiceCollection` and `ServiceProvider`, explicitly starts the Producer, waits for the send receipt,
stops the Producer in `finally`, and asynchronously disposes the service provider.

## When to use this shape

Use this pattern only when the process owns a non-Generic-Host lifetime, such as an embedded runtime or an existing
application host. In a normal .NET Generic Host, `AddGrpcProducer` registers the SDK lifecycle service and `RunAsync`
starts and stops the role; application code must not call `IGrpcProducer.StartAsync` or `StopAsync` again. See the
[Generic Host Producer sample](../../GenericHost/Producer/README.md) for that integration.

`BuildServiceProvider` creates registrations but does not run registered `IHostedService` instances. The explicit
lifecycle calls in `Program.cs` are therefore required here, not a second way to manage a role that the Host already
owns.

## Workflow and delivery

The sample creates a protocol-specific `Message` with the `sample` tag, calls `SendAsync`, and logs the resulting
`GrpcSendReceipt`. A completed receipt confirms that RocketMQ accepted the message; it does not mean that a Consumer
has processed it. The message's generated key is metadata for lookup and correlation, not a deduplication guarantee.

`IGrpcProducer` connects to a RocketMQ 5 Proxy through `RocketMQ:Client:Endpoint`; it does not connect directly to a
Broker or NameServer. The Producer does not use a consumer group.

## Run the sample

Start the [local RocketMQ environment](../../../../test-environments/rocketmq/README.md), then run the project:

```shell
docker compose -f test-environments/rocketmq/compose.yaml up -d --wait
dotnet run --project samples/grpc/NonHost/Producer
```

The environment creates `eventhorizon-test-topic`, which is also consumed by the non-Host
[SimpleConsumer](../SimpleConsumer/README.md) and [PushConsumer](../PushConsumer/README.md) samples. Change the Proxy
connection with standard .NET configuration, for example
`RocketMQ__Client__Endpoint=proxy.example:8081`.

See the [gRPC protocol guide](../../../../src/EventHorizon.RocketMQ.Grpc/README.md) for message modes, send failure
handling, and complete Producer options.
