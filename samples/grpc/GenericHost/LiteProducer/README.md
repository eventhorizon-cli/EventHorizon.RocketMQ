# gRPC Lite Producer

[English](README.md) | [Simplified Chinese](README.zh-CN.md)

`IGrpcProducer` sends a Lite message by publishing to a LITE parent topic and setting the message's `LiteTopic`.
This Generic Host Web API sample publishes to `eventhorizon-test-lite-parent-topic` with the logical LiteTopic
`eventhorizon-test-lite-topic`. Run it with the companion
[LitePushConsumer](../LitePushConsumer/README.md) to exercise the complete Lite publish-and-dispatch path.

## When to use it

Use this shape when a RocketMQ 5 gRPC application must publish messages for a LITE-enabled deployment. The parent
topic identifies the broker resource; `LiteTopic` identifies the logical destination beneath that parent. A LiteTopic
is not an independently created ordinary topic and is not a tag filter.

Use the ordinary [Producer](../Producer/README.md) for standard, FIFO, delayed, priority, or transaction workflows
that do not target a LiteTopic. That sample deliberately leaves `Message.LiteTopic` unset, so its messages do not
reach the LitePushConsumer subscription in this environment.

## Registration and lifecycle

The sample registers `IGrpcProducer` through `AddRocketMQGrpc` and `AddGrpcProducer`. Its `WebApplication` is a
Generic Host, so the SDK hosted service starts the Producer when the application starts and stops it during host
shutdown. Application code injects and uses `IGrpcProducer`; it must not manually call `StartAsync` or `StopAsync`.

## Sending a Lite message

The HTTP endpoint constructs a protocol-specific `Message` with the LITE parent topic, then sets `LiteTopic` before
calling `SendAsync`:

```csharp
var message = new Message(
    "eventhorizon-test-lite-parent-topic",
    Encoding.UTF8.GetBytes(body))
{
    LiteTopic = "eventhorizon-test-lite-topic"
};

GrpcSendReceipt receipt = await producer.SendAsync(message, cancellationToken);
```

Setting `LiteTopic` selects the Lite message type. It is mutually exclusive with `MessageGroup`,
`DeliveryTimestamp`, and `Priority`; do not combine those specialized properties on one message. A send receipt
confirms that RocketMQ accepted the message, not that a consumer has finished processing it.

## Prerequisites and run

Use the dedicated [LitePush environment](../../../../test-environments/rocketmq-litepush/README.md). It starts a
cluster-mode Proxy at `localhost:8081`, creates the LITE parent topic, and binds the LitePush consumer group to that
parent topic.

Start the companion Consumer first:

```shell
docker compose -f test-environments/rocketmq-litepush/compose.yaml up -d --wait
dotnet run --project samples/grpc/GenericHost/LitePushConsumer
```

In a second terminal, start this Web API and post a message:

```shell
dotnet run --project samples/grpc/GenericHost/LiteProducer
curl --request POST http://localhost:5232/messages \
  --header 'Content-Type: application/json' \
  --data '{"message":"hello Lite"}'
```

The API returns the send receipt data. The LitePushConsumer logs the delivered message after its LiteTopic
subscription is synchronized. The sample keeps only the Proxy connection settings in `appsettings.json`; the parent
topic and LiteTopic remain next to the message construction so the Lite routing decision is visible.

For server capability requirements and the full Producer contract, see the
[gRPC guide](../../../../src/EventHorizon.RocketMQ.Grpc/README.md) and the
[LitePushConsumer sample](../LitePushConsumer/README.md).
