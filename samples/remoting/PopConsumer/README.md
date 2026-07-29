# Remoting POP Consumer

[简体中文](README.zh-CN.md)

`IRemotingPopConsumer` exposes classic Broker POP as a caller-driven, receipt-based consumption model. The
application selects a physical queue and issues `PopAsync`; the Broker makes returned messages invisible for a
limited time and issues a receipt for each one. Processing is completed by acknowledging that receipt, not by
committing a queue offset.

The SDK does not run background queue assignment, receive dispatch, or automatic acknowledgement for this role.

## When to use POP Consumer

Choose POP when work should be claimed temporarily and settled per message, especially when processing duration can
vary and an application benefits from extending a message's invisibility. It is useful for caller-controlled workers
that want Broker-managed redelivery without managing the next queue offset themselves.

Prefer Push Consumer when the SDK should manage consumer-group assignment, background long polling, handler
concurrency, and retry outcomes. Prefer Pull or Lite Pull when queue positions, replay, and explicit offset commits are
the required model.

POP requires Broker support for POP, acknowledgement, and invisibility-change commands. This client currently supports
direct checkpoints for normal physical topics only; it does not provide automatic assignment, broadcast POP, orderly
POP, batch acknowledgement, or retry/logical-topic checkpoint handling.

## SDK workflow

Register the role with `AddRemotingPopConsumer`. Calling `ConsumerOptions.Subscribe` on its options records a default
filter for that topic; it does **not** start a subscription, create an assignment, or start a receive loop.

The public API keeps the receipt lifecycle visible:

1. `GetMessageQueuesAsync` discovers the readable physical queues for a topic.
2. `PopAsync` targets one queue and returns `RemotingPopResult` with a `Status`, `RestNum`, and
   `RemotingPopMessage` values.
3. Each returned item contains a `RemotingMessageView` and its Broker-issued `RemotingPopReceipt`.
4. After processing succeeds, `AcknowledgeAsync` settles that individual receipt.
5. If processing needs longer, call `ChangeInvisibleTimeAsync` before expiry. It returns a **replacement receipt**;
   every later extension or acknowledgement must use the replacement.

`PopAsync` can override the configured batch size, invisible duration, and filter per call. Classic Brokers accept at
most 32 messages in one POP request.

## Receipts, failures, and concurrency

An unacknowledged message becomes eligible for delivery again after its invisible interval. A crash after business
effects but before acknowledgement, an acknowledgement failure, or work that outlives the interval can therefore
produce duplicate processing. Keep handlers idempotent and renew long-running work with enough time left to handle a
failed renewal attempt.

There is no application-visible offset commit in this model. Do not treat receipt acquisition as completion: only a
successful acknowledgement settles the delivery. If `ChangeInvisibleTimeAsync` succeeds, the old receipt is obsolete.

The caller chooses queue selection, the number of outstanding `PopAsync` calls, and processing concurrency. Broker
invisibility prevents a normally claimed message from remaining immediately available to other POP requests, but it
does not remove at-least-once redelivery or the need to bound local concurrency.

The registered default JSON command serializer is required for POP command codes. Do not replace it with RocketMQ's
optional binary command-header serializer for this role.

## Run the sample

The runnable sample rotates through the physical queues of `eventhorizon-test-topic` and acknowledges messages as
group `remoting-sample-pop-consumer`.

Start the [local RocketMQ environment](../../../test-environments/rocketmq/README.md), which provides a compatible
RocketMQ 5.5.0 Broker, then run:

```shell
docker compose -f test-environments/rocketmq/compose.yaml up -d --wait
dotnet run --project samples/remoting/PopConsumer/EventHorizon.RocketMQ.Samples.Remoting.PopConsumer.csproj
```

The default NameServer is `localhost:9876`. An external setup must expose both the NameServer and every advertised
Broker address, create the normal topic, and grant the group route-query, POP, acknowledgement, and visibility-change
permissions.

See the [Remoting guide](../../../src/EventHorizon.RocketMQ.Remoting/README.md) for complete options and API examples.
