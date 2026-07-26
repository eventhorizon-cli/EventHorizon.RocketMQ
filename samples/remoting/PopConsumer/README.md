# Remoting POP Consumer sample

[简体中文](README.zh-CN.md)

This Generic Host uses `IRemotingPopConsumer` for explicit physical-queue selection and receipt-based POP
delivery. It rotates through discovered queues, processes each returned message, and acknowledges its Broker
receipt.

## Public role and default configuration

| Key | Default | Purpose |
| --- | --- | --- |
| `RocketMQ:Remoting:NamesrvAddr` | `localhost:9876` | NameServer used to discover Broker routes. |
| `RocketMQ:PopConsumer:GroupName` | `remoting-sample-pop-consumer` | Consumer group sent with POP and receipt operations. |
| `RocketMQ:PopConsumer:BatchSize` | `32` | Maximum messages per POP request; a classic Broker accepts at most 32. |
| `RocketMQ:PopConsumer:InvisibleDuration` | `00:00:30` | Default time a returned message remains hidden from other POP consumers. |
| `RocketMQ:PopConsumer:LongPollingTimeout` | `00:00:05` | Maximum Broker hold time for an empty POP request. |
| Fixed topic | `eventhorizon-test-topic` | Shared normal topic whose queues the sample rotates through. |
| `Sample:Filter` | `*` | Default filter passed to POP operations. |
| `Sample:RetryDelay` | `00:00:01` | Delay before route or POP retry after a failure. |

The public API exposes `GetMessageQueuesAsync`, `PopAsync`, `AcknowledgeAsync`, and
`ChangeInvisibleTimeAsync`. The subscription filter is a default for `PopAsync`; it does not create background
assignment or a receive loop.

## Broker and network prerequisites

- Use a RocketMQ 5-era classic Broker with `POP_MESSAGE`, acknowledgement, and visibility-change command support.
  A NameServer alone is not enough, and the configured `NamesrvAddr` is not a Proxy endpoint.
- The process must reach the NameServer and the Broker addresses returned by route discovery.
- Create the normal topic `eventhorizon-test-topic` and allow the group `remoting-sample-pop-consumer` when using
  an external Broker. With ACL enabled, give the group route-query, POP, acknowledgement, and visibility-change
  permissions.
- The supplied [local RocketMQ environment](../../../test-environments/rocketmq/README.md) runs RocketMQ 5.5.0 and
  is suitable for this normal-topic POP example when it runs on the host. It initializes the shared topic
  automatically:

```shell
docker compose -f test-environments/rocketmq/compose.yaml up -d --wait
```

The bundled Broker advertises a host-reachable address. Replace `localhost:9876` and the Broker advertisement for
an application running in another container or network.

## Run

```shell
dotnet run --project samples/remoting/PopConsumer/EventHorizon.RocketMQ.Samples.Remoting.PopConsumer.csproj
```

The host runs until Ctrl+C. Use the Remoting Producer sample or another producer to add messages to the topic.

## Important behavior

- POP is client-initiated. This sample manually selects one physical queue at a time and has no background queue
  assignment, message dispatch, or automatic acknowledgement.
- Every returned message carries a receipt. Acknowledge it before its invisibility expires; otherwise it becomes
  eligible for redelivery. Long-running work must call `ChangeInvisibleTimeAsync` before expiry and acknowledge
  the **replacement** receipt it returns.
- The example acknowledges immediately after logging because logging is its whole processing step. Replace that
  point with successful business processing, and design it to tolerate duplicate delivery.
- POP is not a PushConsumer variant. It exposes receipt lifecycle and physical queues directly, so scaling and
  queue ownership remain application responsibilities.
