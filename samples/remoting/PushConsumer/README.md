# Remoting Push Consumer sample

[简体中文](README.zh-CN.md)

This Generic Host registers a scoped batch-oriented `PushConsumerMessageHandler` through
`AddRemotingPushConsumer<PushConsumerMessageHandler>` and processes messages through `IRemotingPushConsumer`.
It is the high-level classic Remoting consumer with automatic assignment, long polling, batch dispatch, retry, and
offset handling.

## Public role and default configuration

| Key | Default | Purpose |
| --- | --- | --- |
| `RocketMQ:Remoting:NamesrvAddr` | `localhost:9876` | NameServer used to discover Broker routes. |
| `RocketMQ:PushConsumer:GroupName` | `remoting-sample-push-consumer` | Clustered consumer group. |
| `RocketMQ:PushConsumer:MaxConcurrency` | `4` | Maximum concurrent message-handler batch invocations. |
| `RocketMQ:PushConsumer:BatchSize` | `32` | Maximum messages requested from the Broker by each long poll. |
| `RocketMQ:PushConsumer:ConsumeMessageBatchSize` | `1` | Maximum messages passed to one handler invocation; it is independent of `BatchSize`. |
| `RocketMQ:PushConsumer:LongPollingTimeout` | `00:00:05` | Maximum Broker hold time for an empty long poll. |
| `RocketMQ:PushConsumer:RetryDelay` | `00:00:01` | Delay before retrying a failed receive or handler result. |
| `RocketMQ:PushConsumer:ConsumeTimeout` | `00:15:00` | Time limit for a concurrent clustered non-FIFO handler batch before cancellation and Broker retry are requested. |
| Fixed topic | `eventhorizon-test-topic` | Shared subscribed normal topic. |
| `Sample:Filter` | `*` | Subscription filter. |

The handler receives an `IReadOnlyList<RemotingMessageView>`, logs every message in the list, and returns one
`ConsumeResult` for the complete list. The sample keeps `ConsumeMessageBatchSize` at its default of `1`; set it in
configuration to batch concurrent non-`MessageGroup` messages from the same Broker physical queue. The typed
registration lets the handler receive its `ILogger<PushConsumerMessageHandler>` from the application service provider
with a scoped lifetime per batch handling attempt.

## Broker and network prerequisites

- Use a classic Remoting NameServer and Broker. The client discovers routes through `NamesrvAddr` and then opens
  long-poll connections to the advertised Brokers, so both hops must be reachable.
- Create the normal topic `eventhorizon-test-topic` and allow the group `remoting-sample-push-consumer` when using
  an external Broker. ACL-enabled Brokers require route-query and consume permissions, including retry and
  dead-letter-topic permissions when those paths are used.
- The supplied [local RocketMQ environment](../../../test-environments/rocketmq/README.md) is suitable for this
  normal-topic Remoting consumer when it runs on the host. It initializes the shared topic automatically:

```shell
docker compose -f test-environments/rocketmq/compose.yaml up -d --wait
```

The supplied stack advertises a host-reachable Broker address. A process in another container must use network
reachable NameServer and Broker addresses rather than `localhost:9876`.

## Run

```shell
dotnet run --project samples/remoting/PushConsumer/EventHorizon.RocketMQ.Samples.Remoting.PushConsumer.csproj
```

The application starts the client role with the host and remains active until Ctrl+C. Send messages with the
Remoting Producer sample or another producer after startup.

## Important behavior

- Despite its name, this is not a protocol-level Broker push to the application. The client owns queue assignment
  and repeatedly issues long-poll pull requests, then dispatches received messages to the handler.
- A handler outcome applies to every message in its list. Return a retry result for a transient failure; Broker retry
  and dead-letter behavior is consumer-group based, so handlers must be idempotent and tolerate redelivery.
- `MessageGroup` FIFO messages and `ConsumeOrderly` dispatch always receive singleton lists, even when
  `ConsumeMessageBatchSize` is greater than `1`.
- For concurrent clustered non-FIFO delivery, `ConsumeTimeout` cancels the handler token and requests Broker
  redelivery when a batch runs too long. It cannot forcibly stop application code that ignores cancellation; its
  late result is ignored and can overlap a redelivered invocation, so handlers must honor the token and remain
  idempotent. FIFO and orderly delivery are intentionally excluded to preserve their ordering guarantees.
- The default is clustering mode. Instances in the same group share queue assignments. Broadcasting mode gives
  every instance every readable queue and stores offsets locally instead.
- Broker queue locks are used only when `ConsumeOrderly` is `true` **and** `ConsumerMode` is `Clustering`.
  Ordinary concurrent consumption does not acquire those locks, and broadcasting orderly processing is only local.
