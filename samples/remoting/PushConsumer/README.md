# Remoting Push Consumer sample

[简体中文](README.zh-CN.md)

This Generic Host registers a scoped `PushConsumerMessageHandler` through
`AddRemotingPushConsumer<PushConsumerMessageHandler>` and processes messages through `IRemotingPushConsumer`.
It is the high-level classic Remoting consumer with automatic assignment, long polling, dispatch, retry, and
offset handling.

## Public role and default configuration

| Key | Default | Purpose |
| --- | --- | --- |
| `RocketMQ:Remoting:NamesrvAddr` | `localhost:9876` | NameServer used to discover Broker routes. |
| `RocketMQ:PushConsumer:GroupName` | `remoting-sample-push-consumer` | Clustered consumer group. |
| `RocketMQ:PushConsumer:MaxConcurrency` | `4` | Maximum concurrent message-handler invocations. |
| `RocketMQ:PushConsumer:BatchSize` | `32` | Maximum messages requested by each long poll. |
| `RocketMQ:PushConsumer:LongPollingTimeout` | `00:00:05` | Maximum Broker hold time for an empty long poll. |
| `RocketMQ:PushConsumer:RetryDelay` | `00:00:01` | Delay before retrying a failed receive or handler result. |
| `Sample:Subscriptions[0]:Topic` | `rocketmq-dotnet-manual` | Default subscribed normal topic. |
| `Sample:Subscriptions[0]:Filter` | `*` | Subscription filter. |

The handler logs the message and returns `ConsumeResult.Success`. The typed registration lets the handler receive
its `ILogger<PushConsumerMessageHandler>` from the application service provider with a scoped lifetime.

## Broker and network prerequisites

- Use a classic Remoting NameServer and Broker. The client discovers routes through `NamesrvAddr` and then opens
  long-poll connections to the advertised Brokers, so both hops must be reachable.
- Create the normal topic `rocketmq-dotnet-manual` and allow the group
  `remoting-sample-push-consumer`, or change both values. ACL-enabled Brokers require route-query and consume
  permissions, including retry and dead-letter-topic permissions when those paths are used.
- The supplied [local RocketMQ environment](../../../test-environments/rocketmq/README.md) is suitable for this
  normal-topic Remoting consumer when it runs on the host. Prepare its resources from the repository root:

```shell
docker compose -f test-environments/rocketmq/compose.yaml up -d --wait
docker compose -f test-environments/rocketmq/compose.yaml exec broker sh mqadmin updateTopic \
  -n nameserver:9876 -c DefaultCluster -t rocketmq-dotnet-manual
docker compose -f test-environments/rocketmq/compose.yaml exec broker sh mqadmin updateSubGroup \
  -n nameserver:9876 -c DefaultCluster -g remoting-sample-push-consumer
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
- `ConsumeResult.Success` advances processing. Return a retry result for a transient failure; Broker retry and
  dead-letter behavior is consumer-group based, so handlers must be idempotent and tolerate redelivery.
- The default is clustering mode. Instances in the same group share queue assignments. Broadcasting mode gives
  every instance every readable queue and stores offsets locally instead.
- Broker queue locks are used only when `ConsumeOrderly` is `true` **and** `ConsumerMode` is `Clustering`.
  Ordinary concurrent consumption does not acquire those locks, and broadcasting orderly processing is only local.
