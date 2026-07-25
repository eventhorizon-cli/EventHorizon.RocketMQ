# Remoting Pull Consumer sample

[简体中文](README.zh-CN.md)

This Generic Host uses `IRemotingPullConsumer` for explicit queue discovery, offsets, long-poll requests, and
Broker offset updates. It is the appropriate starting point when the application, rather than a background
dispatcher, must choose queues and offsets.

## Public role and default configuration

| Key | Default | Purpose |
| --- | --- | --- |
| `RocketMQ:Remoting:NamesrvAddr` | `localhost:9876` | NameServer used to discover Broker routes. |
| `RocketMQ:PullConsumer:GroupName` | `remoting-sample-pull-consumer` | Consumer group whose offsets the sample reads and writes. |
| `RocketMQ:PullConsumer:BatchSize` | `32` | Maximum messages requested per pull. |
| `RocketMQ:PullConsumer:LongPollingTimeout` | `00:00:05` | Maximum Broker hold time for an empty pull. |
| `Sample:Subscriptions[0]:Topic` | `rocketmq-dotnet-manual` | The one topic the sample reads. |
| `Sample:Subscriptions[0]:Filter` | `*` | Subscription filter for that topic. |
| `Sample:InitialOffset` | `End` | Position used only when the group has no committed offset. |
| `Sample:InitialTimestamp` | unset | Required when `InitialOffset` is `Timestamp`. |

The sample validates that exactly one `Sample:Subscriptions` entry is configured. It discovers all readable
physical queues, calls `PullAsync` with an explicit offset for each one, and calls `UpdateOffsetAsync` with each
returned next offset.

## Broker and network prerequisites

- Use a classic Remoting NameServer and Broker. `NamesrvAddr` is not a Proxy endpoint: routes come from the
  NameServer and every advertised Broker address must also be reachable from this process.
- Create the normal topic `rocketmq-dotnet-manual` and allow the group
  `remoting-sample-pull-consumer`, or change both settings. ACL-enabled Brokers also need route-query and consume
  permissions for this group.
- The supplied [local RocketMQ environment](../../../test-environments/rocketmq/README.md) is suitable for this
  normal-topic sample when it runs on the host. Prepare its default resources from the repository root:

```shell
docker compose -f test-environments/rocketmq/compose.yaml up -d --wait
docker compose -f test-environments/rocketmq/compose.yaml exec broker sh mqadmin updateTopic \
  -n nameserver:9876 -c DefaultCluster -t rocketmq-dotnet-manual
docker compose -f test-environments/rocketmq/compose.yaml exec broker sh mqadmin updateSubGroup \
  -n nameserver:9876 -c DefaultCluster -g remoting-sample-pull-consumer
```

The local stack advertises a host-reachable Broker. For a containerized application, configure a NameServer and
Broker advertisement reachable from that container instead of keeping `localhost:9876`.

## Run

```shell
dotnet run --project samples/remoting/PullConsumer/EventHorizon.RocketMQ.Samples.Remoting.PullConsumer.csproj
```

The host continues until interrupted with Ctrl+C. Send messages with the Remoting Producer sample or another
producer after it is running.

## Important behavior

- `InitialOffset` is not a reset switch. The sample always uses an existing committed group offset first; `End`,
  `Beginning`, or `Timestamp` applies only to a queue with no committed offset for this group. Use a new group or
  reset offsets deliberately when replaying messages.
- `End` is the default for a new group, so messages already in the queue are skipped on first startup. Choose
  `Beginning` or `Timestamp` explicitly when that is not desired.
- This is manual queue and offset control, not automatic consumer-group assignment. Do not scale several copies
  with the same group unless the application coordinates queue ownership and offset writes.
- The offset is committed after the sample logs the returned batch. Replace logging with business processing and
  commit only after that processing is safe to repeat or complete.
