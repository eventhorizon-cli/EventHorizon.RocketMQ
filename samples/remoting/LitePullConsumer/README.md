# Remoting Lite Pull Consumer sample

[简体中文](README.zh-CN.md)

This Generic Host uses `IRemotingLitePullConsumer` for subscription-driven, client-managed queue assignment and
caller-driven `PollAsync` operations. It is the classic Remoting Lite Pull API, not the RocketMQ 5 gRPC LitePush
feature.

## Public role and default configuration

| Key | Default | Purpose |
| --- | --- | --- |
| `RocketMQ:Remoting:NamesrvAddr` | `localhost:9876` | NameServer used to discover Broker routes. |
| `RocketMQ:LitePullConsumer:GroupName` | `remoting-sample-lite-pull-consumer` | Clustered consumer group used for assignment and offset commits. |
| `RocketMQ:LitePullConsumer:BatchSize` | `32` | Maximum messages requested by one poll. |
| `RocketMQ:LitePullConsumer:LongPollingTimeout` | `00:00:05` | Maximum Broker hold time for an empty poll. |
| `RocketMQ:LitePullConsumer:InitialOffset` | `End` | Position used only for a newly assigned queue without a committed group offset. |
| Fixed topic | `eventhorizon-test-topic` | Shared subscribed normal topic. |
| `Sample:Filter` | `*` | Subscription filter. |
| `Sample:RetryDelay` | `00:00:01` | Delay after assignment or polling failures. |

The API supports subscription mode and manual-assignment mode, but the sample uses subscriptions. It polls the
client's current assignment, logs every returned message, then calls `CommitAsync` to persist local positions.

## Broker and network prerequisites

- Use a classic Remoting NameServer and Broker. The process must reach the configured NameServer and the Broker
  addresses returned by its route data; `NamesrvAddr` is not a Proxy endpoint.
- Create the normal topic `eventhorizon-test-topic` and permit the clustered group
  `remoting-sample-lite-pull-consumer` when using an external Broker. If ACL is enabled, grant route-query and
  consume permissions.
- The supplied [local RocketMQ environment](../../../test-environments/rocketmq/README.md) is suitable for this
  classic normal-topic example when it runs on the host. It initializes the shared topic automatically:

```shell
docker compose -f test-environments/rocketmq/compose.yaml up -d --wait
```

The bundled Broker advertises a host-reachable address. Use a reachable NameServer and Broker advertisement when
running the sample in a different network namespace or container.

## Run

```shell
dotnet run --project samples/remoting/LitePullConsumer/EventHorizon.RocketMQ.Samples.Remoting.LitePullConsumer.csproj
```

The host keeps polling until Ctrl+C. Start a producer after the consumer has obtained an assignment; the sample
logs a debug message and retries while no queue is assigned.

## Important behavior

- Lite Pull is client-driven and clustered-only in this implementation. It performs consumer-group coordination in
  the client; it is not a server-side push API and it does not use the gRPC LitePush protocol.
- `PollAsync` advances **local** queue positions only. `CommitAsync` is required to persist them to the Broker.
  The sample commits after logging because logging is its whole processing step; production code must commit only
  after successful business processing.
- `InitialOffset` applies only when the group has no committed position for an assigned queue. Reusing the same
  group preserves committed offsets; it does not re-read history merely because `InitialOffset` changes.
- Subscription mode and explicit `AssignAsync` mode are mutually exclusive. Do not add manual assignments while
  retaining the fixed sample subscription.
