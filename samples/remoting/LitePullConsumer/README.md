# Remoting Lite Pull Consumer

[简体中文](README.zh-CN.md)

`IRemotingLitePullConsumer` combines caller-driven polling with client-managed queue assignment. In subscription
mode, the SDK participates in classic clustered consumer-group allocation and long-polls the queues assigned to this
consumer. The application still decides when to call `PollAsync`, how to process each result, and when to commit.

This is the classic Remoting Lite Pull model. It is unrelated to the RocketMQ 5 gRPC LitePush protocol and is not
protocol-level Broker push.

## When to use Lite Pull

Choose Lite Pull when an application wants a polling API and control over processing cadence, but does not want to
implement clustered queue allocation itself. It fits batch pipelines, controlled ingestion loops, and applications
that need to pause, resume, or seek assigned queues.

Prefer the lower-level Pull Consumer when an external system owns physical queue allocation or exact request offsets.
Prefer Push Consumer when the SDK should also own the receive loop, concurrent dispatch, and retry settlement. Lite
Pull currently supports clustering only; use Push Consumer broadcasting when every instance must consume every queue.

Subscription mode and manual-assignment mode are mutually exclusive. Use subscription mode for normal consumer-group
load balancing. Use manual mode only when the application deliberately selects queues and accepts responsibility for
their ownership.

## SDK workflow

Register the role with `AddRemotingLitePullConsumer` and add topic filters through `ConsumerOptions.Subscribe`. At
startup, the SDK resolves the subscriptions, sends active-consumer heartbeats, and maintains the clustered assignment.
`Assignment` exposes a snapshot of the queues currently owned by this consumer. When a group has more consumers than
readable queues, some consumers can legitimately have no assignment.

One `AddRocketMQRemoting` registration may add multiple Lite Pull consumers, each with its own options and lifecycle.
The registration reuses shared NameServer and route state, while same-group consumers remain distinct active members
for queue allocation. Different groups may use different topics and filters.

All instances in the same group must use the same topic and filter subscriptions. Allocation uses the group's complete
consumer-id list for each topic; inconsistent subscriptions can assign a queue to a member that is not consuming that
topic, while inconsistent filters can make accepted messages depend on the current queue owner.

The subscription-mode processing loop is:

1. Call `PollAsync` to long-poll the next active assigned queue.
2. Process the `RemotingPullResult.Messages`.
3. Call `CommitAsync()` to persist all current local positions, or `CommitAsync(queue)` for one assigned queue.

Runtime `SubscribeAsync` and `UnsubscribeAsync` change the subscription and trigger assignment reconciliation.
For manual mode, configure no subscriptions, discover queues with `GetMessageQueuesAsync`, and pass the selected set to
`AssignAsync`. `Pause`, `Resume`, `Seek`, `SeekToBeginningAsync`, and `SeekToEndAsync` control local polling positions.

`InitialOffset` is consulted only when an assigned queue has no committed group position. Changing it does not reset an
existing committed offset.

## Positions, failures, and concurrency

`PollAsync` advances the selected queue's **local** position to `NextOffset` before returning; it does not update the
Broker offset. A processing failure therefore requires the application to retain and retry that result, or to rewind
the queue with `Seek` before polling past it. Merely omitting `CommitAsync` does not rewind the in-process position.

Commit only after business processing is complete. On restart or reassignment, another consumer resumes from the last
Broker-committed position, so work completed after that position can be delivered again. Processing must be
idempotent.

Parallel processing needs a per-queue completion rule. Do not commit a current local position while an earlier result
from the same queue is unfinished. `CommitAsync()` covers every assigned queue's current position, so use it only when
all work through those positions is complete; otherwise coordinate queue-specific commits and keep polling from
advancing beyond gaps.

The SDK manages assignment and long polling, but application code owns business retries and the decision to commit.
Polling or assignment failures do not automatically retry a returned business batch or route it to a dead-letter
queue.

## Run the sample

The runnable sample subscribes to all messages on `eventhorizon-test-topic` as group
`remoting-sample-lite-pull-consumer` and starts new queue assignments at `End`.

Start the [local RocketMQ environment](../../../test-environments/rocketmq/README.md), then run:

```shell
docker compose -f test-environments/rocketmq/compose.yaml up -d --wait
dotnet run --project samples/remoting/LitePullConsumer/EventHorizon.RocketMQ.Samples.Remoting.LitePullConsumer.csproj
```

Only the NameServer connection settings are externalized in `appsettings.json`; the consumer role values remain
visible beside its registration in `Program.cs`. The default NameServer is `localhost:9876`. An external setup must
make both the NameServer and its advertised Broker addresses reachable, create the normal topic, and grant the group
route-query and consume permissions. Use the Remoting Producer sample to publish messages.

See the [Remoting guide](../../../src/EventHorizon.RocketMQ.Remoting/README.md) for complete options and API examples.
