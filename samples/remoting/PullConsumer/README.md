# Remoting Pull Consumer

[简体中文](README.zh-CN.md)

`IRemotingPullConsumer` is the lowest-level classic Remoting consumption model. It exposes Broker physical queues
and queue offsets directly: the application chooses which queue to read, which offset to request, when processing is
complete, and which offset to commit. The SDK performs route discovery, protocol requests, decoding, and Broker offset
operations, but it does not allocate queues among application instances or dispatch messages to a handler.

## When to use Pull Consumer

Choose Pull Consumer when the application needs exact queue and position control, for example:

- a replay, backfill, inspection, or migration job that starts at the beginning, end, or a timestamp;
- an external scheduler already owns queue allocation;
- processing must coordinate a RocketMQ offset with another checkpoint or transaction;
- polling order, pacing, and per-queue concurrency are application decisions.

Prefer Lite Pull when you want caller-driven polling but want the SDK to participate in clustered queue allocation.
Prefer Push Consumer when automatic assignment, background long polling, handler dispatch, and Broker retry are the
desired model. Prefer POP when work is settled through per-message receipts and invisibility instead of queue offsets.

Pull Consumer is not suitable for scaling several identical instances in one group without an external queue-ownership
protocol. Those instances can discover the same physical queues and overwrite the same group offsets.

## SDK workflow

Register the role with `AddRemotingPullConsumer`. `ConsumerOptions.Subscribe` associates a topic with its tag or SQL92
filter; SQL92 filtering also requires the target Broker to allow property filtering.

One `AddRocketMQRemoting` registration may contain multiple independently configured Pull consumers and reuses its
NameServer, route, and connection infrastructure across them. Each call owns its group, subscription, options, and
lifecycle. Raw Pull consumers in the same group still require application-owned queue coordination.

The normal read path is explicit:

1. `GetMessageQueuesAsync` returns the readable `RemotingPullMessageQueue` values for a subscribed topic.
2. `GetOffsetAsync` reads the group's committed offset for one queue and returns `-1` when none exists.
3. `QueryOffsetAsync` resolves a starting offset with `Beginning`, `End`, or `Timestamp`.
4. `PullAsync` requests one queue from one offset and returns `RemotingPullResult`, including `Status`, `Messages`,
   `NextOffset`, and the Broker's current offset bounds.
5. After processing succeeds, `UpdateOffsetAsync` persists that queue's next offset for the group.

`PullAsync` may use Broker long polling, but it never commits automatically. `NextOffset` is the position for the next
request, not proof that the returned messages have been processed.

## Offsets, failures, and concurrency

Offsets belong to Broker physical queues. Keep a separate position and completion state for every queue; a global
offset has no meaning. If one queue is processed concurrently, commit only the highest contiguous completed offset.
A later batch completing first must not move the committed position past an earlier unfinished batch.

Process first and commit second. A process failure before `UpdateOffsetAsync` can cause the same messages to be read
again, while committing before durable processing can skip work after a crash. Processing code therefore needs to
tolerate duplicates, and applications that require stronger atomicity must coordinate the RocketMQ checkpoint with
their own durable state.

The application also owns pull-error backoff, processing retry, dead-letter policy, and queue reassignment. A failed
`PullAsync` or business operation does not create an automatic handler retry pipeline. Parallel queue loops are valid
only when ownership and offset updates remain isolated per queue.

## Run the sample

The runnable sample consumes all messages from `eventhorizon-test-topic` as group
`remoting-sample-pull-consumer`. When that group has no committed offset, it starts at `End`.

Start the [local RocketMQ environment](../../../test-environments/rocketmq/README.md), then run:

```shell
docker compose -f test-environments/rocketmq/compose.yaml up -d --wait
dotnet run --project samples/remoting/PullConsumer/EventHorizon.RocketMQ.Samples.Remoting.PullConsumer.csproj
```

Only the NameServer connection settings are externalized in `appsettings.json`; the consumer role values remain
visible beside its registration in `Program.cs`. The default NameServer is `localhost:9876`. An external setup must
expose both the NameServer and every Broker address advertised in its route data, create the normal topic, and grant
the group route-query and consume permissions. Use the Remoting Producer sample to publish messages after the
consumer starts.

See the [Remoting guide](../../../src/EventHorizon.RocketMQ.Remoting/README.md) for complete options and API examples.
