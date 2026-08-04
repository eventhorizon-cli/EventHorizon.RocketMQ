# Remoting Push Consumer

[English](README.md) | [简体中文](README.zh-CN.md)

`IRemotingPushConsumer` is the high-level classic Remoting consumption model. The SDK maintains consumer-group
assignment, initiates Broker long polls, buffers received messages, dispatches handlers, and settles retry or
dead-letter outcomes. Application code supplies a message handler instead of driving a poll loop.

Despite the name, messages are not pushed from the Broker directly into application code. The client performs long
polling and also handles classic Broker callbacks such as rebalance and offset reset.

## When to use Push Consumer

Choose Push Consumer for continuously running services that want the SDK to own queue allocation, receive scheduling,
handler concurrency, retries, and offset progression. It is the normal choice when business code can be expressed as
an asynchronous callback and can tolerate at-least-once delivery.

Prefer Lite Pull when the application must control polling, manual physical-queue assignment, seeking, and commits.
Broker-side POP is an internal Push reception mode; use the gRPC `IGrpcSimpleConsumer` when application code must own
individual receipts and invisibility changes.

Push Consumer is not a fixed thread-per-queue execution model. Applications that require thread affinity or an
external scheduler to own every queue should use a caller-driven consumer instead.

## Registration and handler contract

Register a typed handler with
`AddRemotingPushConsumer<TMessageHandler>(ServiceLifetime, Action<RemotingPushConsumerOptions>)`. The handler
implements `IRemotingPushMessageHandler.HandleAsync` and receives:

- an ordered `IReadOnlyList<RemotingMessageView>` from one Broker physical queue;
- a `RemotingPushConsumeContext` that controls partial acknowledgement and retry delay;
- a cancellation token for shutdown and eligible consume timeouts.

One Remoting client registration can own multiple independently configured Push Consumers:

```csharp
const string Topic = "eventhorizon-test-topic";
var allMessagesQueueAssignmentMode = builder.Configuration.GetValue(
    "Sample:AllMessagesQueueAssignmentMode",
    RemotingPushQueueAssignmentMode.Client);

builder.Services
    .AddRocketMQRemoting(remotingSection.Bind)
    .AddRemotingPushConsumer<PushConsumerMessageHandler>(ServiceLifetime.Scoped, options =>
    {
        options.GroupName = "remoting-sample-push-consumer";
        options.QueueAssignmentMode = RemotingPushQueueAssignmentMode.Client;
        options.MaxConcurrency = 4;
        options.PullBatchSize = 32;
        options.ConsumeMessageBatchSize = 4;
        options.Subscribe(Topic, new FilterExpression("sample"));
    })
    .AddRemotingPushConsumer<AllMessagesMessageHandler>(ServiceLifetime.Scoped, options =>
    {
        options.GroupName = "remoting-all-messages-push-consumer";
        options.QueueAssignmentMode = allMessagesQueueAssignmentMode;
        options.MaxConcurrency = 2;
        options.PullBatchSize = 8;
        options.ConsumeMessageBatchSize = 1;
        options.Subscribe(Topic);
    });
```

Each call owns its group, subscriptions, handler, scheduling, offset state, and hosted lifecycle. Consumers under the
same `AddRocketMQRemoting` registration reuse NameServer discovery, route state, and compatible underlying Broker
connections. Connection and credential settings, plus the optional all-messages assignment-mode toggle, are read from
`appsettings.json`; the two Consumer registrations and all remaining role choices stay visible in `Program.cs`.

This executable runs in a Generic Host. `RunAsync` invokes the hosted services registered by
`AddRemotingPushConsumer`, which start and stop the Consumer roles. Do not call `IRemotingPushConsumer.StartAsync` or
`StopAsync` from this application. A process without a Generic Host should follow the complete
[non-Host sample](../../NonHost/PushConsumer/README.md).

A `Singleton` handler can receive concurrent calls and must be thread-safe. `Scoped` and `Transient` handlers are
resolved in a new asynchronous DI scope for each batch attempt. Automatic dispatch always uses a typed
`IRemotingPushMessageHandler` resolved from the application container. Choose `Scoped` when the handler needs scoped
application services such as a database context; those dependencies are available through constructor injection.

Subscriptions can be configured with `ConsumerOptions.Subscribe` before startup. Runtime `SubscribeAsync` and
`UnsubscribeAsync` update classic heartbeats and reconcile active receivers.

## Assignment, reception, and concurrent dispatch

`QueueAssignmentMode.Client` is the default. It performs classic client-side average allocation and creates PULL receivers.
In `Clustering` mode, live consumers with the same group share the topic's physical queues; consumers in different
groups consume independently. `Broadcasting` and `ConsumeOrderly` also use client assignment.

`QueueAssignmentMode.Broker` sends `QUERY_ASSIGNMENT` for concurrent clustered consumption and creates a PULL or POP
receiver for each assignment returned by the Broker. Broadcasting and `ConsumeOrderly` force effective client
assignment and PULL even when `Broker` is requested. The runtime Consumer never sends `SET_MESSAGE_REQUEST_MODE`: an
operator configures PULL or POP for the topic/group on the Broker. A failed or unsupported eligible assignment query
fails reconciliation instead of falling back to client assignment, which could create overlapping ownership.
Broker assignment therefore does not imply POP; the official
[5.5.0 Broker processor](https://github.com/apache/rocketmq/blob/rocketmq-all-5.5.0/broker/src/main/java/org/apache/rocketmq/broker/processor/QueryAssignmentProcessor.java#L92-L132)
places the selected request mode on every returned assignment.
Every clustered mode also advertises and reconciles `%RETRY%{consumerGroup}`. Broker assignment forces that retry
topic to PULL so PULL send-back results remain consumable.

All clustering instances in the same group must use the same topic and filter subscriptions. Allocation uses the
group's complete consumer-id list for each topic; inconsistent subscriptions can assign a queue to a member that is
not consuming that topic, while inconsistent filters can make accepted messages depend on the current queue owner.
The registration validates this constraint locally. Compatible same-group registrations receive distinct active
consumer identities and share queues; they are not independent fan-out subscriptions.

Every PULL assignment maintains a local `PullProcessQueue`. It holds pulled messages and their consumption state; it is
not a new Broker queue and creates no server resource. The name and ownership concept follow the official RocketMQ
Java client.

`PullBatchSize` limits messages requested by one Broker PULL. `PullMaxCachedMessages` and
`PullMaxCachedMessageBytes` bound PULL admission. `ConsumeMessageBatchSize` independently limits messages passed to
one handler invocation. Concurrent PULL batches contain only messages from the same physical queue. Messages with a
`MessageGroup` and all `ConsumeOrderly` deliveries remain singleton batches.

Ready `PullProcessQueue` instances share asynchronous consume loops. Each turn takes one batch from one ready queue and
places a queue with more work at the back, providing fair progress across active queues and Brokers. `MaxConcurrency`
limits handler dispatches across the entire Consumer, not per queue. These loops are .NET ThreadPool tasks; they are
not dedicated threads and are not permanently bound to a queue. This fair scheduler is a .NET design rather than an
exact copy of the Java concurrent dispatch path.

`PullMaxCachedMessages` limits admission of newly pulled messages waiting for first dispatch. In-flight or settlement-
blocked messages do not continue to hold that admission capacity, and a blocked PULL queue pauses new admission until
it can progress. The value is therefore not a strict count of every message resident in the process.

A Broker-assigned POP receiver uses separate receipt state rather than `PullProcessQueue` offsets. `PopBatchSize` controls
each POP request, `PopInvisibleDuration` sets the initial lease, and `PopMaxInflightMessagesPerAssignment` bounds
messages awaiting settlement for one assignment. The client renews receipts while a handler is active and always uses
the latest renewed receipt for final settlement.

## Acknowledgement, retry, and offsets

The handler returns one `ConsumeResult` for its batch:

- `Success` settles the complete batch by default: PULL advances its contiguous offset watermark, while POP
  acknowledges Broker receipts.
- For a concurrent non-FIFO batch, set `context.AckIndex` to the zero-based last successful index and return
  `Success`. The contiguous prefix is settled and only the tail is sent for Broker retry. The default
  `int.MaxValue` accepts all messages; `-1` accepts none.
- `Retry` and `DeadLetter` ignore `AckIndex` and apply to the complete batch.

For a clustered concurrent non-FIFO batch, `context.DelayLevelWhenNextConsume` defaults to `0`, allowing the Broker to
select the retry interval. Set it to a positive RocketMQ delay level to request that level, or to a negative value to
request direct dead-letter delivery. PULL retries use classic send-back; POP retries map the selected level through the
classic POP retry schedule and change the receipt's invisibility. POP dead-letter handling forwards before
acknowledging the receipt.
`MessageGroup` FIFO, `ConsumeOrderly`, and broadcasting paths ignore this context setting. In clustering mode,
`MaxDeliveryAttempts` bounds retry delivery; a retried message that reaches the limit is sent to the dead-letter queue.

Every PULL `PullProcessQueue` maintains an independent contiguous completion watermark. A later batch, or a batch from another
queue or Broker, cannot skip an earlier unresolved queue offset. Pulling may run ahead of handler completion. In
clustering mode, the Broker-committed offset cannot run ahead of successful settlement; broadcasting instead treats
unavailable retry and dead-letter outcomes as locally resolved drops, as described below.

In the clustered concurrent `PullProcessQueue` path, failed retry/dead-letter settlement is retried locally after
`RetryDelay` without invoking the application handler again; the watermark remains blocked. Offset persistence
failures are also retried. If an assignment is revoked, a late handler result from that old assignment is ignored and
cannot advance its replacement.

POP settlement is valid only before the fixed invisible deadline returned by the Broker. The client does not renew the
receipt while the handler is active. A result that arrives after the deadline is ignored and Broker redelivery becomes
authoritative. Removing or changing an assignment also invalidates late settlement from the old generation.

`ConsumeTimeout` applies to concurrent clustered non-FIFO batches. On expiry, the client cancels the handler token and
requests Broker redelivery for the whole batch. Code that ignores cancellation cannot be forcibly stopped; its late
result is ignored but its external work can overlap the redelivered invocation. Handlers must honor cancellation and
remain idempotent. FIFO `MessageGroup` and `ConsumeOrderly` deliveries are excluded so timeout recovery cannot break
their ordering.

## Ordering and broadcasting

These modes always use effective client assignment and PULL; a configured `QueueAssignmentMode.Broker` request is ignored
for assignment selection. `MessageGroup` gives producer-side queue affinity and singleton FIFO dispatch.
`ConsumeOrderly` instead serializes
every assigned physical queue. In clustering mode, orderly consumption acquires and renews classic Broker queue locks;
`MaxConcurrency` can still process different locked queues concurrently. A retry blocks that queue so later messages
cannot overtake it. Queue locks preserve order but do not change at-least-once delivery, so orderly handlers must also
be idempotent.

`Broadcasting` assigns every readable queue to every Consumer instance and stores offsets locally. It has no
group-owned Broker retry or dead-letter flow: `Retry`, `DeadLetter`, and an unacknowledged batch tail are dropped while
the local offset advances. Use a stable, instance-specific `LocalOffsetStorePath` when client identity is not stable.

For a normal PULL queue, `InitialPosition` supplies a starting offset only when the active offset store has no
value: the Broker group offset in clustering mode, or the instance's local offset file in broadcasting mode. It does
not reset an existing position. Retry queues retain their recovery boundaries and do not use this normal-queue
starting policy.

For Broker-assigned POP, `Beginning` maps to Broker init mode MIN; `End` and `Timestamp` map to MAX, matching the
classic Java client. POP does not provide orderly or cross-member `MessageGroup` FIFO guarantees.

This sample sets `InitialPosition` to `End`, the default for a fresh group. `StartAsync` does not make that initial
offset query a deployment cutover: a message published before a newly assigned receiver resolves its first offset can
be considered existing backlog and skipped. Choose `Beginning` or `Timestamp` for deliberate replay, or use a
committed group offset or application readiness handshake when the cutover must include every message.

## Run the sample

The runnable sample registers two scoped typed handlers over `eventhorizon-test-topic`. Group
`remoting-sample-push-consumer` always uses client assignment and accepts only the `sample` tag. Group
`remoting-all-messages-push-consumer` accepts every tag and reads its assignment mode from sample configuration. It
defaults to `Client`, so the normal workflow requires no server-side assignment or POP configuration. Set
`Sample__AllMessagesQueueAssignmentMode=Broker` only against a Broker configured for server-side assignment; that Broker may
return PULL or POP for each topic/group assignment. A `sample` message reaches both handlers because the groups consume
independently.

Start the [local RocketMQ environment](../../../../test-environments/rocketmq/README.md), then run:

```shell
docker compose -f test-environments/rocketmq/compose.yaml up -d --wait
dotnet run --project samples/remoting/GenericHost/PushConsumer/EventHorizon.RocketMQ.Samples.Remoting.PushConsumer.csproj
```

The default NameServer is `localhost:9876`. An external setup must make both the NameServer and every advertised Broker
address reachable, create the normal topic, and grant route-query and consume access, including access to any retry or
dead-letter topics used by its outcomes. Broker assignment also requires `QUERY_ASSIGNMENT` support. To exercise POP,
an operator must separately configure POP request mode for the Broker-assigned group; this is not a client option.

Start this host before using the [Remoting Producer sample](../Producer/README.md) to publish a matching `sample`
message. Both typed handlers log their independently delivered copies and return `Success`, so the SDK settles their
batches and advances their source offsets. Press Ctrl+C to stop the host and its Consumer roles.

See the [Remoting guide](../../../../src/EventHorizon.RocketMQ.Remoting/README.md) for complete options and API examples.
