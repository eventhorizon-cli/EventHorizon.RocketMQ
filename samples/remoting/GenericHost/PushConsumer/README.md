# Remoting Push Consumer

[English](README.md) | [简体中文](README.zh-CN.md)

`IRemotingPushConsumer` is the high-level classic Remoting consumption model. The SDK maintains consumer-group
assignment, initiates Broker long polls, buffers received messages, dispatches handlers, settles retry or dead-letter
outcomes, and persists offsets. Application code supplies a message handler instead of driving a poll loop.

Despite the name, messages are not pushed from the Broker directly into application code. The client performs long
polling and also handles classic Broker callbacks such as rebalance and offset reset.

## When to use Push Consumer

Choose Push Consumer for continuously running services that want the SDK to own queue allocation, receive scheduling,
handler concurrency, retries, and offset progression. It is the normal choice when business code can be expressed as
an asynchronous callback and can tolerate at-least-once delivery.

Prefer Lite Pull when the SDK should allocate queues but the application must control when polling and commits occur.
Prefer Pull Consumer for exact physical-queue and request-offset control. Prefer POP for per-message receipt and
invisibility semantics.

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

builder.Services
    .AddRocketMQRemoting(remotingSection.Bind)
    .AddRemotingPushConsumer<PushConsumerMessageHandler>(ServiceLifetime.Scoped, options =>
    {
        options.GroupName = "remoting-sample-push-consumer";
        options.MaxConcurrency = 4;
        options.BatchSize = 32;
        options.ConsumeMessageBatchSize = 4;
        options.Subscribe(Topic, new FilterExpression("sample"));
    })
    .AddRemotingPushConsumer<AllMessagesMessageHandler>(ServiceLifetime.Scoped, options =>
    {
        options.GroupName = "remoting-all-messages-push-consumer";
        options.MaxConcurrency = 2;
        options.BatchSize = 8;
        options.ConsumeMessageBatchSize = 1;
        options.Subscribe(Topic);
    });
```

Each call owns its group, subscriptions, handler, scheduling, offset state, and hosted lifecycle. Consumers under the
same `AddRocketMQRemoting` registration reuse NameServer discovery, route state, and compatible underlying Broker
connections. Connection and credential settings remain in `appsettings.json`; the Consumer choices are visible in
`Program.cs`.

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

## Queue allocation and concurrent dispatch

In the default `Clustering` mode, live consumers with the same group share the topic's Broker physical queues. A queue
is assigned to one consumer in that group, so adding consumers increases useful parallelism only until the consumer
count reaches the total readable queue count across all Brokers. Additional consumers can remain idle. Consumers in
different groups receive independent consumption of the topic.

All clustering instances in the same group must use the same topic and filter subscriptions. Allocation uses the
group's complete consumer-id list for each topic; inconsistent subscriptions can assign a queue to a member that is
not consuming that topic, while inconsistent filters can make accepted messages depend on the current queue owner.
The registration validates this constraint locally. Compatible same-group registrations receive distinct active
consumer identities and share queues; they are not independent fan-out subscriptions.

The client maintains one local `ProcessQueue` for every assigned Broker `MessageQueue`. A `ProcessQueue` holds pulled
messages and their consumption state; it is not a new Broker queue and creates no server resource. The name and
ownership concept follow the official RocketMQ Java client.

`BatchSize` limits messages requested by one Broker pull. `ConsumeMessageBatchSize` independently limits messages
passed to one handler invocation. Concurrent batches contain only messages from the same physical queue. Messages with
a `MessageGroup` and all `ConsumeOrderly` deliveries remain singleton batches.

Ready `ProcessQueue` instances share asynchronous consume loops. Each turn takes one batch from one ready queue and
places a queue with more work at the back, providing fair progress across active queues and Brokers. `MaxConcurrency`
limits handler dispatches across the entire Consumer, not per queue. These loops are .NET ThreadPool tasks; they are
not dedicated threads and are not permanently bound to a queue. This fair scheduler is a .NET design rather than an
exact copy of the Java concurrent dispatch path.

`MaxCachedMessages` limits admission of newly pulled messages waiting for first dispatch. In-flight or settlement-
blocked messages do not continue to hold that admission capacity, and a blocked queue pauses new admission until it
can progress. The value is therefore not a strict count of every message resident in the process.

## Acknowledgement, retry, and offsets

The handler returns one `ConsumeResult` for its batch:

- `Success` acknowledges the complete batch by default.
- For a concurrent non-FIFO batch, set `context.AckIndex` to the zero-based last successful index and return
  `Success`. The contiguous prefix is accepted and only the tail is sent for Broker retry. The default
  `int.MaxValue` accepts all messages; `-1` accepts none.
- `Retry` and `DeadLetter` ignore `AckIndex` and apply to the complete batch.

For a clustered concurrent non-FIFO batch, `context.DelayLevelWhenNextConsume` starts from the Broker delay level
mapped from `RetryDelay`. Set it to `0` to let the Broker choose, to a positive RocketMQ delay level to request that
level, or to a negative value to request direct dead-letter delivery. `MessageGroup` FIFO, `ConsumeOrderly`, and
broadcasting paths ignore this context setting. In clustering mode, `MaxDeliveryAttempts` bounds retry delivery; a
retried message that reaches the limit is sent to the dead-letter queue.

Every `ProcessQueue` maintains an independent contiguous completion watermark. A later batch, or a batch from another
queue or Broker, cannot skip an earlier unresolved queue offset. Pulling may run ahead of handler completion. In
clustering mode, the Broker-committed offset cannot run ahead of successful settlement; broadcasting instead treats
unavailable retry and dead-letter outcomes as locally resolved drops, as described below.

In the clustered concurrent `ProcessQueue` path, failed retry/dead-letter settlement is retried locally after
`RetryDelay` without invoking the application handler again; the watermark remains blocked. Offset persistence
failures are also retried. If an assignment is revoked, a late handler result from that old assignment is ignored and
cannot advance its replacement.

`ConsumeTimeout` applies to concurrent clustered non-FIFO batches. On expiry, the client cancels the handler token and
requests Broker redelivery for the whole batch. Code that ignores cancellation cannot be forcibly stopped; its late
result is ignored but its external work can overlap the redelivered invocation. Handlers must honor cancellation and
remain idempotent. FIFO `MessageGroup` and `ConsumeOrderly` deliveries are excluded so timeout recovery cannot break
their ordering.

## Ordering and broadcasting

`MessageGroup` gives producer-side queue affinity and singleton FIFO dispatch. `ConsumeOrderly` instead serializes
every assigned physical queue. In clustering mode, orderly consumption acquires and renews classic Broker queue locks;
`MaxConcurrency` can still process different locked queues concurrently. A retry blocks that queue so later messages
cannot overtake it. Queue locks preserve order but do not change at-least-once delivery, so orderly handlers must also
be idempotent.

`Broadcasting` assigns every readable queue to every Consumer instance and stores offsets locally. It has no
group-owned Broker retry or dead-letter flow: `Retry`, `DeadLetter`, and an unacknowledged batch tail are dropped while
the local offset advances. Use a stable, instance-specific `LocalOffsetStorePath` when client identity is not stable.

For a normal subscribed queue, `InitialPosition` supplies a starting offset only when the active offset store has no
value: the Broker group offset in clustering mode, or the instance's local offset file in broadcasting mode. It does
not reset an existing position. Retry queues retain their recovery boundaries and do not use this normal-queue
starting policy.

This sample sets `InitialPosition` to `End`, the default for a fresh group. `StartAsync` does not make that initial
offset query a deployment cutover: a message published before a newly assigned receiver resolves its first offset can
be considered existing backlog and skipped. Choose `Beginning` or `Timestamp` for deliberate replay, or use a
committed group offset or application readiness handshake when the cutover must include every message.

## Run the sample

The runnable sample registers two scoped typed handlers over `eventhorizon-test-topic`. Group
`remoting-sample-push-consumer` accepts only the `sample` tag with `MaxConcurrency` 4, while group
`remoting-all-messages-push-consumer` accepts every tag with `MaxConcurrency` 2. A `sample` message is delivered to both
handlers because the groups consume independently.

Start the [local RocketMQ environment](../../../../test-environments/rocketmq/README.md), then run:

```shell
docker compose -f test-environments/rocketmq/compose.yaml up -d --wait
dotnet run --project samples/remoting/GenericHost/PushConsumer/EventHorizon.RocketMQ.Samples.Remoting.PushConsumer.csproj
```

The default NameServer is `localhost:9876`. An external setup must make both the NameServer and every advertised Broker
address reachable, create the normal topic, and grant route-query and consume access, including access to any retry or
dead-letter topics used by its outcomes.

Start this host before using the [Remoting Producer sample](../Producer/README.md) to publish a matching `sample`
message. Both typed handlers log their independently delivered copies and return `Success`, so the SDK settles their
batches and advances their source offsets. Press Ctrl+C to stop the host and its Consumer roles.

See the [Remoting guide](../../../../src/EventHorizon.RocketMQ.Remoting/README.md) for complete options and API examples.
