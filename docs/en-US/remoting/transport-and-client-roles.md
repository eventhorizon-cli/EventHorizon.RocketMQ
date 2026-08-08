# Classic Remoting Transport and Client Roles

[English](transport-and-client-roles.md) | [Simplified Chinese](../../zh-CN/remoting/transport-and-client-roles.md)

## Context

Classic RocketMQ clients do not connect to a Proxy as their primary discovery endpoint. They query a
NameServer for a topic route and then connect directly to the Broker addresses advertised in that
route. This gives the client access to the classic feature set, but it also makes Broker network
reachability part of the application deployment contract.

The implementation deliberately owns this transport rather than carrying a legacy framework
dependency. It uses .NET sockets, streams, and `System.IO.Pipelines` for framing and lifecycle
control.

## Decision

`EventHorizon.RocketMQ.Remoting` keeps all classic wire behavior inside its `Protocol` directory
and exposes protocol-specific roles through `AddRocketMQRemoting`.

```text
Application
    -> Remoting role
    -> TopicRouteService
    -> NameServer (route query)
    -> advertised Broker endpoint
    -> RemotingClient / RemotingConnection
    -> classic frame serializer and Broker command
```

The project does not reintroduce Bedrock Framework, does not use a gRPC Proxy as a substitute for a
NameServer, and does not share a transport abstraction with the gRPC project.

## How It Works

### Route discovery and Broker reachability

`RemotingClientOptions.NamesrvAddr` accepts the configured NameServer addresses. The
[`NameServer`](../../../src/EventHorizon.RocketMQ.Remoting/Protocol/Route/NameServer.cs)
tries those addresses in round-robin order and requests the topic route. The
[`TopicRouteService`](../../../src/EventHorizon.RocketMQ.Remoting/Protocol/Route/TopicRouteService.cs)
caches a successful route for `PollNameServerInterval`; while a valid cached route exists, it can
be used when a refresh fails.

Route discovery is only the first hop. The application process must be able to reach every Broker
host and port returned by the NameServer. A valid `NamesrvAddr` does not guarantee that the data
plane works from the application's network.

### Built-in wire transport

[`RemotingClient`](../../../src/EventHorizon.RocketMQ.Remoting/Protocol/RemotingClient.cs)
keeps usable connections by endpoint, correlates concurrent requests with opaque IDs, runs receive
loops, and dispatches Broker-initiated requests through a bounded channel. A failed connection is
removed so a later request can establish a replacement.

[`RemotingConnection`](../../../src/EventHorizon.RocketMQ.Remoting/Protocol/RemotingConnection.cs)
wraps the socket stream in `PipeReader` and `PipeWriter`. It serializes writes, parses complete
frames without accepting incomplete data, faults the connection on terminal transport errors, and
supports TLS when configured. The
[`RemoteCommandSerializer`](../../../src/EventHorizon.RocketMQ.Remoting/Protocol/RemoteCommandSerializer.cs)
owns the classic command frame encoding and decoding.

This lets the project apply frame-size limits, cancellation, disposal, and request correlation in
one place without depending on Bedrock Framework.

### Roles

The Remoting builder offers roles with semantics that belong to classic RocketMQ:

| Role | Purpose |
| --- | --- |
| `IRemotingProducer` | Standard, FIFO, delayed, priority, Lite, transactional, selected-queue, batch, one-way, request-reply, and recall-capable Producer operations supported by this project. |
| `IRemotingAdmin` | Read-only queue, offset, and stored-message inspection. |
| `IRemotingLitePullConsumer` | Application polling over SDK-managed assignments, background PULL receives, local positions, and automatic or explicit commits. |
| `IRemotingPushConsumer` | SDK-managed assignment, PULL/POP reception, handler dispatch, settlement, and classic Remoting coordination. |

The role registrations live in
[`RemotingRocketMQBuilderExtensions`](../../../src/EventHorizon.RocketMQ.Remoting/RemotingRocketMQBuilderExtensions.cs).
Low-level PULL and POP wire operations are not exposed as public roles; they remain internal to LitePull and Push.
These classic models are not identical to gRPC Consumers and should not be treated as common transport-neutral roles.

LitePull supports subscription-driven allocation and explicit `AssignAsync` with `RemotingConsumerQueue` values; the
two modes are mutually exclusive. One background PULL loop per assignment fills a bounded shared local buffer, and
`PollAsync` drains that buffer as `IReadOnlyList<RemotingMessageView>`. `PullBatchSize` limits each Broker receive;
`MaxCachedMessages` and `MaxCachedMessageBytes` bound the LitePull buffer. `ConsumerOptions.InitialPosition`, whose
type is `ConsumeFromPosition`, selects Beginning, End, or Timestamp only when a queue has no committed group
position. Manual assignment plus `Seek` and explicit `CommitAsync` replaces the former direct queue/offset workflow.

Remoting Push supports two `QueueAssignmentMode` values. The default `Client` mode preserves classic client-side
allocation (average allocation for clustering and all readable queues for broadcasting) with PULL reception. `Broker`
mode requests `QUERY_ASSIGNMENT` for concurrent clustered consumption and creates an internal PULL or POP receiver
for each returned assignment. Broadcasting and `ConsumeOrderly` force effective client assignment with PULL even when
`Broker` is requested, matching Java Remoting.
Runtime Consumers never send `SET_MESSAGE_REQUEST_MODE`; the desired topic/group request mode belongs to
operator-managed Broker configuration.

Push PULL settings are named `PullBatchSize`, `PullMaxCachedMessages`, and `PullMaxCachedMessageBytes`. Broker-assigned
POP uses `PopBatchSize`, `PopInvisibleDuration`, and `PopMaxInflightMessagesPerAssignment`. POP receipt acquisition,
fixed-deadline validation, acknowledgement, and retry remain internal Push responsibilities rather than a
caller-managed Consumer API. The client does not renew a POP receipt while the handler is active.

Remoting Push has one batch-oriented handler API: `IRemotingPushMessageHandler.HandleAsync` receives an
`IReadOnlyList<RemotingMessageView>` and a `RemotingPushConsumeContext`. `ConsumeMessageBatchSize` independently limits
how many messages one handler invocation receives and defaults to `1`. Concurrent non-`MessageGroup` messages from the
same Broker physical queue can be grouped and those batches can run in parallel up to `MaxConcurrency`.
`MessageGroup` FIFO deliveries and `ConsumeOrderly` deliveries remain singleton and serialized.

`Success` normally confirms the complete batch. A concurrent non-FIFO handler can set `AckIndex` to the zero-based
index of the final accepted message before returning `Success`; the client confirms that contiguous prefix and retries
only its tail. `Retry` applies to every message regardless of `AckIndex`. The context also exposes
`DelayLevelWhenNextConsume` for the failed batch or unacknowledged tail: `0` delegates retry timing to the Broker, a
positive value selects a RocketMQ delay level, and a negative value requests direct dead-lettering. This two-result
contract matches the classic Java and Go concurrent consumers; dead-lettering is a retry-policy terminal choice rather
than a separate handler result. Broadcasting does not have a Broker retry or dead-letter path, so an unacknowledged
tail is skipped.

`ConsumeTimeout` defaults to 15 minutes and applies only to concurrent clustered non-FIFO batches. When it elapses,
the client cancels the handler token and requests Broker redelivery for the whole batch. It cannot forcibly terminate
application code that ignores cancellation; its late result is ignored and can overlap redelivery, so handlers must
cooperate with the token and remain idempotent. FIFO and orderly delivery intentionally remain outside this recovery
path to preserve ordering guarantees.

#### Push group membership, Rebalance, and connection ownership

One `AddRocketMQRemoting` registration may add multiple Push Consumers. Every instance owns its options, logical
client identity, consumer engine, typed handler binding, assignment state, and hosted lifecycle. Consumers in
different groups can share NameServer discovery, routes, and a physical `RemotingClient`. Repeated configured members
of the same group use separate physical clients because the classic Broker's consumer-group table keys membership by
connection channel; a heartbeat for another client ID on that channel replaces the existing entry.

Each physical client allocated to an active Push or LitePull member owns one `RemotingRebalanceService` and therefore
one `NotifyConsumerIdsChanged` request handler:

```text
Broker membership change
    -> RemotingClient inbound request loop
    -> RemotingRebalanceService group lookup
    -> coalesced participant wakeup
    -> route refresh / heartbeat / consumer-ID query
    -> queue allocation and assignment reconciliation
```

The inbound handler only records a wakeup; it does not perform route or Broker I/O on the receive loop. Every group
has an independent single-flight participant, so a blocked or failing group cannot serialize unrelated groups.
Failures retry after one second, and a periodic fallback capped at 20 seconds repairs missed notifications. Shared
heartbeat, membership-query, and unregister operations belong to `RemotingConsumerGroupSession`; orderly queue lock
and unlock commands belong to `OrderlyPullQueueLockClient`.

A physical `RemotingClient` is transport only and has no group identity of its own. Each active Push or LitePull
session sends its own heartbeat through its assigned client on initial and periodic coordination; LitePull becomes
active when it has subscriptions or manual queue assignments. Thus a shared client can keep several distinct groups
alive, while isolated same-group members each maintain their own membership. Internal PULL and POP receivers belong
to those two public group sessions rather than registering additional memberships. Producers maintain their own
producer heartbeat lifecycle.

Clearing a LitePull manual assignment with `AssignAsync([])` ends that active membership immediately: the session
unregisters from its known Brokers, while retaining those endpoints so shutdown can retry an unsuccessful unregister.

Consumer lifecycle readiness and initial-offset readiness are separate. `StartAsync` creates the lifecycle and starts
assignment reconciliation, but concurrent queue receivers resolve their initial offsets asynchronously. For a fresh
group using the default end position, a record written before a receiver resolves its first `maxOffset` can be behind
that position and is intentionally outside the group's consumption range. This is the classic Java
`CONSUME_FROM_LAST_OFFSET` behavior, not a Rebalance or typed-handler failure. Applications that need a precise
cutover must choose a beginning or timestamp position, preserve a committed group offset, or establish an explicit
readiness handshake. Integration tests must not use the default end position to assert delivery across this boundary.

For orderly Push, a denied initial lock or a failed lock renewal leaves reconciliation incomplete so the participant
retries it. The consumer never dispatches a queue until the Broker has granted its lock.

Members of one Push group must use identical topic/filter subscriptions, clustering or broadcasting mode, orderly
mode, and initial position. Concurrency, timeouts, and typed handlers remain instance-local. These are group-wide
rules, but registration can fail fast only for members visible in the same process and client registration. The
classic heartbeat does not carry the orderly flag, so deployments must keep other processes consistent. This matches
the official Java and Go protocol behavior: group membership is Broker-visible, while orderly locking is a local
client Rebalance decision.

Once another same-group member is registered locally, an individual Remoting Push or LitePull member cannot change
subscriptions with `SubscribeAsync` or `UnsubscribeAsync`. Update the identical group-wide subscription set and
restart the local members together. Cross-process members still require coordinated deployment.

Docker integration coverage exercises both two members under one registration and two independent operating-system
processes. The cross-process case runs concurrent and orderly modes, proves mutually exclusive complete assignments,
then gracefully stops one member and verifies notification-driven takeover before the periodic fallback can run. A
separate concurrent case force-terminates a member. Orderly force termination is not asserted against a short deadline
because Broker queue-lock lease expiry, rather than client notification, determines its recovery bound.

#### Concurrent Push dispatch and offset safety

In concurrent PULL mode, every assigned Broker physical queue owns one client-side `PullProcessQueue`. A `PullProcessQueue`
holds
locally pulled messages and their consumption state; it is not another Broker queue and does not create any
server-side resource. Pulling can advance independently of handler completion. `PullMaxCachedMessages` and
`PullMaxCachedMessageBytes` bound admission of newly pulled messages waiting for their first dispatch. Batches already
handed to handlers do not retain that capacity. When settlement is unresolved or a FIFO message has an incomplete
predecessor, including one in another physical queue, messages already cached by that `PullProcessQueue` release their
admission and new admission for the queue pauses until it can make progress. A blocked queue therefore cannot
monopolize capacity needed by healthy queues, and the setting is not a strict limit on every resident message.

Ready `PullProcessQueue` instances enter a coalesced ready queue at most once. The shared logical consume loops take one
handler batch from a ready queue, put it at the back again when more work remains, and thereby round-robin across
active physical queues instead of allowing one hot queue to occupy the entire dispatch path. `MaxConcurrency` is the
total concurrency shared by all assigned queues. These loops are asynchronous tasks scheduled by the .NET ThreadPool;
they are not dedicated or thread-affine consumer threads.

Each `PullProcessQueue` also maintains its own contiguous completion watermark. A later batch that finishes first cannot
move the persisted offset past an earlier unresolved message, and a faster queue cannot advance another queue's
offset. Failed retry/dead-letter settlement remains unresolved and is retried locally after `RetryDelay` without
invoking the application handler again; offset persistence is also retried without crossing the gap. When an
assignment is dropped, its `PullProcessQueue` is marked dropped so a late handler completion cannot mutate the replacement
assignment or commit its offset.

The internal `PullProcessQueue` corresponds to the official RocketMQ Java client's `ProcessQueue` role and its
per-physical-queue ownership.
The Java concurrent path creates consume requests from a newly pulled message list; this implementation's coalesced
ready queue and one-batch-per-turn extraction from `PullProcessQueue` are a .NET scheduling design, not an exact copy of
the Java data path.

### Read-only Admin and physical message IDs

`IRemotingAdmin` is intentionally a separate, read-only role. It discovers readable queues as
`RemotingConsumerQueue` values, accepts those values for min/max/timestamp and consumer-offset queries, and can view
a physical message. Producer publish queues remain `RemotingMessageQueue` values owned by the Producer API. The Admin API is
documented in
[`IRemotingAdmin`](../../../src/EventHorizon.RocketMQ.Remoting/Admin/IRemotingAdmin.cs), and the
[Admin sample](../../../samples/remoting/GenericHost/Admin/README.md) exposes it through a small Swagger UI.

`ViewMessageAsync` expects `RemotingSendResult.OffsetMessageId`, not the producer-assigned
`MessageId`. The physical offset ID encodes a Broker endpoint and commit-log offset. The internal
[`LegacyOffsetMessageId`](../../../src/EventHorizon.RocketMQ.Remoting/Protocol/LegacyOffsetMessageId.cs)
decoder extracts that endpoint, and the Admin role connects to it directly. NameServer route
discovery is not used for this operation.

This is useful for inspection, but it adds a deployment constraint: the exact address embedded in
the offset ID must be reachable from the process using the Admin API. An ID produced in another
network can be valid syntactically and still be unusable from the current network.

## Trade-offs and Constraints

- Remoting has a broader classic feature surface, but that surface relies on direct Broker access
  and classic server compatibility.
- The client preserves public Consumer queue identities, offsets, and send-result types in the Remoting namespace.
  PULL/POP wire results and POP receipts are internal implementation details. Neither category should be moved into a
  third generic Project or substituted with gRPC types.
- The Admin role performs read-only operations. It is not a general Broker-administration API and
  does not create topics or mutate consumer offsets.
- ACL credentials, TLS settings, frame-size limits, and advertised Broker addresses are part of
  the Remoting environment, not universal RocketMQ settings.
- A target deployment behind NAT, containers, or load balancers must advertise Broker addresses
  that the actual client can reach. Test this from the same network as the application.

## Related Reading

- [classic Remoting project guide](../../../src/EventHorizon.RocketMQ.Remoting/README.md)
- [Remoting Admin sample](../../../samples/remoting/GenericHost/Admin/README.md)
- [Remoting LitePullConsumer sample](../../../samples/remoting/GenericHost/LitePullConsumer/README.md)
- [Remoting PushConsumer sample](../../../samples/remoting/GenericHost/PushConsumer/README.md)
- [Classic Remoting consumer model](consumer-model.md)
- [Protocol Boundaries and Type Ownership](../architecture/protocol-boundaries.md)
- [Local and integration testing](../testing/local-and-integration-testing.md)
