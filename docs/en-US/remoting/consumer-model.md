# Classic Remoting Consumer Model

[English](consumer-model.md) | [Simplified Chinese](../../zh-CN/remoting/consumer-model.md)

## Status

Accepted. This note defines the Remoting consumer API revision and the rationale for the resulting public surface.

## Context

Before this revision, the Remoting project exposed four consumer roles:

| Historical role | Historical model |
| --- | --- |
| `IRemotingPullConsumer` | The caller selects a physical queue and offset for every Broker pull. |
| `IRemotingLitePullConsumer` | The caller polls assigned queues and explicitly commits local positions. |
| `IRemotingPopConsumer` | The caller selects a physical queue and manages POP receipts directly. |
| `IRemotingPushConsumer` | The client assigns queues, long polls, dispatches handlers, and settles messages. |

That surface had two duplicate low-level roles. Direct Pull exposed a deprecated Java client model, while direct POP
exposed a Broker command that the official classic clients keep behind Push. It also made the application own queue
selection, route changes, multi-member coordination, retry topics, receipt flow control, and settlement races.

The audit also found two related implementation problems:

- LitePull performs one Broker long poll inside `PollAsync`. An empty selected queue can block messages already
  available on another assigned queue. Official LitePull implementations receive per assignment in the background and
  let `poll` drain a local cache.
- `RemotingPullMessageQueue` contains an endpoint snapshot and mutable preferred Broker ID. Queue identity outlives a
  route snapshot, so active consumers can keep using an obsolete endpoint after the route changes.

## Official Baseline

This decision uses the Java client in the Apache RocketMQ main repository as its classic Remoting baseline. The
RocketMQ 5.x website's Java SDK examples describe the separate gRPC SDK in `apache/rocketmq-clients`; their
PushConsumer load-balancing model must not be projected onto the classic Remoting client. The 4.x PushConsumer guide
and the `apache/rocketmq` 5.5.0 `client` module describe the Remoting API considered here.

The Java classic client marks `DefaultMQPullConsumer` deprecated and recommends
`DefaultLitePullConsumer` for application-driven polling. Java LitePull maintains per-assignment receive tasks and a
local consume-request cache.

Java classic does not expose a high-level manual POP consumer. `DefaultMQPushConsumer` publicly exposes
`clientRebalance`, which defaults to `true`. When it is `false`, an eligible Push consumer sends `QUERY_ASSIGNMENT`,
and the Broker returns assignments whose mode is individually `PULL` or `POP`. POP can assign queue ID `-1`, meaning
all readable queues for one topic on one Broker, and can share physical queues across consumer members.

The Java `RebalancePushImpl` forces client rebalance for broadcasting and orderly listeners even when
`clientRebalance=false`; those paths therefore remain on PULL. For concurrent clustering, `RebalanceImpl` treats the
mode in each Broker assignment as an internal receiver choice and creates either a PullRequest or a PopRequest. The
application listener and its consumption result do not change.

The Broker stores request mode by topic and consumer group. Its default is PULL; POP is selected through Broker
configuration or the administrative `SET_MESSAGE_REQUEST_MODE` operation. The runtime Push consumer queries that
decision but never writes it. Push heartbeats continue to advertise passive consumption regardless of the effective
assignment or receive mode.

Every clustered Java Push consumer also subscribes to its classic `%RETRY%{consumerGroup}` topic. That subscription
participates in the same client or Broker assignment path as application topics. The Broker assignment processor
forces retry topics to PULL even when an application topic uses POP, so a PULL receiver's send-back result remains
eligible for redelivery under Broker assignment.

## Decision

The public Remoting consumer surface is reduced to two application models:

| Public role | Responsibility |
| --- | --- |
| `IRemotingLitePullConsumer` | Application-driven polling over SDK-managed assignments, receive loops, local positions, and automatic or explicit commits. |
| `IRemotingPushConsumer` | SDK-driven assignment, reception, dispatch, retry, acknowledgement, and shutdown. |

`IRemotingPullConsumer`, `IRemotingPopConsumer`, their registration methods, role options, hosted services, samples,
and public direct-operation result types are removed without a compatibility period. The internal Pull and POP wire
operations remain because LitePull and Push own them.

Applications that need an explicit physical-queue workflow disable auto commit and use LitePull manual assignment,
`Seek`, and explicit commit. Read-only offset inspection remains in `IRemotingAdmin`. Applications that need
caller-owned receipt timing use the gRPC `IGrpcSimpleConsumer`; this revision does not add a Remoting SimpleConsumer
facade.

The removed direct Pull operations map to the retained APIs as follows:

| Direct Pull capability | Replacement |
| --- | --- |
| Discover readable queues | `IRemotingLitePullConsumer.GetMessageQueuesAsync` |
| Start at an exact physical offset | Manual `AssignAsync`, then `Seek` |
| Receive and advance a caller-owned position | `PollAsync` and `Position` |
| Read or persist a group position | `GetCommittedOffsetAsync` and `CommitAsync` |
| Inspect min, max, timestamp, or another group's offset | `IRemotingAdmin` |

Low-level Broker pull statuses and min/max fields are internal protocol concerns. LitePull handles offset correction
inside its receive loop and exposes messages, positions, and commits instead of a wire-operation result.

### Queue identity and route ownership

`RemotingPullMessageQueue` is replaced by the immutable, value-comparable `RemotingConsumerQueue`. It is the
protocol-specific public readable-queue identity used by LitePull and read-only Admin offset operations, and contains
only the logical topic, Broker name, and non-negative queue ID. `IRemotingAdmin.GetMessageQueuesAsync` discovers
readable queues and returns this same type, so its result can be passed directly to min/max/timestamp and consumer
offset queries. Producer publish queues remain the separate `RemotingMessageQueue` type.

This Admin return type is an intentional .NET API adaptation, not a claim that every official Admin API discovers
readable queues. The official Java client uses the same `MessageQueue` identity but separates writable queue discovery
on [`MQProducer`](https://github.com/apache/rocketmq/blob/rocketmq-all-5.5.0/client/src/main/java/org/apache/rocketmq/client/producer/MQProducer.java#L30-L35)
from readable subscription queue discovery on
[`MQConsumer`](https://github.com/apache/rocketmq/blob/rocketmq-all-5.5.0/client/src/main/java/org/apache/rocketmq/client/consumer/MQConsumer.java#L48-L53).
This client instead gives Producer and Consumer queues distinct CLR types. Its read-only Admin offset workflow therefore
returns the Consumer identity that those offset methods accept.

Broker endpoints and suggested Broker IDs belong to an internal route resolver. Topic snapshots remain keyed by topic,
while the last valid address set is keyed by Broker name, matching the official Java client's Broker address table.
This distinction lets retry-topic offset initialization continue through a Broker already learned from an application
topic even when the retry topic is not yet visible through NameServer. Suggested Broker IDs remain keyed by consumer
queue identity because a PULL response's replica preference applies to that queue. Broker-assigned POP uses a separate
internal assignment type because its logical queue ID may be `-1`.

Every Pull, offset, send-back, POP, ACK, and invisibility request resolves its endpoint from the latest valid route.
The route cache may provide the last known valid route during a transient NameServer failure, but a queue value passed
by application code is never the authoritative endpoint source.

All `RemotingConsumerQueue` construction paths enforce a non-negative queue ID and store no endpoint snapshot or
mutable Broker hint. A queue returned by Admin, discovered by LitePull, or created directly by an application therefore
has identical capabilities. Internal Broker-assigned POP state never constructs a public queue for its `queueId=-1`
sentinel.

Each Consumer role owns one `RemotingConsumerRouteResolver`. A successful refresh atomically replaces that topic's
resolved queues and refreshes the role-local Broker-name address table. A failed topic refresh may use the topic's
previous valid snapshot or an address already learned for the same Broker name; it never borrows an address from a
different Broker name. Suggested Broker IDs learned from PULL responses are also resolver state and are scoped to the
owning Consumer. Manual LitePull assignments resolve their topic and Broker through this same component before
reception and group heartbeats begin. Their behavior does not depend on whether a queue originated from Admin,
LitePull discovery, or a public constructor.

### Shared internal Consumer boundaries

The `Consumer` root contains only public foundational models and small protocol composition types. Active behavior is
organized by ownership rather than by a generic Consumer base class:

| Location | Responsibility |
| --- | --- |
| `Consumer` root | Public `ConsumerOptions`, filters, positions, results, logical queue identity, and message view. |
| `Route` | Per-role route snapshots, readable-queue discovery, endpoint resolution, last-valid-route fallback, and suggested Broker IDs. |
| `Coordination` | Heartbeat and membership wire operations, heartbeat encoding, consumer-list decoding, and the per-run group session. |
| `Coordination/Rebalance` | Shared client-rebalance scheduling, registration, wakeup, and the deterministic average queue allocator. |
| `Pull` | Stateless internal PULL request/response contracts and wire execution used by LitePull and Push PULL receivers; each pull receives its filter from the current receive target. |
| `Offset` | Internal queue-position queries and consumer-group offset persistence. |
| `Settlement` | Classic `CONSUMER_SEND_MSG_BACK` execution shared by Push PULL retry and POP dead-letter forwarding. |
| `LitePull` and `Push` | The two public role facades and their role-specific assignment, receive, dispatch, and run state. |

The protocol composition root still creates one `IRemotingConsumerEngine` per registered role. The engine owns only
the role lifecycle and composes `PullWireClient`, `RemotingConsumerOffsetClient`, and `RemotingSettlementClient` with
the role's route resolver. It does not own assignment policy, receive loops, group membership, or a second public
lifecycle. Role-specific collaborators consume narrow route, PULL, offset, or settlement interfaces, so a POP
settlement path does not depend on PULL or offset behavior.

`RemotingConsumerGroupClient` is the stateless heartbeat, membership-query, and unregister wire client.
`RemotingConsumerGroupSession` is created for one started generation and owns the known-Broker set, subscription
version, rebalance registration, and unregister boundary. The Push and LitePull runs own their queue reconciliation and do
not inherit from a shared Consumer base class. Query-assignment JSON stays under `Push/Assignment`; heartbeat JSON and
consumer-list decoding stay under `Coordination`; the average allocator is a pure coordination policy rather than a
protocol helper.

Locally registered consumers using the same namespaced group must have compatible Broker-visible behavior. In addition
to identical subscriptions, role, message model, ordering, and initial-position settings, Push members must use the
same `QueueAssignmentMode`. Mixing client and Broker assignment in one group is rejected during options validation
because the two members can otherwise calculate overlapping ownership.

### LitePull receive model

After subscription rebalance or manual assignment, LitePull starts one cancellable receive loop per desired receive target
(queue and filter):

```text
subscription or manual assignment
        -> assignment reconciliation
        -> queue/filter receive targets
        -> per-target Broker long polls
        -> bounded shared local buffer
        -> application PollAsync
        -> delivered local positions
        -> automatic or explicit CommitAsync
```

Fetch positions and delivered positions are separate. A receive loop advances its fetch position when a Broker result
is admitted to the local buffer. The committable position advances only when `PollAsync` returns those messages to the
application. Consequently, a prefetched but undelivered message cannot be skipped by a commit.

The revised LitePull contract has these properties:

- `PollAsync` returns `IReadOnlyList<RemotingMessageView>`, not the low-level `RemotingPullResult`.
- `PollTimeout` is independent from the Broker `LongPollingTimeout`. `PollAsync(null)` uses `PollTimeout`, zero performs
  a non-blocking drain, and a local timeout returns an empty list. Only caller cancellation or shutdown throws
  cancellation.
- `MaxCachedMessages` and `MaxCachedMessageBytes` bound the shared prefetched-message buffer by count and bytes.
- `Position` returns the next local position for an assigned queue; it never performs Broker I/O.
- `Pause` stops new receives for a queue; already buffered messages may still drain. `Resume` restarts reception.
- `Seek` and assignment removal cancel the affected receive generation and discard its stale buffered messages before
  a new generation becomes visible. A route replacement cancels only the in-flight request; already buffered messages
  remain valid because the logical queue identity has not changed. Seek and position receiver replacement preserve the
  current target filter.
- `SeekToBeginningAsync`, `SeekToEndAsync`, and `SeekToTimestampAsync` cover application positioning.
- `GetCommittedOffsetAsync` exposes the persisted group position. Min/max inspection stays on Admin.
- Manual `AssignAsync` accepts queues plus an optional read-only dictionary of per-topic filters. A missing topic
  filter means match all. Subscription mode and manual-assignment mode remain mutually exclusive. Manual assignments
  and their filters remain the desired state until replaced; every manual-mode heartbeat and queue/filter receive target
  uses the corresponding filter.
- `EnableAutoCommit` defaults to `true`; `AutoCommitInterval` defaults to five seconds. Automatic and explicit commits
  persist delivered positions only, never fetch positions. When auto commit is enabled, assignment reconciliation
  retains each revoked queue state through receiver drain and persists its delivered position before completing
  reconciliation. This follows
  Java LitePull's
  [`removeUnnecessaryMessageQueue`](https://github.com/apache/rocketmq/blob/rocketmq-all-5.5.0/client/src/main/java/org/apache/rocketmq/client/impl/consumer/RebalanceLitePullImpl.java#L60-L65).
- Route refresh, known-Broker heartbeat, and assignment reconciliation are failure-isolated. A NameServer failure does
  not suppress heartbeats to Brokers that are still reachable.

### Internal LitePull ownership layout

LitePull is a top-level application model, not a subtype of the internal PULL wire operation. Its public namespace and
source root are `EventHorizon.RocketMQ.Remoting.Consumer.LitePull` and `Consumer/LitePull`; `Consumer/Pull` remains internal.
Like Push, one LitePull start creates per-run collaborators rather than growing the public facade:

| Location and type | Owns | Does not own |
| --- | --- | --- |
| Root `RemotingLitePullConsumer` | Public lifecycle serialization and validation of subscribe, assign, poll, seek, pause, resume, position, and commit calls. | Per-run composition, receive loops, route state, heartbeat targets, or mutable queue positions. |
| Root `LitePullConsumerRunLifecycle` | Per-start collaborator composition, initial coordination, and ordered close, drain, final commit, unregister, and engine shutdown. | Public API state or reusable singleton services. |
| Root `RemotingLitePullConsumerRun` | One started generation, operation admission, cancellation, and references to that generation's owned collaborators. | Public configuration or cross-generation mutable state. |
| `Assignment/LitePullSubscriptionState` | The subscription or manual-assignment intent, per-topic filters, and the subscription version as one serialized snapshot. | Route lookup, queue allocation, or receiver startup. |
| `Assignment/LitePullAssignmentCoordinator` | Route refresh, group membership, client allocation, heartbeat preparation, and desired queue/filter receive targets. | Receiver generations, local buffering, or offset advancement. |
| `Assignment/LitePullAssignmentReconciler` | Diffing desired and current queue/filter targets, stopping and draining removed generations, committing revoked delivered positions when auto commit is enabled, initializing offsets, and starting missing generations with their target filters. | Allocation policy or heartbeat contents. |
| `Receive/LitePullReceiverRegistry` | One generation-aware receiver per assigned queue/filter target, stop-drain-replace transitions that preserve target filters, and observation of unexpected completion. | Queue allocation or application positions. |
| `Receive/LitePullBuffer` | Shared FIFO delivery storage and atomic count-and-byte admission for decoded batches awaiting `PollAsync`. | In-flight Broker requests, Broker offsets, or assignment policy. |
| `Receive/LitePullDeliveryState` | Under one lock, maintains assignment generations and their queue/filter targets, shared buffer, fetch and delivered positions, seek/pause state, and committable snapshots. | Broker I/O or timer scheduling. |
| `Offset/LitePullCommitLoop` | Periodic commit timing, explicit commit serialization, and the final auto-commit during shutdown. | Message reception or handler dispatch. |

Subscription contents and the version placed in a heartbeat are one coordination transition. The same immutable
subscription/filter snapshot produces the heartbeat and the desired queue/filter receive targets, so a concurrent
periodic rebalance cannot observe a new version paired with an older filter snapshot. Manual assignment follows the
same rule: the run retains the desired queues and resolved per-topic filters, and both the initial and periodic
heartbeats and receive targets include them, matching Java LitePull's assignment-mode heartbeat. A target replacement
creates a new receiver generation with the requested filter; a filter replacement removes stale buffered messages from the
old generation. An in-flight old request may complete with the old filter, but no later request uses it. `Seek` and
position receiver replacement preserve the current target filter. Removing a subscription removes its receive target; a
missing topic/filter entry is never treated as `FilterExpression.All`. A transient route, heartbeat, or
offset-initialization failure does not erase that intent; periodic coordination retries reconciliation until the desired
receiver generations exist.
See Java
[`updateAssignPullTask`](https://github.com/apache/rocketmq/blob/rocketmq-all-5.5.0/client/src/main/java/org/apache/rocketmq/client/impl/consumer/DefaultLitePullConsumerImpl.java#L1032-L1038).

The receiver registry observes every receiver task. If a receiver ends unexpectedly while its queue generation is
still current, the registry wakes coordination and the reconciler starts a replacement. Completion from a revoked or
replaced generation is stale and must not recreate that generation. This supervision belongs to assignment recovery;
the receive loop remains responsible only for one queue's Broker polling and batch admission.

A queue receiver keeps at most one PULL or returned batch in progress. The request count and byte limits are capped by
`PullBatchSize`, `MaxCachedMessages`, `MaxMessageBytes`, and `MaxCachedMessageBytes`, but an empty long poll does not
reserve shared delivery-buffer capacity. This is necessary for failure isolation: one queue waiting for messages must
not prevent another assigned queue from receiving ready messages.

When a non-empty response returns, its decoded batch enters the shared buffer through one atomic count-and-byte
admission. If another queue currently occupies the capacity, the receiver retains that one response and waits for
`PollAsync`, seek, assignment removal, or shutdown to change the state; it does not issue another PULL or advance the
fetch position while waiting. A single message larger than the byte limit is admitted only when the shared buffer is
otherwise empty. This keeps the pollable cache within its configured aggregate limits without turning an empty Broker
long poll into a cross-queue head-of-line blocker. Assignment removal, seek, and cancellation discard the staged
response without advancing its generation's fetch position.

Java LitePull checks its global request cache and per-queue count, size, and span thresholds before a pull, then adds a
returned batch to `PullProcessQueue`; concurrent receive tasks can therefore cross a threshold transiently. This client
retains the official background-per-assignment model but strengthens admission to the pollable shared cache. A returned
batch waiting for admission is bounded to one per assignment and is not counted as pollable cache until admission.
See Java
[`PullTaskImpl`](https://github.com/apache/rocketmq/blob/rocketmq-all-5.5.0/client/src/main/java/org/apache/rocketmq/client/impl/consumer/DefaultLitePullConsumerImpl.java#L909-L998)
and
[`ProcessQueue.putMessage`](https://github.com/apache/rocketmq/blob/rocketmq-all-5.5.0/client/src/main/java/org/apache/rocketmq/client/impl/consumer/ProcessQueue.java#L129-L168).

Short state mutations such as a `PollAsync` buffer drain, `Seek`, pause, resume, and explicit commit participate in the
run's operation gate. Engine-backed queries such as `GetCommittedOffsetAsync` use the same admission and run
cancellation so they cannot outlive the engine generation they call. Assignment and subscription changes remain
serialized by the outer lifecycle gate. Shutdown first closes both admission paths, cancels Broker receives and
waiting polls, and drains operations that were already admitted, so no delivered position or receiver generation can
appear after the final snapshot. Caller cancellation stops waiting for shutdown promptly but does not abandon the
cleanup: the same shutdown task continues to drain admitted work, commit, unregister, stop the engine, and dispose the
run. A later `StartAsync` or `DisposeAsync` observes that task before it can reuse or dispose the role-owned engine.

When `EnableAutoCommit` is enabled, shutdown then makes one best-effort final commit of that delivered-position
snapshot before unregistering group membership. Manual-commit mode never advances the Broker offset merely because
the consumer stops; only positions already passed to `CommitAsync` are persisted. This follows Java LitePull, whose
[`shutdown`](https://github.com/apache/rocketmq/blob/rocketmq-all-5.5.0/client/src/main/java/org/apache/rocketmq/client/impl/consumer/DefaultLitePullConsumerImpl.java#L239-L255)
persists its offset store before unregistering the consumer. Finally, shutdown disposes the role-owned engine.

### Push queue assignment modes

`RemotingPushConsumerOptions` gains a `QueueAssignmentMode` property of type `RemotingPushQueueAssignmentMode`.
Queue-assignment ownership and receive mode are separate dimensions:

| Value | Who calculates queue assignments | Possible internal receive modes | Application API |
| --- | --- | --- | --- |
| `Client` | The client uses topic routes, group membership, and its allocation strategy. This is the default. | PULL only. | The Push handler receives messages. |
| `Broker` | The Broker returns assignments for concurrent clustered consumption. | PULL or POP, selected by each returned assignment. | The same Push handler receives messages. |

`QueueAssignmentMode` selects who owns queue-assignment calculation; it is not a public PULL/POP selector. As in Java Remoting,
broadcasting or `ConsumeOrderly` internally uses client assignment and PULL even when `Broker` is requested. Eligible
concurrent clustered consumers query the Broker and obey each returned request mode. They do not call
`SET_MESSAGE_REQUEST_MODE`; operators configure the desired topic/group request mode on the Broker. A missing
assignment mode decodes as `PULL`, a `null` assignment set preserves the prior state, and an empty set clears it. An
unsupported or failed Broker assignment query fails that reconciliation; it never silently falls back to client
assignment because that can create overlapping ownership.

This boundary follows the official classic
[PushConsumer guide](https://rocketmq.apache.org/docs/4.x/consumer/02push/), which keeps rebalance and pull logic away
from application handlers. In the 5.5.0 source,
[`RebalancePushImpl`](https://github.com/apache/rocketmq/blob/rocketmq-all-5.5.0/client/src/main/java/org/apache/rocketmq/client/impl/consumer/RebalancePushImpl.java#L125-L128)
defines when client assignment remains effective, while
[`QueryAssignmentProcessor`](https://github.com/apache/rocketmq/blob/rocketmq-all-5.5.0/broker/src/main/java/org/apache/rocketmq/broker/processor/QueryAssignmentProcessor.java#L92-L132)
places the configured PULL or POP request mode on each Broker assignment.

All clustered modes advertise and reconcile the classic retry topic. With Broker assignment, the consumer queries an
assignment for that topic and accepts the Broker-forced PULL mode. Excluding it would make a successful PULL
`CONSUMER_SEND_MSG_BACK` operation unreachable by the same Push consumer.

The Broker may return PULL for one topic and POP for another, or change a topic between them. Reconciliation treats
the mode as part of assignment identity: the old receiver is stopped and drained before its replacement starts.

Broker-assigned POP adds these Push options:

| Option | Default | Purpose |
| --- | ---: | --- |
| `PopBatchSize` | `32` | Maximum messages in one POP request; valid range is 1 through 32. |
| `PopInvisibleDuration` | 60 seconds | Initial invisibility lease; valid range is 5 seconds through 5 minutes. |
| `PopMaxInflightMessagesPerAssignment` | `96` | Maximum received POP messages awaiting settlement for one assignment. |

PULL settings use the explicit names `PullBatchSize`, `PullMaxCachedMessages`, and `PullMaxCachedMessageBytes` so their
scope is not confused with POP in a mixed Broker assignment. LitePull uses `PullBatchSize` for the same wire operation.
`ConsumerOptions` owns the shared `InitialPosition` and optional `ConsumeTimestamp`; the former public
`QueryOffsetPolicy` type is removed.

`InitialPosition.Beginning` maps to POP init mode MIN. End and Timestamp map to MAX, matching the Java classic
behavior. A POP assignment does not provide orderly or cross-member `MessageGroup` FIFO guarantees.

### Separate PULL and POP state

PULL and POP share subscription state, group membership, heartbeats, Broker assignment queries, handler concurrency,
telemetry, and lifecycle. Their receive and settlement state remains separate:

| Concern | PULL receiver | POP receiver |
| --- | --- | --- |
| Progress | Queue offset and contiguous commit watermark | Broker receipt and invisible deadline |
| Flow control | Cached messages waiting for dispatch | Messages received but not yet settled |
| Success | Advance and persist offset | ACK the latest receipt |
| Retry | `CONSUMER_SEND_MSG_BACK` | One `CHANGE_MESSAGE_INVISIBLETIME` with `suspend=false` |
| Assignment removal | Drop process queue and reject late commits | Drop POP queue and reject late receipt settlement |

POP never enters the offset-based `PullProcessQueue`. Its decoder supports queue ID `-1`, retry-v1 and retry-v2 receipt
markers, and uses the physical queue encoded in each receipt for ACK and invisibility changes.

The receipt retry marker also determines the real wire topic for ACK and invisibility requests. Marker `0` uses the
normal topic, marker `1` uses the retry-v1 topic, and marker `2` uses the retry-v2 topic. A receipt returned by the
retry invisibility change preserves that marker; settlement must never reconstruct every request with the logical
normal topic.

### Internal Push ownership layout

The public Push role remains one consumer, but its implementation is composed from per-run collaborators. These types
are internal construction details; they are created by the consumer and are not registered independently in application
DI.

| Location and type | Owns | Does not own |
| --- | --- | --- |
| Root `RemotingPushConsumer` | Public lifecycle serialization, subscriptions, reset-offset request registration, and read-only assignment snapshots. | Route discovery, receive loops, handler scheduling, or settlement. |
| Root `RemotingPushConsumerRun` | One started generation, its cancellation source, collaborator composition, and ordered shutdown. | Public API state or reusable singleton services. |
| `Assignment/PushAssignmentCoordinator` | Atomic subscription/version snapshots inside the coordination gate, serialized route refresh, known-Broker heartbeats, client or Broker assignment, queue-lock renewal, and PULL/POP receive targets carrying the snapshot's filters. | Message processing or protocol settlement. |
| `Assignment/PushReceiverRegistry` and `PushReceiverFactory` | The active mode-aware receiver set, receiver construction, target-filter preservation, and stop-drain-dispose transitions. | Queue-allocation decisions. |
| `Processing/PushMessageHandlerInvoker` | The only application-handler entry point, the consumer-wide `MaxConcurrency` gate, consume context/result mapping, timeout handling, process telemetry, and rejection of late results. | PULL offset/send-back or POP receipt settlement. |
| `Pull/Dispatch/PullDeliveryCoordinator` | Consumer-wide PULL cache admission, ready-queue rotation, consume batching, fixed concurrent consume loops, `MessageGroup` predecessor chains, and queue-drop cleanup. | Queue-local message state, handler outcomes, or protocol I/O. |
| `Pull/Dispatch/PullProcessQueue` | One physical PULL queue's cached messages, pull/commit positions, contiguous completion watermark, settlement retry state, and FIFO barriers under one synchronization boundary. | Consumer-wide scheduling or wire I/O. |
| `Pull/Receive/ConcurrentPullReceiveLoop` | One concurrent PULL assignment's long polling with its target filter, cache admission, and serialized offset-persistence loop. | Application-handler results or send-back. |
| `Pull/Dispatch/PullConsumeRequestProcessor` | Concurrent handler-result mapping, queue-local completion, and activation of stored local settlement retries. | Receive polling, retry-topic preparation, send-back execution, or orderly Broker-lock handling. |
| `Pull/Settlement/PullSendBackSettlement` | Retry-topic preparation, send-back execution, and retry-queue offset initialization. | Local settlement retry state or scheduling, receive polling, handler-result mapping, or queue-local completion. |
| `Pull/Receive/OrderlyPullReceiveLoop` | Broker-lock validation, target-filtered long polling, serial handler retry, send-back, and inline offset persistence for orderly consumption. | Concurrent `PullProcessQueue` dispatch. |
| `Pull/Offset/PullOffsetManager` | PULL initial and committed offsets, broadcasting storage, reset boundaries, and retry-topic initialization boundaries. | POP progress. |
| `Pull/Receive/PullAssignmentReceiver` | One PULL assignment's identity, cancellation, observed Broker-lock lease, and completion boundary for its receive loop plus active consume requests. | POP wire operations or handler-result policy. |
| `Pop/PopAssignmentReceiver` | One POP assignment's target-filtered long polling, receive batching, cancellation, and receive-failure isolation. | Handler invocation, receipt lifetime, or settlement. |
| `Pop/PopDeliveryProcessor` | POP handler invocation, fixed invisible-deadline checks, one-shot retry invisibility, deadline-bounded ACK/dead-letter settlement, and dead-letter ordering. | Assignment reconciliation, receive polling, `PullProcessQueue`, or offset commits. |
| `Pull/Orderly/OrderlyPullQueueLockClient` and `Pop/PopWireClient` | Their respective lock/unlock and POP wire commands. The queue-lock client accepts only physical PULL lock targets. | Assignment policy or application-handler invocation. |

The control and delivery flows are deliberately different:

```text
RemotingPushConsumer
  -> RemotingPushConsumerRun
     -> PushAssignmentCoordinator
        -> immutable subscription/version snapshot
        -> heartbeat and filter-carrying PULL/POP receive targets
        -> PushReceiverRegistry -> PULL or POP receiver

PULL receiver -> ConcurrentPullReceiveLoop -> PullDeliveryCoordinator -> PullProcessQueue
                                                        \-> ready-queue dispatch
                                                            -> PullConsumeRequestProcessor
                                                               -> PushMessageHandlerInvoker
                                                               -> PULL send-back
                                                        \-> offset commit

orderly PULL receiver -> OrderlyPullReceiveLoop -> PushMessageHandlerInvoker
                                               \-> PULL send-back / offset commit

POP receiver  -> PopDeliveryProcessor -> receipt and lease state
                                    \-> PushMessageHandlerInvoker
                                    \-> POP ACK / invisible-time change / dead letter
```

The following ownership rules are part of the behavior, not merely file organization:

- Subscription changes, reset-offset commands, periodic rebalance, and assignment wakeups mutate receivers through one
  coordination gate. The coordinator atomically snapshots subscriptions and their version there, then uses that same
  snapshot for heartbeats and filter-carrying PULL/POP receive targets; receive loops never mutate the assignment set
  directly.
- A receive target carries its topic filter. Replacing a target creates a new receiver generation; an in-flight old
  request may complete with the old filter, but no later request uses it. Removing a subscription removes its target and
  never falls back to `FilterExpression.All` because a topic/filter entry is missing.
- Receive mode is part of assignment identity. A PULL-to-POP or POP-to-PULL transition first marks the old state as
  dropped, stops it, waits for its receive loop and active consume requests to leave receiver-owned protocol work, and
  releases its lock or receipt ownership before starting the new receiver. An application handler that ignores
  cancellation may still unwind later, but its result is rejected at the dropped `PullProcessQueue` boundary and cannot
  start retry-offset initialization, send-back, or offset advancement.
- A concurrent PULL handler outcome crosses into settlement only through an atomic queue-local acceptance operation.
  A zero cache reservation is a valid accepted result and is distinct from rejection because the queue was dropped or
  the entry no longer belongs to it.
- PULL cache count and byte reservations are atomic across assignments. Admission is work-conserving: a blocked batch
  does not monopolize the waiter path when a later batch fits. Bypass is bounded so a large or temporarily blocked
  batch eventually becomes the admission barrier and cannot be starved by a continuous stream of smaller batches.
- Every PULL, POP, concurrent, orderly, and mixed-mode delivery enters `PushMessageHandlerInvoker`. Consequently,
  `MaxConcurrency` limits the whole Push consumer rather than creating one independent limit per receive engine.
- Handler results return to the receiver-specific processor. PULL alone may advance offsets and send back; POP alone may
  ACK or change a receipt's invisibility. Neither path converts its state into the other's representation.
- Shutdown stops new reconciliation before it cancels receivers, drains dispatch, releases Broker locks, unregisters
  membership, flushes broadcasting offsets, and stops the consumer engine. If caller cancellation detaches a
  non-cooperative handler, its late result is ignored and run-owned resources are disposed only after observation ends.
- Refactoring must preserve the existing OpenTelemetry topology: PULL/POP wire clients own receive operations,
  `PushMessageHandlerInvoker` owns process operations, and the PULL/POP settlement owners complete their own settlement
  operations exactly once. See [OpenTelemetry instrumentation](../architecture/opentelemetry-instrumentation.md).

This follows the official Java 5.5.0 separation rather than copying its classes wholesale. Java
[`RebalanceImpl`](https://github.com/apache/rocketmq/blob/rocketmq-all-5.5.0/client/src/main/java/org/apache/rocketmq/client/impl/consumer/RebalanceImpl.java)
maintains distinct PULL `PullProcessQueue` and Java POP `PopProcessQueue` tables, while
[`ConsumeMessageConcurrentlyService`](https://github.com/apache/rocketmq/blob/rocketmq-all-5.5.0/client/src/main/java/org/apache/rocketmq/client/impl/consumer/ConsumeMessageConcurrentlyService.java)
and
[`ConsumeMessagePopConcurrentlyService`](https://github.com/apache/rocketmq/blob/rocketmq-all-5.5.0/client/src/main/java/org/apache/rocketmq/client/impl/consumer/ConsumeMessagePopConcurrentlyService.java)
keep PULL and POP settlement separate. The classic
[Go Push consumer](https://github.com/apache/rocketmq-client-go/blob/master/consumer/push_consumer.go) likewise describes
Push as a callback facade over PULL and keeps queue/offset state below that facade; it is a PULL lifecycle reference,
not a source for POP semantics.

### Handler outcomes and POP fixed deadlines

The existing Push handler contract remains common to both receiver types:

- `Success` settles the acknowledged prefix selected by `AckIndex`.
- `Retry` sends the PULL tail back or makes one `CHANGE_MESSAGE_INVISIBLETIME` request with `suspend=false` for the POP
  tail, using `DelayLevelWhenNextConsume`.
- `DeadLetter`, a negative delay level, or the delivery-attempt limit forwards a PULL message directly. For POP, the
  client first performs classic dead-letter send-back and ACKs the receipt only after forwarding succeeds. ACK, including
  the ACK after dead-letter forwarding, is retried only while the original invisible deadline remains valid; after
  expiry no further settlement is sent.

`DelayLevelWhenNextConsume` defaults to `0`, matching Java's
[`ConsumeConcurrentlyContext`](https://github.com/apache/rocketmq/blob/rocketmq-all-5.5.0/client/src/main/java/org/apache/rocketmq/client/consumer/listener/ConsumeConcurrentlyContext.java).
PULL passes the level to classic send-back. POP maps a positive value through Java's POP-specific retry schedule;
`0` selects that schedule by the zero-based reconsume count (`DeliveryAttempt - 1`). This schedule starts at 10 seconds
and is distinct from the normal delayed-message level table. A negative value selects dead-letter forwarding instead
of changing invisibility.

POP does not renew a receipt while its handler is active. The client checks the fixed invisible deadline before handler
processing and again before settlement. If the deadline has passed, the late handler result is ignored and the Broker
may redeliver without a settlement operation. A `Retry` outcome makes exactly one `CHANGE_MESSAGE_INVISIBLETIME` request
with `suspend=false`; an indeterminate failure is not retried with the old receipt. This follows the official Java
result handling and guard: it consumes the context delay level in
[`processConsumeResult`](https://github.com/apache/rocketmq/blob/rocketmq-all-5.5.0/client/src/main/java/org/apache/rocketmq/client/impl/consumer/ConsumeMessagePopConcurrentlyService.java#L266-L299),
and its POP consume request checks
[`isPopTimeout`](https://github.com/apache/rocketmq/blob/rocketmq-all-5.5.0/client/src/main/java/org/apache/rocketmq/client/impl/consumer/ConsumeMessagePopConcurrentlyService.java#L335-L360)
before handling and checks it again before `processConsumeResult`, skipping settlement after expiry.

This fixed-deadline rule is specific to classic Remoting Push. It must not be inferred from gRPC Push, whose receive
contract can request Proxy-managed auto-renewal through `AutoRenew=true`. Remoting has no Proxy-owned client channel and
receipt registry, and `CHANGE_MESSAGE_INVISIBLETIME` after Retry is not equivalent to handler-active gRPC renewal.

### gRPC consumer review

The gRPC roles remain unchanged:

- `IGrpcSimpleConsumer` is the supported caller-driven receipt model.
- `IGrpcPushConsumer` owns ordinary automatic dispatch.
- `IGrpcLitePushConsumer` owns LiteTopic subscription synchronization plus automatic dispatch.
- No public gRPC PullConsumer is added because the supported Proxy path does not implement the complete classic
  queue-offset workflow.

These are protocol-specific conclusions; they do not introduce shared consumer interfaces or cross-project models.

## Composition and Ownership

Only LitePull and Push register active Remoting consumer roles. Each role owns a group-member client, route resolver,
consumer engine, per-start run, group session, rebalance participant, receive state, and hosted lifecycle. Repeated
same-group members remain channel-isolated so Broker membership does not overwrite another local member.

The Push and LitePull orchestrators delegate assignment, reception, dispatch or buffering, positions, and settlement to
their focused run-owned collaborators. Route, PULL, offset, and send-back components are composed with the role's
engine at the Remoting composition root; none is registered as an independently resolvable application service. The
internal consumer protocol operations are not exposed through a low-level public facade.

The solution, unit tests, integration tests, benchmarks, Generic Host and non-Host samples, protocol guides, root
READMEs, and test-environment guides expose only the accepted API. The removed Pull and POP sample projects are
deleted rather than retained as historical examples. The Push sample demonstrates both assignment modes without
making its default workflow depend on Broker-side POP configuration.

## Test Contract

Unit coverage must establish:

- assignment request JSON and `null` versus empty response handling;
- PULL/POP additions, removals, and mode transitions;
- old receiver drain before a receive-mode replacement becomes active;
- one consumer-wide handler concurrency limit across mixed PULL and POP assignments;
- reset-offset serialization, late-handler result rejection, and deterministic shutdown ownership;
- queue ID `-1`, retry receipt markers, and physical ACK targets;
- POP in-flight flow control, fixed invisible deadlines with no handler-active renewal, pre/post-handler expiry checks,
  one-shot Retry invisibility, ACK/dead-letter settlement retries only before the original deadline, dead-letter ordering,
  and stale settlement;
- unchanged receive, process, commit, send-back, ACK, and invisible-time telemetry outcomes after responsibility moves;
- public, Admin-returned, and LitePull-discovered queue identities resolving and heartbeating identically;
- route replacement and route-refresh failure using resolver-owned latest or last-valid endpoints without queue-carried
  snapshots, including suggested Broker selection and same-Broker fallback from an application topic to its retry topic;
- rejection of locally registered same-group Push members with different `QueueAssignmentMode` values;
- Broker-assigned POP `queueId=-1` remaining an internal assignment and never becoming a public Consumer queue;
- LitePull background receives without head-of-line blocking, aggregate multi-queue count-and-byte cache admission,
  delivered-position commits, seek, pause/resume, isolated heartbeat failures, seek-versus-stop admission, and
  shutdown committing the final delivered position only when auto commit is enabled.

Remoting integration coverage configures the Broker request mode through test administration, verifies Push POP
delivery and ACK against a real Broker, exercises POP invisibility retry and Broker-assigned PULL send-back redelivery,
retains normal PULL Push coverage, and assigns LitePull to multiple physical queues while proving that an empty queue's
long poll cannot delay a message explicitly routed to another queue. PULL and POP scenarios use independently created
topics and consumer groups so request-mode state, offsets, retry topics, and test cleanup cannot interfere. The Broker
key is topic plus consumer group, so distinct groups are technically sufficient; separate topics are the stricter
deterministic test boundary. Integration tests must not expose
`SET_MESSAGE_REQUEST_MODE` as a production consumer API.

## Trade-offs and Non-goals

- Removing direct Pull and POP deliberately trades low-level convenience for two coherent application models.
- Broker assignment requires a RocketMQ Broker that supports `QUERY_ASSIGNMENT`; client assignment remains the
  default, matching the official Java Remoting client.
- Broker POP request mode is an operator-controlled server setting.
- The first Broker POP path excludes broadcasting, orderly consumption, batch ACK, and protocol-level POP orderly
  semantics.
- Background LitePull reception consumes more concurrent Broker requests than the current single-poll implementation,
  but removes cross-queue head-of-line blocking and matches the official consumer model.
- Moving LitePull into the peer `Consumer.LitePull` namespace is an intentional namespace break. It makes the public role
  a peer of Push and leaves `Consumer.Pull` as an internal wire concern; no compatibility forwarding types remain.
- Atomic LitePull buffer admission is stricter than Java's post-receive threshold accounting. A returned batch may wait
  behind the pollable cache, but an empty long poll never consumes another queue's cache capacity.
- This change adds no shared production project, transport-neutral Consumer API, or gRPC/Remoting abstraction.

## Related Reading

- [Classic Remoting transport and client roles](transport-and-client-roles.md)
- [gRPC consumer model](../grpc/consumer-model.md)
- [Dependency-injection registrations and lifetimes](../architecture/dependency-injection-and-lifetimes.md)
- [Local and integration testing](../testing/local-and-integration-testing.md)
- [Official SDK overview: Remoting versus gRPC](https://rocketmq.apache.org/docs/sdk/01overview/)
- [Official classic PushConsumer guide](https://rocketmq.apache.org/docs/4.x/consumer/02push/)
- [Java Remoting `DefaultMQPushConsumer`](https://github.com/apache/rocketmq/blob/rocketmq-all-5.5.0/client/src/main/java/org/apache/rocketmq/client/consumer/DefaultMQPushConsumer.java)
- [Java Push subscription construction](https://github.com/apache/rocketmq/blob/rocketmq-all-5.5.0/client/src/main/java/org/apache/rocketmq/client/impl/consumer/DefaultMQPushConsumerImpl.java)
- [Java Push effective rebalance selection](https://github.com/apache/rocketmq/blob/rocketmq-all-5.5.0/client/src/main/java/org/apache/rocketmq/client/impl/consumer/RebalancePushImpl.java)
- [Java Broker-assigned Push reconciliation](https://github.com/apache/rocketmq/blob/rocketmq-all-5.5.0/client/src/main/java/org/apache/rocketmq/client/impl/consumer/RebalanceImpl.java)
- [Java `DefaultMQPullConsumer` deprecation](https://github.com/apache/rocketmq/blob/rocketmq-all-5.5.0/client/src/main/java/org/apache/rocketmq/client/consumer/DefaultMQPullConsumer.java)
- [Java LitePull receive tasks](https://github.com/apache/rocketmq/blob/rocketmq-all-5.5.0/client/src/main/java/org/apache/rocketmq/client/impl/consumer/DefaultLitePullConsumerImpl.java)
- [Broker assignment and request-mode processor](https://github.com/apache/rocketmq/blob/rocketmq-all-5.5.0/broker/src/main/java/org/apache/rocketmq/broker/processor/QueryAssignmentProcessor.java)
- [Broker request-mode defaults](https://github.com/apache/rocketmq/blob/rocketmq-all-5.5.0/common/src/main/java/org/apache/rocketmq/common/BrokerConfig.java)
