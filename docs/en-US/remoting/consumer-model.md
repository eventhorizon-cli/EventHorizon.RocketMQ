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

Broker endpoints and suggested Broker IDs belong to an internal route resolver keyed by the consumer queue identity.
Broker-assigned POP uses a separate internal assignment type because its logical queue ID may be `-1`.

Every Pull, offset, send-back, POP, ACK, and invisibility request resolves its endpoint from the latest valid route.
The route cache may provide the last known valid route during a transient NameServer failure, but a queue value passed
by application code is never the authoritative endpoint source.

### LitePull receive model

After subscription rebalance or manual assignment, LitePull starts one cancellable receive loop per assigned queue:

```text
subscription or manual assignment
        -> assignment reconciliation
        -> per-queue Broker long polls
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
  remain valid because the logical queue identity has not changed.
- `SeekToBeginningAsync`, `SeekToEndAsync`, and `SeekToTimestampAsync` cover application positioning.
- `GetCommittedOffsetAsync` exposes the persisted group position. Min/max inspection stays on Admin.
- Manual `AssignAsync` accepts queues plus an optional read-only dictionary of per-topic filters. A missing topic
  filter means match all. Subscription mode and manual-assignment mode remain mutually exclusive.
- `EnableAutoCommit` defaults to `true`; `AutoCommitInterval` defaults to five seconds. Automatic and explicit commits
  persist delivered positions only, never fetch positions.
- Route refresh, known-Broker heartbeat, and assignment reconciliation are failure-isolated. A NameServer failure does
  not suppress heartbeats to Brokers that are still reachable.

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
| Retry | `CONSUMER_SEND_MSG_BACK` | `CHANGE_MESSAGE_INVISIBLETIME` |
| Assignment removal | Drop process queue and reject late commits | Drop POP queue and reject late receipt settlement |

POP never enters the offset-based `ProcessQueue`. Its decoder supports queue ID `-1`, retry-v1 and retry-v2 receipt
markers, and uses the physical queue encoded in each receipt for ACK and invisibility changes.

The receipt retry marker also determines the real wire topic for ACK and invisibility requests. Marker `0` uses the
normal topic, marker `1` uses the retry-v1 topic, and marker `2` uses the retry-v2 topic. A renewed receipt preserves
that marker; settlement must never reconstruct every request with the logical normal topic.

### Internal Push ownership layout

The `Consumer/Push` source tree mirrors cohesive internal ownership while keeping the public Push contracts and the
lifecycle facade at its root:

| Folder | Responsibility |
| --- | --- |
| `Assignment` | Broker/client assignment values and receive-mode decisions. |
| `Dispatch` | PULL process queues, consume requests, and concurrent handler scheduling. |
| `Offset` | Broadcasting offset persistence and Broker reset-offset decoding. |
| `Orderly` | Broker queue-lock commands and lock renewal ownership. |
| `Pull` | The PULL receiver adapter. |
| `Pop` | POP wire operations, receipts, decoding, leases, and settlement. |

This layout does not introduce another public abstraction or imply that PULL and POP have interchangeable state.
`RemotingPushConsumer` remains the single lifecycle and reconciliation facade in this revision; extracting its internal
orchestration is a separate change that requires focused characterization tests.

### Handler outcomes and POP leases

The existing Push handler contract remains common to both receiver types:

- `Success` settles the acknowledged prefix selected by `AckIndex`.
- `Retry` sends the PULL tail back or changes the POP tail's invisibility using
  `DelayLevelWhenNextConsume`.
- `DeadLetter`, a negative delay level, or the delivery-attempt limit forwards a PULL message directly. For POP, the
  client first performs classic dead-letter send-back and ACKs the receipt only after forwarding succeeds. If the ACK
  then fails, duplicate dead-letter forwarding remains possible and is covered by the normal at-least-once contract.

While a POP handler is active, the client renews its receipt before the current invisible deadline. Renewal and final
settlement are serialized because every successful renewal replaces the receipt required by later operations. If the
lease can no longer be renewed, the late handler result is ignored and the Broker is allowed to redeliver. Settlement
failures are retried only while the current receipt can still be valid; after its deadline, local in-flight capacity is
released and Broker redelivery is authoritative.

Handler-active renewal sends `CHANGE_MESSAGE_INVISIBLETIME` with `suspend=true` so an eventual lease expiry does not
count the renewal itself as another failed delivery attempt. A real `Retry` settlement sends `suspend=false` and lets
the Broker advance the retry attempt.

### gRPC consumer review

The gRPC roles remain unchanged:

- `IGrpcSimpleConsumer` is the supported caller-driven receipt model.
- `IGrpcPushConsumer` owns ordinary automatic dispatch.
- `IGrpcLitePushConsumer` owns LiteTopic subscription synchronization plus automatic dispatch.
- No public gRPC PullConsumer is added because the supported Proxy path does not implement the complete classic
  queue-offset workflow.

These are protocol-specific conclusions; they do not introduce shared consumer interfaces or cross-project models.

## Composition and Ownership

Only LitePull and Push register active Remoting consumer roles. Each role still owns a group-member client,
`RemotingConsumerGroupSession`, rebalance participant, receive state, and hosted lifecycle. Repeated same-group members
remain channel-isolated so Broker membership does not overwrite another local member.

The Push orchestrator delegates Broker assignment, PULL reception, POP reception, handler dispatch, and settlement to
focused internal collaborators. The internal consumer protocol operations are not registered as application services
and are not exposed through a low-level public facade.

The solution, unit tests, integration tests, benchmarks, Generic Host and non-Host samples, protocol guides, root
READMEs, and test-environment guides expose only the accepted API. The removed Pull and POP sample projects are
deleted rather than retained as historical examples. The Push sample demonstrates both assignment modes without
making its default workflow depend on Broker-side POP configuration.

## Test Contract

Unit coverage must establish:

- assignment request JSON and `null` versus empty response handling;
- PULL/POP additions, removals, and mode transitions;
- queue ID `-1`, retry receipt markers, and physical ACK targets;
- POP in-flight flow control, renewal replacement, partial ACK, retry, dead-letter ordering, and stale settlement;
- route replacement without stale endpoint reuse;
- LitePull background receives without head-of-line blocking, bounded buffering, delivered-position commits, seek,
  pause/resume, and isolated heartbeat failures.

Remoting integration coverage configures the Broker request mode through test administration, verifies Push POP
delivery and ACK against a real Broker, exercises POP invisibility retry and Broker-assigned PULL send-back redelivery,
and retains normal PULL Push coverage. PULL and POP scenarios use independently created topics and consumer groups so
request-mode state, offsets, retry topics, and test cleanup cannot interfere. The Broker key is topic plus consumer
group, so distinct groups are technically sufficient; separate topics are the stricter deterministic test boundary.
Integration tests must not expose
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
