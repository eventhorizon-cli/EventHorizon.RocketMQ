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
| `IRemotingPullConsumer` | Explicit route, queue, pull, offset, and commit control. |
| `IRemotingLitePullConsumer` | Client-managed Lite Pull subscriptions, polling, and offset commits. |
| `IRemotingPopConsumer` | POP receipt acquisition, acknowledgement, and invisibility renewal. |
| `IRemotingPushConsumer` | Automatic long-poll batch dispatch with classic Remoting compatibility and Broker callback handling. |

The role registrations live in
[`RemotingRocketMQBuilderExtensions`](../../../src/EventHorizon.RocketMQ.Remoting/RemotingRocketMQBuilderExtensions.cs).
Push is still driven by classic pull/long-poll behavior internally; it is not identical to the
gRPC Push implementation and should not be treated as a common transport-neutral role.

Remoting Push has one batch-oriented handler API: `IRemotingPushMessageHandler.HandleAsync` receives an
`IReadOnlyList<RemotingMessageView>`. `BatchSize` limits how many messages one long poll requests from the Broker;
`ConsumeMessageBatchSize` independently limits how many messages one handler invocation receives and defaults to
`1`. Concurrent non-`MessageGroup` messages from the same Broker physical queue can be grouped and those batches
can run in parallel up to `MaxConcurrency`. `MessageGroup` FIFO deliveries and `ConsumeOrderly` deliveries remain
singleton and serialized. One `ConsumeResult` applies to every message in its batch.

`ConsumeTimeout` defaults to 15 minutes and applies only to concurrent clustered non-FIFO batches. When it elapses,
the client cancels the handler token and requests Broker redelivery for the whole batch. It cannot forcibly terminate
application code that ignores cancellation; its late result is ignored and can overlap redelivery, so handlers must
cooperate with the token and remain idempotent. FIFO and orderly delivery intentionally remain outside this recovery
path to preserve ordering guarantees.

### Read-only Admin and physical message IDs

`IRemotingAdmin` is intentionally a separate, read-only role. It can discover writable queues,
inspect min/max/timestamp offsets, query a consumer offset, and view a physical message. The API is
documented in
[`IRemotingAdmin`](../../../src/EventHorizon.RocketMQ.Remoting/Admin/IRemotingAdmin.cs), and the
[Admin sample](../../../samples/remoting/Admin/README.md) exposes it through a small Swagger UI.

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
- The client preserves route, queue, offset, receipt, and send-result types in the Remoting
  namespace. They should not be moved into a third generic Project or substituted with gRPC types.
- The Admin role performs read-only operations. It is not a general Broker-administration API and
  does not create topics or mutate consumer offsets.
- ACL credentials, TLS settings, frame-size limits, and advertised Broker addresses are part of
  the Remoting environment, not universal RocketMQ settings.
- A target deployment behind NAT, containers, or load balancers must advertise Broker addresses
  that the actual client can reach. Test this from the same network as the application.

## Related Reading

- [classic Remoting project guide](../../../src/EventHorizon.RocketMQ.Remoting/README.md)
- [Remoting Admin sample](../../../samples/remoting/Admin/README.md)
- [Remoting PullConsumer sample](../../../samples/remoting/PullConsumer/README.md)
- [Protocol Boundaries and Type Ownership](../architecture/protocol-boundaries.md)
- [Local and integration testing](../testing/local-and-integration-testing.md)
