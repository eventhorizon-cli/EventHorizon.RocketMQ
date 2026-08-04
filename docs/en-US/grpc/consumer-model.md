# gRPC Consumer Model

[English](consumer-model.md) | [Simplified Chinese](../../zh-CN/grpc/consumer-model.md)

## Context

The RocketMQ 5 client protocol is protobuf/gRPC, but the client does not connect directly to a
NameServer or Broker. It connects to a RocketMQ Proxy, which mediates route queries, receive calls,
acknowledgements, telemetry, and other protocol operations. Consequently, a client API is only
usable when the target Proxy implements the corresponding RPC behavior.

The important distinction is between the application model and the TCP direction. A type named
"PushConsumer" describes automatic application dispatch. It does not mean the Broker establishes a
server-initiated push connection to the application.

## Decision

The gRPC project exposes the consumer models that the supported Proxy path can perform:

| API | Application controls | Receive model |
| --- | --- | --- |
| `IGrpcSimpleConsumer` | Receive timing, acknowledgement, invisible duration, and subscriptions | The caller invokes `ReceiveAsync`. |
| `IGrpcPushConsumer` | Subscription and handler configuration; the client manages dispatch and completion | Assignment queries plus repeated `ReceiveMessage` long polls. |
| `IGrpcLitePushConsumer` | LiteTopic subscriptions under a configured bind topic | The same client-initiated long-poll dispatcher, with Lite subscription synchronization. |

There is deliberately no public `IGrpcPullConsumer` in this repository. The checked-in service
definition declares `PullMessage`, `GetOffset`, `UpdateOffset`, and `QueryOffset`, but the internal
[gRPC client abstraction](../../../src/EventHorizon.RocketMQ.Grpc/Protocol/IRocketMQGrpcClient.cs)
does not call them and the project exposes no API around them. The Apache Proxy used by the supplied
RocketMQ 5.5.0 environment does not provide the complete Pull and offset behavior needed by a
classic Pull consumer.

That distinction is intentional: a protobuf declaration is a contract shape, not proof that a
target Proxy implements it. The declarations remain in the checked-in
[service proto](../../../src/EventHorizon.RocketMQ.Grpc/Protocol/Protos/apache/rocketmq/v2/service.proto)
because they are part of the upstream wire definition. They do not make gRPC Pull a supported
client feature.

## How It Works

### Connection, metadata, and routes

`GrpcClientOptions.Endpoint` is a Proxy endpoint list. The internal
[`RocketMQGrpcClient`](../../../src/EventHorizon.RocketMQ.Grpc/Protocol/RocketMQGrpcClient.cs)
opens gRPC channels for Proxy endpoints and applies metadata and request deadlines. The
[`GrpcRouteService`](../../../src/EventHorizon.RocketMQ.Grpc/Protocol/Route/GrpcRouteService.cs)
queries routes through that client and caches them for the configured route-cache duration.

The result is a Proxy-backed flow:

```text
Application -> gRPC role -> RocketMQ Proxy -> route / receive / completion operations -> Broker
```

The application must configure Proxy-reachable `Endpoint` values. A NameServer address is not a
gRPC endpoint and is not a substitute for a Proxy.

### SimpleConsumer

SimpleConsumer is the explicit-control model. After it starts and has at least one subscription,
`ReceiveAsync` selects a subscription and assignment, issues a long-poll receive request, and gives
the returned `GrpcMessageView` values to the application. The application decides when to call
`AckAsync` and whether to change invisibility. Leaving a message unsettled allows it to become visible again after its
current invisible duration.

This is often the right model when application code needs to control batching, commit timing, or
retry boundaries. It is not classic queue Pull: it uses the gRPC receive and acknowledgement
semantics provided by the Proxy.

### PushConsumer

PushConsumer automates the same family of operations:

```text
subscriptions
    -> assignment query
    -> ReceiveMessage long poll
    -> bounded client dispatch queue
    -> concurrent or FIFO message handler
    -> acknowledgement or failure handling
```

The implementation in
[`GrpcPushConsumer`](../../../src/EventHorizon.RocketMQ.Grpc/Consumer/Push/GrpcPushConsumer.cs)
starts the receive engine, coordinates assignment loops, applies bounded cached-message limits, and
dispatches work according to the configured concurrency and FIFO settings. It is client-initiated
long polling from end to end. Calling it protocol-level Broker push would give the wrong operational
model.

Ordinary Push and LitePush receive requests set `AutoRenew=true`. A compatible Proxy with `enableProxyAutoRenew`
enabled owns receipt renewal through the client connection while a handler is active; the .NET dispatcher does not
run a second client-side renewal timer. SimpleConsumer sets `AutoRenew=false`, so its caller owns the initial invisible
duration and every explicit `ChangeInvisibleDurationAsync` call. This follows the
[Apache Java gRPC client](https://github.com/apache/rocketmq-clients/blob/9fe1449d19449b41442aa3a97ab168ed6b5bd6b1/java/client/src/main/java/org/apache/rocketmq/client/java/impl/consumer/ConsumerImpl.java#L263-L278),
while the [Proxy registers auto-renew receipts](https://github.com/apache/rocketmq/blob/2238256de1d227a4384ba969dfa31473187b4d08/proxy/src/main/java/org/apache/rocketmq/proxy/grpc/v2/consumer/ReceiveMessageActivity.java#L100-L140)
only when both its configuration and the request enable the feature. Classic Remoting Push remains a separate model
with a fixed POP deadline.

For non-FIFO dispatch, `ConsumeTimeout` defaults to 15 minutes. When it expires, the dispatcher cancels the handler
token and requests retry; a late successful result is ignored. This recovers
consume-loop capacity even when application code ignores cancellation, but the client cannot forcibly terminate that code.
Retried delivery can overlap an invocation that ignored cancellation, so handlers must be idempotent. FIFO message
groups are intentionally excluded because detaching a timed-out handler could break their ordering.

#### Multiple Push instances and handler ownership

One `AddRocketMQGrpc` registration may add multiple Simple, Push, or LitePush Consumer instances. Every instance owns
its options, logical client identity, receive engine, subscriptions, typed handler binding, and hosted lifecycle. The
instances share HTTP/2 channels through the registration's `GrpcChannelPool`; sharing a channel does not merge their
consumer groups or runtime state.

Ordinary Push instances in the same consumer group must declare identical topic and filter subscriptions. Instances
in different groups may consume the same or different topics independently. LitePush members in one group must use
the same bind topic, while their LiteTopic sets may differ. Registration validates combinations visible in the same
process, and the Proxy remains authoritative for remote members and server-side group configuration.

When an ordinary Push group has another local member, `SubscribeAsync` and `UnsubscribeAsync` reject individual
subscription changes. Update every member to the same subscription set and restart them together. This does not
block LitePush LiteTopic changes, which have separate group semantics under their shared bind topic.

Automatic Push dispatch accepts only a typed `IGrpcPushMessageHandler` registered through the generic Consumer
method. There is no delegate handler on the options object. A `Scoped` or `Transient` handler is resolved in a fresh
async DI scope for each handling attempt, allowing normal scoped application dependencies such as database contexts;
a `Singleton` handler is owned by its Consumer instance and must be safe for concurrent calls.

The handler returns only `ConsumeResult.Success` or `ConsumeResult.Failure`, matching the public outcomes in the
[Apache Java gRPC client](https://github.com/apache/rocketmq-clients/blob/9fe1449d19449b41442aa3a97ab168ed6b5bd6b1/java/client-apis/src/main/java/org/apache/rocketmq/client/apis/consumer/ConsumeResult.java).
For concurrent non-FIFO delivery, `Failure` changes invisibility according to the effective retry policy and leaves
retry and dead-letter progression to the service. For FIFO delivery, the client retries the handler locally and uses
the protocol's dead-letter RPC internally after the maximum attempts are exhausted, matching the
[Java process-queue behavior](https://github.com/apache/rocketmq-clients/blob/9fe1449d19449b41442aa3a97ab168ed6b5bd6b1/java/client/src/main/java/org/apache/rocketmq/client/java/impl/consumer/ProcessQueueImpl.java#L431-L445).
Neither the handler result nor SimpleConsumer exposes that internal RPC. The OpenTelemetry settlement operation for
retry scheduling remains `nack`; that telemetry term is not a RocketMQ handler result.

### LitePushConsumer

LitePush wraps the automatic dispatcher with a
[`GrpcLiteSubscriptionManager`](../../../src/EventHorizon.RocketMQ.Grpc/Consumer/LitePush/GrpcLiteSubscriptionManager.cs).
It receives LiteTopic messages under one configured bind topic and synchronizes the Lite
subscriptions with the Proxy.

LitePush requires more than a reachable Proxy:

- The Broker must support and enable Lite message behavior.
- The configured consumer group must use the correct bind topic.
- The target Proxy must implement `SyncLiteSubscription`.

The local Compose environment starts a cluster-mode Proxy specifically for this path. Its detailed
requirements are documented in the [LitePush sample](../../../samples/grpc/GenericHost/LitePushConsumer/README.md)
and [local environment guide](../../../test-environments/rocketmq/README.md).

## Trade-offs and Constraints

- gRPC consumer capability depends on Proxy behavior as well as client code. Test against the exact
  Proxy deployment used in production.
- Push reduces application orchestration but does not remove acknowledgement, invisibility, retry,
  concurrency, or FIFO decisions. Those remain operational choices.
- SimpleConsumer exposes explicit control but requires the caller to make completion and failure
  decisions for every delivered message.
- The lack of a public gRPC Pull API is deliberate. Use `IGrpcSimpleConsumer` or
  `IGrpcPushConsumer` when their semantics fit, or use the classic
  [`IRemotingLitePullConsumer`](../../../src/EventHorizon.RocketMQ.Remoting/Consumer/LitePull/IRemotingLitePullConsumer.cs)
  with manual assignment, seek, and explicit commit when a classic queue-and-offset workflow is required.
- gRPC Push and LitePush are not interchangeable with Remoting Push. They use different route,
  assignment, server, and compatibility paths even though both perform long polling internally.

## Related Reading

- [gRPC project guide](../../../src/EventHorizon.RocketMQ.Grpc/README.md)
- [gRPC SimpleConsumer sample](../../../samples/grpc/GenericHost/SimpleConsumer/README.md)
- [gRPC PushConsumer sample](../../../samples/grpc/GenericHost/PushConsumer/README.md)
- [Protocol Boundaries and Type Ownership](../architecture/protocol-boundaries.md)
- [Dependency-injection client registrations and lifetimes](../architecture/dependency-injection-and-lifetimes.md)
