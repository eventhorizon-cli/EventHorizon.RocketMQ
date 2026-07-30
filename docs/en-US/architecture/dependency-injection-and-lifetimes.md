# Dependency-Injection Client Registrations and Lifetimes

[English](dependency-injection-and-lifetimes.md) | [Simplified Chinese](../../zh-CN/architecture/dependency-injection-and-lifetimes.md)

## Context

A RocketMQ role is not a lightweight request object. Producers can own route and telemetry state;
consumers own subscriptions, background receive work, acknowledgement state, and shutdown logic.
An application can also need more than one independent RocketMQ configuration, for example a
default client registration and an audit keyed client registration.

The client must compose those resources through the application's existing `IServiceCollection`
without constructing a hidden `ServiceProvider`, while still allowing a push handler to depend on
scoped application services such as a database context.

## Decision

Each protocol adds an explicit client registration:

```text
IServiceCollection
  |
  +-- AddRocketMQGrpc(...) or AddRocketMQRemoting(...)
        |
        +-- client registration options
        +-- one Producer (and one Remoting Admin) at most
        +-- one or more protocol-specific Consumer instances
        +-- protocol transport infrastructure shared where safe
        +-- hosted start/stop integration
```

The default overload adds a default client registration and exposes unkeyed services. The keyed overload adds a
keyed client registration. Its registration name is also used as the .NET keyed-service key when resolving its roles.
gRPC starts at
[`AddRocketMQGrpc`](../../../src/EventHorizon.RocketMQ.Grpc/GrpcRocketMQServiceCollectionExtensions.cs);
Remoting starts at
[`AddRocketMQRemoting`](../../../src/EventHorizon.RocketMQ.Remoting/RemotingRocketMQServiceCollectionExtensions.cs).

Each client registration exposes only its protocol-specific roles. For example, a gRPC client registration can add
`IGrpcProducer`, `IGrpcSimpleConsumer`, `IGrpcPushConsumer`, or `IGrpcLitePushConsumer`; a
Remoting client registration can add `IRemotingProducer`, `IRemotingAdmin`, Pull, LitePull, POP, or Push roles.
There is no common `IProducer` or `IConsumer` registration.

## How It Works

### Client registrations and roles

Each client registration has its own named options configuration. The registration code validates the connection
settings and then derives a logical client name for every role instance. A Producer can be registered only once per
client registration; the same is true for Remoting Admin. Consumer extension methods can be called repeatedly on the
same builder without changing their signatures. Every call creates a separate Consumer options name, logical client
identity, engine, handler binding, and hosted lifecycle.

The role registration helpers are transport-local:

- [gRPC role registration](../../../src/EventHorizon.RocketMQ.Grpc/GrpcRocketMQRegistration.cs)
- [Remoting role registration](../../../src/EventHorizon.RocketMQ.Remoting/RemotingRocketMQRegistration.cs)

Default registrations expose all instances of a Consumer interface through `IEnumerable<TConsumer>`. Keyed client
registrations expose them through `GetKeyedServices<TConsumer>(registrationName)`. Normal singular resolution follows
Microsoft DI ordering and returns the last registered Consumer. Code that registers multiple Consumers should therefore
request the collection unless the last-instance behavior is intentional.

The logical Consumer boundary does not require a separate physical transport for every instance. Within one client
registration, gRPC Consumers share HTTP/2 channels while retaining distinct logical client IDs and request metadata.
Remoting roles share NameServer discovery, route state, and Broker connections where classic protocol identity permits
it. Distinct Remoting consumer groups can share connections, but repeated active group members (Push or LitePull) in
the same group use separate Broker connections because classic Broker consumer membership is identified by the
connection channel.

Each physical Remoting client owns one internal `RemotingRebalanceService`. Distinct groups that share that client also
share its single `NotifyConsumerIdsChanged` request handler and periodic fallback, while each group has an independent,
single-flight participant so a blocked or failing group cannot delay another. The Broker callback only coalesces a
wakeup signal; route refresh, membership queries, and queue reconciliation run outside the inbound request loop. A
repeated member of the same group uses its isolated physical client and therefore a separate Rebalance service.

Push and LitePull keep queue-allocation decisions in their own Consumer implementations. Their shared classic group
protocol operations are owned by `RemotingConsumerGroupSession`; orderly Push queue-lock wire operations are owned by
`RemotingPushQueueLockManager`. This keeps transport I/O, Rebalance triggering, and Consumer state reconciliation as
separate responsibilities.

### Consumer-group combinations

For ordinary Push Consumers, every instance in the same consumer group must declare the same topic set and the same
filter expression for each topic. Registration rejects a same-group mismatch before the host starts. The supported
combinations are:

| Registration combination | Behavior |
| --- | --- |
| Same group and identical subscriptions | Valid clustered replicas; queue assignments are load-balanced across the instances. |
| Different groups and the same topic | Fan-out; each group receives its own copy of the topic stream. |
| Different groups and different topics | Independent consumption. |
| Same group and different topics or filters | Invalid for ordinary Push; use consistent subscriptions or different groups. |

Classic Remoting also requires same-group Push instances to use the same clustering/broadcasting mode, orderly mode,
and initial position, and same-group LitePull instances to use the same initial offset policy. Push and LitePull cannot
share a group because they advertise different classic Broker consumption types. Instance-local settings such as
concurrency, timeouts, and handlers may still differ. These are group-wide configuration rules, but registration can
only fail fast for members declared in the same process and client registration. Deployments must keep members in
other processes consistent; the classic Broker heartbeat does not carry every client-side setting, including orderly
mode.

This ordinary-topic consistency rule does not apply to gRPC LitePush LiteTopic sets. Members of one Lite consumer group
may own different LiteTopic sets, but they must use the same bind topic. The Proxy remains authoritative for that group
and bind-topic configuration.

An ordinary gRPC Push member cannot change group-wide subscriptions with `SubscribeAsync` or `UnsubscribeAsync` while
another member of that group exists in the same client registration. The same local guard applies to Remoting Push and
LitePull. Configure the new identical subscription set and restart all local members together; members in other
processes remain a deployment-coordination responsibility. gRPC LitePush LiteTopic changes are separate from this
ordinary-topic rule.

The existing method signatures configure each instance independently:

```csharp
var rocketMQ = services.AddRocketMQGrpc(options => options.Endpoint = "proxy:8081");

rocketMQ.AddGrpcPushConsumer<OrderHandler>(ServiceLifetime.Scoped, options =>
{
    options.GroupName = "orders-consumer";
    options.Subscribe("orders");
});

rocketMQ.AddGrpcPushConsumer<AuditHandler>(ServiceLifetime.Scoped, options =>
{
    options.GroupName = "audit-consumer";
    options.Subscribe("audit");
});
```

### Host lifecycle

Public roles are registered as singletons. Their companion hosted services start and stop the role
with the .NET Generic Host, so applications do not need to build a provider or manually coordinate
background tasks. The role extension methods are the composition root:

- [gRPC role registrations](../../../src/EventHorizon.RocketMQ.Grpc/GrpcRocketMQBuilderExtensions.cs)
- [Remoting role registrations](../../../src/EventHorizon.RocketMQ.Remoting/RemotingRocketMQBuilderExtensions.cs)

This applies to Producers as well as Consumers. It lets the producer initialize its protocol state
and ensures orderly teardown when the host stops.

### Consumer engines

The reusable receive machinery is internal and protocol-owned. A gRPC Push or Simple role receives
one `IGrpcReceiveConsumerEngine`; a classic consumer role receives one
`IRemotingConsumerEngine`. The builder constructs that engine at the protocol composition root and
injects it into the consumer implementation. It is not a global DI service, and the consumer does
not instantiate it itself.

The relevant implementations are
[`GrpcReceiveConsumerEngine`](../../../src/EventHorizon.RocketMQ.Grpc/Consumer/GrpcReceiveConsumerEngine.cs)
and
[`RemotingConsumerEngine`](../../../src/EventHorizon.RocketMQ.Remoting/Consumer/RemotingConsumerEngine.cs).
This gives each consumer instance one coherent lifecycle for its receive loops, subscriptions, and
shutdown work.

### Push-handler lifetimes

The generic Push registration overloads accept a handler type and a `ServiceLifetime`. The factory
registration is keyed to the internal role-instance key, so handlers cannot cross Consumer-instance or
client-registration boundaries.

| Handler lifetime | Resolution behavior |
| --- | --- |
| `Singleton` | The handler is resolved from the root provider and reused. It must be safe for concurrent delivery. |
| `Scoped` | A new async scope is created for each handler invocation, then disposed after handling. |
| `Transient` | A new async scope is created for each handler invocation; the transient handler and its scoped dependencies are disposed with that scope. |

The concrete implementation is visible in the
[gRPC handler factory](../../../src/EventHorizon.RocketMQ.Grpc/Consumer/Push/GrpcPushMessageHandlerFactory.cs)
and [Remoting handler factory](../../../src/EventHorizon.RocketMQ.Remoting/Consumer/Push/RemotingPushMessageHandlerFactory.cs).
Automatic dispatch for Push and LitePush requires a typed handler resolved from the application container. Choose
`Scoped` when the handler depends on scoped application services such as a database context. gRPC Push and LitePush
invoke their `IGrpcPushMessageHandler` for each message. Remoting Push invokes its one
`IRemotingPushMessageHandler` API with an
`IReadOnlyList<RemotingMessageView>` and a `RemotingPushConsumeContext` for each batch; its
`ConsumeMessageBatchSize` defaults to `1`, so existing settings retain singleton delivery unless configured
otherwise. For a concurrent non-FIFO batch, a handler can return `Success` with `AckIndex` set to confirm a
contiguous prefix and retry its tail; `Retry` and `DeadLetter` remain whole-batch outcomes.

For non-FIFO gRPC Push and LitePush messages, `ConsumeTimeout` cancels the handler token, stops client-side
invisibility renewal, and requests retry when the configured limit elapses. The dispatcher ignores a late result and
releases its consume-loop slot, but it cannot terminate application code. A timed-out invocation can overlap redelivery and must
be idempotent. Its typed-handler async scope remains alive until that invocation returns, so scoped dependencies are
not disposed while handler code is still running. FIFO message groups are excluded to preserve ordering.

For concurrent clustered non-FIFO Remoting batches, `ConsumeTimeout` cancels the handler token and requests Broker
redelivery when the configured limit elapses. A late result is ignored and can overlap redelivery, so the handler still
owns cancellation cooperation and must be idempotent; the runtime cannot forcibly stop application code. FIFO and
orderly delivery remain excluded to preserve ordering guarantees.

## Trade-offs and Constraints

- A client registration may add multiple Consumer instances, but at most one Producer and, for Remoting, one Admin.
  Add another keyed client registration when the application needs independent connection settings or another
  Producer/Admin instance.
- A singleton handler must not capture a scoped dependency. Choose `Scoped` when a message operation
  uses scoped services, and keep the handler's work bounded by the delivery cancellation token.
- A registration name identifies an application-level keyed client registration, not a RocketMQ Broker feature. It can
  point to the same cluster or to a different cluster and is also used as the .NET keyed-service key.
- The registration APIs intentionally do not call `BuildServiceProvider`. The application's final
  host owns validation, construction, scopes, and disposal.
- Do not promote a consumer engine to a shared global service just to avoid a constructor parameter.
  Its state belongs to the individual role that starts and stops it.

## Related Reading

- [Protocol Boundaries and Type Ownership](protocol-boundaries.md)
- [gRPC consumer model](../grpc/consumer-model.md)
- [Classic Remoting transport and client roles](../remoting/transport-and-client-roles.md)
- [Keyed client-registration examples](../../../samples/README.md#producer-web-api-and-swagger)
