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
        +-- one or more protocol-specific roles
        +-- a role-specific transport graph
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
settings and then derives a logical client name for every role. A role is registered only once per client registration; a
second registration of the same role fails early rather than accidentally sharing a lifecycle.

The role registration helpers are transport-local:

- [gRPC role registration](../../../src/EventHorizon.RocketMQ.Grpc/GrpcRocketMQRegistration.cs)
- [Remoting role registration](../../../src/EventHorizon.RocketMQ.Remoting/RemotingRocketMQRegistration.cs)

Transport services are also keyed by the internal role key. A role therefore owns the metadata,
route client, and transport lifecycle appropriate to its logical client identity. This avoids a
global client whose state is accidentally shared by an unrelated role.

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
This gives each consumer role one coherent lifecycle for its receive loops, subscriptions, and
shutdown work.

### Push-handler lifetimes

The generic Push registration overloads accept a handler type and a `ServiceLifetime`. The factory
registration is keyed to the internal role key, so handlers cannot cross client-registration boundaries.

| Handler lifetime | Resolution behavior |
| --- | --- |
| `Singleton` | The handler is resolved from the root provider and reused. It must be safe for concurrent delivery. |
| `Scoped` | A new async scope is created for each handler invocation, then disposed after handling. |
| `Transient` | A new async scope is created for each handler invocation; the transient handler and its scoped dependencies are disposed with that scope. |

The concrete implementation is visible in the
[gRPC handler factory](../../../src/EventHorizon.RocketMQ.Grpc/Consumer/Push/GrpcPushMessageHandlerFactory.cs)
and [Remoting handler factory](../../../src/EventHorizon.RocketMQ.Remoting/Consumer/Push/RemotingPushMessageHandlerFactory.cs).
Use the typed overload when a handler needs DI. The `MessageHandler` option is a direct delegate and
does not create an application DI scope for the caller. gRPC Push and LitePush invoke their handlers for each
message. Remoting Push invokes its one `IRemotingPushMessageHandler` API with an
`IReadOnlyList<RemotingMessageView>` and a `RemotingPushConsumeContext` for each batch; its
`ConsumeMessageBatchSize` defaults to `1`, so existing settings retain singleton delivery unless configured
otherwise. For a concurrent non-FIFO batch, a handler can return `Success` with `AckIndex` set to confirm a
contiguous prefix and retry its tail; `Retry` and `DeadLetter` remain whole-batch outcomes. The direct Remoting
`MessageHandler` delegate uses the same list-and-context contract.

For non-FIFO gRPC Push and LitePush messages, `ConsumeTimeout` cancels the handler token, stops client-side
invisibility renewal, and requests retry when the configured limit elapses. The dispatcher ignores a late result and
releases its worker, but it cannot terminate application code. A timed-out invocation can overlap redelivery and must
be idempotent. Its typed-handler async scope remains alive until that invocation returns, so scoped dependencies are
not disposed while handler code is still running. FIFO message groups are excluded to preserve ordering.

For concurrent clustered non-FIFO Remoting batches, `ConsumeTimeout` cancels the handler token and requests Broker
redelivery when the configured limit elapses. A late result is ignored and can overlap redelivery, so the handler still
owns cancellation cooperation and must be idempotent; the runtime cannot forcibly stop application code. FIFO and
orderly delivery remain excluded to preserve ordering guarantees.

## Trade-offs and Constraints

- A client registration may add each role once. Add another keyed client registration when the application needs a
  second independent instance of the same role.
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
