# Protocol Boundaries and Type Ownership

[English](protocol-boundaries.md) | [Simplified Chinese](../../zh-CN/architecture/protocol-boundaries.md)

## Context

RocketMQ 5 gRPC and classic Remoting solve different connectivity problems. A gRPC client talks to a
RocketMQ Proxy through an `Endpoint`; a classic client asks a NameServer for routes and connects to
the advertised Brokers. Their discovery, wire contracts, Consumer semantics, result types, and
server requirements are therefore different.

The protocols also use a few models with similar behavior: `Message`, filters, `ConsumerOptions`, and the client
exception base. Both protocols define a `ConsumeResult`, but its outcomes follow the owning consumer model rather
than a cross-protocol shape. Putting those models in a third production Project
would add another MSBuild boundary, Package version, and release dependency for a very small API
surface.

## Decision

Production contains exactly two independent client Projects:

```text
+-----------------------------------+      +---------------------------------------+
| EventHorizon.RocketMQ.Grpc        |      | EventHorizon.RocketMQ.Remoting        |
| Proxy and protobuf/gRPC           |      | NameServer and classic wire           |
| EventHorizon.RocketMQ.Grpc.*      |      | EventHorizon.RocketMQ.Remoting.*      |
+-----------------------------------+      +---------------------------------------+
                  no production ProjectReference between them
```

- Each Project physically owns every source file compiled into its assembly.
- Each NuGet Package is self-contained at the client API boundary and has no dependency on another
  EventHorizon.RocketMQ Package.
- Neither protocol Project references the other, and there is no third production Project.
- There is no transport selector, common client builder, transport-independent Producer interface,
  or transport-independent Consumer interface.
- `GrpcClientOptions` owns Proxy endpoints, credentials, namespace, TLS, and gRPC timing. The gRPC
  client ID is generated internally for each logical role.
- `RemotingClientOptions` owns NameServer discovery, credentials, namespace, TLS, Remoting timing,
  and the classic client identity used in Broker coordination. It clones that identity for each
  logical role.
- Neither Options type derives from a repository-defined client-options base class. Their shapes may
  differ when the protocols require different configuration.

The duplicated model files are an intentional trade-off. They keep namespace ownership, compilation,
packing, and release behavior visible in each protocol Project.

## Type Ownership

The small foundational models are declared separately in each protocol namespace:

| Concept | gRPC type | Remoting type |
| --- | --- | --- |
| Outbound message | `EventHorizon.RocketMQ.Grpc.Producer.Message` | `EventHorizon.RocketMQ.Remoting.Producer.Message` |
| Consumer result | `EventHorizon.RocketMQ.Grpc.Consumer.ConsumeResult` | `EventHorizon.RocketMQ.Remoting.Consumer.ConsumeResult` |
| Consumer Options base | `EventHorizon.RocketMQ.Grpc.Consumer.ConsumerOptions` | `EventHorizon.RocketMQ.Remoting.Consumer.ConsumerOptions` |
| Filter expression | `EventHorizon.RocketMQ.Grpc.Consumer.FilterExpression` | `EventHorizon.RocketMQ.Remoting.Consumer.FilterExpression` |
| Filter type | `EventHorizon.RocketMQ.Grpc.Consumer.FilterExpressionType` | `EventHorizon.RocketMQ.Remoting.Consumer.FilterExpressionType` |
| Client exception base | `EventHorizon.RocketMQ.Grpc.Exceptions.RocketMQClientException` | `EventHorizon.RocketMQ.Remoting.Exceptions.RocketMQClientException` |

The two `ConsumeResult` types intentionally differ. gRPC Push and LitePush follow the Apache Java gRPC listener
contract and expose `Success` and `Failure`; the service owns non-FIFO retry and dead-letter progression, while FIFO
dead-letter forwarding is internal client behavior. Classic Remoting exposes `Success`, `Retry`, and `DeadLetter`
because its callback settlement contract lets the application select those outcomes. Compatibility tests therefore
lock each protocol's enum independently instead of requiring parity.

Classic Remoting `ConsumerOptions.InitialPosition` uses the protocol-owned `ConsumeFromPosition` type. LitePull and
Push apply it only when an assigned queue has no committed group position; a timestamp start also uses
`ConsumerOptions.ConsumeTimestamp`.

The different fully qualified names let both Packages coexist in one application. They are also
different CLR types: a gRPC `Message`, filter, Consumer Options, or exception base cannot be passed
directly to a Remoting API.

Importing both protocol namespaces can still make an unqualified short name such as `Message`
ambiguous. That is normal C# namespace resolution (`CS0104`), not a duplicate CLR type (`CS0433`). Use
aliases at call sites that need both:

```csharp
using GrpcMessage = EventHorizon.RocketMQ.Grpc.Producer.Message;
using RemotingMessage = EventHorizon.RocketMQ.Remoting.Producer.Message;
```

## Protocol Ownership

Everything that exposes protocol behavior remains in the owning Project:

| Concern | gRPC ownership | Remoting ownership |
| --- | --- | --- |
| Producer API and send result | `IGrpcProducer` and `GrpcSendReceipt` | `IRemotingProducer`, `RemotingSendResult`, and `RemotingMessageQueue` |
| Consumer message view | `GrpcMessageView` | `RemotingMessageView` |
| Public readable-queue identity | Not exposed; assignments are service-owned | `RemotingConsumerQueue` for LitePull and read-only Admin queries |
| Consumer roles | Simple, Push, and LitePush | LitePull and Push |
| Route and wire infrastructure | `Protocol` with protobuf/gRPC services | `Protocol` with sockets, frames, JSON, and NameServer routes |
| Protocol failure | `GrpcServiceException` | `RemotingCommandException` |

Communication infrastructure reused within one transport belongs in that transport's `Protocol`
directory. Scheduling, retries, transaction work, Consumer Engines, and feature-specific headers
remain with the owning Producer or Consumer feature. This prevents a generic `Common` or `Internal`
folder from becoming a second, undocumented architecture.

The public registration methods also preserve the protocol choice:

```text
AddRocketMQGrpc(options => options.Endpoint = "proxy:8081")
AddRocketMQRemoting(options => options.NamesrvAddr = "nameserver:9876")
```

An application that needs both installs both Packages and registers the clients independently. DI
still resolves protocol-specific interfaces, Options, and model types.

## Trade-offs and Constraints

- A few small source files are duplicated. This reduces MSBuild, packing, and release complexity at
  the cost of reviewing two implementations when their common behavior changes.
- Changing one of the matching foundational models requires reviewing the counterpart in the other
  protocol Project. Apply the same change only when its semantics remain valid for that protocol.
- `EventHorizon.RocketMQ.Compatibility.Tests` references both Projects and guards API behavior,
  namespace separation, and dual-Package consumption.
- Similar-looking transport results, message views, queues, and service contracts remain
  protocol-owned; visual similarity is not a reason to merge their types.
- Adding a capability to one Project does not add it to the other. Documentation and samples must
  state the protocol and server prerequisites rather than relying on a generic feature claim.
- A gRPC API is not justified by a matching declaration in the protobuf file alone. It also needs a
  working server-side implementation, a stable client contract, tests, and a supported-server note.

## Related Reading

- [Dependency-injection client registrations and lifetimes](dependency-injection-and-lifetimes.md)
- [gRPC consumer model](../grpc/consumer-model.md)
- [Classic Remoting consumer model](../remoting/consumer-model.md)
- [Classic Remoting transport and client roles](../remoting/transport-and-client-roles.md)
- [Runnable samples](../../../samples/README.md)
