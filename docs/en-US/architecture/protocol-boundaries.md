# Protocol Boundaries and Dependencies

[English](protocol-boundaries.md) | [Simplified Chinese](../../zh-CN/architecture/protocol-boundaries.md)

## Context

RocketMQ 5 gRPC and classic Remoting solve different connectivity problems. A gRPC client talks to a
RocketMQ Proxy through an `Endpoint`; a classic client asks a NameServer for routes and connects to
the advertised Brokers. Their discovery, wire contracts, consumer semantics, result types, and
server requirements are therefore different.

Treating those differences as interchangeable behind one client interface tends to leak the
transport at the worst possible time: configuration becomes ambiguous, a result lacks the data a
caller needs, or an API appears usable against a server that cannot perform it. The package layout
makes the choice visible at compile time instead.

## Decision

The production dependency graph has exactly one protocol-neutral center:

```text
                         +----------------------------------+
                         | EventHorizon.RocketMQ.Shared     |
                         | Message, filters, common options,|
                         | ConsumeResult, base exception    |
                         +----------------------------------+
                            ^                         ^
                            |                         |
          +-----------------+--+                   +--+------------------+
          | EventHorizon.RocketMQ.Grpc |           | EventHorizon.RocketMQ.Remoting |
          | Proxy and protobuf/gRPC    |           | NameServer and classic wire     |
          +----------------------------+           +---------------------------------+
```

- `EventHorizon.RocketMQ.Shared` has no reference to either transport or to Microsoft DI/options
  packages. Its [project file](../../../src/EventHorizon.RocketMQ.Shared/EventHorizon.RocketMQ.Shared.csproj)
  defines a small, protocol-neutral surface.
- `EventHorizon.RocketMQ.Grpc` references only Shared. Its public APIs use the `Grpc` prefix and
  connect to a Proxy. See its [project file](../../../src/EventHorizon.RocketMQ.Grpc/EventHorizon.RocketMQ.Grpc.csproj)
  and [public guide](../../../src/EventHorizon.RocketMQ.Grpc/README.md).
- `EventHorizon.RocketMQ.Remoting` references only Shared. Its public APIs use the `Remoting`
  prefix and discover Brokers through a NameServer. See its
  [project file](../../../src/EventHorizon.RocketMQ.Remoting/EventHorizon.RocketMQ.Remoting.csproj)
  and [public guide](../../../src/EventHorizon.RocketMQ.Remoting/README.md).
- Neither protocol project references the other. There is no transport selector, shared client
  builder, transport-independent producer interface, or transport-independent consumer interface.

This is a deliberate architectural boundary, not merely a source-folder convention.

## How It Works

Shared concepts are deliberately narrow. The shared package owns the protocol-neutral
[`Message`](../../../src/EventHorizon.RocketMQ.Shared/Producer/Message.cs), filters and consumer
models under [`Consumer`](../../../src/EventHorizon.RocketMQ.Shared/Consumer), common client-option
bases, and [`RocketMQClientException`](../../../src/EventHorizon.RocketMQ.Shared/Exceptions/RocketMQClientException.cs).
Those types can be useful to both clients without implying that the clients have the same wire
behavior.

Everything that exposes protocol behavior remains in the owning package. For example:

| Concern | gRPC ownership | Remoting ownership |
| --- | --- | --- |
| Producer API and send result | `IGrpcProducer` and `GrpcSendReceipt` | `IRemotingProducer`, `RemotingSendResult`, and `RemotingMessageQueue` |
| Consumer message view | `GrpcMessageView` | `RemotingMessageView` |
| Consumer roles | Simple, Push, and LitePush | Pull, LitePull, POP, and Push |
| Route and wire infrastructure | `Protocol` with protobuf/gRPC services | `Protocol` with sockets, frames, JSON, and NameServer routes |
| Protocol failure | `GrpcServiceException` | `RemotingCommandException` |

The folder layout follows the same rule. Communication infrastructure that is reused within one
transport belongs in that transport's `Protocol` directory. Scheduling, retries, transaction work,
consumer engines, and feature-specific headers remain with the owning Producer or Consumer feature.
This prevents a generic `Common` or `Internal` folder from becoming a second, undocumented
architecture.

The public registration methods also preserve the choice:

```text
AddRocketMQGrpc(options => options.Endpoint = "proxy:8081")
AddRocketMQRemoting(options => options.NamesrvAddr = "nameserver:9876")
```

They intentionally do not share a root `AddRocketMQ` method. An application that needs both can
install both packages and register separate profiles; it still receives protocol-specific interfaces
from DI.

## Trade-offs and Constraints

- Similar concepts are intentionally duplicated when their behavior is transport-specific. A
  `GrpcMessageView` is not silently substituted for a `RemotingMessageView`, and the send results
  preserve the identifiers and status relevant to their protocol.
- Adding a capability to one package does not add it to the other. Documentation and samples must
  state the protocol and server prerequisites rather than relying on a generic feature claim.
- Shared must remain free of Proxy, NameServer, socket, protobuf, DI, and options dependencies. Put
  a new type in Shared only when it is genuinely protocol-neutral and can be understood without
  either transport.
- A gRPC API is not justified by a matching declaration in the protobuf file alone. It also needs a
  working server-side implementation, a stable client contract, tests, and a supported-server note.

## Related Reading

- [Dependency-injection profiles and lifetimes](dependency-injection-and-lifetimes.md)
- [gRPC consumer model](../grpc/consumer-model.md)
- [Classic Remoting transport and client roles](../remoting/transport-and-client-roles.md)
- [Runnable samples](../../../samples/README.md)
