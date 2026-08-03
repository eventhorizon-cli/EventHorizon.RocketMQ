# EventHorizon.RocketMQ Design Notes

[English](README.md) | [Simplified Chinese](../zh-CN/README.md)

These notes record the architectural decisions behind EventHorizon.RocketMQ. They explain why the
gRPC and classic Remoting clients are separate, how they are composed in a .NET host, and where the
transport-specific behavior lives. They are design material, not a replacement for the runnable
samples or protocol guides.

Start with the [repository overview](../../README.md) to choose a package, then use the
[runnable samples](../../samples/README.md) for setup and executable code. The protocol project
guides contain the complete public API and compatibility references:

- [gRPC guide](../../src/EventHorizon.RocketMQ.Grpc/README.md)
- [classic Remoting guide](../../src/EventHorizon.RocketMQ.Remoting/README.md)

## Articles

| Article | Focus |
| --- | --- |
| [RocketMQ deployment and client architecture](architecture/rocketmq-architecture.md) | NameServer, Broker, Proxy, the gRPC and Remoting paths, LitePush, and the bundled Compose topology. |
| [Protocol boundaries and type ownership](architecture/protocol-boundaries.md) | Why gRPC and Remoting physically own their API models and can be consumed together. |
| [Dependency-injection client registrations and lifetimes](architecture/dependency-injection-and-lifetimes.md) | Default and keyed client registrations, hosted lifecycle, and per-role consumer engines. |
| [OpenTelemetry instrumentation](architecture/opentelemetry-instrumentation.md) | Protocol-owned tracing and metrics, context propagation, and application-owned exporters. |
| [gRPC consumer model](grpc/consumer-model.md) | Proxy-backed Simple, Push, and LitePush consumers, including the absence of a public gRPC Pull API. |
| [Classic Remoting consumer model](remoting/consumer-model.md) | The public LitePull and Push models, Broker-assigned PULL/POP, receipt settlement, and queue-route ownership. |
| [Classic Remoting transport and client roles](remoting/transport-and-client-roles.md) | NameServer routing, the built-in pipeline transport, and the LitePull, Push, and Admin roles. |
| [Local and integration testing](testing/local-and-integration-testing.md) | Testcontainers isolation, the manual Compose environment, and when to use each. |

## Reading order

Read [protocol boundaries](architecture/protocol-boundaries.md) first when deciding where new code
belongs. Follow it with [dependency injection and lifetimes](architecture/dependency-injection-and-lifetimes.md)
when adding a role or integrating the client into an application. The protocol articles explain the
runtime model and server assumptions that are intentionally not shared between the two projects.

## Scope

The documentation describes the checked-in implementation and its supported RocketMQ 5.5.0 local
environment. It does not turn a protocol declaration into a supported feature: a target Broker or
Proxy must implement and enable the relevant server-side behavior.
