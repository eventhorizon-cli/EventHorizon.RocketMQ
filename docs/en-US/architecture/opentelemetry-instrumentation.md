# OpenTelemetry Instrumentation

[English](opentelemetry-instrumentation.md) | [Simplified Chinese](../../zh-CN/architecture/opentelemetry-instrumentation.md)

## Decision

Each protocol Package owns its OpenTelemetry instrumentation. The gRPC implementation lives in
[`src/EventHorizon.RocketMQ.Grpc/Instrumentation`](../../../src/EventHorizon.RocketMQ.Grpc/Instrumentation) and
the classic Remoting implementation lives in
[`src/EventHorizon.RocketMQ.Remoting/Instrumentation`](../../../src/EventHorizon.RocketMQ.Remoting/Instrumentation).
There is no third production Package and neither protocol Package references the other.

The client provides Activity and Meter instrumentation only. The application owns the OpenTelemetry SDK, resource
identity, sampler, processors, and exporters. This keeps the protocol Packages independently publishable and lets an
application select OTLP, Prometheus, a console exporter, or another supported backend without a client dependency on
an exporter package.

## Registration

`AddRocketMQGrpc` and `AddRocketMQRemoting` register the internal telemetry implementation through its
protocol-specific abstraction. They also register the standard Microsoft `IMeterFactory` through `AddMetrics`.
Applications do not register or resolve the internal telemetry interfaces directly.

Subscribe to the public source and meter names when configuring the application's OpenTelemetry SDK:

```csharp
builder.Services.AddOpenTelemetry()
    .WithTracing(tracing => tracing
        .AddRocketMQGrpcInstrumentation()
        .AddRocketMQRemotingInstrumentation())
    .WithMetrics(metrics => metrics
        .AddRocketMQGrpcInstrumentation()
        .AddRocketMQRemotingInstrumentation());
```

An application that uses one protocol needs only that protocol's source and meter. Add SDK instrumentation and an
exporter at the application boundary. The [OpenTelemetry samples](../../../samples/opentelemetry/README.md) show an
OTLP setup for the bundled Grafana environment.

`AddRocketMQGrpcInstrumentation` and `AddRocketMQRemotingInstrumentation` are overloads for both
`TracerProviderBuilder` and `MeterProviderBuilder`. They are the protocol-specific equivalents of an OpenTelemetry
instrumentation extension such as `AddAspNetCoreInstrumentation`.

## Sample Topology

The repository's OpenTelemetry demonstration separates the producing and consuming processes so that the complete
messaging path can be inspected across service boundaries:

| Project | Endpoint | Default `service.name` | Responsibility |
| --- | --- | --- | --- |
| [Producer Web API](../../../samples/opentelemetry/ProducerWebApi/README.md) | `http://localhost:5241` | `eventhorizon-rocketmq-otel-producer` | Sends through explicit gRPC and classic Remoting routes. |
| [Consumer Web API](../../../samples/opentelemetry/ConsumerWebApi/README.md) | `http://localhost:5242` | `eventhorizon-rocketmq-otel-consumer` | Runs gRPC and classic Remoting Push consumers with independent consumer groups. |

The supplied `rocketmq` environment creates the shared `eventhorizon-test-topic` before startup completes. The
Producer request model accepts only a `message`; its topic and tag are fixed for the sample. Each web host exports to
the local Grafana OTEL LGTM collector by default, while its `Sample__ServiceName` and `Sample__OtlpEndpoint` settings
remain application-owned configuration.

## Trace Model

The instrumentation follows the messaging model used by the official RocketMQ OpenTelemetry instrumentation:

```text
application work -> send -> receive -> process
                              \-> producer context link
```

- A Producer send creates a `Producer` Activity and injects the current distributed context into message properties.
- A non-empty receive creates a `Client` Activity. It captures the receive RPC or long-poll duration and becomes the
  parent of automatic handler work.
- An automatic Push or LitePush handler creates a `Consumer` process Activity. The receive Activity is its parent;
  the propagated producer context is recorded as an Activity link. When no receive context is available, the
  propagated producer context becomes the process parent instead.
- Pull and Simple APIs return messages to application code. They create receive telemetry, but do not create a
  process Activity because the client does not own the application's processing boundary.

Receive spans are not created for successful empty polls. Failed receive operations create an error Activity. Context
is injected only when the corresponding Activity source has a listener, and existing propagation fields on a message
are preserved.

## Covered Operations

| Operation | gRPC | Classic Remoting |
| --- | --- | --- |
| Producer send | All `IGrpcProducer` send paths, including transactional sends. | Standard, reply, batch, and one-way send paths. |
| Consumer receive | Simple, Push, and LitePush receive engine. | Pull, LitePull, Push receive engine, and POP receive. |
| Automatic handler processing | Push and LitePush handlers. | Push handlers. |
| Consumer settlement | Acknowledge, negative acknowledgement, and dead-letter forwarding. | Offset commit, retry/dead-letter send-back, POP acknowledge, and POP invisibility changes. |

Activities use OpenTelemetry messaging semantic-convention attributes such as `messaging.system`,
`messaging.destination.name`, `messaging.consumer.group.name`, message ID, partition ID, body size, and batch size.
Failures set the Activity status to error and record `error.type`.

## Metrics

Both protocol meters emit these instruments:

| Instrument | Unit | Meaning |
| --- | --- | --- |
| `rocketmq.client.operations` | `{operation}` | Completed client operations. |
| `rocketmq.client.messages` | `{message}` | Messages handled by the operation. |
| `rocketmq.client.operation.duration` | `s` | Client operation duration. |

Metrics carry the messaging system, operation name and type, destination, and success status. Failed operations also
carry an error type. Metrics can be collected even when tracing is not enabled.

## Constraints

- The instrumentation does not create an Activity or inject propagation fields when no tracing listener and no metric
  listener needs the operation.
- The gRPC and Remoting source and meter names are distinct because the two protocol Packages are distinct client
  boundaries.
- Existing RocketMQ protocol telemetry sessions remain control-plane behavior under each protocol's `Protocol`
  directory. They are not OpenTelemetry instrumentation.

## Related Reading

- [Dependency-injection client registrations and lifetimes](dependency-injection-and-lifetimes.md)
- [gRPC consumer model](../grpc/consumer-model.md)
- [Classic Remoting transport and client roles](../remoting/transport-and-client-roles.md)
- [OpenTelemetry samples](../../../samples/opentelemetry/README.md)
