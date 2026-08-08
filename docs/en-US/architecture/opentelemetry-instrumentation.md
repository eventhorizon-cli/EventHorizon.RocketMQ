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
| [Producer Web API](../../../samples/opentelemetry/GenericHost/ProducerWebApi/README.md) | `http://localhost:5241` | `eventhorizon-rocketmq-otel-producer` | Sends through explicit gRPC and classic Remoting routes. |
| [Consumer Web API](../../../samples/opentelemetry/GenericHost/ConsumerWebApi/README.md) | `http://localhost:5242` | `eventhorizon-rocketmq-otel-consumer` | Runs gRPC and classic Remoting Push consumers with independent consumer groups. |

The supplied `rocketmq` environment creates the shared `eventhorizon-test-topic` before startup completes. The
Producer request model accepts only a `message`; its topic and tag are fixed for the sample. Each web host exports to
the local Grafana OTEL LGTM collector by default, while its `OpenTelemetry__ServiceName` and
`OpenTelemetry__OtlpEndpoint` settings remain application-owned configuration.

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
- LitePull and Simple APIs return messages to application code. They create receive telemetry, but do not create a
  process Activity because the client does not own the application's processing boundary.

Receive spans are not created for successful empty polls. Failed receive operations create an error Activity. Context
is injected only when the corresponding Activity source has a listener, and existing propagation fields on a message
are preserved.

## Covered Operations

| Operation | gRPC | Classic Remoting |
| --- | --- | --- |
| Producer send | All `IGrpcProducer` send paths, including transactional sends. | Standard, reply, batch, and one-way send paths. |
| Consumer receive | Simple, Push, and LitePush receive engine. | LitePull receives and Push PULL/POP receivers. |
| Automatic handler processing | Push and LitePush handlers. | Push handlers. |
| Consumer settlement | Acknowledge, negative acknowledgement, and dead-letter forwarding. | LitePull and Push PULL offset commits, retry/dead-letter send-back, and internal Push POP acknowledgement and invisibility changes. |

For gRPC, Proxy-managed renewal requested by `ReceiveMessageRequest.AutoRenew` is not a separate client wire operation
and must not create a client settlement span. A client-issued `ChangeInvisibleDuration` must be classified by intent:
`renew` when SimpleConsumer explicitly extends a lease, and `nack` when Push or LitePush schedules redelivery after a
retry result, handler timeout, or corrupted message. The shared receive engine keeps these intent-specific internal
operations separate even though they use the same RPC. Proxy-managed handler renewal creates no duplicate client
timer, span, or metric.

Classic Remoting instrumentation follows the wire-operation owner. `PullWireClient` owns receive completion for
non-empty, empty, canceled, and failed PULL operations. `RemotingConsumerOffsetClient` owns commit settlement, while
`RemotingSettlementClient` owns PULL retry and dead-letter send-back. `PopWireClient` continues to own POP receive,
acknowledgement, and invisibility-change telemetry. The role-level consumer engine only composes these clients; moving
a responsibility between internal types must not duplicate or drop its Activity or metric completion.

POP settlement telemetry records each actual wire operation independently: `ack` for acknowledgement and `nack` for
the one-shot retry/defer `CHANGE_MESSAGE_INVISIBLETIME`. A normal `Retry` at the delivery-attempt limit remains `nack`
while the Java-compatible message-age guard selects another delay, then becomes `ack` after the terminal age threshold;
it never creates a `reject` operation. Explicit dead-letter forwarding and its follow-up ACK remain separate operations.
A handler result that arrives after the fixed invisible deadline creates no settlement operation. An indeterminate
`nack` failure is not retried with the old receipt; acknowledgement and the ACK after explicit dead-letter forwarding
are retried only while the original deadline remains valid.

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
