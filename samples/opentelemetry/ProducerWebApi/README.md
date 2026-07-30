# OpenTelemetry Producer integration

[OpenTelemetry samples](../README.md) | [Simplified Chinese](README.zh-CN.md)

Add RocketMQ Producer instrumentation to an application's existing OpenTelemetry pipeline when message sends must
participate in the same distributed traces and operational metrics as incoming HTTP requests, database calls, and
other application work. The integration is protocol-specific: gRPC and classic Remoting publish distinct activity
sources and meters because they have different client and transport boundaries.

Do not create a second provider solely for RocketMQ. Register only the protocol extensions used by the process, and
keep the application responsible for its resource identity, sampling, processors, metric views, and exporters. The
RocketMQ packages emit `Activity` and `Meter` signals; they do not select an observability backend.

## OpenTelemetry registration

`AddRocketMQGrpcInstrumentation` and `AddRocketMQRemotingInstrumentation` are available independently on both
`TracerProviderBuilder` and `MeterProviderBuilder`. Adding an extension to tracing does not add it to metrics, or the
other way around:

```csharp
builder.Services.AddOpenTelemetry()
    .ConfigureResource(resource => resource.AddService(serviceName))
    .WithTracing(tracing => tracing
        .AddAspNetCoreInstrumentation()
        .AddRocketMQGrpcInstrumentation()
        .AddRocketMQRemotingInstrumentation()
        .AddOtlpExporter(options => options.Endpoint = otlpEndpoint))
    .WithMetrics(metrics => metrics
        .AddAspNetCoreInstrumentation()
        .AddRocketMQGrpcInstrumentation()
        .AddRocketMQRemotingInstrumentation()
        .AddOtlpExporter(options => options.Endpoint = otlpEndpoint));
```

The public activity-source and meter names are `EventHorizon.RocketMQ.Grpc` and
`EventHorizon.RocketMQ.Remoting`. Applications normally use the extension methods rather than subscribe to those
names manually.

Instrumentation registration does not create a RocketMQ client. Register each protocol and Producer role through
its own public SDK API:

```csharp
builder.Services
    .AddRocketMQGrpc(grpcClientSection.Bind)
    .AddGrpcProducer(static options =>
        options.SendMsgTimeout = TimeSpan.FromSeconds(3));

builder.Services
    .AddRocketMQRemoting(remotingClientSection.Bind)
    .AddRemotingProducer(static options =>
    {
        options.GroupName = "eventhorizon-otel-remoting-producer";
        options.SendMsgTimeout = TimeSpan.FromSeconds(3);
    });
```

Inject `IGrpcProducer` or `IRemotingProducer` directly. There is no transport-neutral Producer abstraction, even
when both protocols export to the same OpenTelemetry provider.

## Send signals and propagation

A Producer send creates an `ActivityKind.Producer` activity beneath the current application activity. When the
protocol activity source has a listener, the client injects the current distributed context into the outbound
message properties and preserves propagation fields already present on the message. A matching automatic Consumer
can then relate its process activity to that Producer context across the service boundary.

The gRPC instrumentation covers all `IGrpcProducer` send paths, including transactional sends. The Remoting
instrumentation covers standard, reply, batch, and one-way send paths. Both protocol meters emit:

| Instrument | Meaning |
| --- | --- |
| `rocketmq.client.operations` | Completed client operations, tagged by operation and outcome. |
| `rocketmq.client.messages` | Messages handled by the operation. |
| `rocketmq.client.operation.duration` | Operation duration in seconds. |

The send activity and metrics describe the client operation. A send receipt does not mean a Consumer has processed
the message. Sampling, histogram buckets, cardinality limits, retention, and export behavior remain application
decisions.

## Failure semantics

When an SDK send throws, the activity is marked as an error and `error.type` is recorded on the activity and failed
operation metrics. Cancellation should flow from the application operation into `SendAsync`; cancelled and timed-out
sends are not placed in a durable local outbox by the instrumentation. The application remains responsible for
retry policy, ambiguous outcomes, and idempotency.

Classic Remoting also reports some durability outcomes as `RemotingSendResult.Status` values such as
`FlushDiskTimeout`, `FlushSlaveTimeout`, or `SlaveNotAvailable`. These are returned protocol results rather than
exceptions, so callers must inspect `Status`; the presence of a completed send activity is not a substitute for that
check. One-way completion likewise does not confirm Broker storage.

Trace context injection requires an activity listener. Metrics can still be collected independently when tracing is
disabled. Existing message propagation fields are not overwritten.

For complete span attributes, links, and metric tags, see the
[OpenTelemetry design note](../../../docs/en-US/architecture/opentelemetry-instrumentation.md). Producer message and
receipt semantics remain documented in the [gRPC guide](../../../src/EventHorizon.RocketMQ.Grpc/README.md) and
[Remoting guide](../../../src/EventHorizon.RocketMQ.Remoting/README.md).

## Configuration and run

The application-owned settings are `OpenTelemetry:ServiceName` and `OpenTelemetry:OtlpEndpoint`. The endpoint must
be an absolute HTTP or HTTPS URI; this project exports with OTLP/gRPC. Override them with
`OpenTelemetry__ServiceName` and `OpenTelemetry__OtlpEndpoint`. RocketMQ connection settings stay in the
protocol-specific `RocketMQ:Grpc` and `RocketMQ:Remoting` sections; fixed Producer behavior is declared next to each
registration in code.

Start the shared RocketMQ and Grafana OTEL LGTM environments, then run the Producer integration:

```shell
docker compose -f test-environments/rocketmq/compose.yaml up -d --wait
docker compose -f test-environments/otel-lgtm/compose.yaml up -d --wait
dotnet run --project samples/opentelemetry/ProducerWebApi
```

The runnable host exposes separate gRPC and Remoting send operations through Swagger. Use the
[Consumer integration](../ConsumerWebApi/README.md) to observe receive, process, and settlement signals, and the
[OpenTelemetry overview](../README.md) for the supplied dashboards and complete local topology.
