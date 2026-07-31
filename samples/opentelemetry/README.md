# OpenTelemetry integration

[All samples](../README.md) | [Simplified Chinese](README.zh-CN.md)

The gRPC and classic Remoting clients emit protocol-specific activities and metrics. They do not create an
OpenTelemetry provider or choose a resource, sampler, processor, or exporter. The application owns those decisions
and opts into the signals for each protocol it uses.

## When to use it

Add the RocketMQ instrumentation to an application's existing OpenTelemetry pipeline when send, receive, process,
settlement, and client request operations must participate in distributed traces or operational metrics. This is the
normal integration for services that already export ASP.NET Core, database, or other application telemetry.

Do not create a second provider solely for RocketMQ. Register the protocol-specific source and meter on the same
provider as the rest of the application so trace context, resource identity, sampling, and exporter behavior remain
consistent.

## SDK registration

Register instrumentation independently for tracing and metrics:

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

Register only the protocol extensions used by the process. `AddRocketMQGrpcInstrumentation` and
`AddRocketMQRemotingInstrumentation` are available on both `TracerProviderBuilder` and `MeterProviderBuilder`; adding
one does not implicitly add the other.

The public activity-source and meter names are both `EventHorizon.RocketMQ.Grpc` for gRPC and
`EventHorizon.RocketMQ.Remoting` for classic Remoting. The extensions subscribe to these names without exposing the
client's internal telemetry services.

## Signal semantics

Producer activities use `ActivityKind.Producer`; message processing uses `ActivityKind.Consumer`; route, assignment,
receive, acknowledgement, offset, and other request operations use the client kind appropriate to the operation.
Errors are recorded on the activity and metrics when the SDK operation fails. Application handler work remains part
of the consumer processing activity, so cancellation, retry, and settlement outcomes can be correlated with it.

Both clients publish these common instruments:

| Instrument | Meaning |
| --- | --- |
| `rocketmq.client.operations` | Completed client operations, tagged by operation and outcome. |
| `rocketmq.client.messages` | Messages handled by an operation. |
| `rocketmq.client.operation.duration` | Operation duration in seconds. |

Choose histogram views, cardinality limits, sampling, retention, and exporters according to the application's
observability policy. The SDK does not make those deployment decisions.

For the complete span names, attributes, propagation behavior, and metric tags, see the
[OpenTelemetry design note](../../docs/en-US/architecture/opentelemetry-instrumentation.md).

## Run the integration samples

The two Web API projects place gRPC and Remoting roles in separate producer and consumer processes so one message can
be followed across HTTP, send, receive, process, and settlement telemetry. They export OTLP/gRPC to the supplied
Grafana OTEL LGTM environment by default.

Run the following commands from the repository root. The RocketMQ environment creates
`eventhorizon-test-topic` before it reports ready, so no manual topic setup is required.

```shell
docker compose -f test-environments/rocketmq/compose.yaml up -d --wait
docker compose -f test-environments/otel-lgtm/compose.yaml up -d --wait
```

Start the Generic Host Consumer in one terminal:

```shell
dotnet run --project samples/opentelemetry/GenericHost/ConsumerWebApi
```

Then start the Generic Host Producer in another terminal:

```shell
dotnet run --project samples/opentelemetry/GenericHost/ProducerWebApi
```

With both hosts running, use the Producer Swagger page at `http://localhost:5241/swagger`, or publish one message
through each protocol directly:

```shell
curl --fail --request POST http://localhost:5241/messages/grpc \
  --header 'content-type: application/json' \
  --data '{"message":"Hello from the OpenTelemetry sample."}'

curl --fail --request POST http://localhost:5241/messages/remoting \
  --header 'content-type: application/json' \
  --data '{"message":"Hello from the OpenTelemetry sample."}'
```

Each request returns an HTTP `200` send receipt. The Consumer host logs processing from both the gRPC and Remoting
consumer groups: the groups are independent subscriptions to the same topic, so either producer route can be
observed by both consumers. Open Grafana at `http://127.0.0.1:3000` and use the Producer and Consumer dashboards to
inspect the corresponding HTTP, send, receive, process, and settlement telemetry.

The applications read `OpenTelemetry:ServiceName` and `OpenTelemetry:OtlpEndpoint` directly. Override them with
`OpenTelemetry__ServiceName` and `OpenTelemetry__OtlpEndpoint`; RocketMQ connection settings remain in their
protocol-specific `RocketMQ` sections, while fixed role behavior is declared in code.

Use the [Generic Host Producer integration](GenericHost/ProducerWebApi/README.md) and
[Generic Host Consumer integration](GenericHost/ConsumerWebApi/README.md) guides for the two sides of the workflow.
