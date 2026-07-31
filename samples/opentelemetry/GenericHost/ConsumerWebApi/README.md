# OpenTelemetry Push Consumer integration

[OpenTelemetry overview](../../README.md) | [Generic Host samples](../README.md) | [Simplified Chinese](README.zh-CN.md)

Use this integration when an application runs automatic Push Consumers and needs receive, handler processing, and
settlement to participate in its existing OpenTelemetry traces and metrics. Push describes automatic application
dispatch: both clients initiate long polls; neither protocol relies on a Broker opening a push connection to the
application.

gRPC and classic Remoting remain separate SDK modes. A gRPC Consumer connects through a RocketMQ 5 Proxy, while a
Remoting Consumer discovers routes through a NameServer and connects to advertised Brokers. Register only the
protocol used by a production process. Hosting both is useful for comparing their signals, but does not make their
subscriptions, consumer groups, retries, or offsets interchangeable.

## Instrumentation and role registration

Add the protocol instrumentation to the application's existing tracing and metric providers. The two provider
builders are independent:

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

`AddRocketMQGrpcInstrumentation` and `AddRocketMQRemotingInstrumentation` subscribe to the public source and meter
for their protocol. They do not register a Consumer role or choose the resource, sampler, metric views, or exporter.

Register Push Consumers with their protocol-specific APIs and a typed handler:

```csharp
builder.Services
    .AddRocketMQGrpc(grpcClientSection.Bind)
    .AddGrpcPushConsumer<GrpcConsumerMessageHandler>(ServiceLifetime.Scoped, options =>
    {
        options.GroupName = "eventhorizon-otel-grpc-consumer";
        options.BatchSize = 16;
        options.MaxConcurrency = 4;
        options.MaxDeliveryAttempts = 16;
        options.InvisibleDuration = TimeSpan.FromSeconds(30);
        options.ConsumeTimeout = TimeSpan.FromMinutes(15);
        options.LongPollingTimeout = TimeSpan.FromSeconds(15);
        options.Subscribe("eventhorizon-test-topic");
    });

builder.Services
    .AddRocketMQRemoting(remotingClientSection.Bind)
    .AddRemotingPushConsumer<RemotingConsumerMessageHandler>(ServiceLifetime.Scoped, options =>
    {
        options.GroupName = "eventhorizon-otel-remoting-consumer";
        options.BatchSize = 16;
        options.ConsumeMessageBatchSize = 4;
        options.MaxConcurrency = 4;
        options.ConsumeTimeout = TimeSpan.FromMinutes(15);
        options.LongPollingTimeout = TimeSpan.FromSeconds(5);
        options.RetryDelay = TimeSpan.FromSeconds(1);
        options.Subscribe("eventhorizon-test-topic");
    });
```

The .NET host starts and stops both Consumer roles. A scoped gRPC handler is resolved in a new async DI scope for
each handling attempt; a scoped Remoting handler gets one scope per batch attempt. A singleton handler is shared by
concurrent deliveries and must be thread-safe. Both automatic Consumer roles resolve their typed handlers and handler
dependencies from the application container.

## Trace and metric model

The automatic Consumer path separates transport, application work, and settlement:

```text
producer send -> receive -> process(handler) -> acknowledge / retry / dead-letter / offset commit
```

- A non-empty receive creates an `ActivityKind.Client` activity for the receive RPC or long poll. Successful empty
  polls do not create receive activities; failed receives do.
- Each automatic handler invocation creates an `ActivityKind.Consumer` process activity. Its parent is the receive
  activity, and propagated Producer contexts are represented as links. If no receive context exists, a propagated
  Producer context becomes the process parent.
- Acknowledgement, retry or dead-letter work, and offset commits create separate settlement operations. Handler
  success therefore does not by itself prove that Broker settlement completed.

Handler exceptions, timeouts, `Retry`, `DeadLetter`, and partial Remoting success mark process telemetry as
unsuccessful and record the relevant outcome; exceptions also record `error.type`. A later settlement failure is
reported on the settlement operation rather than rewriting the completed process activity.

Both protocol meters emit `rocketmq.client.operations`, `rocketmq.client.messages`, and
`rocketmq.client.operation.duration`. The protocol-specific meter identifies the client boundary; measurements carry
operation, destination, consumer group, success, and error attributes as applicable. Histogram views, sampling,
cardinality limits, and exporters remain application policy.

## Handler and failure semantics

`IGrpcPushMessageHandler.HandleAsync` handles one `GrpcMessageView` and returns `ConsumeResult`:

- `Success` acknowledges the message.
- `Retry` makes it available for redelivery; delivery-attempt limits can move it to the dead-letter queue.
- `DeadLetter` requests immediate dead-letter forwarding.

Corrupted gRPC messages do not reach the application handler. Ordinary corrupted messages become available for
redelivery, while corrupted FIFO messages are dead-lettered before the next group member is released. If a non-FIFO
handler exceeds `ConsumeTimeout`, its token is cancelled, its late result is ignored, and redelivery can overlap code
that ignored cancellation. Handlers must honor cancellation and remain idempotent.

`IRemotingPushMessageHandler.HandleAsync` receives an `IReadOnlyList<RemotingMessageView>` and a
`RemotingPushConsumeContext`. Returning `Success` confirms the complete batch by default. For concurrent non-FIFO
batches, set `AckIndex` to the zero-based last accepted index before returning `Success` to confirm a contiguous
prefix and retry only the tail; `-1` confirms none. `Retry` and `DeadLetter` ignore `AckIndex` and apply to the whole
batch. `DelayLevelWhenNextConsume` controls retry timing or direct dead-lettering.

Remoting FIFO and orderly paths deliver singleton lists and do not support partial batch confirmation. Broadcasting
has no Broker retry or dead-letter flow. A concurrent clustered non-FIFO batch that exceeds `ConsumeTimeout` is
requested for redelivery; code that ignores cancellation can overlap that redelivery. Failed Remoting
retry/dead-letter settlement is retried locally without invoking the handler again, and the per-queue contiguous
offset watermark does not advance across the unresolved gap.

See the [gRPC Consumer model](../../../../docs/en-US/grpc/consumer-model.md) and
[Remoting transport and roles](../../../../docs/en-US/remoting/transport-and-client-roles.md) for the complete delivery,
concurrency, and offset rules. The [OpenTelemetry design note](../../../../docs/en-US/architecture/opentelemetry-instrumentation.md)
defines span attributes, links, and metric tags.

## Configuration and run

The application-owned settings are `OpenTelemetry:ServiceName` and `OpenTelemetry:OtlpEndpoint`; override them with
`OpenTelemetry__ServiceName` and `OpenTelemetry__OtlpEndpoint`. The endpoint must be an absolute HTTP or HTTPS URI,
and this project exports with OTLP/gRPC. `appsettings.json` keeps only the environment-dependent RocketMQ client
connections under `RocketMQ:Grpc:Client` and `RocketMQ:Remoting:Client`. Consumer groups, subscriptions, concurrency,
timeouts, retries, and batch sizes are explicit beside each role registration in `Program.cs`.

The defaults require Docker Compose and the .NET SDK selected by `global.json`. They target the supplied RocketMQ
Proxy at `localhost:8081`, NameServer at `localhost:9876`, and OTLP/gRPC collector at `http://127.0.0.1:4317`.
The RocketMQ environment's initializer creates `eventhorizon-test-topic` before its `up --wait` command returns.

From the repository root, start the shared environments and then the Consumer integration:

```shell
docker compose -f test-environments/rocketmq/compose.yaml up -d --wait
docker compose -f test-environments/otel-lgtm/compose.yaml up -d --wait
dotnet run --project samples/opentelemetry/GenericHost/ConsumerWebApi
```

With the default launch profile, Swagger is available at [http://localhost:5242/swagger](http://localhost:5242/swagger).
`GET /health` verifies only that the web host is responding. `GET /consumers` lists the configured topic and the two
Consumer role types; it does not probe Broker connectivity. The host starts both registered Consumer roles during
application startup and stops them during host shutdown, so application code must not call their lifecycle methods.

In another terminal, start the [Producer integration](../ProducerWebApi/README.md). Its Swagger UI at
`http://localhost:5241/swagger` can send through either `POST /messages/grpc` or `POST /messages/remoting`. A message
sent after this Consumer host starts is processed independently by the gRPC and Remoting Consumer groups, producing
their corresponding console log entries. After the exporter flushes, use the Consumer dashboard described in the
[OpenTelemetry overview](../../README.md) to inspect receive, process, and settlement telemetry.
