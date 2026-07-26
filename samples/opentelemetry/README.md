# OpenTelemetry samples

[All samples](../README.md) | [Simplified Chinese](README.zh-CN.md)

These samples demonstrate application-owned OpenTelemetry SDK configuration for the two independently published
RocketMQ clients. The packages emit their own activities and metrics; an application chooses which sources and meters
to subscribe to, configures exporters, and controls the `service.name` resource.

The end-to-end example uses two independent ASP.NET Core Minimal API hosts. Each host registers both protocol-specific
clients, while the Producer and Consumer processes remain independently deployable and observable.

| Project | Local endpoint | Default `service.name` | Role |
| --- | --- | --- | --- |
| [Producer Web API](ProducerWebApi/README.md) | [http://localhost:5241/swagger](http://localhost:5241/swagger) | `eventhorizon-rocketmq-otel-producer` | Sends through explicit gRPC and classic Remoting routes. |
| [Consumer Web API](ConsumerWebApi/README.md) | [http://localhost:5242/swagger](http://localhost:5242/swagger) | `eventhorizon-rocketmq-otel-consumer` | Runs gRPC and classic Remoting Push consumers with separate consumer groups. |

Both clients use the shared standard topic `eventhorizon-test-topic`. The supplied `rocketmq` environment creates it
before Compose reports that startup is complete.

## Local verification

From the repository root, start the normal RocketMQ environment and the independent Grafana OTEL LGTM environment:

```shell
docker compose -f test-environments/rocketmq/compose.yaml up -d --wait
docker compose -f test-environments/otel-lgtm/compose.yaml up -d --wait
```

Start the Consumer Web API first so its Push consumers are ready to receive messages:

```shell
dotnet run --project samples/opentelemetry/ConsumerWebApi
```

In a second terminal, start the Producer Web API:

```shell
dotnet run --project samples/opentelemetry/ProducerWebApi
```

Open the Producer Swagger UI at [http://localhost:5241/swagger](http://localhost:5241/swagger) and send a message
through each route. The request model accepts only a JSON `message`; the producer fixes the topic and tag for this
shared sample.
The Consumer Swagger UI at [http://localhost:5242/swagger](http://localhost:5242/swagger) exposes its health and
subscription status.

Open [Grafana](http://127.0.0.1:3000) with `admin` / `admin`. The preconfigured
[Producer dashboard](http://127.0.0.1:3000/d/rocketmq-client-observability/rocketmq-producer-opentelemetry) focuses
on sends: message volume, send rate, P95 send duration, and producer traces. The separate
[Consumer dashboard](http://127.0.0.1:3000/d/rocketmq-consumer-observability/rocketmq-consumer-opentelemetry) focuses
on receive/process/settle activity, process duration, failed operations, and consumer traces.

## Instrumentation configuration

Both web projects configure tracing and metrics with the public source and meter names below, along with ASP.NET Core
request instrumentation. They export over OTLP/gRPC to `http://127.0.0.1:4317` by default.

| Client | Activity source | Meter |
| --- | --- | --- |
| RocketMQ 5 gRPC | `EventHorizon.RocketMQ.Grpc` | `EventHorizon.RocketMQ.Grpc` |
| Classic Remoting | `EventHorizon.RocketMQ.Remoting` | `EventHorizon.RocketMQ.Remoting` |

Override `Sample__ServiceName` to distinguish an application instance, or `Sample__OtlpEndpoint` to target a
different OTLP/gRPC collector. The Grafana environment also exposes OTLP/HTTP at `http://127.0.0.1:4318`; use an
OTLP/HTTP exporter when targeting that receiver. See the [Grafana OTEL LGTM guide](../../test-environments/otel-lgtm/README.md)
for its data sources, dashboard, and reset commands.
