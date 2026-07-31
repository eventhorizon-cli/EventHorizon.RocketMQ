# Grafana OTEL LGTM test environment

[All test environments](../README.md) | [Simplified Chinese](README.zh-CN.md)

This directory defines a local [Grafana OTEL LGTM](https://hub.docker.com/r/grafana/otel-lgtm) environment for
manually validating the OpenTelemetry data emitted by this repository's clients. It starts one all-in-one container
with an OpenTelemetry Collector, Grafana, Tempo, Prometheus, Loki, and Pyroscope. It is a development and diagnostic
tool, not a production observability deployment.

The environment receives telemetry only. It does not start RocketMQ or alter any application's OpenTelemetry SDK
configuration. Start a RocketMQ environment such as [`rocketmq`](../rocketmq) separately when the manual test needs
one.

| Service | Host endpoint | Purpose |
| --- | --- | --- |
| Grafana | [http://127.0.0.1:3000](http://127.0.0.1:3000) | Inspect traces, metrics, logs, and profiles. |
| OTLP/gRPC | `http://127.0.0.1:4317` | Receive OpenTelemetry signals over gRPC. |
| OTLP/HTTP | `http://127.0.0.1:4318` | Receive OpenTelemetry signals over HTTP/protobuf. |

All published ports bind to `127.0.0.1`. They are intended only for local development.

## Start and inspect

Run these commands from this directory, or add `-f test-environments/otel-lgtm/compose.yaml` when running them from
the repository root. The first run pulls the pinned multi-architecture `grafana/otel-lgtm:0.29.0` image.

```shell
docker compose config --quiet
docker compose up -d --wait
docker compose ps
curl --fail http://127.0.0.1:3000/api/health
```

Open [Grafana](http://127.0.0.1:3000) and sign in with the image's local default credentials:

```text
Username: admin
Password: admin
```

Use Grafana Explore with the Tempo data source to inspect traces and the Prometheus data source to inspect metrics.

## Preconfigured Grafana

Compose provisions the following Grafana resources without any UI setup:

| Resource | Grafana UID | Purpose |
| --- | --- | --- |
| Prometheus data source | `rocketmq-otel-prometheus` | Query RocketMQ client metrics received through OTLP. |
| Tempo data source | `rocketmq-otel-tempo` | Search RocketMQ client traces with TraceQL. |
| Producer dashboard | `rocketmq-client-observability` | Inspect send volume, send rate, P95 send duration, and producer traces. |
| Consumer dashboard | `rocketmq-consumer-observability` | Inspect receive/process/settle activity, process duration, failures, and consumer traces. |

After data arrives, open the
[Producer dashboard](http://127.0.0.1:3000/d/rocketmq-client-observability/rocketmq-producer-opentelemetry) for sends or the
[Consumer dashboard](http://127.0.0.1:3000/d/rocketmq-consumer-observability/rocketmq-consumer-opentelemetry) for processing.
Each dashboard's trace link is scoped to its corresponding `service.name` in Tempo.

For a complete local verification, start the general RocketMQ environment and the two bundled OpenTelemetry web
projects. The RocketMQ environment's one-shot initializer creates the shared `eventhorizon-test-topic` before the
first command returns:

```shell
docker compose -f test-environments/rocketmq/compose.yaml up -d --wait
docker compose -f test-environments/otel-lgtm/compose.yaml up -d --wait
```

In one terminal, start the Consumer Web API at `http://localhost:5242`; its gRPC and Remoting Push consumers use
separate consumer groups:

```shell
dotnet run --project samples/opentelemetry/GenericHost/ConsumerWebApi
```

In a second terminal, start the Producer Web API at `http://localhost:5241`:

```shell
dotnet run --project samples/opentelemetry/GenericHost/ProducerWebApi
```

Use the Producer's gRPC and Remoting send routes to publish a request such as `{"message":"Hello from Grafana."}`.
The producer fixes the shared topic and tag, so the request model does not accept them. The two hosts export HTTP and
RocketMQ client telemetry to this Compose environment's OTLP/gRPC endpoint with distinct service names:
`eventhorizon-rocketmq-otel-producer` and `eventhorizon-rocketmq-otel-consumer`. Use the Producer dashboard for send
telemetry and the Consumer dashboard for receive, processing, and settlement telemetry. The [OpenTelemetry sample overview](../../samples/opentelemetry/README.md)
describes the shared SDK and Grafana setup; the [Producer Web API guide](../../samples/opentelemetry/GenericHost/ProducerWebApi/README.md)
and [Consumer Web API guide](../../samples/opentelemetry/GenericHost/ConsumerWebApi/README.md) document the two hosts.

## Connect a manual client test

Configure the application's OpenTelemetry SDK to subscribe to the RocketMQ source and meter used by the protocol
under test, then add an OTLP exporter. The client packages emit the telemetry; the application owns SDK and exporter
configuration.

| Protocol | Activity source | Meter |
| --- | --- | --- |
| gRPC | `EventHorizon.RocketMQ.Grpc` | `EventHorizon.RocketMQ.Grpc` |
| Classic Remoting | `EventHorizon.RocketMQ.Remoting` | `EventHorizon.RocketMQ.Remoting` |

For an OTLP/HTTP exporter, set the standard endpoint before starting the application:

```shell
export OTEL_EXPORTER_OTLP_PROTOCOL=http/protobuf
export OTEL_EXPORTER_OTLP_ENDPOINT=http://127.0.0.1:4318
export OTEL_SERVICE_NAME=eventhorizon-rocketmq-manual
```

The HTTP receiver accepts the standard per-signal paths such as `/v1/traces` and `/v1/metrics`. An application using
an OTLP/gRPC exporter should instead target `http://127.0.0.1:4317`.

Run a producer and consumer against a local RocketMQ environment. In Grafana, look for the RocketMQ `send`, `receive`,
`process`, and `settle` operations in Tempo, and query the `rocketmq.client.operations`, `rocketmq.client.messages`,
and `rocketmq.client.operation.duration` metrics in Prometheus. Metric visibility depends on the application's export
interval.

## Data and reset

The Compose stack stores backend data in the `lgtm-data` Docker named volume. Stop it while keeping the data:

```shell
docker compose down
```

Remove the container and all collected local telemetry:

```shell
docker compose down -v --remove-orphans
```

Do not run this environment with another local Grafana or OTLP collector that already uses ports `3000`, `4317`, or
`4318`.
