# OpenTelemetry Producer Web API sample

[OpenTelemetry samples](../README.md) | [Simplified Chinese](README.zh-CN.md)

This ASP.NET Core Minimal API is the producer-side demonstration for the repository's RocketMQ OpenTelemetry
instrumentation. It registers one gRPC producer and one classic Remoting producer in the same host, but keeps them
behind explicit protocol-specific routes. It does not send messages in a background loop. Run the separate
[Consumer Web API](../ConsumerWebApi/README.md) to observe the consumer side of the trace.

The [OpenTelemetry sample overview](../README.md) covers the shared SDK, OTLP, Compose, and Grafana setup.
Use the [Producer dashboard](http://127.0.0.1:3000/d/rocketmq-client-observability/rocketmq-producer-opentelemetry)
to inspect send volume, rate, duration, and traces.

## Run

After starting the environments described in the overview, run this project from the repository root:

```shell
dotnet run --project samples/opentelemetry/ProducerWebApi
```

Swagger is available at [http://localhost:5241/swagger](http://localhost:5241/swagger) when the HTTP launch profile
is used. For a predictable command-line address, disable that profile:

```shell
ASPNETCORE_URLS=http://127.0.0.1:5241 \
  dotnet run --no-launch-profile --project samples/opentelemetry/ProducerWebApi
```

## Routes

| Method | Route | Client | Success response |
| --- | --- | --- | --- |
| `GET` | `/health` | None | `200 OK` when the sample host is running. |
| `POST` | `/messages/grpc` | `IGrpcProducer` | gRPC message ID, topic, and offset. |
| `POST` | `/messages/remoting` | `IRemotingProducer` | Remoting message ID, topic, Broker, queue, offset, and send status. |

Both send routes accept the same optional JSON body. The sample always sends to the shared Topic
`eventhorizon-test-topic` with the `observability` tag; those values are deliberately fixed so that the paired
consumer sample receives every request. Only the message text is accepted:

```json
{
  "message": "Hello from curl."
}
```

The supplied [`rocketmq` environment](../../../test-environments/rocketmq/README.md) creates the shared standard
sample topic during startup, so the following requests are ready to run. Each request generates a distinct RocketMQ
message key. `message` is required; a missing, empty, or whitespace-only value returns a `400` validation problem.

Send through both clients with:

```shell
curl --request POST http://127.0.0.1:5241/messages/grpc \
  --header 'Content-Type: application/json' \
  --data '{"message":"Hello through gRPC."}'

curl --request POST http://127.0.0.1:5241/messages/remoting \
  --header 'Content-Type: application/json' \
  --data '{"message":"Hello through Remoting."}'
```

Example gRPC response:

```json
{
  "messageId": "01...",
  "topic": "eventhorizon-test-topic",
  "offset": 0
}
```

Example Remoting response:

```json
{
  "messageId": "C0A801...",
  "topic": "eventhorizon-test-topic",
  "brokerName": "broker-a",
  "queueId": 0,
  "queueOffset": 0,
  "status": "SendOk"
}
```

Failures to reach RocketMQ are logged and returned as `503 Service Unavailable`; a cancelled HTTP request is allowed
to cancel the matching producer operation.

## RocketMQ configuration

The project uses the following local defaults from `appsettings.json`:

| Key | Default | Purpose |
| --- | --- | --- |
| `RocketMQ:Grpc:Client:Endpoint` | `localhost:8081` | RocketMQ 5 Proxy endpoint for `/messages/grpc`. |
| `RocketMQ:Grpc:Producer:SendMsgTimeout` | `00:00:03` | gRPC producer send timeout. |
| `RocketMQ:Remoting:Client:NamesrvAddr` | `localhost:9876` | NameServer for `/messages/remoting`. |
| `RocketMQ:Remoting:Producer:GroupName` | `eventhorizon-otel-remoting-producer` | Classic Remoting producer group. |
| `RocketMQ:Remoting:Producer:SendMsgTimeout` | `00:00:03` | Remoting producer send timeout. |
| `Sample:ServiceName` | `eventhorizon-rocketmq-otel-producer` | Resource service name exported with traces and metrics. |
| `Sample:OtlpEndpoint` | `http://127.0.0.1:4317` | OTLP/gRPC collector endpoint. |

Override any setting through standard .NET configuration. For example, target a different Proxy with
`RocketMQ__Grpc__Client__Endpoint=proxy.example:8081`. The two client registrations remain protocol-specific and
independently configurable.
