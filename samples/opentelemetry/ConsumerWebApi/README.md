# OpenTelemetry Consumer Web API sample

[OpenTelemetry samples](../README.md) | [Simplified Chinese](README.zh-CN.md)

This ASP.NET Core Minimal API runs the gRPC and classic Remoting Push consumers in one host, using separate
protocol-specific consumer groups. Both subscribe to the shared standard topic `eventhorizon-test-topic` and process
every tag. The host uses scoped message handlers, so each gRPC delivery and each Remoting batch has its own
dependency-injection scope.

The [OpenTelemetry sample overview](../README.md) covers the shared SDK, OTLP, Compose, and Grafana setup. Use the
separate producer Web API to publish messages after this consumer host has started.

## Run

After starting the environments described in the overview, run this project from the repository root:

```shell
dotnet run --project samples/opentelemetry/ConsumerWebApi
```

Swagger is available at [http://localhost:5242/swagger](http://localhost:5242/swagger) when the HTTP launch profile
is used. For a predictable command-line address, disable that profile:

```shell
ASPNETCORE_URLS=http://127.0.0.1:5242 \
  dotnet run --no-launch-profile --project samples/opentelemetry/ConsumerWebApi
```

The two consumers are hosted services. They start long polling during application startup and log each successful
message handler invocation. A gRPC message whose body fails integrity validation is returned as `DeadLetter`; all
other received messages wait for a random 250-1000 ms before being acknowledged with `Success`. The intentional delay
makes the `process` operation duration easy to inspect in the bundled
[Consumer dashboard](http://127.0.0.1:3000/d/rocketmq-consumer-observability/rocketmq-consumer-opentelemetry), which
uses sub-second duration buckets for this sample.

## Routes

| Method | Route | Success response |
| --- | --- | --- |
| `GET` | `/health` | `200 OK` when the consumer web host is running. |
| `GET` | `/consumers` | `200 OK` with the subscribed topic and `IGrpcPushConsumer` / `IRemotingPushConsumer` types. |

Example `GET /consumers` response:

```json
{
  "topic": "eventhorizon-test-topic",
  "grpcConsumer": "IGrpcPushConsumer",
  "remotingConsumer": "IRemotingPushConsumer"
}
```

## RocketMQ configuration

The project uses the following local defaults from `appsettings.json`:

| Key | Default | Purpose |
| --- | --- | --- |
| `RocketMQ:Grpc:Client:Endpoint` | `localhost:8081` | RocketMQ 5 Proxy endpoint for the gRPC Push consumer. |
| `RocketMQ:Grpc:Consumer:GroupName` | `eventhorizon-otel-grpc-consumer` | Consumer group for the gRPC Push consumer. |
| `RocketMQ:Grpc:Consumer:MaxConcurrency` | `4` | Concurrent gRPC message-handler invocations. |
| `RocketMQ:Grpc:Consumer:ConsumeTimeout` | `00:15:00` | Maximum non-FIFO gRPC handler time before retry is requested. |
| `RocketMQ:Remoting:Client:NamesrvAddr` | `localhost:9876` | NameServer for the Remoting Push consumer. |
| `RocketMQ:Remoting:Consumer:GroupName` | `eventhorizon-otel-remoting-consumer` | Consumer group for the Remoting Push consumer. |
| `RocketMQ:Remoting:Consumer:ConsumeMessageBatchSize` | `4` | Maximum messages processed by one Remoting handler invocation. |
| `RocketMQ:Remoting:Consumer:MaxConcurrency` | `4` | Concurrent Remoting message-handler invocations. |
| `RocketMQ:Remoting:Consumer:ConsumeTimeout` | `00:15:00` | Maximum non-FIFO Remoting batch-handler time before retry is requested. |
| `Sample:ServiceName` | `eventhorizon-rocketmq-otel-consumer` | OpenTelemetry `service.name` resource value. |
| `Sample:OtlpEndpoint` | `http://127.0.0.1:4317` | OTLP/gRPC collector endpoint. |

The shared Topic is fixed in the sample so that it matches the Producer Web API and the resource initializer. Override
the remaining settings through standard .NET configuration. For example, set
`RocketMQ__Grpc__Consumer__GroupName=orders-grpc-consumer` to use a different gRPC group, or
`Sample__OtlpEndpoint=http://collector.example:4317` to send telemetry to another collector. Keep the gRPC and
Remoting group names distinct: they are separate RocketMQ client protocols and use independently managed consumer
state.
