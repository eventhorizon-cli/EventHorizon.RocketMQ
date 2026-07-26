# OpenTelemetry Consumer Web API 示例

[OpenTelemetry 示例](../README.zh-CN.md) | [English](README.md)

这个 ASP.NET Core Minimal API 在同一个 host 中运行 gRPC 和 classic Remoting Push Consumer，并为两个协议使用彼此独立的
Consumer Group。两者都订阅共享的普通 Topic `eventhorizon-test-topic`，消费所有 tag。host 使用 scoped message handler，
因此每条 gRPC 消息和每个 Remoting batch 都有独立的依赖注入作用域。

共享的 SDK、OTLP、Compose 与 Grafana 配置请参阅 [OpenTelemetry 示例总览](../README.zh-CN.md)。启动本 Consumer host 后，
可使用独立的 Producer Web API 发布消息。

## 运行

先按总览中的说明启动环境，再从仓库根目录运行本项目：

```shell
dotnet run --project samples/opentelemetry/ConsumerWebApi
```

使用 HTTP launch profile 时，Swagger 位于 [http://localhost:5242/swagger](http://localhost:5242/swagger)。若需要可预测的
命令行地址，请禁用该 profile：

```shell
ASPNETCORE_URLS=http://127.0.0.1:5242 \
  dotnet run --no-launch-profile --project samples/opentelemetry/ConsumerWebApi
```

两个 Consumer 都是 hosted service，会在应用启动期间开始 long polling，并记录每次成功的 message handler 调用。gRPC 消息若
无法通过 body 完整性验证，会返回 `DeadLetter`；其他收到的消息会随机等待 250-1000 ms 后以 `Success` 确认。这个刻意加入的延迟
便于在随附的
[Consumer dashboard](http://127.0.0.1:3000/d/rocketmq-consumer-observability/rocketmq-consumer-opentelemetry) 中观察 `process`
operation duration；该示例为此配置了亚秒级 duration bucket。

## 路由

| 方法 | 路由 | 成功响应 |
| --- | --- | --- |
| `GET` | `/health` | Consumer Web host 正常运行时返回 `200 OK`。 |
| `GET` | `/consumers` | 返回已订阅的 Topic，以及 `IGrpcPushConsumer` / `IRemotingPushConsumer` 类型。 |

`GET /consumers` 响应示例：

```json
{
  "topic": "eventhorizon-test-topic",
  "grpcConsumer": "IGrpcPushConsumer",
  "remotingConsumer": "IRemotingPushConsumer"
}
```

## RocketMQ 配置

项目在 `appsettings.json` 中提供以下本地默认值：

| Key | 默认值 | 用途 |
| --- | --- | --- |
| `RocketMQ:Grpc:Client:Endpoint` | `localhost:8081` | gRPC Push Consumer 使用的 RocketMQ 5 Proxy 端点。 |
| `RocketMQ:Grpc:Consumer:GroupName` | `eventhorizon-otel-grpc-consumer` | gRPC Push Consumer 的 Consumer Group。 |
| `RocketMQ:Grpc:Consumer:MaxConcurrency` | `4` | 并发执行的 gRPC message handler 数量。 |
| `RocketMQ:Grpc:Consumer:ConsumeTimeout` | `00:15:00` | 请求重试前，非 FIFO gRPC handler 允许运行的最长时间。 |
| `RocketMQ:Remoting:Client:NamesrvAddr` | `localhost:9876` | Remoting Push Consumer 使用的 NameServer。 |
| `RocketMQ:Remoting:Consumer:GroupName` | `eventhorizon-otel-remoting-consumer` | Remoting Push Consumer 的 Consumer Group。 |
| `RocketMQ:Remoting:Consumer:ConsumeMessageBatchSize` | `4` | 单次 Remoting handler 调用处理的最大消息数量。 |
| `RocketMQ:Remoting:Consumer:MaxConcurrency` | `4` | 并发执行的 Remoting message handler 数量。 |
| `RocketMQ:Remoting:Consumer:ConsumeTimeout` | `00:15:00` | 请求重试前，非 FIFO Remoting batch handler 允许运行的最长时间。 |
| `Sample:ServiceName` | `eventhorizon-rocketmq-otel-consumer` | OpenTelemetry `service.name` resource 值。 |
| `Sample:OtlpEndpoint` | `http://127.0.0.1:4317` | OTLP/gRPC collector 端点。 |

共享 Topic 会在示例中固定，以确保与 Producer Web API 和资源初始化服务保持一致。其余设置可通过标准 .NET 配置覆盖。例如，设置
`RocketMQ__Grpc__Consumer__GroupName=orders-grpc-consumer` 可以使用其他 gRPC Group，设置
`Sample__OtlpEndpoint=http://collector.example:4317` 可以将 telemetry 发送到其他 collector。请保持 gRPC 与
Remoting 的 Group 名称不同：它们属于独立的 RocketMQ client protocol，并分别维护 Consumer state。
