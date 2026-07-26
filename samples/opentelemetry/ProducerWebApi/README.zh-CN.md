# OpenTelemetry Producer Web API 示例

[OpenTelemetry 示例](../README.zh-CN.md) | [English](README.md)

这个 ASP.NET Core Minimal API 是本仓库 RocketMQ OpenTelemetry instrumentation 的 Producer 侧示例。它在同一个 host 中注册一个
gRPC Producer 和一个 classic Remoting Producer，但通过明确的协议专用路由分别调用。它不会在后台循环发送消息。运行独立的
[Consumer Web API](../ConsumerWebApi/README.zh-CN.md)，即可查看 trace 的 Consumer 侧。

共享的 SDK、OTLP、Compose 与 Grafana 配置请参阅 [OpenTelemetry 示例总览](../README.zh-CN.md)。
发送量、速率、时延和 trace 请在
[Producer dashboard](http://127.0.0.1:3000/d/rocketmq-client-observability/rocketmq-producer-opentelemetry) 中查看。

## 运行

先按总览中的说明启动环境，再从仓库根目录运行本项目：

```shell
dotnet run --project samples/opentelemetry/ProducerWebApi
```

使用 HTTP launch profile 时，Swagger 位于 [http://localhost:5241/swagger](http://localhost:5241/swagger)。若需要可预测的
命令行地址，请禁用该 profile：

```shell
ASPNETCORE_URLS=http://127.0.0.1:5241 \
  dotnet run --no-launch-profile --project samples/opentelemetry/ProducerWebApi
```

## 路由

| 方法 | 路由 | 客户端 | 成功响应 |
| --- | --- | --- | --- |
| `GET` | `/health` | 无 | host 正常运行时返回 `200 OK`。 |
| `POST` | `/messages/grpc` | `IGrpcProducer` | gRPC message ID、topic 和 offset。 |
| `POST` | `/messages/remoting` | `IRemotingProducer` | Remoting message ID、topic、Broker、队列、offset 和发送状态。 |

两个发送路由接受相同的可选 JSON 请求体。示例始终发送到共享 Topic `eventhorizon-test-topic`，并使用
`observability` tag；这两个值会固定，以确保配套的 Consumer 示例可以收到每一条请求。请求只接收消息文本：

```json
{
  "message": "Hello from curl."
}
```

仓库提供的 [`rocketmq` 环境](../../../test-environments/rocketmq/README.zh-CN.md)会在启动时创建共享的普通示例 Topic，
因此下面的请求可以直接运行。每次请求都会生成独立的 RocketMQ message key。`message` 为必填字段；缺失、空字符串或仅含空白字符时会返回
`400` validation problem。

通过两个客户端分别发送：

```shell
curl --request POST http://127.0.0.1:5241/messages/grpc \
  --header 'Content-Type: application/json' \
  --data '{"message":"Hello through gRPC."}'

curl --request POST http://127.0.0.1:5241/messages/remoting \
  --header 'Content-Type: application/json' \
  --data '{"message":"Hello through Remoting."}'
```

gRPC 响应示例：

```json
{
  "messageId": "01...",
  "topic": "eventhorizon-test-topic",
  "offset": 0
}
```

Remoting 响应示例：

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

无法连接 RocketMQ 时会记录错误并返回 `503 Service Unavailable`；如果 HTTP 请求被取消，对应的 Producer 操作也会被取消。

## RocketMQ 配置

项目在 `appsettings.json` 中提供以下本地默认值：

| Key | 默认值 | 用途 |
| --- | --- | --- |
| `RocketMQ:Grpc:Client:Endpoint` | `localhost:8081` | `/messages/grpc` 使用的 RocketMQ 5 Proxy 端点。 |
| `RocketMQ:Grpc:Producer:SendMsgTimeout` | `00:00:03` | gRPC Producer 发送超时。 |
| `RocketMQ:Remoting:Client:NamesrvAddr` | `localhost:9876` | `/messages/remoting` 使用的 NameServer。 |
| `RocketMQ:Remoting:Producer:GroupName` | `eventhorizon-otel-remoting-producer` | classic Remoting Producer Group。 |
| `RocketMQ:Remoting:Producer:SendMsgTimeout` | `00:00:03` | Remoting Producer 发送超时。 |
| `Sample:ServiceName` | `eventhorizon-rocketmq-otel-producer` | traces 和 metrics 导出的 resource service name。 |
| `Sample:OtlpEndpoint` | `http://127.0.0.1:4317` | OTLP/gRPC collector 端点。 |

可通过标准 .NET 配置覆盖任意设置。例如，使用
`RocketMQ__Grpc__Client__Endpoint=proxy.example:8081` 连接其他 Proxy。两个客户端注册仍保持协议专用，并可分别配置。
