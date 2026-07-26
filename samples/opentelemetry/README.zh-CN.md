# OpenTelemetry 示例

[所有示例](../README.zh-CN.md) | [English](README.md)

这些示例演示如何由应用自行配置 OpenTelemetry SDK，并同时使用两个可独立发布的 RocketMQ 客户端。客户端包负责产生各自的
activity 和 metric；应用决定订阅哪些 source 和 meter、配置 exporter，并设置 `service.name` resource。

端到端示例由两个独立的 ASP.NET Core Minimal API host 组成。每个 host 都注册两个协议专用客户端，而 Producer 与 Consumer
进程可以独立部署和观测。

| 项目 | 本地端点 | 默认 `service.name` | 作用 |
| --- | --- | --- | --- |
| [Producer Web API](ProducerWebApi/README.zh-CN.md) | [http://localhost:5241/swagger](http://localhost:5241/swagger) | `eventhorizon-rocketmq-otel-producer` | 通过明确的 gRPC 和 classic Remoting 路由发送消息。 |
| [Consumer Web API](ConsumerWebApi/README.zh-CN.md) | [http://localhost:5242/swagger](http://localhost:5242/swagger) | `eventhorizon-rocketmq-otel-consumer` | 使用彼此独立的 Consumer Group 运行 gRPC 和 classic Remoting Push Consumer。 |

两个客户端都使用共享的普通 Topic `eventhorizon-test-topic`。仓库提供的 `rocketmq` 环境会在 Compose 报告启动完成前创建该 Topic。

## 本地验证

在仓库根目录启动通用 RocketMQ 环境，以及彼此独立的 Grafana OTEL LGTM 环境：

```shell
docker compose -f test-environments/rocketmq/compose.yaml up -d --wait
docker compose -f test-environments/otel-lgtm/compose.yaml up -d --wait
```

先启动 Consumer Web API，让 Push Consumer 先进入接收状态：

```shell
dotnet run --project samples/opentelemetry/ConsumerWebApi
```

在第二个终端启动 Producer Web API：

```shell
dotnet run --project samples/opentelemetry/ProducerWebApi
```

打开 Producer Swagger [http://localhost:5241/swagger](http://localhost:5241/swagger)，并通过两个路由分别发送消息。请求模型只接受 JSON
`message`；Producer 会为这个共享示例固定 Topic 和 tag。Consumer Swagger
[http://localhost:5242/swagger](http://localhost:5242/swagger) 提供健康状态和订阅状态。

访问 [Grafana](http://127.0.0.1:3000)，使用 `admin` / `admin` 登录。预置的
[Producer dashboard](http://127.0.0.1:3000/d/rocketmq-client-observability/rocketmq-producer-opentelemetry) 聚焦发送：消息量、发送速率、
P95 发送时延和 Producer trace。独立的
[Consumer dashboard](http://127.0.0.1:3000/d/rocketmq-consumer-observability/rocketmq-consumer-opentelemetry) 聚焦
receive/process/settle、处理时延、失败操作和 Consumer trace。

## Instrumentation 配置

两个 Web 项目都会使用下表所列的公开 source/meter 名称配置 tracing 和 metrics，并额外接入 ASP.NET Core 请求
instrumentation。默认通过 OTLP/gRPC 导出到 `http://127.0.0.1:4317`。

| 客户端 | Activity source | Meter |
| --- | --- | --- |
| RocketMQ 5 gRPC | `EventHorizon.RocketMQ.Grpc` | `EventHorizon.RocketMQ.Grpc` |
| Classic Remoting | `EventHorizon.RocketMQ.Remoting` | `EventHorizon.RocketMQ.Remoting` |

可通过 `Sample__ServiceName` 覆盖服务名以区分应用实例，或通过 `Sample__OtlpEndpoint` 改为其他 OTLP/gRPC collector。Grafana
环境也在 `http://127.0.0.1:4318` 提供 OTLP/HTTP；如需接入该 receiver，请使用 OTLP/HTTP exporter。数据源、dashboard 与重置
命令请参阅 [Grafana OTEL LGTM 说明](../../test-environments/otel-lgtm/README.zh-CN.md)。
