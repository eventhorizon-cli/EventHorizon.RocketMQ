# Grafana OTEL LGTM 测试环境

[所有测试环境](../README.zh-CN.md) | [English](README.md)

此目录提供一套本地 [Grafana OTEL LGTM](https://hub.docker.com/r/grafana/otel-lgtm) 环境，用于手动验证本仓库客户端
发出的 OpenTelemetry 数据。它启动一个一体化容器，包含 OpenTelemetry Collector、Grafana、Tempo、Prometheus、Loki 和
Pyroscope。该环境仅用于开发和诊断，不是生产级可观测性部署方案。

此环境只接收遥测数据。它不会启动 RocketMQ，也不会改动应用中的 OpenTelemetry SDK 配置。需要手动测试 RocketMQ 时，
请另外启动 [`rocketmq`](../rocketmq) 等 RocketMQ 环境。

| 服务 | 宿主机端点 | 用途 |
| --- | --- | --- |
| Grafana | [http://127.0.0.1:3000](http://127.0.0.1:3000) | 查看 traces、metrics、logs 和 profiles。 |
| OTLP/gRPC | `http://127.0.0.1:4317` | 通过 gRPC 接收 OpenTelemetry 信号。 |
| OTLP/HTTP | `http://127.0.0.1:4318` | 通过 HTTP/protobuf 接收 OpenTelemetry 信号。 |

所有公开端口都绑定到 `127.0.0.1`，仅用于本地开发。

## 启动与检查

在本目录执行以下命令；若从仓库根目录执行，请额外传入 `-f test-environments/otel-lgtm/compose.yaml`。首次运行会拉取
固定版本且支持多架构的 `grafana/otel-lgtm:0.29.0` 镜像。

```shell
docker compose config --quiet
docker compose up -d --wait
docker compose ps
curl --fail http://127.0.0.1:3000/api/health
```

访问 [Grafana](http://127.0.0.1:3000)，使用镜像的本地默认凭据登录：

```text
用户名：admin
密码：admin
```

在 Grafana Explore 中使用 Tempo 数据源查看 traces，使用 Prometheus 数据源查看 metrics。

## 预置 Grafana 资源

Compose 会预置以下 Grafana 资源，无需在 UI 中手动设置：

| 资源 | Grafana UID | 用途 |
| --- | --- | --- |
| Prometheus 数据源 | `rocketmq-otel-prometheus` | 查询通过 OTLP 接收的 RocketMQ 客户端 metrics。 |
| Tempo 数据源 | `rocketmq-otel-tempo` | 使用 TraceQL 搜索 RocketMQ 客户端 traces。 |
| Producer dashboard | `rocketmq-client-observability` | 查看发送量、发送速率、P95 发送时延和 Producer trace。 |
| Consumer dashboard | `rocketmq-consumer-observability` | 查看 receive/process/settle、处理时延、失败和 Consumer trace。 |

数据到达后，发送侧访问
[Producer dashboard](http://127.0.0.1:3000/d/rocketmq-client-observability/rocketmq-producer-opentelemetry)，处理侧访问
[Consumer dashboard](http://127.0.0.1:3000/d/rocketmq-consumer-observability/rocketmq-consumer-opentelemetry)。每个预置 dashboard 的
trace 链接都会在 Tempo 中固定筛选其对应的 `service.name`。

如需完成一次本地验证，请启动通用 RocketMQ 环境和仓库内置的两个 OpenTelemetry Web 项目。RocketMQ 环境的一次性初始化服务会在
第一条命令返回前创建共享的 `eventhorizon-test-topic`：

```shell
docker compose -f test-environments/rocketmq/compose.yaml up -d --wait
docker compose -f test-environments/otel-lgtm/compose.yaml up -d --wait
```

在一个终端启动 Consumer Web API（`http://localhost:5242`）；其中的 gRPC 和 Remoting Push Consumer 使用独立的
Consumer Group：

```shell
dotnet run --project samples/opentelemetry/ConsumerWebApi
```

在第二个终端启动 Producer Web API（`http://localhost:5241`）：

```shell
dotnet run --project samples/opentelemetry/ProducerWebApi
```

通过 Producer 的 gRPC 和 Remoting 发送路由发布类似 `{"message":"Hello from Grafana."}` 的请求。Producer 会固定共享 Topic 和
tag，因此请求模型不接受这两个字段。两个 host 会将 HTTP 和 RocketMQ 客户端遥测数据导出到此 Compose 环境的 OTLP/gRPC 端点，且
使用不同的服务名：`eventhorizon-rocketmq-otel-producer` 和 `eventhorizon-rocketmq-otel-consumer`。发送遥测请使用 Producer dashboard，
接收、处理和确认遥测请使用 Consumer dashboard。共享 SDK 和 Grafana 设置请参阅
[OpenTelemetry 示例总览](../../samples/opentelemetry/README.zh-CN.md)；两个 host 的接口说明请参阅
[Producer Web API 说明](../../samples/opentelemetry/ProducerWebApi/README.zh-CN.md) 和
[Consumer Web API 说明](../../samples/opentelemetry/ConsumerWebApi/README.zh-CN.md)。

## 连接手动客户端测试

在应用的 OpenTelemetry SDK 中订阅待测试协议对应的 RocketMQ source 和 meter，并配置 OTLP exporter。客户端包负责产生
遥测数据；SDK 和 exporter 的配置由应用负责。

| 协议 | Activity source | Meter |
| --- | --- | --- |
| gRPC | `EventHorizon.RocketMQ.Grpc` | `EventHorizon.RocketMQ.Grpc` |
| Classic Remoting | `EventHorizon.RocketMQ.Remoting` | `EventHorizon.RocketMQ.Remoting` |

使用 OTLP/HTTP exporter 时，请在启动应用前设置标准端点：

```shell
export OTEL_EXPORTER_OTLP_PROTOCOL=http/protobuf
export OTEL_EXPORTER_OTLP_ENDPOINT=http://127.0.0.1:4318
export OTEL_SERVICE_NAME=eventhorizon-rocketmq-manual
```

HTTP receiver 接受 `/v1/traces` 和 `/v1/metrics` 等标准信号路径。使用 OTLP/gRPC exporter 的应用应改为连接
`http://127.0.0.1:4317`。

针对本地 RocketMQ 环境运行 Producer 和 Consumer。在 Grafana 中，通过 Tempo 查找 RocketMQ 的 `send`、`receive`、
`process` 和 `settle` 操作；通过 Prometheus 查询 `rocketmq.client.operations`、`rocketmq.client.messages` 和
`rocketmq.client.operation.duration` 指标。指标何时可见取决于应用配置的导出间隔。

## 数据与重置

Compose stack 会将后端数据保存在 `lgtm-data` Docker 命名卷中。停止服务但保留数据：

```shell
docker compose down
```

删除容器和全部本地已采集遥测数据：

```shell
docker compose down -v --remove-orphans
```

请不要与已经使用 `3000`、`4317` 或 `4318` 端口的本地 Grafana 或 OTLP collector 同时运行此环境。
