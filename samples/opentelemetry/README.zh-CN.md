# OpenTelemetry 集成

[全部示例](../README.zh-CN.md) | [English](README.md)

gRPC 和 classic Remoting 客户端会分别产生协议专用的 Activity 与 Metric。它们不会创建 OpenTelemetry provider，
也不会替应用选择 resource、sampler、processor 或 exporter；这些决策由应用负责，应用再按实际使用的协议订阅信号。

## 适用场景

当发送、接收、处理、结算和客户端请求需要进入分布式追踪或运行指标时，应把 RocketMQ instrumentation 加入应用现有的
OpenTelemetry pipeline。对于已经导出 ASP.NET Core、数据库或其他应用 telemetry 的服务，这是常规接入方式。

不要只为 RocketMQ 创建第二套 provider。协议专用 source 和 meter 应与应用其他信号注册到同一 provider，确保 trace
上下文、resource identity、采样和 exporter 行为保持一致。

## SDK 注册

Tracing 和 Metrics 需要分别注册 instrumentation：

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

进程只需注册实际使用的协议扩展。`AddRocketMQGrpcInstrumentation` 和
`AddRocketMQRemotingInstrumentation` 都同时为 `TracerProviderBuilder` 与 `MeterProviderBuilder` 提供 overload；
注册其中一种 provider 不会隐式注册另一种。

gRPC 的公共 activity source 和 meter 名称都是 `EventHorizon.RocketMQ.Grpc`；classic Remoting 对应的名称都是
`EventHorizon.RocketMQ.Remoting`。扩展方法会订阅这些名称，应用无需接触客户端内部 telemetry service。

## 信号语义

Producer activity 使用 `ActivityKind.Producer`，消息处理使用 `ActivityKind.Consumer`；路由、分配、接收、确认、位点及
其他请求会使用与操作相符的 client kind。SDK 操作失败时，Activity 和 Metric 都会记录错误。应用 handler 工作位于
consumer processing activity 内，因此取消、重试与结算结果可以和处理过程关联。

两种客户端都会发布以下通用 instrument：

| Instrument | 含义 |
| --- | --- |
| `rocketmq.client.operations` | 按操作和结果标记的已完成客户端操作数。 |
| `rocketmq.client.messages` | 某项操作处理的消息数。 |
| `rocketmq.client.operation.duration` | 以秒计的操作耗时。 |

Histogram view、基数限制、采样、保留和 exporter 应由应用的可观测性策略决定，SDK 不替部署做这些选择。

完整 span 名称、属性、上下文传播和 metric tag 请参阅
[OpenTelemetry 设计说明](../../docs/zh-CN/architecture/opentelemetry-instrumentation.md)。

## 运行集成示例

两个 Web API 项目把 gRPC 与 Remoting 角色分别放在 Producer 和 Consumer 进程中，因此可以沿 HTTP、发送、接收、处理和
结算观察同一条消息。默认会通过 OTLP/gRPC 导出到仓库提供的 Grafana OTEL LGTM 环境。

```shell
docker compose -f test-environments/rocketmq/compose.yaml up -d --wait
docker compose -f test-environments/otel-lgtm/compose.yaml up -d --wait
dotnet run --project samples/opentelemetry/ConsumerWebApi
dotnet run --project samples/opentelemetry/ProducerWebApi
```

应用会直接读取 `OpenTelemetry:ServiceName` 和 `OpenTelemetry:OtlpEndpoint`。可通过
`OpenTelemetry__ServiceName` 与 `OpenTelemetry__OtlpEndpoint` 覆盖；RocketMQ SDK options 仍留在各协议专用的
`RocketMQ` 配置节。

两端工作流分别参阅 [Producer 集成](ProducerWebApi/README.zh-CN.md)和
[Consumer 集成](ConsumerWebApi/README.zh-CN.md)。仓库提供的 dashboard 可从
`http://127.0.0.1:3000` 的 Grafana 打开。
