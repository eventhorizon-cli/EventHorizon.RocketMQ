# OpenTelemetry 埋点

[English](../../en-US/architecture/opentelemetry-instrumentation.md) | [简体中文](opentelemetry-instrumentation.md)

## 决策

每个协议 Package 分别拥有自己的 OpenTelemetry 埋点。gRPC 实现在
[`src/EventHorizon.RocketMQ.Grpc/Instrumentation`](../../../src/EventHorizon.RocketMQ.Grpc/Instrumentation)，classic
Remoting 实现在
[`src/EventHorizon.RocketMQ.Remoting/Instrumentation`](../../../src/EventHorizon.RocketMQ.Remoting/Instrumentation)。
不引入第三个生产 Package，两个协议 Package 也不相互引用。

客户端只提供 Activity 和 Meter 埋点。OpenTelemetry SDK、资源标识、采样器、processor 与 exporter 由应用负责。
这样两个协议 Package 可以独立发布，应用也能自行选择 OTLP、Prometheus、console exporter 或其他受支持的后端，
而不需要让客户端依赖某一个 exporter Package。

## 注册方式

`AddRocketMQGrpc` 和 `AddRocketMQRemoting` 通过各自协议专用的内部抽象注册 telemetry 实现，并通过 `AddMetrics`
注册标准 Microsoft `IMeterFactory`。应用不需要直接注册或解析这些内部 telemetry interface。

应用配置 OpenTelemetry SDK 时，订阅公开的 source 与 meter 名称即可：

```csharp
builder.Services.AddOpenTelemetry()
    .WithTracing(tracing => tracing
        .AddRocketMQGrpcInstrumentation()
        .AddRocketMQRemotingInstrumentation())
    .WithMetrics(metrics => metrics
        .AddRocketMQGrpcInstrumentation()
        .AddRocketMQRemotingInstrumentation());
```

应用只使用一种协议时，只需添加该协议对应的 source 和 meter。SDK 自带埋点与 exporter 应在应用边界添加。
[OpenTelemetry 示例](../../../samples/opentelemetry/README.zh-CN.md)展示了如何通过 OTLP 接入仓库提供的
Grafana 环境。

`AddRocketMQGrpcInstrumentation` 和 `AddRocketMQRemotingInstrumentation` 同时提供
`TracerProviderBuilder` 与 `MeterProviderBuilder` 的 overload，是与 `AddAspNetCoreInstrumentation` 同类的协议专用
OpenTelemetry instrumentation 扩展。

## 示例拓扑

仓库中的 OpenTelemetry 演示将 Producer 与 Consumer 进程拆开，以便跨服务边界查看完整的消息路径：

| 项目 | 端点 | 默认 `service.name` | 职责 |
| --- | --- | --- | --- |
| [Producer Web API](../../../samples/opentelemetry/ProducerWebApi/README.zh-CN.md) | `http://localhost:5241` | `eventhorizon-rocketmq-otel-producer` | 通过明确的 gRPC 和 classic Remoting 路由发送消息。 |
| [Consumer Web API](../../../samples/opentelemetry/ConsumerWebApi/README.zh-CN.md) | `http://localhost:5242` | `eventhorizon-rocketmq-otel-consumer` | 使用独立的 Consumer Group 运行 gRPC 和 classic Remoting Push Consumer。 |

仓库提供的 `rocketmq` 环境会在启动完成前创建共享的 `eventhorizon-test-topic`。Producer 请求模型只接受 `message`，其 Topic 和
tag 会为该示例固定。两个 Web host 默认都会导出到本地 Grafana OTEL LGTM collector，同时 `Sample__ServiceName` 和
`Sample__OtlpEndpoint` 仍由应用自行配置。

## Trace 模型

埋点遵循 RocketMQ 官方 OpenTelemetry instrumentation 使用的消息模型：

```text
应用工作 -> send -> receive -> process
                         \-> 生产端上下文 link
```

- Producer 发送会创建 `Producer` Activity，并把当前分布式上下文注入消息属性。
- 非空 receive 会创建 `Client` Activity，记录 receive RPC 或长轮询耗时，并作为自动 handler 工作的父节点。
- 自动 Push 或 LitePush handler 会创建 `Consumer` process Activity。receive Activity 是它的父节点，生产端传播的
  上下文作为 Activity link；如果没有 receive 上下文，则生产端上下文直接成为 process 的父节点。
- Pull 和 Simple API 会把消息交给应用代码。它们会创建 receive telemetry，但不会创建 process Activity，因为客户端
  并不拥有应用处理逻辑的边界。

成功但为空的轮询不会产生 receive span。失败的 receive 操作会创建错误 Activity。只有对应 Activity source 存在
listener 时才会注入上下文，并且不会覆盖消息中已有的传播字段。

## 覆盖范围

| 操作 | gRPC | classic Remoting |
| --- | --- | --- |
| Producer 发送 | 所有 `IGrpcProducer` 发送路径，包括事务消息发送。 | 普通、reply、batch 与 one-way 发送路径。 |
| Consumer 接收 | Simple、Push 与 LitePush 的 receive engine。 | Pull、LitePull、Push 的 receive engine，以及 POP receive。 |
| 自动 handler 处理 | Push 与 LitePush handler。 | Push handler。 |
| Consumer 完结操作 | 确认、负确认和转发死信。 | 位点提交、重试/死信 send-back、POP 确认和 POP 不可见时间变更。 |

Activity 使用 OpenTelemetry 消息语义约定属性，例如 `messaging.system`、`messaging.destination.name`、
`messaging.consumer.group.name`、消息 ID、分区 ID、body 大小和 batch 大小。失败时会把 Activity 状态设为 error，
并记录 `error.type`。

## 指标

两个协议 meter 都会发出以下 instruments：

| Instrument | Unit | 含义 |
| --- | --- | --- |
| `rocketmq.client.operations` | `{operation}` | 已完成的客户端操作数量。 |
| `rocketmq.client.messages` | `{message}` | 该操作处理的消息数量。 |
| `rocketmq.client.operation.duration` | `s` | 客户端操作耗时。 |

指标带有消息系统、操作名称与类型、目标地址和成功状态等 tag。失败操作还会带上错误类型。即使未启用 tracing，也可以采集
metrics。

## 约束

- 当 tracing 和 metrics 都没有 listener 时，埋点不会创建 Activity，也不会注入传播字段。
- gRPC 和 Remoting 的 source 与 meter 名称不同，因为这两个协议 Package 本身就是独立的客户端边界。
- 原有的 RocketMQ protocol telemetry session 仍属于各协议 `Protocol` 目录中的控制面行为，不是 OpenTelemetry
  instrumentation。

## 延伸阅读

- [依赖注入与生命周期](dependency-injection-and-lifetimes.md)
- [gRPC 消费模型](../grpc/consumer-model.md)
- [classic Remoting 传输与客户端角色](../remoting/transport-and-client-roles.md)
- [OpenTelemetry 示例](../../../samples/opentelemetry/README.zh-CN.md)
