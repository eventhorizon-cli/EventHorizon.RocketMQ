# OpenTelemetry Producer 集成

[OpenTelemetry 示例](../README.zh-CN.md) | [English](README.md)

当消息发送需要与入口 HTTP 请求、数据库调用及其他应用工作进入同一套分布式追踪和运行指标时，应把 RocketMQ
Producer instrumentation 加入应用现有的 OpenTelemetry pipeline。该集成按协议区分：gRPC 与 classic Remoting
具有不同的客户端和传输边界，因此发布不同的 activity source 与 meter。

不要只为 RocketMQ 创建第二套 provider。进程只注册实际使用的协议扩展；resource identity、采样、processor、
metric view 和 exporter 仍由应用负责。RocketMQ package 只产生 `Activity` 与 `Meter` 信号，不替应用选择可观测性
后端。

## OpenTelemetry 注册

`AddRocketMQGrpcInstrumentation` 和 `AddRocketMQRemotingInstrumentation` 分别为 `TracerProviderBuilder` 与
`MeterProviderBuilder` 提供扩展。为 tracing 注册不会自动注册 metrics，反之亦然：

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

公共 activity source 与 meter 名称分别为 `EventHorizon.RocketMQ.Grpc` 和
`EventHorizon.RocketMQ.Remoting`。应用通常使用这些扩展方法，无需手动订阅名称。

instrumentation 注册不会创建 RocketMQ 客户端。每种协议及其 Producer 角色仍通过对应的公共 SDK API 注册：

```csharp
builder.Services
    .AddRocketMQGrpc(grpcClientSection.Bind)
    .AddGrpcProducer(static options =>
        options.SendMsgTimeout = TimeSpan.FromSeconds(3));

builder.Services
    .AddRocketMQRemoting(remotingClientSection.Bind)
    .AddRemotingProducer(static options =>
    {
        options.GroupName = "eventhorizon-otel-remoting-producer";
        options.SendMsgTimeout = TimeSpan.FromSeconds(3);
    });
```

直接注入 `IGrpcProducer` 或 `IRemotingProducer`。即使两种协议向同一个 OpenTelemetry provider 导出信号，也不存在
传输无关的 Producer 抽象。

## 发送信号与上下文传播

Producer 发送会在当前应用 Activity 下创建 `ActivityKind.Producer` Activity。对应协议的 activity source 存在
listener 时，客户端会把当前分布式上下文注入出站消息属性，并保留消息中已有的传播字段。配套的自动 Consumer
随后可以跨服务边界把 process Activity 与该 Producer 上下文关联起来。

gRPC instrumentation 覆盖 `IGrpcProducer` 的所有发送路径，包括事务发送。Remoting instrumentation 覆盖普通、
响应、批量和单向发送路径。两种协议的 meter 都会产生：

| Instrument | 含义 |
| --- | --- |
| `rocketmq.client.operations` | 按操作和结果标记的已完成客户端操作数。 |
| `rocketmq.client.messages` | 该操作处理的消息数。 |
| `rocketmq.client.operation.duration` | 以秒计的操作耗时。 |

发送 Activity 和指标描述的是客户端操作；拿到发送回执不代表 Consumer 已经处理消息。采样、直方图 bucket、基数
限制、保留和导出行为仍由应用决定。

## 失败语义

SDK 发送抛出异常时，Activity 会标记为 error，Activity 和失败操作指标都会记录 `error.type`。应用应将取消信号传给
`SendAsync`；instrumentation 不会把取消或超时的发送放入持久化本地 outbox。重试策略、不确定结果和幂等性仍由
应用负责。

classic Remoting 还会通过 `RemotingSendResult.Status` 返回部分持久性结果，例如 `FlushDiskTimeout`、
`FlushSlaveTimeout` 或 `SlaveNotAvailable`。这些是协议返回值，不是异常，因此调用方必须检查 `Status`；存在一条已
完成的发送 Activity 不能替代该检查。单向发送完成同样不代表 Broker 已经存储消息。

Trace 上下文注入需要 activity listener。关闭 tracing 后仍可独立采集 metrics。客户端不会覆盖消息中已有的传播字段。

完整的 span 属性、link 和 metric tag 请参阅
[OpenTelemetry 设计说明](../../../docs/zh-CN/architecture/opentelemetry-instrumentation.md)。Producer 消息和回执语义
仍分别由 [gRPC 指南](../../../src/EventHorizon.RocketMQ.Grpc/README.zh-CN.md)与
[Remoting 指南](../../../src/EventHorizon.RocketMQ.Remoting/README.zh-CN.md)说明。

## 配置与运行

应用自有配置为 `OpenTelemetry:ServiceName` 和 `OpenTelemetry:OtlpEndpoint`。Endpoint 必须是绝对 HTTP 或 HTTPS
URI；本项目使用 OTLP/gRPC 导出。可通过 `OpenTelemetry__ServiceName` 和 `OpenTelemetry__OtlpEndpoint` 覆盖。
RocketMQ 连接配置仍放在协议专用的 `RocketMQ:Grpc` 和 `RocketMQ:Remoting` 配置节；固定的 Producer 行为直接写在
各自的注册代码旁。

启动共享 RocketMQ 与 Grafana OTEL LGTM 环境，再运行 Producer 集成：

```shell
docker compose -f test-environments/rocketmq/compose.yaml up -d --wait
docker compose -f test-environments/otel-lgtm/compose.yaml up -d --wait
dotnet run --project samples/opentelemetry/ProducerWebApi
```

可运行 host 通过 Swagger 分别暴露 gRPC 与 Remoting 发送操作。使用
[Consumer 集成](../ConsumerWebApi/README.zh-CN.md)观察接收、处理和结算信号；仓库提供的 dashboard 与完整本地拓扑
请参阅 [OpenTelemetry 总览](../README.zh-CN.md)。
