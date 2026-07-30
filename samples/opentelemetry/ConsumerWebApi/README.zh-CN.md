# OpenTelemetry Push Consumer 集成

[OpenTelemetry 示例](../README.zh-CN.md) | [English](README.md)

当应用运行自动 Push Consumer，并需要把接收、handler 处理和结算纳入现有 OpenTelemetry trace 与 metric 时，适合
使用这种集成方式。Push 描述的是自动向应用分派消息：两种客户端都主动发起 long poll，不依赖 Broker 向应用建立
推送连接。

gRPC 与 classic Remoting 仍是两种独立 SDK 模式。gRPC Consumer 通过 RocketMQ 5 Proxy 连接；Remoting Consumer
通过 NameServer 发现路由，再连接 Broker 公布地址。生产进程只应注册实际使用的协议。在同一进程运行两者有助于
比较信号，但不代表它们的订阅、Consumer Group、重试或位点可以互换。

## Instrumentation 与角色注册

把协议 instrumentation 加入应用现有的 tracing 和 metric provider。两种 provider builder 需要分别注册：

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

`AddRocketMQGrpcInstrumentation` 和 `AddRocketMQRemotingInstrumentation` 分别订阅对应协议的公共 source 与 meter。
它们不会注册 Consumer 角色，也不会替应用选择 resource、sampler、metric view 或 exporter。

Push Consumer 通过协议专用 API 和 typed handler 注册：

```csharp
builder.Services
    .AddRocketMQGrpc(grpcClientSection.Bind)
    .AddGrpcPushConsumer<GrpcConsumerMessageHandler>(ServiceLifetime.Scoped, options =>
    {
        options.GroupName = "eventhorizon-otel-grpc-consumer";
        options.BatchSize = 16;
        options.MaxConcurrency = 4;
        options.LongPollingTimeout = TimeSpan.FromSeconds(15);
        options.Subscribe("eventhorizon-test-topic");
    });

builder.Services
    .AddRocketMQRemoting(remotingClientSection.Bind)
    .AddRemotingPushConsumer<RemotingConsumerMessageHandler>(ServiceLifetime.Scoped, options =>
    {
        options.GroupName = "eventhorizon-otel-remoting-consumer";
        options.BatchSize = 16;
        options.ConsumeMessageBatchSize = 4;
        options.MaxConcurrency = 4;
        options.LongPollingTimeout = TimeSpan.FromSeconds(5);
        options.RetryDelay = TimeSpan.FromSeconds(1);
        options.Subscribe("eventhorizon-test-topic");
    });
```

.NET Host 会启动和停止两个 Consumer 角色。Scoped gRPC handler 的每次处理尝试都会创建新的异步 DI scope；Scoped
Remoting handler 则每个批次尝试使用一个 scope。Singleton handler 会被并发投递共享，因此必须线程安全。两个自动
Consumer 角色都会从应用容器解析 typed handler 及其依赖。

## Trace 与 metric 模型

自动 Consumer 路径会区分传输、应用处理与结算：

```text
producer send -> receive -> process(handler) -> acknowledge / retry / dead-letter / offset commit
```

- 非空接收会为 receive RPC 或 long poll 创建 `ActivityKind.Client` Activity。成功的空轮询不会创建 receive
  Activity，失败的接收会创建。
- 每次自动 handler 调用会创建 `ActivityKind.Consumer` process Activity。receive Activity 是其 parent，传播的
  Producer 上下文记录为 link。没有 receive 上下文时，传播的 Producer 上下文会成为 process parent。
- 确认、重试或死信处理以及位点提交会创建独立的 settlement 操作。因此 handler 成功本身不能证明 Broker 结算完成。

handler 异常、超时、`Retry`、`DeadLetter` 以及 Remoting 部分成功会把 process telemetry 标为失败并记录对应结果；
异常还会记录 `error.type`。之后发生的结算失败记录在 settlement 操作上，不会改写已经完成的 process Activity。

两种协议的 meter 都产生 `rocketmq.client.operations`、`rocketmq.client.messages` 和
`rocketmq.client.operation.duration`。协议专用 meter 标识客户端边界；measurement 会按实际情况携带操作、目标、
Consumer Group、成功状态和错误属性。直方图 view、采样、基数限制和 exporter 仍由应用策略决定。

## Handler 与失败语义

`IGrpcPushMessageHandler.HandleAsync` 每次处理一个 `GrpcMessageView`，并返回 `ConsumeResult`：

- `Success`：确认消息。
- `Retry`：使消息可以重新投递；达到投递次数上限后可进入死信队列。
- `DeadLetter`：请求立即转发到死信队列。

损坏的 gRPC 消息不会进入应用 handler。普通损坏消息会重新变为可投递状态；FIFO 损坏消息会先进入死信队列，再
释放同组下一条消息。非 FIFO handler 超过 `ConsumeTimeout` 时，其 token 会被取消，迟到结果会被忽略；忽略取消的
代码可能与重新投递重叠，因此 handler 必须响应取消并保持幂等。

`IRemotingPushMessageHandler.HandleAsync` 接收 `IReadOnlyList<RemotingMessageView>` 和
`RemotingPushConsumeContext`。默认返回 `Success` 会确认完整批次。并发非 FIFO 批次可先把 `AckIndex` 设为最后一条
已接受消息的从零开始索引，再返回 `Success`，从而确认连续前缀并只重试尾部；`-1` 表示一条也不确认。`Retry` 与
`DeadLetter` 会忽略 `AckIndex` 并作用于整个批次。`DelayLevelWhenNextConsume` 控制重试时间或直接进入死信队列。

Remoting FIFO 与 orderly 路径只投递单消息列表，不支持批量部分确认。广播模式没有 Broker 重试或死信流程。并发
集群非 FIFO 批次超过 `ConsumeTimeout` 后会请求重新投递；忽略取消的代码可能与重新投递重叠。Remoting
重试/死信结算失败时会在本地重试，不会再次调用 handler；对应物理队列的连续位点水位不会跨过该未决缺口。

完整投递、并发和位点规则请参阅 [gRPC Consumer 模型](../../../docs/zh-CN/grpc/consumer-model.md)与
[Remoting 传输和角色](../../../docs/zh-CN/remoting/transport-and-client-roles.md)。span 属性、link 与 metric tag 由
[OpenTelemetry 设计说明](../../../docs/zh-CN/architecture/opentelemetry-instrumentation.md)定义。

## 配置与运行

应用自有配置为 `OpenTelemetry:ServiceName` 和 `OpenTelemetry:OtlpEndpoint`；可通过
`OpenTelemetry__ServiceName` 和 `OpenTelemetry__OtlpEndpoint` 覆盖。Endpoint 必须是绝对 HTTP 或 HTTPS URI，
本项目使用 OTLP/gRPC 导出。`appsettings.json` 仅在 `RocketMQ:Grpc:Client` 和
`RocketMQ:Remoting:Client` 中保留随环境变化的 RocketMQ 客户端连接信息；Consumer Group、订阅、并发、超时、
重试与批量设置都直接写在 `Program.cs` 的对应角色注册旁。

启动共享环境，再运行 Consumer 集成：

```shell
docker compose -f test-environments/rocketmq/compose.yaml up -d --wait
docker compose -f test-environments/otel-lgtm/compose.yaml up -d --wait
dotnet run --project samples/opentelemetry/ConsumerWebApi
```

可运行 host 仅通过 Swagger 暴露健康状态与 Consumer 状态；hosted Consumer 启动后就会开始处理消息。使用
[Producer 集成](../ProducerWebApi/README.zh-CN.md)发布带 trace 的消息；仓库提供的 dashboard 与本地拓扑请参阅
[OpenTelemetry 总览](../README.zh-CN.md)。
