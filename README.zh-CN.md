# EventHorizon.RocketMQ

[![.NET Build](https://github.com/eventhorizon-cli/EventHorizon.RocketMQ/actions/workflows/dotnet-build.yml/badge.svg?branch=main)](https://github.com/eventhorizon-cli/EventHorizon.RocketMQ/actions/workflows/dotnet-build.yml)
[![NuGet gRPC](https://img.shields.io/nuget/vpre/EventHorizon.RocketMQ.Grpc.svg?label=NuGet%20gRPC)](https://www.nuget.org/packages/EventHorizon.RocketMQ.Grpc)
[![NuGet Remoting](https://img.shields.io/nuget/vpre/EventHorizon.RocketMQ.Remoting.svg?label=NuGet%20Remoting)](https://www.nuget.org/packages/EventHorizon.RocketMQ.Remoting)
[![Codecov](https://codecov.io/gh/eventhorizon-cli/EventHorizon.RocketMQ/graph/badge.svg)](https://codecov.io/gh/eventhorizon-cli/EventHorizon.RocketMQ)

[English](https://github.com/eventhorizon-cli/EventHorizon.RocketMQ/blob/main/README.md) |
[简体中文](https://github.com/eventhorizon-cli/EventHorizon.RocketMQ/blob/main/README.zh-CN.md)

这是一个**非官方的 Apache RocketMQ .NET 客户端**。

项目提供**完整的客户端功能**，支持 RocketMQ 5 protobuf/gRPC 协议和 classic Remoting 协议，覆盖 Producer、
Consumer 和 Admin API，以及事务消息、定时/延迟消息、FIFO 消息、优先级消息等能力。

当前版本提供 `net8.0` 和 `net10.0` 目标，原生集成 **Microsoft DI**，可直接接入 Generic Host、Options 和日志体系，
提供**现代化、强类型的 API 编程体验**。

> **需要应用层 EventBus？** 请查看
> [EventHorizon.RocketMQ.EventBus](https://github.com/eventhorizon-cli/EventHorizon.RocketMQ.EventBus)，它基于两个
> 协议客户端提供更简便的应用层 EventBus 服务。

客户端行为由高覆盖率单元测试保障，并通过 Docker 驱动的集成测试验证真实的 NameServer、Broker 和 Proxy
链路。

两个协议 Package 都内置 **OpenTelemetry tracing 和 metrics**。应用可以在 OpenTelemetry SDK pipeline 中添加
协议专用的 `AddRocketMQGrpcInstrumentation` 或 `AddRocketMQRemotingInstrumentation` 扩展，再自行选择 exporter
和可观测性后端。

> **请先查看[可运行示例](samples/README.zh-CN.md)。** 每个示例目录都说明所需的 RocketMQ Broker 或 Proxy
> 能力、资源准备、配置、运行命令和协议注意事项。

## 功能与状态

`✅` 表示对应功能已经实现；实际使用时，仍需满足相应的服务端能力与配置要求。

### RocketMQ 5 gRPC

有关配置方式和兼容性，请参阅 [gRPC 指南](src/EventHorizon.RocketMQ.Grpc/README.zh-CN.md) 与
[可运行示例](samples/README.zh-CN.md)。

| 功能 | 状态 | 说明 |
| --- | :---: | --- |
| 普通消息 Producer | ✅ | 使用 `IGrpcProducer.SendAsync`。 |
| FIFO 消息 Producer | ✅ | 设置 `MessageGroup`。 |
| 定时/延迟消息 Producer | ✅ | 设置 `DeliveryTimestamp`。 |
| 事务消息 Producer | ✅ | 使用 `SendTransactionAsync` 并配置事务检查器。 |
| 撤回定时/延迟消息 | ✅ | 在消息投递前使用回执中的 `RecallHandle` 撤回消息。 |
| SimpleConsumer | ✅ | `IGrpcSimpleConsumer` 提供显式接收、确认、不可见时长管理和死信队列操作。 |
| 并发消费的 PushConsumer | ✅ | `IGrpcPushConsumer` 可并发分发消息。 |
| FIFO 消费的 PushConsumer | ✅ | 每个 `MessageGroup` 内按顺序处理。 |
| 启用 FIFO 消费加速的 PushConsumer | ✅ | 不同 `MessageGroup` 可以并行处理，同一 `MessageGroup` 内仍保持顺序。 |
| 优先级消息 | ✅ | 设置 `Message.Priority`。 |
| Lite 消息 Producer 和 LitePushConsumer | ✅ | LitePush 需要使用支持 `SyncLiteSubscription` RPC 的 Proxy。 |
| Tag 和 SQL 过滤 | ✅ | SQL 过滤要求服务端启用相应配置。 |
| 运行时订阅、重试和死信处理 | ✅ | 具体支持范围取决于所用 API，由 SimpleConsumer、PushConsumer 或 LitePushConsumer 提供。 |
| Microsoft DI、Options、日志、Generic Host 生命周期，以及默认或 keyed 客户端注册 | ✅ | 使用 `AddRocketMQGrpc` 注册。 |
| OpenTelemetry tracing 和 metrics | ✅ | 使用 `AddRocketMQGrpcInstrumentation` 注册；SDK processor 和 exporter 由应用选择。 |

gRPC Push 和 LitePush 都依靠客户端主动查询分配结果并执行长轮询，并不是由 Broker 主动推送消息。

### classic Remoting

有关配置方式和兼容性，请参阅 [Remoting 指南](src/EventHorizon.RocketMQ.Remoting/README.zh-CN.md) 与
[可运行示例](samples/README.zh-CN.md)。

| 功能 | 状态 | 说明 |
| --- | :---: | --- |
| 支持普通、FIFO、定时/延迟、优先级和 Lite 消息的 Producer | ✅ | 使用 `IRemotingProducer`；目标 Broker 必须支持所选消息类型。 |
| 指定队列、队列选择器、批量发送和单向发送 | ✅ | 均由 `IRemotingProducer` 提供。 |
| 事务消息、延迟消息撤回和请求-响应 | ✅ | 需要 Broker 支持相应功能。 |
| 只读 Admin API | ✅ | `IRemotingAdmin` 查询队列、消息和位点。 |
| LitePullConsumer | ✅ | 应用从 SDK 后台接收的本地缓存轮询消息，支持订阅或手工分配、seek、暂停/恢复和提交；目前仅支持集群消费。 |
| PushConsumer | ✅ | 支持客户端分配的集群或广播消费、可选的 Broker-assigned 并发集群 PULL/POP、可部分确认前缀的并发批量回调或单消息 FIFO 分发、运行时订阅、重试和死信处理。 |
| Microsoft DI、Options、日志、Generic Host 生命周期，以及默认或 keyed 客户端注册 | ✅ | 使用 `AddRocketMQRemoting` 注册。 |
| OpenTelemetry tracing 和 metrics | ✅ | 使用 `AddRocketMQRemotingInstrumentation` 注册；SDK processor 和 exporter 由应用选择。 |

经典 Push 同样由客户端发起长轮询。`QueueAssignmentMode` 默认为使用 PULL 的 `Client`；可选的 `Broker` 值可以按
assignment 返回 PULL 或 POP，但要求运维侧准备兼容的 Broker 配置。POP 是 Push 的内部接收模式，不是独立的
公开 Consumer。SQL 过滤同样需要 Broker 启用相应配置。每个协议 Package 都有各自的
`RocketMQClientException` 基类。

## Packages

| Package | 连接目标 | 主要 API | 文档 |
| --- | --- | --- | --- |
| `EventHorizon.RocketMQ.Grpc` | RocketMQ 5 Proxy | `IGrpcProducer`、`IGrpcSimpleConsumer`、`IGrpcPushConsumer`、`IGrpcLitePushConsumer` | [gRPC 指南](https://github.com/eventhorizon-cli/EventHorizon.RocketMQ/blob/main/src/EventHorizon.RocketMQ.Grpc/README.zh-CN.md) |
| `EventHorizon.RocketMQ.Remoting` | NameServer 和 Broker | `IRemotingProducer`、`IRemotingAdmin`、`IRemotingLitePullConsumer`、`IRemotingPushConsumer` | [Remoting 指南](https://github.com/eventhorizon-cli/EventHorizon.RocketMQ/blob/main/src/EventHorizon.RocketMQ.Remoting/README.zh-CN.md) |

应用只需安装实际使用的协议 Package。两个协议 Package 在客户端 API 层面都完全自包含：公开模型位于
各自的程序集，不依赖其他 EventHorizon.RocketMQ Package。

同一应用可以同时引用两个 Package，但各自的 API 模型仍属于对应协议，不能直接互换。详见
[协议边界与类型归属](docs/zh-CN/architecture/protocol-boundaries.md)。

## 相关资源

- [可运行示例](samples/README.zh-CN.md)
- [OpenTelemetry Web API 示例](samples/opentelemetry/README.zh-CN.md)
- [OpenTelemetry 埋点设计](docs/zh-CN/architecture/opentelemetry-instrumentation.md)
- [设计文档](docs/zh-CN/README.md)
- [本地测试环境](https://github.com/eventhorizon-cli/EventHorizon.RocketMQ/blob/main/test-environments/README.zh-CN.md)
- [贡献与编码说明](https://github.com/eventhorizon-cli/EventHorizon.RocketMQ/blob/main/AGENTS.md)

## 许可证

本项目采用 [Apache License 2.0](https://github.com/eventhorizon-cli/EventHorizon.RocketMQ/blob/main/LICENSE) 许可证。
