# EventHorizon.RocketMQ

[English](https://github.com/eventhorizon-cli/EventHorizon.RocketMQ/blob/main/README.md) |
[简体中文](https://github.com/eventhorizon-cli/EventHorizon.RocketMQ/blob/main/README.zh-CN.md)

> **请从[可运行示例](samples/README.zh-CN.md)开始。** 每个样例目录都说明所需的 RocketMQ Broker 或 Proxy
> 能力、资源准备、配置、运行命令和协议注意事项。

一个符合 .NET 习惯的 Apache RocketMQ .NET 客户端。当前版本支持 .NET 8 及以上，并分别为
RocketMQ 5 protobuf/gRPC 和经典 Remoting 提供独立 API。

## 功能与状态

`✅` 表示该包已实现对应客户端 API。服务端能力和配置仍需满足该功能的要求。

### RocketMQ 5 gRPC

配置和兼容性细节请参阅 [gRPC 指南](src/EventHorizon.RocketMQ.Grpc/README.zh-CN.md) 与
[可运行样例](samples/README.zh-CN.md)。

| 功能 | 状态 | 说明 |
| --- | :---: | --- |
| 普通消息 Producer | ✅ | `IGrpcProducer.SendAsync`。 |
| FIFO 消息 Producer | ✅ | 设置 `MessageGroup`。 |
| 定时/延迟消息 Producer | ✅ | 设置 `DeliveryTimestamp`。 |
| 事务消息 Producer | ✅ | 使用 `SendTransactionAsync` 并配置事务检查器。 |
| 定时/延迟消息撤回 Producer | ✅ | 在消息投递前使用回执中的 recall handle 撤回。 |
| SimpleConsumer | ✅ | `IGrpcSimpleConsumer` 提供显式接收、确认、不可见时间和死信队列操作。 |
| 带并发消息监听器的 PushConsumer | ✅ | `IGrpcPushConsumer` 并发分发消息。 |
| 带 FIFO 消息监听器的 PushConsumer | ✅ | 每个 `MessageGroup` 按顺序处理。 |
| 带 FIFO 消费加速器的 PushConsumer | ✅ | 不同 message group 并行分发，同时保持每个 group 内的顺序。 |
| 优先级消息 | ✅ | 设置 `Message.Priority`。 |

#### 本项目扩展能力

| 功能 | 状态 | 说明 |
| --- | :---: | --- |
| Lite Producer 消息和 LitePushConsumer | ✅ | LitePush 要求 Proxy 实现 `SyncLiteSubscription`。 |
| Tag 和 SQL 过滤 | ✅ | SQL 过滤要求目标服务端具备相应的过滤配置。 |
| 运行时订阅、重试和死信处理 | ✅ | 视具体 API 而定，可通过 SimpleConsumer、PushConsumer 或 LitePushConsumer 使用。 |
| 依赖注入、Options、日志、Generic Host 生命周期以及命名/键控 profile | ✅ | 使用 `AddRocketMQGrpc` 注册。 |

gRPC Push 和 LitePush 通过客户端发起的分配与长轮询实现，并非协议层面的 Broker Push。

### 经典 Remoting

配置和兼容性细节请参阅 [Remoting 指南](src/EventHorizon.RocketMQ.Remoting/README.zh-CN.md) 与
[可运行样例](samples/README.zh-CN.md)。

| 功能 | 状态 | 说明 |
| --- | :---: | --- |
| 普通、FIFO、定时/延迟、优先级或 Lite 消息 Producer | ✅ | 使用 `IRemotingProducer`；目标 Broker 必须支持所选消息类型。 |
| 指定队列、队列选择器、批量发送和单向发送 | ✅ | 经典 Producer API。 |
| 事务消息、延迟消息撤回和请求-响应 | ✅ | 要求对应的经典 Broker 能力。 |
| 只读 Admin API | ✅ | `IRemotingAdmin` 查询队列、消息和位点。 |
| PullConsumer | ✅ | 显式队列和位点，支持 Tag 或 SQL 过滤。 |
| LitePullConsumer | ✅ | 客户端侧轮询、分配、seek、暂停/恢复和提交；目前仅支持集群消费。 |
| POPConsumer | ✅ | 显式选择队列、receipt 确认和不可见时间续租；仅适用于普通 topic。 |
| PushConsumer | ✅ | 集群或广播、并发或 FIFO 分发、运行时订阅、重试和死信处理。 |
| 依赖注入、Options、日志、Generic Host 生命周期以及命名/键控 profile | ✅ | 使用 `AddRocketMQRemoting` 注册。 |

经典 Push 也通过客户端发起的长轮询实现。SQL 过滤要求 Broker 配置，POP 要求 Broker 支持 POP。
协议专用异常统一继承 `RocketMQClientException` 基类。

## 包

| 包 | 连接目标 | 主要 API | 文档 |
| --- | --- | --- | --- |
| `EventHorizon.RocketMQ.Grpc` | RocketMQ 5 Proxy | `IGrpcProducer`、`IGrpcSimpleConsumer`、`IGrpcPushConsumer`、`IGrpcLitePushConsumer` | [gRPC 指南](https://github.com/eventhorizon-cli/EventHorizon.RocketMQ/blob/main/src/EventHorizon.RocketMQ.Grpc/README.zh-CN.md) |
| `EventHorizon.RocketMQ.Remoting` | NameServer 和 Broker | `IRemotingProducer`、`IRemotingAdmin`、`IRemotingPullConsumer`、`IRemotingLitePullConsumer`、`IRemotingPopConsumer`、`IRemotingPushConsumer` | [Remoting 指南](https://github.com/eventhorizon-cli/EventHorizon.RocketMQ/blob/main/src/EventHorizon.RocketMQ.Remoting/README.zh-CN.md) |

应用只需安装实际使用的协议包。两个协议包都会引用 `EventHorizon.RocketMQ.Shared`；该包包含协议无关的
消息、过滤器、结果、选项基类和公共异常类型，应用通常通过传递依赖获得它。

两个协议分别拥有独立的注册方法、选项、接口、结果和异常。Proxy 客户端使用
`AddRocketMQGrpc`，经典 NameServer 客户端使用 `AddRocketMQRemoting`。同一个 Host 也可以安装
两个包，并通过不同的命名配置同时使用两种协议。

## 仓库

- [可运行示例](samples/README.zh-CN.md)
- [设计文档](docs/README.md)
- [本地测试环境](https://github.com/eventhorizon-cli/EventHorizon.RocketMQ/blob/main/test-environments/README.zh-CN.md)
- [贡献与编码说明](https://github.com/eventhorizon-cli/EventHorizon.RocketMQ/blob/main/AGENTS.md)

## 许可证

本项目基于 [Apache License 2.0](https://github.com/eventhorizon-cli/EventHorizon.RocketMQ/blob/main/LICENSE) 授权。
