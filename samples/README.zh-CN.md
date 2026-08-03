# EventHorizon.RocketMQ 示例

[English](README.md) | [简体中文](README.zh-CN.md)

每个项目演示一个协议专用的 SDK 角色或集成方式。主代码路径会直接呈现注册、公共 API 调用、完成和失败处理；对应指南
则说明这种模式的含义及适用场景。

## 选择传输协议

两个客户端彼此独立，连接方式和能力模型也不同：

- **gRPC** 通过 `GrpcClientOptions.Endpoint` 连接 RocketMQ 5 Proxy。适用于 Proxy 部署和 RocketMQ 5 gRPC 消息模型。
- **classic Remoting** 通过 `RemotingClientOptions.NamesrvAddr` 发现 Broker，再直连路由中公布的端点。适用于 classic
  部署、由应用驱动定位的 LitePull、由 SDK 驱动的 Push 消费，以及 classic Producer 操作。

示例不会用传输无关的包装层隐藏这些差异。应先根据部署实际公开的协议进行选择。

## 托管模型

示例树按生命周期分为两种形态：

- **GenericHost** 项目运行在 .NET Generic Host 或 `WebApplication` 中。SDK 注册的 `IHostedService` 会启动和停止每个
  受生命周期管理的角色；应用代码不能再手动调用该角色的 `StartAsync` 或 `StopAsync`。
- **NonHost** 项目只构造普通 `ServiceProvider`。它们在 `try`/`finally` 中显式调用 `StartAsync` 和 `StopAsync`，因为
  `BuildServiceProvider()` 不会运行已注册的 hosted service。

## gRPC 角色

| 模式 | 公共 API | 适用场景 |
| --- | --- | --- |
| [GenericHost：Producer](grpc/GenericHost/Producer/README.zh-CN.md) | `IGrpcProducer` | 应用通过 RocketMQ 5 Proxy 发布消息。 |
| [GenericHost：LiteProducer](grpc/GenericHost/LiteProducer/README.zh-CN.md) | `IGrpcProducer` | 启用 LITE 的部署需要在一个 parent topic 和逻辑 LiteTopic 下发布消息。 |
| [GenericHost：SimpleConsumer](grpc/GenericHost/SimpleConsumer/README.zh-CN.md) | `IGrpcSimpleConsumer` | 应用代码需要主动接收、延长不可见期、确认或转发死信。 |
| [GenericHost：PushConsumer](grpc/GenericHost/PushConsumer/README.zh-CN.md) | `IGrpcPushConsumer` | SDK 应负责分配轮询、接收循环、handler 并发、不可见期续期和结算。 |
| [GenericHost：LitePushConsumer](grpc/GenericHost/LitePushConsumer/README.zh-CN.md) | `IGrpcLitePushConsumer` | 启用 LITE 的部署在同一个 bind topic 下使用服务端管理的 LiteTopic。 |
| [NonHost 集合](grpc/NonHost/README.zh-CN.md) | 全部四种角色 | 未运行 Generic Host 的进程显式启动和停止选定的 gRPC 角色。 |

gRPC Push 仍由客户端发起长轮询，不是 Broker 主动建立的网络 Push 连接。

## classic Remoting 角色

| 模式 | 公共 API | 适用场景 |
| --- | --- | --- |
| [GenericHost：Producer](remoting/GenericHost/Producer/README.zh-CN.md) | `IRemotingProducer` | 应用通过 NameServer/Broker Remoting 发布，或需要 classic 的选队列、批量、单向、事务、撤回和请求/响应操作。 |
| [GenericHost：Admin](remoting/GenericHost/Admin/README.zh-CN.md) | `IRemotingAdmin` | 只读工具需要检查物理队列、位点边界、已提交位置或存储消息。 |
| [GenericHost：Lite Pull Consumer](remoting/GenericHost/LitePullConsumer/README.zh-CN.md) | `IRemotingLitePullConsumer` | 应用在 SDK 管理的订阅或显式手工队列分配上执行轮询和提交。 |
| [GenericHost：Push Consumer](remoting/GenericHost/PushConsumer/README.zh-CN.md) | `IRemotingPushConsumer` | SDK 负责客户端或 Broker 分配、PULL 或 POP 接收、分发、结算与进度。 |
| [NonHost 集合](remoting/NonHost/README.zh-CN.md) | 所有受生命周期管理的角色 | 未运行 Generic Host 的进程显式启动和停止选定的 Remoting 角色。 |

Remoting Push 同样使用客户端发起的长轮询。同一个集群 group 内的 Consumer 会分摊 Broker 物理队列；不同 group 会
独立消费。

## OpenTelemetry 集成

[使用 GenericHost 的 OpenTelemetry 示例](opentelemetry/GenericHost/README.zh-CN.md)说明应用如何订阅两个客户端分别产生的 ActivitySource 和 Meter。
OpenTelemetry provider、resource、采样、view、processor 和 exporter 都由应用负责。

## 本地运行

通用环境通过 Proxy 支持普通 gRPC Producer、SimpleConsumer 和 PushConsumer，也通过 NameServer 与 Broker 支持 classic
Remoting 角色。环境会在启动完成前创建 `eventhorizon-test-topic`：

```shell
docker compose -f test-environments/rocketmq/compose.yaml up -d --wait
dotnet run --project samples/grpc/GenericHost/SimpleConsumer
```

将项目路径替换为任一普通示例即可。每个项目 README 会说明额外资源、权限、服务端能力和完整命令。

LiteProducer 和 LitePushConsumer 工作流需要 LITE parent topic、绑定的 consumer group、Broker LMQ 能力，以及实现
`SyncLiteSubscription` 的 cluster-mode Proxy。应使用专用环境，而不是通用 stack。先启动 Consumer，再在第二个终端启动
LiteProducer：

```shell
docker compose -f test-environments/rocketmq-litepush/compose.yaml up -d --wait
dotnet run --project samples/grpc/GenericHost/LitePushConsumer
# 在另一个终端：
dotnet run --project samples/grpc/GenericHost/LiteProducer
```

可观测性工作流需要同时运行通用 RocketMQ 环境和 `test-environments/otel-lgtm/compose.yaml`，然后启动 OpenTelemetry
Consumer 与 Producer 项目。

## 配置约定

SDK 自己的连接与角色 options 使用协议专用的 `RocketMQ` 配置节，并直接传给匹配的注册委托。普通示例的资源名、过滤
条件、消息属性和简单方法参数会留在公共 SDK 调用附近，使工作流保持可见。OpenTelemetry 应用设置使用独立的
`OpenTelemetry` 配置节。

每个项目的 `appsettings.json` 都提供可运行默认值，也支持标准 .NET 配置覆盖，例如
`RocketMQ__Client__Endpoint=proxy.example:8081` 或
`RocketMQ__Remoting__NamesrvAddr=nameserver.example:9876`。Producer 与 Admin Web API 项目会提供 Swagger；其 HTTP
端点只是对应指南中 SDK 工作流的应用外壳。

## 覆盖边界

每个项目演示一条完整连贯的端到端工作流，而不是每个 overload。需要不同资源、协作端或真实处理时机的高级路径会先留在
协议指南中，直到它们值得拥有独立的可运行示例，例如 Producer 事务与请求/响应、运行时订阅变更、LitePull 手工分配与
seek，以及由运维选择 Broker 侧 POP 请求模式。

完整公共 API 请参阅 [gRPC 指南](../src/EventHorizon.RocketMQ.Grpc/README.zh-CN.md)和
[Remoting 指南](../src/EventHorizon.RocketMQ.Remoting/README.zh-CN.md)。
