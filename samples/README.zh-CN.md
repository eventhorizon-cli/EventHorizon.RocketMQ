# EventHorizon.RocketMQ 示例

[English](README.md) | [简体中文](README.zh-CN.md)

每个项目演示一个协议专用的 SDK 角色或集成方式。主代码路径会直接呈现注册、公共 API 调用、完成和失败处理；对应指南
则说明这种模式的含义及适用场景。

## 选择传输协议

两个客户端彼此独立，连接方式和能力模型也不同：

- **gRPC** 通过 `GrpcClientOptions.Endpoint` 连接 RocketMQ 5 Proxy。适用于 Proxy 部署和 RocketMQ 5 gRPC 消息模型。
- **classic Remoting** 通过 `RemotingClientOptions.NamesrvAddr` 发现 Broker，再直连路由中公布的端点。适用于 classic
  部署，以及需要物理队列、位点、POP 或 classic Producer 操作的场景。

示例不会用传输无关的包装层隐藏这些差异。应先根据部署实际公开的协议进行选择。

## gRPC 角色

| 模式 | 公共 API | 适用场景 |
| --- | --- | --- |
| [Producer](grpc/Producer/README.zh-CN.md) | `IGrpcProducer` | 应用通过 RocketMQ 5 Proxy 发布消息。 |
| [SimpleConsumer](grpc/SimpleConsumer/README.zh-CN.md) | `IGrpcSimpleConsumer` | 应用代码需要主动接收、延长不可见期、确认或转发死信。 |
| [PushConsumer](grpc/PushConsumer/README.zh-CN.md) | `IGrpcPushConsumer` | SDK 应负责分配轮询、接收循环、handler 并发、不可见期续期和结算。 |
| [LitePushConsumer](grpc/LitePushConsumer/README.zh-CN.md) | `IGrpcLitePushConsumer` | 启用 LITE 的部署在同一个 bind topic 下使用服务端管理的 LiteTopic。 |

gRPC Push 仍由客户端发起长轮询，不是 Broker 主动建立的网络 Push 连接。

## classic Remoting 角色

| 模式 | 公共 API | 适用场景 |
| --- | --- | --- |
| [Producer](remoting/Producer/README.zh-CN.md) | `IRemotingProducer` | 应用通过 NameServer/Broker Remoting 发布，或需要 classic 的选队列、批量、单向、事务、撤回和请求/响应操作。 |
| [Admin](remoting/Admin/README.zh-CN.md) | `IRemotingAdmin` | 只读工具需要检查物理队列、位点边界、已提交位置或存储消息。 |
| [Pull Consumer](remoting/PullConsumer/README.zh-CN.md) | `IRemotingPullConsumer` | 应用负责选择物理队列、请求位点、处理和提交。 |
| [Lite Pull Consumer](remoting/LitePullConsumer/README.zh-CN.md) | `IRemotingLitePullConsumer` | SDK 负责队列分配，但应用代码负责轮询和提交。 |
| [POP Consumer](remoting/PopConsumer/README.zh-CN.md) | `IRemotingPopConsumer` | 工作通过逐消息 receipt 和不可见期结算，而不是提交队列位点。 |
| [Push Consumer](remoting/PushConsumer/README.zh-CN.md) | `IRemotingPushConsumer` | SDK 负责队列分配、长轮询、公平并发分发、重试结算和位点持久化。 |

Remoting Push 同样使用客户端发起的长轮询。同一个集群 group 内的 Consumer 会分摊 Broker 物理队列；不同 group 会
独立消费。

## OpenTelemetry 集成

[OpenTelemetry 示例](opentelemetry/README.zh-CN.md)说明应用如何订阅两个客户端分别产生的 ActivitySource 和 Meter。
OpenTelemetry provider、resource、采样、view、processor 和 exporter 都由应用负责。

## 本地运行

通用环境通过 Proxy 支持普通 gRPC Producer、SimpleConsumer 和 PushConsumer，也通过 NameServer 与 Broker 支持 classic
Remoting 角色。环境会在启动完成前创建 `eventhorizon-test-topic`：

```shell
docker compose -f test-environments/rocketmq/compose.yaml up -d --wait
dotnet run --project samples/grpc/SimpleConsumer
```

将项目路径替换为任一普通示例即可。每个项目 README 会说明额外资源、权限、服务端能力和完整命令。

LitePush 需要 LITE parent topic、绑定的 consumer group、Broker LMQ 能力，以及实现 `SyncLiteSubscription` 的
cluster-mode Proxy。应使用专用环境，而不是通用 stack：

```shell
docker compose -f test-environments/rocketmq-litepush/compose.yaml up -d --wait
dotnet run --project samples/grpc/LitePushConsumer
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
协议指南中，直到它们值得拥有独立的可运行示例，例如 Producer 事务与请求/响应、运行时订阅变更、手动分配与 seek、
延长不可见期，以及其他 Push 投递模式。

完整公共 API 请参阅 [gRPC 指南](../src/EventHorizon.RocketMQ.Grpc/README.zh-CN.md)和
[Remoting 指南](../src/EventHorizon.RocketMQ.Remoting/README.zh-CN.md)。
