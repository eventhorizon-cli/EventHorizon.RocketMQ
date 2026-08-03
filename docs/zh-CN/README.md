# EventHorizon.RocketMQ 设计文档

[English](../en-US/README.md) | [简体中文](README.md)

这里记录项目中不容易从 API 签名看出的设计取舍：为什么 gRPC 与 classic Remoting 是两套独立的客户端，
为什么 Consumer 的生命周期由 Host 管理，以及测试环境如何覆盖真实的 NameServer、Broker 与 Proxy 交互。

这些文章面向需要评估、维护或扩展客户端的开发者。日常接入请先阅读[仓库总览](../../README.zh-CN.md)和
[可运行示例](../../samples/README.zh-CN.md)。

## 阅读路径

| 主题 | 文章 | 适合解决的问题 |
| --- | --- | --- |
| 架构 | [RocketMQ 服务端与客户端路径](architecture/rocketmq-architecture.md) | NameServer、Broker、Proxy、两种协议路径、LitePush 与本地 Compose 拓扑如何配合？ |
| 架构 | [协议边界与类型归属](architecture/protocol-boundaries.md) | 为什么 gRPC 与 Remoting 分别保存自己的 API 模型，又能在同一应用中使用？ |
| 架构 | [依赖注入与生命周期](architecture/dependency-injection-and-lifetimes.md) | 默认客户端注册、keyed 客户端注册、keyed service、Consumer Engine 和 handler 如何组成一个 Host？ |
| 架构 | [OpenTelemetry 埋点](architecture/opentelemetry-instrumentation.md) | tracing、metrics、上下文传播与应用负责的 exporter 如何分工？ |
| gRPC | [消费模型](grpc/consumer-model.md) | Simple、Push 与 LitePush 分别适用于什么场景？为什么没有 gRPC PullConsumer？ |
| Remoting | [Consumer 模型](remoting/consumer-model.md) | LitePull 与 Push 的公开边界、Broker-assigned PULL/POP、receipt 结算和队列路由如何归属？ |
| Remoting | [传输与客户端角色](remoting/transport-and-client-roles.md) | NameServer 路由、Socket 传输，以及 LitePull、Push 与 Admin 角色有什么边界？ |
| 测试 | [本地与集成测试](testing/local-and-integration-testing.md) | 哪些测试不需要 Docker，哪些测试会启动完整 RocketMQ 环境？ |

## 快速定位

- RocketMQ 5 protobuf/gRPC 客户端位于
  [`src/EventHorizon.RocketMQ.Grpc`](../../src/EventHorizon.RocketMQ.Grpc)，连接目标是 Proxy。
- 经典 NameServer/Broker 客户端位于
  [`src/EventHorizon.RocketMQ.Remoting`](../../src/EventHorizon.RocketMQ.Remoting)。
- 两个客户端 Project 分别保存自己的公开模型与实现，彼此没有生产 ProjectReference。
- Docker Compose 手工环境位于 [`test-environments`](../../test-environments)。

文档描述的是仓库当前实现，并不代表 RocketMQ 所有版本和部署形态的通用兼容性承诺。

涉及服务端能力、topic 类型、ACL 或网络拓扑时，请同时阅读相应协议 Project 的 README 和对应示例的 README。
