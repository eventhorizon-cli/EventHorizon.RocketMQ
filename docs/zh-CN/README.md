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
| 架构 | [协议边界](architecture/protocol-boundaries.md) | 为什么没有统一的跨协议 Producer、Consumer 或结果类型？ |
| 架构 | [依赖注入与生命周期](architecture/dependency-injection-and-lifetimes.md) | 默认客户端注册、keyed 客户端注册、keyed service、Consumer Engine 和 handler 如何组成一个 Host？ |
| gRPC | [消费模型](grpc/consumer-model.md) | Simple、Push 与 LitePush 分别适用于什么场景？为什么没有 gRPC PullConsumer？ |
| Remoting | [传输与客户端角色](remoting/transport-and-client-roles.md) | NameServer 路由、Socket 传输和各类经典 Consumer 有什么边界？ |
| 测试 | [本地与集成测试](testing/local-and-integration-testing.md) | 哪些测试不需要 Docker，哪些测试会启动完整 RocketMQ 环境？ |

## 快速定位

- 协议无关的消息、过滤器和公共异常位于
  [`src/EventHorizon.RocketMQ.Shared`](../../src/EventHorizon.RocketMQ.Shared)。
- RocketMQ 5 protobuf/gRPC 客户端位于
  [`src/EventHorizon.RocketMQ.Grpc`](../../src/EventHorizon.RocketMQ.Grpc)，连接目标是 Proxy。
- 经典 NameServer/Broker 客户端位于
  [`src/EventHorizon.RocketMQ.Remoting`](../../src/EventHorizon.RocketMQ.Remoting)。
- Docker Compose 手工环境位于 [`test-environments`](../../test-environments)。

文档描述的是仓库当前实现，并不代表 RocketMQ 所有版本和部署形态的通用兼容性承诺。

涉及服务端能力、topic 类型、ACL 或网络拓扑时，请同时阅读相应协议 Project 的 README 和对应示例的 README。
