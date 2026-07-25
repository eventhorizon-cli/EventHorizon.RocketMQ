# 协议边界：把 gRPC 和 classic Remoting 当作两个客户端

[English](../../en-US/architecture/protocol-boundaries.md) | [简体中文](protocol-boundaries.md)

> 本文讨论的是代码边界，不是让应用在两种协议之间做运行时自动切换。协议选择应由应用的连接目标和
> 需要的能力决定。

## 背景

RocketMQ 5 的 protobuf/gRPC 客户端连接的是 Proxy；经典客户端则先向 NameServer 查询路由，再直接
连接 Broker。这不仅是不同的连接字符串，还是两种不同的服务端能力集合、消息模型和失败语义。

早期把两条路径放在一个“通用客户端”之下看似方便，却会产生误导：一个在经典 Broker 中可用的队列、
位点或回调 API，未必在 Proxy API 中有对应的服务端实现。反过来，gRPC 的 receipt、不可见时间和
telemetry 也不应被伪装成 classic Remoting 的类型。

## 决策

项目把协议边界作为第一层架构边界，而不是在调用时才分支：

```text
EventHorizon.RocketMQ.Grpc ───┐
                               ├── EventHorizon.RocketMQ.Shared
EventHorizon.RocketMQ.Remoting ┘
```

- `EventHorizon.RocketMQ.Grpc` 和 `EventHorizon.RocketMQ.Remoting` 只依赖 Shared，彼此不引用。
- Shared 只保存真正协议无关的概念：`Message`、过滤表达式、`ConsumeResult`、`QueryOffsetPolicy`、
  公共选项基类与 `RocketMQClientException`。
- 每个协议 Project 自行定义 Producer、Consumer、结果、状态、队列模型、配置选项、注册方法和异常类型。
- 应用显式选择 `AddRocketMQGrpc` 或 `AddRocketMQRemoting`；没有隐藏的 transport selector，也没有
  传输无关的 `IProducer` 或 `IConsumer`。

这种设计允许一个 Host 同时安装两个 Package，但要求调用方明确知道每个实例连接的是 Proxy 还是
NameServer/Broker。

## 如何工作

### Shared 是最小语义交集

[`EventHorizon.RocketMQ.Shared`](../../../src/EventHorizon.RocketMQ.Shared) 不知道 endpoint、NameServer、
Broker、gRPC stub 或经典 wire frame。它的 `Message` 是跨协议发送输入，但发送后的结果不是：

- gRPC 返回 [`GrpcSendReceipt`](../../../src/EventHorizon.RocketMQ.Grpc/Producer/GrpcSendReceipt.cs)，
  以 gRPC/Proxy 的确认语义表达消息 ID、位点和可选 handle。
- Remoting 返回
  [`RemotingSendResult`](../../../src/EventHorizon.RocketMQ.Remoting/Producer/RemotingSendResult.cs)，
  其状态需要调用方检查，因为经典 Broker 可以返回非成功存储状态而不以异常结束调用。
- 收到的消息也按协议建模为 `GrpcMessageView` 和 `RemotingMessageView`，避免把 receipt、物理队列或
  协议保留属性错误地提升为公共模型。

Shared 不是“以后再把所有类型放进来的公共文件夹”。只有两个协议均能以相同语义使用的概念才应进入这里。

### gRPC 的公开角色以 Proxy 能力为准

gRPC Project 公开 `IGrpcProducer`、`IGrpcSimpleConsumer`、`IGrpcPushConsumer` 和
`IGrpcLitePushConsumer`。它们位于
[`src/EventHorizon.RocketMQ.Grpc`](../../../src/EventHorizon.RocketMQ.Grpc)，并通过 `Endpoint` 连接
RocketMQ Proxy。

`IGrpcPullConsumer` 不再是公开角色。显式队列/位点的 Pull 工作流要求 Proxy 端提供与之配套的 Pull 和
位点 RPC；本项目不会把缺少可用服务端路径的接口保留为看似可用的 API。需要这种语义的应用应使用
`IRemotingPullConsumer`，或改用 gRPC SimpleConsumer/PushConsumer 所提供的接收模型。

### Remoting 保留经典角色，而不是模拟 gRPC

Remoting Project 公开的角色包括 `IRemotingAdmin`、`IRemotingProducer`、`IRemotingPullConsumer`、
`IRemotingLitePullConsumer`、`IRemotingPopConsumer` 和 `IRemotingPushConsumer`。它们位于
[`src/EventHorizon.RocketMQ.Remoting`](../../../src/EventHorizon.RocketMQ.Remoting)，并使用
`NamesrvAddr` 发现 Broker。

这条路径保留了经典协议真正具有的能力，例如指定 `RemotingMessageQueue`、经典 Pull 位点、Broker
请求-响应回调和 POP receipt。它也承担经典协议的成本：应用网络必须能够到达 NameServer 及其返回的
Broker 地址。

### 目录表达所有权

两个协议项目都按 `Protocol`、`Consumer`、`Producer` 和 `Exceptions` 组织：

| 位置 | 责任 | 不应放入的内容 |
| --- | --- | --- |
| `Protocol` | 可由多个角色复用的通信基础设施，如 route、RPC/socket、序列化和 telemetry | 某一种 Consumer 的调度、重试或 handler 策略 |
| `Consumer` | 消费模型、调度、确认、重试和 feature 专有 decoder/header | Producer 发送流程 |
| `Producer` | 发送、事务、回执、队列选择和请求-响应 | Consumer 生命周期 |
| `Exceptions` | 协议专用失败类型 | 通用业务异常 |

这让一个新增的协议特性先回答“它归谁所有”，而不是先新建模糊的 `Common` 或 `Internal` 文件夹。

## 取舍与约束

**显式优于自动降级。** 一个 gRPC 调用不会在失败后悄悄切换为 Remoting；这种切换会改变认证、网络路径、
重试、消息结果和运维前提。需要双协议时，应用应分别注册 gRPC 和 Remoting 客户端，并在注入点选择正确接口。

**类型会有重复。** `GrpcMessageView` 与 `RemotingMessageView`、两种 send result 以及两套 options 看起来
相似，但它们保留了各自必要的语义。为了减少少量重复而抹平这些差异，长期会让 API 更难正确使用。

**Shared 必须保持轻。** Shared 不引用 Microsoft DI、Options、gRPC、Socket 传输或任一协议 Project。这个约束
避免了依赖反转，也使应用可只安装一个协议 Package。

## 延伸阅读

- [依赖注入与生命周期](dependency-injection-and-lifetimes.md)
- [gRPC 消费模型](../grpc/consumer-model.md)
- [Remoting 传输与客户端角色](../remoting/transport-and-client-roles.md)
- [gRPC Project 指南](../../../src/EventHorizon.RocketMQ.Grpc/README.zh-CN.md)
- [Remoting Project 指南](../../../src/EventHorizon.RocketMQ.Remoting/README.zh-CN.md)
