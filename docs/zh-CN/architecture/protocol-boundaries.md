# 协议边界与类型归属

[English](../../en-US/architecture/protocol-boundaries.md) | [简体中文](protocol-boundaries.md)

## 背景

RocketMQ 5 protobuf/gRPC 客户端连接 Proxy；classic Remoting 客户端则先向 NameServer 查询路由，再连接
Broker。两条路径的服务发现、wire contract、Consumer 语义、结果类型和服务端要求都不相同。

两种协议仍有少量行为相近的模型，例如 `Message`、过滤表达式、`ConsumeResult`、`ConsumerOptions` 和客户端
异常基类。如果为这些类型增加第三个生产 Project，就会为了很小的 API 范围引入额外的 MSBuild 边界、
Package 版本和发布依赖。

## 决策

生产代码只包含两个完全独立的客户端 Project：

```text
+-----------------------------------+      +---------------------------------------+
| EventHorizon.RocketMQ.Grpc        |      | EventHorizon.RocketMQ.Remoting        |
| Proxy + protobuf/gRPC             |      | NameServer + classic wire             |
| EventHorizon.RocketMQ.Grpc.*      |      | EventHorizon.RocketMQ.Remoting.*      |
+-----------------------------------+      +---------------------------------------+
                     两边没有生产 ProjectReference
```

- 每个 Project 都实际保存并负责所有参与自身编译的源码文件。
- 两个 NuGet Package 在客户端 API 边界上都完全自包含，不依赖其他 EventHorizon.RocketMQ Package。
- 两个协议 Project 彼此不引用，也不存在第三个生产 Project。
- 项目不提供 transport selector、公共 client builder，也不提供跨协议的 Producer 或 Consumer 接口。
- `GrpcClientOptions` 负责 Proxy 地址、凭据、namespace、TLS 和 gRPC 时序；gRPC client ID 由内部按每个
  逻辑角色生成。
- `RemotingClientOptions` 负责 NameServer 发现、凭据、namespace、TLS、Remoting 时序，以及 Broker
  协调所需的 classic client identity，并为每个逻辑角色复制独立的身份。
- 两个 Options 都不继承仓库自定义的客户端 Options 基类；协议需要的配置不同，它们的结构也无需保持一致。

保留少量重复源码是有意作出的取舍。这样可以让类型归属、编译内容、打包结果和发布边界在每个协议 Project
中保持直观。

## 类型归属

少量基础模型在两种协议中分别声明：

| 概念 | gRPC 类型 | Remoting 类型 |
| --- | --- | --- |
| 发送消息 | `EventHorizon.RocketMQ.Grpc.Producer.Message` | `EventHorizon.RocketMQ.Remoting.Producer.Message` |
| 消费结果 | `EventHorizon.RocketMQ.Grpc.Consumer.ConsumeResult` | `EventHorizon.RocketMQ.Remoting.Consumer.ConsumeResult` |
| Consumer Options 基类 | `EventHorizon.RocketMQ.Grpc.Consumer.ConsumerOptions` | `EventHorizon.RocketMQ.Remoting.Consumer.ConsumerOptions` |
| 过滤表达式 | `EventHorizon.RocketMQ.Grpc.Consumer.FilterExpression` | `EventHorizon.RocketMQ.Remoting.Consumer.FilterExpression` |
| 过滤类型 | `EventHorizon.RocketMQ.Grpc.Consumer.FilterExpressionType` | `EventHorizon.RocketMQ.Remoting.Consumer.FilterExpressionType` |
| 客户端异常基类 | `EventHorizon.RocketMQ.Grpc.Exceptions.RocketMQClientException` | `EventHorizon.RocketMQ.Remoting.Exceptions.RocketMQClientException` |

classic Remoting 的 `ConsumerOptions.InitialPosition` 使用协议自有的 `ConsumeFromPosition` 类型。LitePull 与
Push 只在已分配队列不存在消费组已提交位点时应用该配置；从时间戳开始时还会读取
`ConsumerOptions.ConsumeTimestamp`。

这些类型的完整类型名不同，因此两个 Package 可以在同一个应用中使用。不过，它们也是不同的 CLR 类型：gRPC
的 `Message`、过滤器、Consumer Options 或异常基类不能直接传给 Remoting API。

如果同一个文件导入了两边的协议命名空间，`Message` 这样的短类型名仍可能产生歧义。这只是普通的 C#
namespace 解析问题（`CS0104`），不是 CLR 类型重复（`CS0433`）。同时使用两边类型时，应设置 alias：

```csharp
using GrpcMessage = EventHorizon.RocketMQ.Grpc.Producer.Message;
using RemotingMessage = EventHorizon.RocketMQ.Remoting.Producer.Message;
```

## 协议负责的内容

所有体现协议行为的实现仍留在对应 Project：

| 关注点 | gRPC | Remoting |
| --- | --- | --- |
| Producer API 与发送结果 | `IGrpcProducer`、`GrpcSendReceipt` | `IRemotingProducer`、`RemotingSendResult`、`RemotingMessageQueue` |
| Consumer 消息模型 | `GrpcMessageView` | `RemotingMessageView` |
| 公开可读队列 identity | 不暴露，由服务端管理 assignment | LitePull 与只读 Admin 查询使用 `RemotingConsumerQueue` |
| Consumer 类型 | Simple、Push、LitePush | LitePull、Push |
| 路由与通信基础设施 | `Protocol` 中的 protobuf/gRPC service | `Protocol` 中的 Socket、frame、JSON 和 NameServer route |
| 协议错误 | `GrpcServiceException` | `RemotingCommandException` |

只在一种协议内部复用的通信基础设施放在该协议的 `Protocol` 目录。调度、重试、事务、Consumer Engine
以及 feature 专用 header 仍放在对应的 Producer 或 Consumer 功能目录中，避免再出现职责模糊的 `Common`
或 `Internal` 目录。

公开注册方法也保留协议选择：

```text
AddRocketMQGrpc(options => options.Endpoint = "proxy:8081")
AddRocketMQRemoting(options => options.NamesrvAddr = "nameserver:9876")
```

需要同时使用两种协议时，应用安装两个 Package 并分别注册客户端即可。通过 DI 解析的接口、Options 和模型
仍然属于具体协议。

## 取舍与约束

- 几个小型源码文件会在两个 Project 中各保留一份。这样减少了 MSBuild、打包和发布的复杂度，代价是修改
  相同行为时需要检查两套实现。
- 修改一边的基础模型时，必须同步审视另一个协议中的对应类型。只有相同语义仍适用于另一种协议时，才同步
  修改另一边。
- `EventHorizon.RocketMQ.Compatibility.Tests` 同时引用两个 Project，用于验证 API 行为、命名空间隔离和
  双 Package 共存。
- 看起来相似的传输结果、消息视图、队列和 service contract 仍然归对应协议所有，外观相似不是合并类型的
  理由。
- 一个 Project 增加功能，并不代表另一个 Project 自动获得该能力。文档和示例必须写清协议与服务端前提。
- protobuf 文件里存在 RPC 声明，不等于 gRPC API 已经可用。还需要可工作的服务端实现、稳定的客户端契约、
  测试和服务端版本说明。

## 延伸阅读

- [依赖注入与生命周期](dependency-injection-and-lifetimes.md)
- [gRPC 消费模型](../grpc/consumer-model.md)
- [classic Remoting Consumer 模型](../remoting/consumer-model.md)
- [Remoting 传输与客户端角色](../remoting/transport-and-client-roles.md)
- [可运行示例](../../../samples/README.zh-CN.md)
