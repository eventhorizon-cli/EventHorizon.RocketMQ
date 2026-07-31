# gRPC Producer

[English](README.md) | [简体中文](README.zh-CN.md)

`IGrpcProducer` 通过 RocketMQ 5 Proxy 发布消息。应用通过 gRPC `Endpoint` 访问 RocketMQ，并希望使用
Proxy 模式的消息模型、生命周期和遥测时，适合选择该角色。它不会直连 Broker 或 NameServer。

如果应用必须选择物理发布队列、批量或单向发送、使用请求-响应等 classic 专属操作，或者部署只暴露了
NameServer 和 Broker 端点而没有 Proxy，应改用 [Remoting Producer](../../../remoting/GenericHost/Producer/README.zh-CN.md)。

## 注册与生命周期

先注册协议客户端，再向该客户端注册添加 Producer 角色：

```csharp
builder.Services
    .AddRocketMQGrpc(clientSection.Bind)
    .AddGrpcProducer(static options =>
        options.SendMsgTimeout = TimeSpan.FromSeconds(3));
```

该角色通过 `IGrpcProducer` 对外提供。使用依赖注入注册后，.NET Host 会随应用启动和停止 Producer；应用服务通常
只需注入并使用它，不需要自行调用 `StartAsync` 或 `StopAsync`。gRPC Producer 不使用 consumer group。

带名称的注册会创建一套独立客户端，该名称同时也是 .NET keyed service 的 key。当同一进程需要使用不同的 Proxy
端点、凭据或 Producer 设置时，可以这样注册：

```csharp
builder.Services
    .AddRocketMQGrpc("audit", auditClientSection.Bind)
    .AddGrpcProducer(static options =>
        options.SendMsgTimeout = TimeSpan.FromSeconds(3));

static Task<GrpcSendReceipt> PublishAsync(
    Message message,
    [FromKeyedServices("audit")] IGrpcProducer producer,
    CancellationToken cancellationToken) =>
    producer.SendAsync(message, cancellationToken);
```

注册名只是应用组合信息，不会创建 Broker 侧的特殊 Producer 类型或审计功能；keyed 注册也不会回退到默认注册。

## 发送与回执

创建协议专属的 `Message`，再调用 `SendAsync`：

```csharp
var message = new Message("orders", Encoding.UTF8.GetBytes(orderId))
{
    Tag = "created"
};
message.Keys.Add(orderId);

GrpcSendReceipt receipt = await producer.SendAsync(message, cancellationToken);
```

`GrpcSendReceipt` 包含服务端分配的 `MessageId`、存储 `Offset`、Broker 端点，以及可选的事务或撤回信息。方法完成
表示 RocketMQ 服务返回了发送回执，不代表 Consumer 已处理消息。发送失败通过异常报告，Producer 也不是持久化的
本地 outbox。应用应传递取消信号，并在能够处理重复发送后果的业务边界决定是否重试。

## 消息模式

未设置专用属性时，`SendAsync` 发送普通消息：

| 需求 | SDK API | 约束 |
| --- | --- | --- |
| FIFO 投递 | `Message.MessageGroup` | 提供稳定的队列亲和性；目标资源必须支持 FIFO。 |
| 延迟投递 | `Message.DeliveryTimestamp` | 目标资源必须支持定时投递。服务端支持撤回时，可在投递前将回执中的 `RecallHandle` 传给 `RecallAsync`。 |
| 优先级投递 | `Message.Priority` | 优先级必须为非负值，并且需要服务端支持。 |
| Lite 消息 | `Message.LiteTopic` | Proxy 和 Broker 都必须支持 Lite 消息。 |
| 事务消息 | `SendTransactionAsync` | 启动前声明事务 Topic 并配置事务检查器；随后提交或回滚返回的事务。 |

这些专用模式取决于 Proxy 和 Broker 能力，不能仅根据客户端存在相应 API 就认定服务端支持。完整配置和事务恢复
语义请参阅 [gRPC 协议指南](../../../../src/EventHorizon.RocketMQ.Grpc/README.zh-CN.md)。

## 前置条件与运行

默认配置要求 `localhost:8081` 上存在 Proxy，并准备好可写的普通 Topic `eventhorizon-test-topic`。仓库提供的环境会
创建该 Topic：

```shell
docker compose -f test-environments/rocketmq/compose.yaml up -d --wait
dotnet run --project samples/grpc/GenericHost/Producer
```

API 启动后，在另一个终端通过默认注册发送消息：

```shell
curl --request POST http://localhost:5231/messages \
  --header 'Content-Type: application/json' \
  --data '{"message":"hello from the gRPC Producer sample"}'
```

要验证独立的具名 `audit` keyed registration，请使用其端点：

```shell
curl --request POST http://localhost:5231/clients/audit/messages \
  --header 'Content-Type: application/json' \
  --data '{"message":"hello from the gRPC audit Producer sample"}'
```

每个成功请求都会返回 HTTP 200，并包含 `messageId`、固定的 `topic` 和 RocketMQ 存储 `offset`。该回执只确认服务已接受
消息，不代表 Consumer 已完成处理。Swagger 在 `http://localhost:5231/swagger` 提供相同操作。

连接外部集群时，将 `RocketMQ:Client:Endpoint` 指向其 Proxy；若集群禁用了自动创建资源，还需预先创建 Topic。示例只在
`appsettings.json` 中保留连接配置，固定的 Producer 行为直接写在各自的注册代码旁。
