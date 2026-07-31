# Remoting Admin

[English](README.md) | [简体中文](README.zh-CN.md)

`IRemotingAdmin` 是 classic Remoting 客户端的只读检查角色。应用或运维工具需要发现 Topic 的物理队列、检查队列
位点边界和时间戳、读取消费组已提交位置，或根据物理标识读取已存储消息时，适合使用该角色。

该角色不会创建 Topic 或 group、重置位点、确认消息，也不会持续消费消息流。这些工作应使用 Producer、相应的
Consumer 角色或 RocketMQ 资源管理工具完成。

## 注册

先注册 Remoting 客户端，再添加 Admin 角色：

```csharp
builder.Services
    .AddRocketMQRemoting(remotingSection.Bind)
    .AddRemotingAdmin();
```

在执行查询的组件中注入 `IRemotingAdmin`。与托管的 Producer 和 Consumer 角色不同，Admin 不会启动后台 hosted
service，也不需要 Producer group 或 consumer group。

配置的 `NamesrvAddr` 是 NameServer 端点，不是 Proxy。Admin 先通过 NameServer 路由确定 Broker，再将队列查询直接
发送到选定的 Broker。因此，应用必须同时访问 NameServer 和每个 Broker 公布端点。

## 公共 API

| SDK API | 结果与用途 |
| --- | --- |
| `GetMessageQueuesAsync(topic)` | 返回 Topic 当前可写的物理队列，并按 Broker 和队列 ID 排序。后续单队列查询应使用这里返回的值。 |
| `GetMinOffsetAsync(queue)` | 返回队列中仍可读取的最早位点；受保留策略影响，该值不一定为零。 |
| `GetMaxOffsetAsync(queue, committed)` | 默认返回已提交且可消费的最高位点。传入 `false` 时，在 Broker 支持的情况下请求当前在途位点。 |
| `GetEarliestMessageStoreTimeAsync(queue)` | 返回最早可用消息的 UTC 存储时间；Broker 报告没有可用消息时返回 `null`。 |
| `SearchOffsetAsync(queue, timestamp, boundary)` | 根据时间戳查找队列位点。`Lower` 选择最低匹配位点，`Upper` 选择最高匹配位点。 |
| `GetConsumerOffsetAsync(group, queue)` | 返回该 group 在一个物理队列上的已提交位置；尚未提交时返回 `null`。 |
| `ViewMessageAsync(topic, offsetMessageId)` | 使用 `RemotingSendResult.OffsetMessageId` 中的物理 ID 读取一条已存储消息。 |

`RemotingMessageQueue` 由 Topic、Broker 名称和队列 ID 共同标识。位点只属于对应的物理队列，不是 Topic 全局位点，
也不能把不同队列的位点当作同一个序列比较。

## 物理消息查询

`ViewMessageAsync` 需要 `RemotingSendResult.OffsetMessageId`，而不是 Producer 分配的 `MessageId`。物理 ID 编码了
Broker 端点和 CommitLog 位点。客户端会解析它，并绕过 NameServer 路由发现，直接连接该端点：

```csharp
RemotingSendResult sent = await producer.SendAsync(message, cancellationToken);
RemotingMessageView stored = await admin.ViewMessageAsync(
    message.Topic,
    sent.OffsetMessageId,
    cancellationToken);
```

在另一个网络中生成的 ID 可能包含当前应用无法访问的地址。即使 NameServer 路由正常，也不能替代这次直接连接。
消息正文是任意字节，应根据 Producer 的内容约定解码。

完整角色契约请参阅 [Remoting 协议指南](../../../../src/EventHorizon.RocketMQ.Remoting/README.zh-CN.md)。

## 前置条件与运行

默认配置要求 `localhost:9876` 上存在 NameServer，并且宿主机能够访问 Broker 公布端点。仓库提供的环境包含该拓扑，
并准备好 `eventhorizon-test-topic`：

```shell
docker compose -f test-environments/rocketmq/compose.yaml up -d --wait
dotnet run --project samples/remoting/GenericHost/Admin/EventHorizon.RocketMQ.Samples.Remoting.Admin.csproj
```

API 启动后，在另一个终端发现 Topic 队列：

```shell
curl http://localhost:5185/topics/eventhorizon-test-topic/queues
```

响应是包含 `brokerName` 和 `queueId` 的非空 JSON 数组。将其中一组返回值替换到单队列查询中：

```shell
curl http://localhost:5185/topics/eventhorizon-test-topic/queues/<broker-name>/<queue-id>/offsets
```

所有只读端点均列在 `http://localhost:5185/swagger`。要读取某一条存储消息，先用 [Remoting Producer](../Producer/README.zh-CN.md)
发送，然后将其 `offsetMessageId` 替换到 `/topics/eventhorizon-test-topic/messages/<offset-message-id>`。Admin API 不会创建或修改
RocketMQ 资源。

连接外部集群时，配置 `RocketMQ:Remoting:NamesrvAddr`，按需创建 Topic，并授予集群所需的路由查询和读取权限。
