# Remoting Admin 示例

[English](README.md) | [简体中文](README.zh-CN.md)

这个 ASP.NET Core Minimal API 通过依赖注入解析只读 `IRemotingAdmin` 角色，并通过 HTTP 暴露经典 RocketMQ
队列、位点和已存储消息查询。它不会创建资源、修改消费位点或更改 Broker 状态。

## HTTP API 和 Swagger

Swagger UI 位于 `/swagger`，OpenAPI 文档位于 `/swagger/v1/swagger.json`。项目内置的 launch profile 会在启动器支持
`launchBrowser` 时打开 `http://localhost:5185/swagger`。

| 路由 | 用途 |
| --- | --- |
| `GET /topics/{topic}/queues` | 从 NameServer 路由发现可写的物理队列。 |
| `GET /topics/{topic}/queues/{brokerName}/{queueId}/offsets?committed=true` | 返回最小位点、最大位点和最早消息存储时间。将 `committed` 设为 `false`，即可请求 Broker 的在途最大位点。 |
| `GET /topics/{topic}/queues/{brokerName}/{queueId}/offsets/search?timestamp={ISO-8601}&boundary=Lower` | 按必填的 ISO 8601 时间戳查找位点；`boundary` 默认值为 `Lower`。 |
| `GET /consumer-groups/{consumerGroup}/topics/{topic}/queues/{brokerName}/{queueId}/offset` | 读取消费组的已提交位点；消费组尚未提交时为 `null`。 |
| `GET /topics/{topic}/messages/{offsetMessageId}` | 使用 Remoting Producer receipt 中的 `OffsetMessageId` 读取一条物理消息。 |

`brokerName` 和 `queueId` 路由参数来自队列发现响应。Broker 调用失败会返回 HTTP 502；无效的队列或
`OffsetMessageId` 输入会返回 HTTP 400。

## 配置

该示例只从 `appsettings.json` 读取 Remoting 连接配置节：

| 键 | 默认值 | 用途 |
| --- | --- | --- |
| `RocketMQ:Remoting:NamesrvAddr` | `localhost:9876` | 用于发现 Broker 路由的 NameServer。 |

可通过标准 .NET 配置覆盖它，例如
`RocketMQ__Remoting__NamesrvAddr=nameserver.example:9876`。该地址是 **NameServer**，不是 Proxy 端点。

## Broker 和网络前置条件

- 使用接受 Remoting 客户端的经典 RocketMQ NameServer 和 Broker。该 API 会先查询 NameServer，然后直接连接路由
  发现返回的 Broker 地址。
- 运行该 API 的进程必须能访问 `NamesrvAddr` 和每个已公布的 Broker 主机及端口。仅能访问 NameServer 不足以完成
  查询。
- 在禁用自动创建 topic 的生产 Broker 上，使用前创建待查询的 topic，例如 `rocketmq-dotnet-manual`。只读 Admin
  角色本身不需要 Producer 或 consumer group；消费位点路由可查询已有 group，未提交位点时会返回 `null`。
- 启用 ACL 或 topic 权限时，授予路由查询、读取和管理权限。

`OffsetMessageId` 还有额外的网络要求。它是 `RemotingSendResult.OffsetMessageId` 中的物理 ID，而不是
Producer 分配的 `MessageId`；其中编码了 Broker 端点和 CommitLog 位点。消息查看路由会解析该端点并直接连接。
即使 NameServer 路由发现正常，API 所在主机也必须能访问 ID 中编码的确切地址和端口。经由不同网络发送的消息，或
Broker 公布了仅内部可访问地址时，不能从此 API 查看该消息。

本仓库提供的[本地 RocketMQ 环境](../../../test-environments/rocketmq/README.zh-CN.md)适合在宿主机运行此示例。在仓库根目录
启动环境并创建 topic：

```shell
docker compose -f test-environments/rocketmq/compose.yaml up -d --wait
docker compose -f test-environments/rocketmq/compose.yaml exec broker sh mqadmin updateTopic \
  -n nameserver:9876 -c DefaultCluster -t rocketmq-dotnet-manual
```

本地环境在 `localhost:9876` 暴露 NameServer，并公布 `localhost:10911` 作为宿主机可访问的 Broker。若在另一个
容器或网络中运行 API，请配置可访问的 NameServer 和 Broker 公布地址，而不是使用仅适用于宿主机的默认值。

## 运行

```shell
dotnet run --project samples/remoting/Admin/EventHorizon.RocketMQ.Samples.Remoting.Admin.csproj
```

若希望在命令行中使用固定的监听地址，请禁用 launch profile：

```shell
ASPNETCORE_URLS=http://localhost:5000 \
  dotnet run --no-launch-profile --project samples/remoting/Admin/EventHorizon.RocketMQ.Samples.Remoting.Admin.csproj

curl http://localhost:5000/topics/rocketmq-dotnet-manual/queues
```

使用返回的 `brokerName` 和 `queueId` 调用位点路由。若要测试消息查看路由，先通过
[Remoting Producer 示例](../Producer/README.zh-CN.md)发送一条消息，然后将其返回的 `OffsetMessageId` 用于
`/topics/rocketmq-dotnet-manual/messages/{offsetMessageId}`。

## 重要行为

- 这是只读检查 API。它不会发送消息、创建 topic 或 group、重置位点或确认消息。
- 队列位点路由默认 `committed=true`。只有需要 Broker 的在途最大位点时，才应选择 `committed=false`。
- `MessageViewResponse.BodyBase64` 使用 Base64，因为 RocketMQ 消息正文可以是任意二进制数据。应根据 Producer 的
  内容约定解码，而不是默认它是 UTF-8 文本。

完整角色 API 和连接选项请参阅[Remoting 指南](../../../src/EventHorizon.RocketMQ.Remoting/README.zh-CN.md)。
