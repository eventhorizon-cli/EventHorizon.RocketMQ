# Remoting Producer 示例

[English](README.md) | [简体中文](README.zh-CN.md)

这是一个 ASP.NET Core Minimal API。它通过依赖注入解析 `IRemotingProducer`，再通过 classic Remoting 协议发送普通
Apache RocketMQ 消息。它同时注册默认客户端注册和一个 `registrationName` 为 `audit` 的独立 keyed 客户端注册。该
`registrationName` 同时作为 keyed service 的 key。

## 公共角色和端点

| 路由 | Producer 客户端注册 | 用途 |
| --- | --- | --- |
| `POST /messages` | 默认客户端注册中的 `IRemotingProducer` | 发送一条普通消息。 |
| `POST /clients/audit/messages` | `registrationName` 为 `"audit"` 的 keyed 客户端注册中的 `IRemotingProducer` keyed service | 通过该 keyed service 发送一条普通消息。 |

Swagger UI 位于 `/swagger`；项目内置的 HTTP launch profile 会在启动器支持时打开
`http://localhost:5184/swagger`。两个路由都接受可选 JSON 正文，其中包括 `topic`、`tag` 和 `body`。提供的值会
覆盖对应的 `Sample` 配置值。

```json
{
  "topic": "rocketmq-dotnet-manual",
  "tag": "sample",
  "body": "Hello from the Remoting producer."
}
```

## 默认配置

示例从 `appsettings.json` 读取下列配置节。

| 键 | 默认值 | 用途 |
| --- | --- | --- |
| `RocketMQ:Remoting:NamesrvAddr` | `localhost:9876` | 默认客户端注册用于发现 Broker 路由的 NameServer。 |
| `RocketMQ:Producer:GroupName` | `remoting-sample-producer` | 默认 Producer 身份。 |
| `RocketMQ:Audit:Remoting:NamesrvAddr` | `localhost:9876` | `registrationName` 为 `audit` 的 keyed 客户端注册使用的 NameServer。 |
| `RocketMQ:Audit:Producer:GroupName` | `remoting-sample-audit-producer` | audit Producer 身份。 |
| `Sample:Topic` | `rocketmq-dotnet-manual` | 请求省略 `topic` 时使用的 topic。 |
| `Sample:Tag` | `sample` | 请求省略 `tag` 时使用的 tag。 |
| `Sample:Message` | `Hello from the RocketMQ Remoting producer sample.` | 请求省略 `body` 时使用的正文。 |

可使用标准 .NET 配置覆盖，例如
`RocketMQ__Remoting__NamesrvAddr=broker-nameserver:9876`。

## Broker 和网络前置条件

- 使用接受 classic Remoting 客户端的 RocketMQ NameServer 和 Broker。配置的地址是 **NameServer**，不是 Proxy
  端点。
- 客户端会先向 NameServer 查询路由，然后直接连接其返回的 Broker 地址。因此运行此 API 的机器必须同时能访问
  `NamesrvAddr` 和每个返回的 Broker 主机及端口。
- 创建可写的普通 topic `rocketmq-dotnet-manual`，或修改 `Sample:Topic` 和请求值。不要依赖生产环境 Broker 允许自动
  创建 topic。
- Producer group 是客户端身份；此示例不需要 consumer group，也不需要调用 `updateSubGroup`。如果启用了 ACL
  或 topic 权限，请授予配置的 Producer group 查询路由和向该 topic 发送消息的权限。

本仓库提供的[本地 RocketMQ 环境](../../../test-environments/rocketmq/README.zh-CN.md)适合在宿主机上运行此示例。在仓库根目录
启动它并创建默认 topic：

```shell
docker compose -f test-environments/rocketmq/compose.yaml up -d --wait
docker compose -f test-environments/rocketmq/compose.yaml exec broker sh mqadmin updateTopic \
  -n nameserver:9876 -c DefaultCluster -t rocketmq-dotnet-manual
```

该环境会公布宿主机可访问的 Broker 地址。在另一个容器中运行此示例时，需要不同的 Broker 公布地址和
`NamesrvAddr`；`localhost:9876` 不是容器到宿主机的地址。

## 运行

```shell
dotnet run --project samples/remoting/Producer/EventHorizon.RocketMQ.Samples.Remoting.Producer.csproj
```

若需要固定的命令行监听地址，请禁用 launch profile 并设置 `ASPNETCORE_URLS`：

```shell
ASPNETCORE_URLS=http://localhost:5000 \
  dotnet run --no-launch-profile --project samples/remoting/Producer/EventHorizon.RocketMQ.Samples.Remoting.Producer.csproj

curl --request POST http://localhost:5000/messages \
  --header 'Content-Type: application/json' \
  --data '{"body":"Hello from curl."}'
```

## 重要行为

- audit 路由不是 `/messages` 的镜像，也不会自动为默认发送创建审计记录。它只解析 `registrationName` 为 `audit` 的
  keyed 客户端注册中的 `IRemotingProducer` keyed service；消息需要使用该 keyed 客户端注册时，必须显式调用该路由。
- 两个客户端注册共用顶层 `Sample` 默认值。只有 `registrationName` 为 `audit` 的 keyed 客户端注册必须使用不同集群或
  Producer 身份时，才需要单独配置 audit 的 `Remoting` 和 `Producer` 配置节。
- 这是普通消息示例，不演示事务消息、延迟消息、请求-响应或单向发送。
