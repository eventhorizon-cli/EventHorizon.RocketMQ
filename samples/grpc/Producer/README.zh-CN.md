# gRPC Producer 示例

[English](README.md) | [简体中文](README.zh-CN.md)

这是一个 ASP.NET Core Minimal API 示例。它通过 `AddRocketMQGrpc` 和 `AddGrpcProducer` 注册
`IGrpcProducer`，只在调用 HTTP 端点时发送标准 RocketMQ 消息，不会在后台循环发布消息。

## 配置

以下值来自 `appsettings.json`，可通过普通 .NET 配置覆盖，例如
`RocketMQ__Client__Endpoint=proxy.example:8081`。

| 配置键 | 默认值 | 用途 |
| --- | --- | --- |
| `RocketMQ:Client:Endpoint` | `localhost:8081` | RocketMQ Proxy gRPC 端点。 |
| `RocketMQ:Client:RequestTimeout` | `00:00:03` | 普通客户端请求的截止时间。 |
| `RocketMQ:Client:UseTLS` | `false` | 为 Proxy 连接启用 TLS。 |
| `RocketMQ:Producer:SendMsgTimeout` | `00:00:03` | 默认 Producer profile 的发送超时。 |
| `RocketMQ:Audit:Client:*` | 与 `RocketMQ:Client` 相同 | 独立 keyed `audit` profile 的连接设置。 |
| `RocketMQ:Audit:Producer:SendMsgTimeout` | `00:00:03` | keyed `audit` profile 的发送超时。 |
| `Sample:Topic` | `rocketmq-dotnet-manual` | 请求未提供 `topic` 时使用的主题。 |
| `Sample:Tag` | `sample` | 请求未提供 `tag` 时使用的 tag；空值会移除 tag。 |
| `Sample:Message` | `Hello from the RocketMQ gRPC producer sample.` | 请求未提供 `body` 时使用的 UTF-8 消息体。 |

该角色没有 consumer group。`POST /messages` 使用未命名的默认 Producer profile；
`POST /profiles/audit/messages` 通过 DI key `audit` 解析独立的 `IGrpcProducer`。此 key 是应用侧配置，
不是 Broker 端的审计功能。

## 前置条件

- `RocketMQ:Client:Endpoint` 必须指向可访问的 RocketMQ 5 Proxy；gRPC 客户端连接 Proxy，不会直接连接
  NameServer。
- 目标 Broker 必须允许向 `rocketmq-dotnet-manual` 发送标准消息，或者该 topic 已经存在。禁用自动建 topic
  时，请显式创建：

  ```shell
  docker compose -f test-environments/rocketmq/compose.yaml exec broker sh mqadmin updateTopic \
    -n nameserver:9876 -c DefaultCluster -t rocketmq-dotnet-manual
  ```

- 随附的 RocketMQ 5.5.0 Compose 环境支持该示例。它在 `localhost:8081` 提供 cluster-mode Proxy，并开启了
  自动建 topic；但生产环境中显式建 topic 更稳妥。

使用默认值时，在仓库根目录启动该环境：

```shell
docker compose -f test-environments/rocketmq/compose.yaml up -d --wait
```

## 运行

支持 `launchBrowser` 的启动器会通过 HTTP launch profile 打开 `http://localhost:5231/swagger`。在仓库根目录执行：

```shell
dotnet run --project samples/grpc/Producer
```

如需固定的命令行地址，请禁用 launch profile：

```shell
ASPNETCORE_URLS=http://localhost:5000 \
  dotnet run --no-launch-profile --project samples/grpc/Producer
```

随后通过默认 profile 发送消息：

```shell
curl --request POST http://localhost:5000/messages \
  --header 'Content-Type: application/json' \
  --data '{"topic":"rocketmq-dotnet-manual","tag":"sample","body":"Hello from curl."}'
```

将相同 JSON 调用到 `POST /profiles/audit/messages` 即可选择 keyed profile。Swagger 位于 `/swagger`，
OpenAPI 文档位于 `/swagger/v1/swagger.json`。

## 语义与常见误解

- HTTP 成功响应表示 `IGrpcProducer.SendAsync` 返回了 `GrpcSendReceipt`。发送失败会记录日志并返回 HTTP 503，
  不会在本地排队等待稍后重试。
- 每个 HTTP 请求都会生成一个新的消息 key。本示例只发送标准消息；FIFO、延迟、优先级、事务、撤回和 Lite
  消息还需要额外消息属性，部分场景还需要 Broker 支持。
- `topic`、`tag` 和 `body` 都是可选请求字段。省略字段会使用 `Sample` 默认值；显式指定空 tag 会清除 tag。
- 默认端点和 `audit` 端点可指向不同 Proxy 或集群。它们恰好使用相同的本地默认值，但它们是独立 DI profile，
  任一路由都不会回退到另一个 profile。

生产配置和非标准消息类型请参阅 [gRPC 指南](../../../src/EventHorizon.RocketMQ.Grpc/README.zh-CN.md)。
