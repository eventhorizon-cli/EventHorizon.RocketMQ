# EventHorizon.RocketMQ 示例

[English](README.md) | [简体中文](README.zh-CN.md)

以下每个目录都是独立的 .NET 8 项目。Consumer 示例使用 Generic Host；Producer 示例是基于 Minimal API 的
标准 ASP.NET Core Web 应用。它们通过协议专用的依赖注入完成注册，因此客户端角色会随应用一起启动和停止。

每个示例目录都有独立说明。运行前请先阅读对应文档：其中列出所需的 Broker 或 Proxy 能力、资源和权限、支持的
服务端版本，以及哪些行为受服务端或协议限制。以下准备步骤只适用于本仓库提供的本地环境；端点能够连通，并不表示每项
RocketMQ 功能都已可用。

## 运行前准备

在仓库根目录启动本地测试环境：

```shell
docker compose -f test-environments/rocketmq/compose.yaml up -d --wait
```

如果 Broker 禁用了自动资源创建，请在运行前创建默认的 `rocketmq-dotnet-manual` Topic 和普通示例使用的
Consumer Group：

```shell
docker compose -f test-environments/rocketmq/compose.yaml exec broker sh mqadmin updateTopic -n nameserver:9876 -c DefaultCluster -t rocketmq-dotnet-manual

for group in rocketmq-dotnet-grpc-simple-sample rocketmq-dotnet-grpc-push-sample remoting-sample-pull-consumer remoting-sample-lite-pull-consumer remoting-sample-pop-consumer remoting-sample-push-consumer
do
    docker compose -f test-environments/rocketmq/compose.yaml exec broker sh mqadmin updateSubGroup -n nameserver:9876 -c DefaultCluster -g "$group"
done
```

端点和其他 Broker 命令请参阅[通用本地环境说明](../test-environments/rocketmq/README.zh-CN.md)。

LitePush 使用独立环境，因为它需要 LITE parent Topic、与之绑定的 Consumer Group，以及支持
`SyncLiteSubscription` RPC 的 cluster-mode Proxy。先停止占用 `localhost:8081` 的环境，再启动专用环境：

```shell
docker compose -f test-environments/rocketmq-litepush/compose.yaml up -d --wait
```

该环境会自动创建 `rocketmq-dotnet-lite-manual` 和 `rocketmq-dotnet-grpc-lite-push-sample`。示例启动时，
`IGrpcLitePushConsumer` 会同步 `rocketmq-dotnet-lite-sample` LiteTopic。完整的准备过程与限制请参阅
[LitePush 环境说明](../test-environments/rocketmq-litepush/README.zh-CN.md)。

gRPC 项目使用 `RocketMQ:Client` 配置 `GrpcClientOptions`，使用 `RocketMQ:Producer` 或
`RocketMQ:Consumer` 配置角色 Options。消息正文、订阅列表等仅供示例运行时使用的设置，会在需要时放在顶层
`Sample` 配置节中。例如：

```shell
RocketMQ__Client__Endpoint=localhost:8081 Sample__Topic=rocketmq-dotnet-manual dotnet run --project samples/grpc/Producer
```

classic Remoting 项目使用 `RocketMQ:Remoting` 配置 NameServer 连接，使用
`RocketMQ:Producer`、`RocketMQ:PullConsumer` 或 `RocketMQ:PushConsumer` 等具体角色配置节。仅供示例
运行时使用的设置同样放在顶层 `Sample` 配置节中。例如：

```shell
RocketMQ__Remoting__NamesrvAddr=localhost:9876 Sample__Topic=rocketmq-dotnet-manual dotnet run --project samples/remoting/Producer
```

每个项目都包含带默认值的 `appsettings.json`。Consumer 示例会持续运行，直到按下 Ctrl+C。两个 Producer
示例和 Remoting Admin 示例都会托管 Web API，并持续提供服务，直到按下 Ctrl+C。这三个 Web 项目各有 HTTP
launch profile，通过 `launchBrowser` 和 `launchUrl` 请求打开 `/swagger`；受支持的 IDE 和调试启动器会遵循该请求。
若浏览器没有打开，请使用应用输出的监听地址并附加 `/swagger`。

## Producer Web API 和 Swagger

两个 Producer 示例均在 `/swagger` 提供 Swagger UI，并在 `/swagger/v1/swagger.json` 提供 OpenAPI 文档。
Swagger UI 可以直接调用默认客户端注册和 `registrationName` 为 `audit` 的 keyed 客户端注册对应的发送端点。

两个 Producer 示例均提供 `POST /messages`。其 JSON 请求正文可包含可选的 `topic`、`tag` 和 `body` 字段。
省略字段时，示例会使用 `Sample` 配置节中的对应值。

通过受支持的 IDE 或调试启动器中的 HTTP launch profile 启动任一 Producer 示例时，会自动打开 Swagger。
若希望在命令行中使用固定的监听地址，请禁用 launch profile、绑定已知的本地地址，然后手动打开
`http://localhost:5000/swagger`：

```shell
ASPNETCORE_URLS=http://localhost:5000 dotnet run --no-launch-profile --project samples/grpc/Producer

curl --request POST http://localhost:5000/messages \
    --header 'Content-Type: application/json' \
    --data '{"topic":"rocketmq-dotnet-manual","tag":"sample","body":"Hello from curl."}'
```

将 `samples/grpc/Producer` 替换为 `samples/remoting/Producer`，即可使用 classic Remoting Producer 示例。

两个 Producer 示例还注册了 `registrationName` 为 `audit` 的 keyed 客户端注册。该 `registrationName` 同时作为
keyed service 的 key。`POST /clients/audit/messages` 会通过对应的 keyed service 发送，与默认客户端注册的
`POST /messages` 路由相互独立。audit handler 通过
`FromKeyedServices` 从 DI 获取其协议专用的 keyed service：

```csharp
[FromKeyedServices("audit")] IGrpcProducer producer
```

Remoting 示例在 `IRemotingProducer` 上使用相同的特性。`registrationName` 为 `audit` 的 keyed 客户端注册默认使用
与默认客户端注册相同的本地 RocketMQ 环境，但可独立配置：gRPC 读取 `RocketMQ:Audit:Client` 和
`RocketMQ:Audit:Producer`；Remoting 读取 `RocketMQ:Audit:Remoting` 和 `RocketMQ:Audit:Producer`。

使用相同的可选 JSON 请求正文，通过 `registrationName` 为 `audit` 的 keyed 客户端注册提供的 keyed service
发送消息：

```shell
curl --request POST http://localhost:5000/clients/audit/messages \
    --header 'Content-Type: application/json' \
    --data '{"body":"Audit event from curl."}'
```

## gRPC

这些项目使用 `EventHorizon.RocketMQ.Grpc`，连接到 RocketMQ 5 Proxy。

| 角色 | 公共 API | 项目 | 演示内容 |
| --- | --- | --- | --- |
| Producer | `IGrpcProducer` | [`grpc/Producer`](grpc/Producer/README.zh-CN.md) | 调用 `POST /messages` 时发送普通消息。 |
| Simple consumer | `IGrpcSimpleConsumer` | [`grpc/SimpleConsumer`](grpc/SimpleConsumer/README.zh-CN.md) | 由应用控制接收、确认和转发死信。 |
| Push consumer | `IGrpcPushConsumer` | [`grpc/PushConsumer`](grpc/PushConsumer/README.zh-CN.md) | 通过 handler 进行基于长轮询的自动分发。 |
| Lite Push consumer | `IGrpcLitePushConsumer` | [`grpc/LitePushConsumer`](grpc/LitePushConsumer/README.zh-CN.md) | 针对配置的 bind topic 和 LiteTopic 自动分发。 |

以项目目录作为参数运行 gRPC 项目，例如：

```shell
dotnet run --project samples/grpc/SimpleConsumer
```

通用 RocketMQ 5.5.0 环境支持标准 gRPC Producer、SimpleConsumer 和 PushConsumer。专用 LitePush 环境支持
LitePush 示例，并自动准备它所需的 LITE parent Topic 和 Consumer Group。完整的 LitePush 兼容性要求请参阅
[gRPC 指南](../src/EventHorizon.RocketMQ.Grpc/README.zh-CN.md)。

## classic Remoting

这些项目使用 `EventHorizon.RocketMQ.Remoting`，并通过 NameServer 发现 Broker。

| 角色 | 公共 API | 项目 | 演示内容 |
| --- | --- | --- | --- |
| Producer | `IRemotingProducer` | [`remoting/Producer`](remoting/Producer/README.zh-CN.md) | 调用 `POST /messages` 时发送普通消息。 |
| Admin | `IRemotingAdmin` | [`remoting/Admin`](remoting/Admin/README.zh-CN.md) | 通过 HTTP/Swagger 只读检查队列、位点和物理消息。 |
| Pull consumer | `IRemotingPullConsumer` | [`remoting/PullConsumer`](remoting/PullConsumer/README.zh-CN.md) | 显式队列发现、位点、拉取和位点提交。 |
| Lite Pull consumer | `IRemotingLitePullConsumer` | [`remoting/LitePullConsumer`](remoting/LitePullConsumer/README.zh-CN.md) | 基于订阅的分配、轮询和客户端管理的位点提交。 |
| POP consumer | `IRemotingPopConsumer` | [`remoting/PopConsumer`](remoting/PopConsumer/README.zh-CN.md) | 显式选择物理队列并确认 receipt。 |
| Push consumer | `IRemotingPushConsumer` | [`remoting/PushConsumer`](remoting/PushConsumer/README.zh-CN.md) | 通过 handler 进行基于长轮询的自动分发。 |

以项目目录作为参数运行 Remoting 项目，例如：

```shell
dotnet run --project samples/remoting/PopConsumer
```

Remoting 项目面向本仓库提供的 NameServer/Broker 测试环境。它们与 gRPC 项目相互独立：请根据应用使用的协议选择
对应样例。
