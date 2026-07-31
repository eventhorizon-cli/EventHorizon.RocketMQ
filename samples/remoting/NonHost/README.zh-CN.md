# 不使用 Generic Host 的 Remoting 角色

[English](README.md) | [简体中文](README.zh-CN.md)

本集合展示不使用 .NET Generic Host 的进程中全部受生命周期管理的 classic Remoting 角色。每个子目录都是可独立运行的
控制台项目：它构造普通 `ServiceProvider`，解析协议专用角色，显式调用 `StartAsync`，执行核心工作流，在 `finally` 中调用
`StopAsync`，最后异步释放服务提供程序。

每个角色专用的 `AddRemoting...` 注册都会添加 `IHostedService`，但 `ServiceCollection.BuildServiceProvider()` 不会启动这些
服务，所以只有这种非 Host 形式由应用拥有生命周期。当应用启动真正的 `IHost` 或 `WebApplication` 时，Host 会调用已注册的
服务；此时应用不能再自行调用角色的 `StartAsync` 或 `StopAsync`。

## 包含的角色

| 角色 | 公共 API | 核心工作流 | 项目 |
| --- | --- | --- | --- |
| Producer | `IRemotingProducer` | 启动、构造并发送一条带 tag 的消息、检查结果状态、停止。 | `Producer` |
| Pull Consumer | `IRemotingPullConsumer` | 启动、发现物理队列、为每个队列解析并持久化位点、拉取并处理消息、停止。 | `PullConsumer` |
| Lite Pull Consumer | `IRemotingLitePullConsumer` | 启动、轮询 SDK 分配的队列、处理消息、提交本地位置、停止。 | `LitePullConsumer` |
| Push Consumer | `IRemotingPushConsumer` | 启动、由 SDK 分发到 scoped handler、处理后返回 `Success`、停止。 | `PushConsumer` |
| POP Consumer | `IRemotingPopConsumer` | 启动、选择物理队列、POP 消息、处理后确认每个 receipt、停止。 | `PopConsumer` |

Consumer 会持续运行到按下 Ctrl+C。Ctrl+C token 被取消后，关闭路径刻意使用 `CancellationToken.None`，这样 `StopAsync`
仍能优雅完成。Producer 发送一条消息后即退出。

这些直接工作流保持分离：Pull 由应用负责队列和位点协调；Lite Pull 基于 SDK 分配但仍由调用方负责 poll 和 commit；Push
由 SDK 负责 handler 分发和重试；POP 由应用负责 receipt 结算。各项目不会用共享的示例抽象掩盖这些协议专用职责。

## 运行项目

先启动仓库提供的本地 RocketMQ 环境：

```shell
docker compose -f test-environments/rocketmq/compose.yaml up -d --wait
```

然后运行一个项目：

```shell
dotnet run --project samples/remoting/NonHost/Producer/EventHorizon.RocketMQ.Samples.Remoting.NonHost.Producer.csproj
dotnet run --project samples/remoting/NonHost/PullConsumer/EventHorizon.RocketMQ.Samples.Remoting.NonHost.PullConsumer.csproj
dotnet run --project samples/remoting/NonHost/LitePullConsumer/EventHorizon.RocketMQ.Samples.Remoting.NonHost.LitePullConsumer.csproj
dotnet run --project samples/remoting/NonHost/PushConsumer/EventHorizon.RocketMQ.Samples.Remoting.NonHost.PushConsumer.csproj
dotnet run --project samples/remoting/NonHost/PopConsumer/EventHorizon.RocketMQ.Samples.Remoting.NonHost.PopConsumer.csproj
```

每个项目默认使用 `localhost:9876`，并接受标准 .NET 配置覆盖，例如
`RocketMQ__Remoting__NamesrvAddr=nameserver.example:9876`。外部环境必须公开 NameServer 和路由发现返回的所有 Broker
端点，创建 `eventhorizon-test-topic`，并授予相应 Producer 或 Consumer 权限。

关于 Generic Host 对应示例和每个角色的投递契约，参见 [Remoting Producer](../GenericHost/Producer/README.zh-CN.md)、
[Pull Consumer](../GenericHost/PullConsumer/README.zh-CN.md)、[Lite Pull Consumer](../GenericHost/LitePullConsumer/README.zh-CN.md)、
[Push Consumer](../GenericHost/PushConsumer/README.zh-CN.md)、[POP Consumer](../GenericHost/PopConsumer/README.zh-CN.md)，以及
[Remoting 协议指南](../../../src/EventHorizon.RocketMQ.Remoting/README.zh-CN.md)。
