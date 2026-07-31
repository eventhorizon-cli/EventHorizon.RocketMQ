# 不使用 Generic Host 的 Remoting POP Consumer

[English](README.md) | [简体中文](README.zh-CN.md)

此控制台项目在不启动 .NET Generic Host 的进程中使用 `IRemotingPopConsumer`。它显式启动 Consumer，发现可读物理队列，
为每个 POP 请求选择一个队列，处理返回消息，在处理后确认每个 receipt，并在 Ctrl+C 时停止。

## 生命周期与工作流

`AddRemotingPopConsumer` 会添加 SDK `IHostedService`，但普通 `ServiceCollection.BuildServiceProvider()` 不会运行它。
非 Host 程序因此会解析 `IRemotingPopConsumer`，调用 `StartAsync`，运行调用方拥有的 POP 循环，并在 `finally` 中调用
`StopAsync`。关闭调用使用 `CancellationToken.None`，因为 Ctrl+C 已经取消了循环 token。
角色停止后，`await using` Service Provider 会被异步释放。

POP 不执行后台分配或结算。示例明确展示由应用拥有的步骤：

1. `GetMessageQueuesAsync` 发现 `eventhorizon-test-topic` 的可读物理队列。
2. 循环选择一个队列并调用 `PopAsync`。
3. 处理每一条返回消息后，把它的 `RemotingPopReceipt` 传给 `AcknowledgeAsync`。

receipt 只在不可见时间内有效。未确认消息会在该时间过期后再次可投递。长时间运行的工作必须使用
`ChangeInvisibleTimeAsync` 续期，并确认替换后的 receipt。示例只做简短的日志工作，因此直接确认已经足够；生产处理在
确认无法完成时必须容忍重新投递。

常规 .NET `IHost` 请使用[对应的 Generic Host POP Consumer 示例](../../GenericHost/PopConsumer/README.zh-CN.md)。该 Host
会驱动 SDK 生命周期；应用代码不能手动调用 `IRemotingPopConsumer.StartAsync` 或 `StopAsync`。

## 运行

先启动仓库提供的本地 RocketMQ 环境，再启动此 Consumer，之后发布消息：

```shell
docker compose -f test-environments/rocketmq/compose.yaml up -d --wait
dotnet run --project samples/remoting/NonHost/PopConsumer/EventHorizon.RocketMQ.Samples.Remoting.NonHost.PopConsumer.csproj
```

按一次 Ctrl+C 可优雅停止。使用[非 Host Producer 示例](../Producer/README.zh-CN.md)发布消息。默认 NameServer 是
`localhost:9876`；可使用标准配置覆盖，例如 `RocketMQ__Remoting__NamesrvAddr=nameserver.example:9876`。
外部集群必须公开 NameServer 和每个公布的 Broker 端点，创建 Topic，并授予路由查询、消费、POP 和确认权限。

receipt 续期、不可见时间、过滤和完整 POP API 参见
[Remoting 协议指南](../../../../src/EventHorizon.RocketMQ.Remoting/README.zh-CN.md)。
