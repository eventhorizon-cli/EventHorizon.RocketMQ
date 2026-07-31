# 不使用 Generic Host 的 Remoting Pull Consumer

[English](README.md) | [简体中文](README.zh-CN.md)

此控制台项目在不启动 .NET Generic Host 的进程中使用 `IRemotingPullConsumer`。它显式启动 Consumer，发现物理队列，
为每个队列维护位点，处理每个拉取批次，仅在处理成功后持久化下一位点，并在 Ctrl+C 时停止。

## 生命周期与工作流

`AddRemotingPullConsumer` 会添加 SDK `IHostedService`，但普通的 `ServiceCollection.BuildServiceProvider()` 不会运行它。
非 Host 程序因此会解析协议专用角色，调用 `StartAsync`，并在 `finally` 中调用 `StopAsync`。`StopAsync` 使用
`CancellationToken.None`，因为 Ctrl+C 已经取消了循环使用的 token。
角色停止后，`await using` Service Provider 会被异步释放。

Raw Pull 将队列和位点的完整所有权交给应用。主路径明确展示了这一契约：

1. `GetMessageQueuesAsync` 发现 `eventhorizon-test-topic` 的可读物理队列。
2. `GetOffsetAsync` 在存在时使用 group 已提交位点；否则以 `QueryOffsetAsync(..., End)` 选择初始位置。
3. `PullAsync` 从所选队列的当前位点读取消息。
4. 程序记录每条返回消息，仅推进该队列的本地位置，然后调用 `UpdateOffsetAsync`。

先处理，后提交。若程序在 `UpdateOffsetAsync` 前失败，后续运行可能再次收到该批次，因此实际处理必须容忍重复。此示例
刻意使用单个循环；需要运行多个实例或并发队列循环的应用，还必须有明确的队列归属和连续位点策略。

常规 .NET `IHost` 请使用[对应的 Generic Host Pull Consumer 示例](../../GenericHost/PullConsumer/README.zh-CN.md)。该 Host
会驱动 SDK 生命周期；应用代码不能手动调用 `IRemotingPullConsumer.StartAsync` 或 `StopAsync`。

## 运行

先启动仓库提供的本地 RocketMQ 环境，再启动此 Consumer，之后发送消息：

```shell
docker compose -f test-environments/rocketmq/compose.yaml up -d --wait
dotnet run --project samples/remoting/NonHost/PullConsumer/EventHorizon.RocketMQ.Samples.Remoting.NonHost.PullConsumer.csproj
```

按一次 Ctrl+C 可优雅停止。使用[非 Host Producer 示例](../Producer/README.zh-CN.md)发布匹配消息。默认 NameServer 为
`localhost:9876`；可通过 `RocketMQ__Remoting__NamesrvAddr=nameserver.example:9876` 覆盖。外部集群必须公开 NameServer
和每个公布的 Broker 地址，创建 Topic，并授予路由查询和消费权限。

完整 Pull Consumer API 和位点语义参见
[Remoting 协议指南](../../../../src/EventHorizon.RocketMQ.Remoting/README.zh-CN.md)。
