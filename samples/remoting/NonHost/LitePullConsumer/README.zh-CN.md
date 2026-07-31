# 不使用 Generic Host 的 Remoting Lite Pull Consumer

[English](README.md) | [简体中文](README.zh-CN.md)

此控制台项目在不启动 .NET Generic Host 的进程中使用 `IRemotingLitePullConsumer`。它显式启动 Consumer，让 SDK 为订阅
分配队列，从这些分配中轮询消息，在处理后提交已推进的本地位置，并在 Ctrl+C 时停止。

## 生命周期与工作流

`AddRemotingLitePullConsumer` 会添加 SDK `IHostedService`，但普通 `ServiceCollection.BuildServiceProvider()` 不会执行它。
非 Host 应用必须解析 `IRemotingLitePullConsumer`，调用 `StartAsync`，运行 poll 循环，并在 `finally` 中调用 `StopAsync`。
Ctrl+C 后关闭逻辑使用 `CancellationToken.None`，所以不会和 poll 循环一起被取消。
角色停止后，`await using` Service Provider 会被异步释放。

示例配置了对 `eventhorizon-test-topic` 的订阅。启动后 SDK 会参与 group 分配，而 `PollAsync` 从一个活跃的已分配队列中
选择消息。程序处理返回的消息后才调用 `CommitAsync`。`PollAsync` 会推进本地位置，但不会自行持久化它们。

在 rebalance 边界，Consumer 可能暂时没有分配队列。示例把由此产生的 `InvalidOperationException` 当作可重试状态，并在
再次 poll 前等待。处理失败会被记录，并在短暂延迟后重试；实际应用仍必须让处理具备幂等性，因为成功 commit 前的失败可能
造成重复投递。

常规 .NET `IHost` 请使用[对应的 Generic Host Lite Pull Consumer 示例](../../GenericHost/LitePullConsumer/README.zh-CN.md)。
该 Host 会驱动 SDK 生命周期；应用代码不能手动调用 `IRemotingLitePullConsumer.StartAsync` 或 `StopAsync`。

## 运行

先启动仓库提供的本地 RocketMQ 环境，再启动此 Consumer，之后发布消息：

```shell
docker compose -f test-environments/rocketmq/compose.yaml up -d --wait
dotnet run --project samples/remoting/NonHost/LitePullConsumer/EventHorizon.RocketMQ.Samples.Remoting.NonHost.LitePullConsumer.csproj
```

按一次 Ctrl+C 可优雅停止。使用[非 Host Producer 示例](../Producer/README.zh-CN.md)发送带 sample tag 的消息。默认
NameServer 是 `localhost:9876`；标准配置覆盖包括 `RocketMQ__Remoting__NamesrvAddr=nameserver.example:9876`。
外部集群必须公开 NameServer 和每个公布的 Broker 端点，创建 Topic，并授予路由查询和消费权限。

订阅模式、手工分配、seek、pause 和完整 commit API 参见
[Remoting 协议指南](../../../../src/EventHorizon.RocketMQ.Remoting/README.zh-CN.md)。
