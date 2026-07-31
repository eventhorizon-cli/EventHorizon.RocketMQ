# 不使用 Generic Host 的 Remoting Push Consumer

[English](README.md) | [简体中文](README.zh-CN.md)

此控制台项目在不启动 .NET Generic Host 的进程中使用 `IRemotingPushConsumer`。它显式启动 Consumer，随后 SDK 会执行 group
分配、Broker 长轮询和 handler 分发，直到 Ctrl+C。scoped handler 处理带 `sample` tag 的批次，并在工作完成后返回
`ConsumeResult.Success`。

## 生命周期与工作流

`AddRemotingPushConsumer` 会添加 SDK `IHostedService`，但普通 `ServiceCollection.BuildServiceProvider()` 从不启动它。
非 Host 程序因此会解析 `IRemotingPushConsumer`，调用 `StartAsync`，等待 Ctrl+C，并在 `finally` 中调用 `StopAsync`。
关闭 token 使用 `CancellationToken.None`，因为 Ctrl+C 已经取消了等待 token。
角色停止后，`await using` Service Provider 会被异步释放。

`NonHostPushConsumerMessageHandler` 注册为 scoped，因此 SDK 会在每次投递时的新异步 scope 中解析它。它记录批次中的每个
`RemotingMessageView`。返回 `Success` 只会在整个批次完成后推进源队列位点；实际 handler 不能完成工作时应返回重试结果，
并且必须容忍重复投递。

当 group 不存在位点时，已配置的 group 从 `End` 开始；它以 `sample` tag filter 订阅
`eventhorizon-test-topic`，并使用有上限的 handler 并发。示例刻意只注册一个 Consumer 角色，让注册、生命周期、订阅和
结算都在一条路径中可见。

常规 .NET `IHost` 请使用[对应的 Generic Host Push Consumer 示例](../../GenericHost/PushConsumer/README.zh-CN.md)。
该 Host 会驱动 SDK 生命周期；应用代码不能手动调用 `IRemotingPushConsumer.StartAsync` 或 `StopAsync`。

## 运行

先启动仓库提供的本地 RocketMQ 环境，再启动此 Consumer，之后发布带 tag 的消息：

```shell
docker compose -f test-environments/rocketmq/compose.yaml up -d --wait
dotnet run --project samples/remoting/NonHost/PushConsumer/EventHorizon.RocketMQ.Samples.Remoting.NonHost.PushConsumer.csproj
```

按一次 Ctrl+C 可优雅停止。使用[非 Host Producer 示例](../Producer/README.zh-CN.md)发布带匹配 `sample` tag 的消息。
默认 NameServer 是 `localhost:9876`；可使用标准配置覆盖，例如
`RocketMQ__Remoting__NamesrvAddr=nameserver.example:9876`。外部集群必须公开 NameServer 和每个公布的 Broker 端点，
创建 Topic，并授予路由查询和消费权限；启用重试和死信结果时还需要相应访问权限。

完整 Push Consumer 配置和投递契约参见
[Remoting 协议指南](../../../../src/EventHorizon.RocketMQ.Remoting/README.zh-CN.md)。
