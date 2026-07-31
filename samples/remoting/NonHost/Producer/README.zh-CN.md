# 不使用 Generic Host 的 Remoting Producer

[English](README.md) | [简体中文](README.zh-CN.md)

此控制台项目在不启动 .NET Generic Host 的进程中使用 `IRemotingProducer`。它构造普通 `ServiceProvider`，自行启动
Producer，发送一条普通消息，检查 Broker 结果状态，停止 Producer，并异步释放容器。

## 生命周期与工作流

`AddRemotingProducer` 会为常规 Host 应用添加 SDK `IHostedService`。只调用
`ServiceCollection.BuildServiceProvider()` 不会执行该服务，因此这个非 Host 程序必须拥有两个生命周期调用：

```csharp
await using var serviceProvider = services.BuildServiceProvider();
var producer = serviceProvider.GetRequiredService<IRemotingProducer>();

try
{
    await producer.StartAsync(CancellationToken.None);
    RemotingSendResult result = await producer.SendAsync(message, CancellationToken.None);
}
finally
{
    await producer.StopAsync(CancellationToken.None);
}
```

即使由应用而非 SDK 发起 `SendAsync`，`StartAsync` 仍会在首次发送前准备 Producer 角色，因而是必需的。示例会构造一条
发往 `eventhorizon-test-topic` 的消息，设置 `sample` tag 和唯一 key，发送后输出所选 Broker 队列、位点和
`RemotingSendStatus`。发送完成仍可能携带非成功的持久化状态，所以状态不是 `SendOk` 时程序会返回非零退出码。

`finally` 块始终用未取消的 token 调用 `StopAsync`。这在调用方拥有的操作 token 已经取消时很重要。随后异步释放
Service Provider，以释放角色拥有的资源。

常规 .NET `IHost` 或 `WebApplication` 请使用[对应的 Generic Host Producer 示例](../../GenericHost/Producer/README.zh-CN.md)。
该 Host 会启动和停止 SDK 注册；应用代码不能再次调用 `IRemotingProducer.StartAsync` 或 `StopAsync`。

## 运行

先启动仓库提供的本地 RocketMQ 环境，再运行项目：

```shell
docker compose -f test-environments/rocketmq/compose.yaml up -d --wait
dotnet run --project samples/remoting/NonHost/Producer/EventHorizon.RocketMQ.Samples.Remoting.NonHost.Producer.csproj
```

默认 NameServer 地址为 `localhost:9876`。可使用标准 .NET 配置覆盖，例如
`RocketMQ__Remoting__NamesrvAddr=nameserver.example:9876`。外部集群必须公开 NameServer 和路由发现返回的每个 Broker
地址，提供可写的 `eventhorizon-test-topic`，并授予 Producer 权限。

队列选择、批量、事务、延迟消息和请求-响应等未包含在单条消息工作流中的操作，参见
[Remoting 协议指南](../../../../src/EventHorizon.RocketMQ.Remoting/README.zh-CN.md)。
