# 使用 Generic Host 的 Remoting 示例

[English](README.md) | [简体中文](README.zh-CN.md)

这些项目运行在 .NET Generic Host 或 `WebApplication` 中。对于受生命周期管理的角色，`AddRocketMQRemoting` 注册的 SDK
hosted service 会随 Host 启停；应用代码不能再自行调用同一角色的 `StartAsync` 或 `StopAsync`。Admin 没有长生命周期角色，
但它同样是 hosted Web 应用，因此归入这里。

| 示例 | 公开角色 | 工作流 |
| --- | --- | --- |
| [Producer](Producer/README.zh-CN.md) | `IRemotingProducer` | 通过小型 HTTP API 进行 classic Remoting 发送。 |
| [Admin](Admin/README.zh-CN.md) | `IRemotingAdmin` | 通过 HTTP API 读取物理队列、位点和存储消息。 |
| [LitePullConsumer](LitePullConsumer/README.zh-CN.md) | `IRemotingLitePullConsumer` | 由应用代码轮询并提交 SDK 管理或手工分配的队列。 |
| [PushConsumer](PushConsumer/README.zh-CN.md) | `IRemotingPushConsumer` | 由 SDK 协调客户端或 Broker 分配、接收、分发和结算。 |

只构造普通 `ServiceProvider` 的进程中，受生命周期管理的角色应使用同级[非 Host 示例](../NonHost/README.zh-CN.md)。
