# 使用 Generic Host 的 gRPC 示例

[English](README.md) | [简体中文](README.zh-CN.md)

这些项目使用正常的 .NET Generic Host。`AddRocketMQGrpc` 和角色注册会添加 SDK hosted service；`RunAsync` 随应用启动它，
并在 Host 关闭时停止它。应用代码不能再手动调用同一角色的 `StartAsync` 或 `StopAsync`。

| 示例 | 公开角色 | 工作流 |
| --- | --- | --- |
| [Producer](Producer/README.zh-CN.md) | `IGrpcProducer` | 通过小型 HTTP API 经 RocketMQ 5 Proxy 发送消息。 |
| [LiteProducer](LiteProducer/README.zh-CN.md) | `IGrpcProducer` | 通过小型 HTTP API 向 LITE parent topic 和逻辑 LiteTopic 发布 Lite message。 |
| [SimpleConsumer](SimpleConsumer/README.zh-CN.md) | `IGrpcSimpleConsumer` | 在应用自有接收循环中接收、确认和调整不可见时间。 |
| [PushConsumer](PushConsumer/README.zh-CN.md) | `IGrpcPushConsumer` | 由 SDK 负责分配轮询、handler 分发、请求 Proxy 续期和结算。 |
| [LitePushConsumer](LitePushConsumer/README.zh-CN.md) | `IGrpcLitePushConsumer` | 在启用 LITE 的部署中同步 LiteTopic 并分发消息。 |

只构造普通 `ServiceProvider` 的进程应使用同级[非 Host 示例](../NonHost/README.zh-CN.md)，其中展示了必需的显式
`StartAsync` 和 `StopAsync` 调用。
