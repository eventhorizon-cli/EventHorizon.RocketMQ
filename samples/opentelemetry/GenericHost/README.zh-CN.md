# 使用 Generic Host 的 OpenTelemetry 示例

[OpenTelemetry 总览](../README.zh-CN.md) | [所有示例](../../README.zh-CN.md) | [English](README.md)

两个 ASP.NET Core 应用都运行在 Generic Host 中。已注册的 RocketMQ Producer 和 Consumer 角色由 Host 负责启动和停止；
端点 handler 和应用代码不会直接调用角色生命周期方法。

| 示例 | 工作流 |
| --- | --- |
| [Producer Web API](ProducerWebApi/README.zh-CN.md) | 通过显式 gRPC 和 Remoting 路由发送消息，并导出 trace 和 metric。 |
| [Consumer Web API](ConsumerWebApi/README.zh-CN.md) | 承载独立的 gRPC 和 Remoting Push Consumer，并导出其投递信号。 |

应先启动 Consumer，使两个 Push 角色在 Producer 接收请求前开始 long poll。两个 Consumer Group 独立订阅同一 topic，因此
任一 Producer 路由发送的消息都可以由两个 group 处理。本地环境、请求和 Grafana dashboard 请参阅
[OpenTelemetry 概览](../README.zh-CN.md)。
