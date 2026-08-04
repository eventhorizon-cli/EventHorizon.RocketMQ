# gRPC Samples with Generic Host

[English](README.md) | [Simplified Chinese](README.zh-CN.md)

These projects use a normal .NET Generic Host. `AddRocketMQGrpc` and the role registration add the SDK hosted
service; `RunAsync` starts it with the application and stops it during host shutdown. Application code must not call
the same role's `StartAsync` or `StopAsync` manually.

| Sample | Public role | Workflow |
| --- | --- | --- |
| [Producer](Producer/README.md) | `IGrpcProducer` | Exposes a small HTTP API that sends through a RocketMQ 5 Proxy. |
| [LiteProducer](LiteProducer/README.md) | `IGrpcProducer` | Publishes Lite messages to a LITE parent topic and logical LiteTopic through a small HTTP API. |
| [SimpleConsumer](SimpleConsumer/README.md) | `IGrpcSimpleConsumer` | Receives, acknowledges, and changes invisibility in an application-owned receive loop. |
| [PushConsumer](PushConsumer/README.md) | `IGrpcPushConsumer` | Lets the SDK own assignment polling, handler dispatch, Proxy renewal requests, and settlement. |
| [LitePushConsumer](LitePushConsumer/README.md) | `IGrpcLitePushConsumer` | Synchronizes LiteTopics and dispatches messages in a LITE-enabled deployment. |

For a process that only builds a plain `ServiceProvider`, use the sibling [non-Host samples](../NonHost/README.md).
They show the required explicit `StartAsync` and `StopAsync` calls.
