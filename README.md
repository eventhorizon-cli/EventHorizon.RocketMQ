# EventHorizon.RocketMQ

[English](https://github.com/eventhorizon-cli/EventHorizon.RocketMQ/blob/main/README.md) |
[简体中文](https://github.com/eventhorizon-cli/EventHorizon.RocketMQ/blob/main/README.zh-CN.md)

An **unofficial Apache RocketMQ client for .NET**.

It provides **full client functionality** across RocketMQ 5 protobuf/gRPC and classic Remoting, covering Producer,
Consumer, and Admin APIs as well as transactional, timed/delay, FIFO, and priority messages.

Designed for **.NET 8 or later**, it integrates with **Microsoft dependency injection**, Generic Host, options, and
logging, and provides a **modern, strongly typed API experience**.

> **Start with the [runnable samples](samples/README.md).** Each sample directory explains its required
> RocketMQ Broker or Proxy capabilities, resource setup, configuration, run command, and protocol caveats.

## Features and status

`✅` means the corresponding client project implements the API. Server-side capabilities and configuration still apply.

### RocketMQ 5 gRPC

See the [gRPC guide](src/EventHorizon.RocketMQ.Grpc/README.md) and
[runnable samples](samples/README.md) for configuration and compatibility details.

| Feature | Status | Notes |
| --- | :---: | --- |
| Producer with standard messages | ✅ | `IGrpcProducer.SendAsync`. |
| Producer with FIFO messages | ✅ | Set `MessageGroup`. |
| Producer with timed/delay messages | ✅ | Set `DeliveryTimestamp`. |
| Producer with transactional messages | ✅ | Use `SendTransactionAsync` and configure a transaction checker. |
| Producer with recalling timed/delay messages | ✅ | Recall with the receipt's recall handle before delivery. |
| Simple consumer | ✅ | `IGrpcSimpleConsumer` provides explicit receive, acknowledgement, invisibility, and DLQ operations. |
| Push consumer with concurrent message listener | ✅ | `IGrpcPushConsumer` dispatches messages concurrently. |
| Push consumer with FIFO message listener | ✅ | Each `MessageGroup` is processed in order. |
| Push consumer with FIFO consume accelerator | ✅ | Independent message groups dispatch in parallel while preserving each group's order. |
| Priority message | ✅ | Set `Message.Priority`. |
| Lite Producer messages and LitePushConsumer | ✅ | LitePush requires a Proxy implementation of `SyncLiteSubscription`. |
| Tag and SQL filtering | ✅ | SQL filtering requires the target server's filtering configuration. |
| Runtime subscriptions, retry, and dead-letter handling | ✅ | Available through SimpleConsumer, PushConsumer, or LitePushConsumer as applicable. |
| Dependency injection, options, logging, Generic Host lifecycle, and default or keyed client registrations | ✅ | Register with `AddRocketMQGrpc`. |

gRPC Push and LitePush use client-initiated assignment and long polling; they are not protocol-level Broker push.

### Classic Remoting

See the [Remoting guide](src/EventHorizon.RocketMQ.Remoting/README.md) and
[runnable samples](samples/README.md) for configuration and compatibility details.

| Feature | Status | Notes |
| --- | :---: | --- |
| Producer with standard, FIFO, timed/delay, priority, or Lite messages | ✅ | `IRemotingProducer`; the target Broker must support the selected message type. |
| Selected queues, queue selectors, batch sends, and one-way sends | ✅ | Classic Producer APIs. |
| Transactional messages, delayed-message recall, and request-reply | ✅ | Require the corresponding classic Broker capability. |
| Read-only Admin API | ✅ | `IRemotingAdmin` queries queues, messages, and offsets. |
| PullConsumer | ✅ | Explicit queues and offsets with tag or SQL filtering. |
| LitePullConsumer | ✅ | Client-side polling, assignment, seek, pause/resume, and commit; clustered consumption only. |
| POPConsumer | ✅ | Explicit queue selection, receipt acknowledgement, and invisibility renewal; normal topics only. |
| PushConsumer | ✅ | Clustering or broadcasting, concurrent or FIFO dispatch, runtime subscriptions, retry, and DLQ handling. |
| Dependency injection, options, logging, Generic Host lifecycle, and default or keyed client registrations | ✅ | Register with `AddRocketMQRemoting`. |

Classic Push is also client-initiated long polling. SQL filtering requires Broker configuration, and POP requires a
Broker that supports POP. Each protocol package provides its own `RocketMQClientException` base type.

## Packages

| Package | Connection | Main APIs | Documentation |
| --- | --- | --- | --- |
| `EventHorizon.RocketMQ.Grpc` | RocketMQ 5 Proxy | `IGrpcProducer`, `IGrpcSimpleConsumer`, `IGrpcPushConsumer`, `IGrpcLitePushConsumer` | [gRPC guide](https://github.com/eventhorizon-cli/EventHorizon.RocketMQ/blob/main/src/EventHorizon.RocketMQ.Grpc/README.md) |
| `EventHorizon.RocketMQ.Remoting` | NameServer and Brokers | `IRemotingProducer`, `IRemotingAdmin`, `IRemotingPullConsumer`, `IRemotingLitePullConsumer`, `IRemotingPopConsumer`, `IRemotingPushConsumer` | [Remoting guide](https://github.com/eventhorizon-cli/EventHorizon.RocketMQ/blob/main/src/EventHorizon.RocketMQ.Remoting/README.md) |

Install only the protocol package used by the application. Each protocol package is self-contained at the client API
boundary: it owns its public models in its own assembly and has no dependency on another EventHorizon.RocketMQ
package.

The two packages can be referenced by the same application, but their API models remain protocol-specific. See
[Protocol Boundaries and Type Ownership](docs/en-US/architecture/protocol-boundaries.md) for details.

## Repository

- [Runnable samples](samples/README.md)
- [Design notes](docs/README.md)
- [Local test environments](https://github.com/eventhorizon-cli/EventHorizon.RocketMQ/blob/main/test-environments/README.md)
- [Contributing and coding instructions](https://github.com/eventhorizon-cli/EventHorizon.RocketMQ/blob/main/AGENTS.md)

## License

Licensed under the [Apache License 2.0](https://github.com/eventhorizon-cli/EventHorizon.RocketMQ/blob/main/LICENSE).
