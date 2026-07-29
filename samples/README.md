# EventHorizon.RocketMQ samples

[English](README.md) | [简体中文](README.zh-CN.md)

Each project demonstrates one protocol-specific SDK role or integration. The main code path keeps registration,
public API calls, completion, and failure handling visible; the linked guide explains what the mode means and when to
choose it.

## Choose a transport

The two clients are independent and have different connection and feature models:

- **gRPC** connects to a RocketMQ 5 Proxy through `GrpcClientOptions.Endpoint`. Choose it for Proxy-based deployments
  and the RocketMQ 5 gRPC message model.
- **Classic Remoting** discovers Brokers through `RemotingClientOptions.NamesrvAddr`, then connects directly to their
  advertised endpoints. Choose it for classic deployments and APIs that expose physical queues, offsets, POP, or
  classic Producer operations.

Samples do not hide these differences behind a transport-neutral wrapper. Start with the protocol actually exposed
by the deployment.

## gRPC roles

| Mode | Public API | Choose it when |
| --- | --- | --- |
| [Producer](grpc/Producer/README.md) | `IGrpcProducer` | The application publishes through a RocketMQ 5 Proxy. |
| [SimpleConsumer](grpc/SimpleConsumer/README.md) | `IGrpcSimpleConsumer` | Application code must drive receive, invisibility changes, acknowledgement, and dead-letter forwarding. |
| [PushConsumer](grpc/PushConsumer/README.md) | `IGrpcPushConsumer` | The SDK should own assignment polling, receive loops, handler concurrency, invisibility renewal, and settlement. |
| [LitePushConsumer](grpc/LitePushConsumer/README.md) | `IGrpcLitePushConsumer` | A LITE-enabled deployment uses service-managed LiteTopics beneath one bind topic. |

gRPC Push is client-initiated long polling; it is not a Broker-opened network push connection.

## Classic Remoting roles

| Mode | Public API | Choose it when |
| --- | --- | --- |
| [Producer](remoting/Producer/README.md) | `IRemotingProducer` | The application publishes through NameServer/Broker Remoting or needs classic queue, batch, one-way, transaction, recall, or request-reply operations. |
| [Admin](remoting/Admin/README.md) | `IRemotingAdmin` | A read-only tool must inspect physical queues, offset bounds, committed positions, or stored messages. |
| [Pull Consumer](remoting/PullConsumer/README.md) | `IRemotingPullConsumer` | The application owns physical queue selection, request offsets, processing, and commits. |
| [Lite Pull Consumer](remoting/LitePullConsumer/README.md) | `IRemotingLitePullConsumer` | The SDK should allocate queues while application code owns polling and commits. |
| [POP Consumer](remoting/PopConsumer/README.md) | `IRemotingPopConsumer` | Work uses per-message receipts and invisibility instead of queue-offset commits. |
| [Push Consumer](remoting/PushConsumer/README.md) | `IRemotingPushConsumer` | The SDK should own queue allocation, long polling, fair concurrent dispatch, retry settlement, and offset persistence. |

Remoting Push also uses client-initiated long polling. Consumers in one clustered group divide the Broker physical
queues; consumers in different groups consume independently.

## OpenTelemetry integration

The [OpenTelemetry samples](opentelemetry/README.md) show how an application subscribes to the protocol-specific
ActivitySource and Meter emitted by both clients. The application owns the OpenTelemetry provider, resource,
sampling, views, processors, and exporters.

## Run locally

The normal environment supports ordinary gRPC Producer, SimpleConsumer, and PushConsumer roles through its Proxy and
the classic Remoting roles through its NameServer and Broker. It creates `eventhorizon-test-topic` before startup
completes:

```shell
docker compose -f test-environments/rocketmq/compose.yaml up -d --wait
dotnet run --project samples/grpc/SimpleConsumer
```

Replace the project path with any ordinary sample. Each project README states its additional resources, permissions,
server capabilities, and complete command.

LitePush requires a LITE parent topic, a bound consumer group, Broker LMQ support, and a cluster-mode Proxy that
implements `SyncLiteSubscription`. Use its dedicated environment instead of the normal stack:

```shell
docker compose -f test-environments/rocketmq-litepush/compose.yaml up -d --wait
dotnet run --project samples/grpc/LitePushConsumer
```

For the observability workflow, run the normal RocketMQ environment together with
`test-environments/otel-lgtm/compose.yaml`, then start the OpenTelemetry consumer and producer projects.

## Configuration conventions

SDK-owned connection and role options use protocol-specific `RocketMQ` sections and are passed directly to the
matching registration delegate. Ordinary sample resource names, filters, message properties, and small method
arguments stay beside the public SDK call so the demonstrated workflow is visible. OpenTelemetry application settings
use their separate `OpenTelemetry` section.

Every project includes runnable defaults in `appsettings.json`. Normal .NET configuration overrides apply, for
example `RocketMQ__Client__Endpoint=proxy.example:8081` or
`RocketMQ__Remoting__NamesrvAddr=nameserver.example:9876`. Producer and Admin Web API projects expose Swagger; their
HTTP endpoints are application shells around the SDK workflows documented in their guides.

## Coverage boundaries

Each project demonstrates one coherent end-to-end workflow, not every overload. Advanced paths that require different
resources, a cooperating peer, or a real processing reason remain in the protocol guides until they justify a
dedicated runnable sample. Examples include Producer transactions and request-reply, runtime subscription changes,
manual assignment and seeking, visibility extension, and alternate Push delivery modes.

See the [gRPC guide](../src/EventHorizon.RocketMQ.Grpc/README.md) and
[Remoting guide](../src/EventHorizon.RocketMQ.Remoting/README.md) for the complete public APIs.
