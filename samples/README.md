# EventHorizon.RocketMQ samples

[English](README.md) | [简体中文](README.zh-CN.md)

Each directory below is an independent .NET 8 project. Consumer samples use the Generic Host; Producer samples
are standard ASP.NET Core Web applications built with Minimal APIs. They use protocol-specific dependency-injection
registration, so client roles start and stop with the application.

Each sample directory has its own README. Read that guide before running the sample: it identifies the required
Broker or Proxy capabilities, resources and permissions, supported server versions, and behavior that can otherwise
be mistaken for a protocol guarantee. The setup below prepares the bundled local environment only; a reachable
endpoint alone does not make every RocketMQ feature available.

## Before running

Start the local test environment from the repository root:

```shell
docker compose -f test-environments/rocketmq/compose.yaml up -d --wait
```

When automatic resource creation is disabled, create the default `rocketmq-dotnet-manual` topic and the consumer
groups used by the standard samples before running them:

```shell
docker compose -f test-environments/rocketmq/compose.yaml exec broker sh mqadmin updateTopic -n nameserver:9876 -c DefaultCluster -t rocketmq-dotnet-manual

for group in rocketmq-dotnet-grpc-simple-sample rocketmq-dotnet-grpc-push-sample remoting-sample-pull-consumer remoting-sample-lite-pull-consumer remoting-sample-pop-consumer remoting-sample-push-consumer
do
    docker compose -f test-environments/rocketmq/compose.yaml exec broker sh mqadmin updateSubGroup -n nameserver:9876 -c DefaultCluster -g "$group"
done
```

See the [general local environment guide](../test-environments/rocketmq/README.md) for the endpoints and additional
Broker commands.

LitePush uses its own environment because it needs a LITE parent Topic, a matching Consumer Group binding, and a
cluster-mode Proxy that implements `SyncLiteSubscription`. Stop any environment that publishes `localhost:8081`, then
start the dedicated environment:

```shell
docker compose -f test-environments/rocketmq-litepush/compose.yaml up -d --wait
```

It automatically prepares `rocketmq-dotnet-lite-manual` and `rocketmq-dotnet-grpc-lite-push-sample`; the sample
synchronizes its `rocketmq-dotnet-lite-sample` LiteTopic subscription when it starts. See the
[LitePush environment guide](../test-environments/rocketmq-litepush/README.md) for the exact setup and limits.

The gRPC projects use `RocketMQ:Client` for `GrpcClientOptions` and `RocketMQ:Producer` or
`RocketMQ:Consumer` for the role options. Sample-only runtime settings, such as a message body or subscription
list, are in the top-level `Sample` section when the sample needs them. For example:

```shell
RocketMQ__Client__Endpoint=localhost:8081 Sample__Topic=rocketmq-dotnet-manual dotnet run --project samples/grpc/Producer
```

The classic Remoting projects use `RocketMQ:Remoting` for the NameServer connection and a concrete role section
such as `RocketMQ:Producer`, `RocketMQ:PullConsumer`, or `RocketMQ:PushConsumer`. Their sample-only runtime
settings are likewise in the top-level `Sample` section. For example:

```shell
RocketMQ__Remoting__NamesrvAddr=localhost:9876 Sample__Topic=rocketmq-dotnet-manual dotnet run --project samples/remoting/Producer
```

Every project also contains an `appsettings.json` file with its default values. Consumer samples continue until
they receive Ctrl+C. Both Producer samples and the Remoting Admin sample host Web APIs and continue serving until
they receive Ctrl+C. Each has a single HTTP launch profile that sets `launchBrowser` and `launchUrl` to open
`/swagger`; supported IDE and debug launchers honor that request. If a browser is not opened, use the listening
address printed by the application and append `/swagger`.

## Producer Web API and Swagger

Both Producer samples expose Swagger UI at `/swagger` and their OpenAPI document at
`/swagger/v1/swagger.json`. Swagger UI can invoke both the default and keyed audit send endpoints directly.

Both Producer samples expose `POST /messages`. Its JSON request body can contain optional `topic`, `tag`, and
`body` fields. When a field is omitted, the sample uses the matching value from the `Sample` configuration section.

Use the HTTP launch profile in a supported IDE or debug launcher to open Swagger automatically. For a predictable
command-line fallback, disable the launch profile, bind a known local address, and then open
`http://localhost:5000/swagger` manually:

```shell
ASPNETCORE_URLS=http://localhost:5000 dotnet run --no-launch-profile --project samples/grpc/Producer

curl --request POST http://localhost:5000/messages \
    --header 'Content-Type: application/json' \
    --data '{"topic":"rocketmq-dotnet-manual","tag":"sample","body":"Hello from curl."}'
```

Replace `samples/grpc/Producer` with `samples/remoting/Producer` to use the classic Remoting Producer sample.

Both Producer samples also register an `audit` keyed client registration with registration name `audit`. `POST /clients/audit/messages`
sends through its .NET keyed service, independently of the default `POST /messages` route. The audit handler receives its protocol-specific
producer from that service. The registration name is also used as the keyed-service key for `FromKeyedServices`:

```csharp
[FromKeyedServices("audit")] IGrpcProducer producer
```

The Remoting sample uses the same attribute with `IRemotingProducer`. The audit keyed client registration defaults to
the same local RocketMQ environment as the default client registration, but can be configured independently: gRPC reads
`RocketMQ:Audit:Client` and `RocketMQ:Audit:Producer`; Remoting reads `RocketMQ:Audit:Remoting` and
`RocketMQ:Audit:Producer`.

Send through the audit keyed service with the same optional JSON body:

```shell
curl --request POST http://localhost:5000/clients/audit/messages \
    --header 'Content-Type: application/json' \
    --data '{"body":"Audit event from curl."}'
```

## gRPC

These projects use `EventHorizon.RocketMQ.Grpc` and connect to a RocketMQ 5 Proxy.

| Role | Public API | Project | What it demonstrates |
| --- | --- | --- | --- |
| Producer | `IGrpcProducer` | [`grpc/Producer`](grpc/Producer/README.md) | Send a standard message when `POST /messages` is called. |
| Simple consumer | `IGrpcSimpleConsumer` | [`grpc/SimpleConsumer`](grpc/SimpleConsumer/README.md) | Application-controlled receive, acknowledgement, and dead-letter forwarding. |
| Push consumer | `IGrpcPushConsumer` | [`grpc/PushConsumer`](grpc/PushConsumer/README.md) | Long-poll-based automatic dispatch through a message handler. |
| Lite Push consumer | `IGrpcLitePushConsumer` | [`grpc/LitePushConsumer`](grpc/LitePushConsumer/README.md) | Automatic dispatch for a configured bind topic and LiteTopic. |

Run a gRPC project with its directory as the project argument, for example:

```shell
dotnet run --project samples/grpc/SimpleConsumer
```

The general RocketMQ 5.5.0 environment supports the standard gRPC Producer, SimpleConsumer, and PushConsumer paths.
The dedicated LitePush environment supports the LitePush sample and provisions its LITE parent Topic and Consumer
Group automatically. See the [gRPC guide](../src/EventHorizon.RocketMQ.Grpc/README.md) for the complete LitePush
compatibility requirements.

## Classic Remoting

These projects use `EventHorizon.RocketMQ.Remoting` and discover Brokers through a NameServer.

| Role | Public API | Project | What it demonstrates |
| --- | --- | --- | --- |
| Producer | `IRemotingProducer` | [`remoting/Producer`](remoting/Producer/README.md) | Send a standard message when `POST /messages` is called. |
| Admin | `IRemotingAdmin` | [`remoting/Admin`](remoting/Admin/README.md) | HTTP/Swagger read-only queue, offset, and physical-message inspection. |
| Pull consumer | `IRemotingPullConsumer` | [`remoting/PullConsumer`](remoting/PullConsumer/README.md) | Explicit queue discovery, offsets, pull, and offset commit. |
| Lite Pull consumer | `IRemotingLitePullConsumer` | [`remoting/LitePullConsumer`](remoting/LitePullConsumer/README.md) | Subscription-driven assignment, polling, and client-managed offset commit. |
| POP consumer | `IRemotingPopConsumer` | [`remoting/PopConsumer`](remoting/PopConsumer/README.md) | Explicit physical-queue selection and receipt acknowledgement. |
| Push consumer | `IRemotingPushConsumer` | [`remoting/PushConsumer`](remoting/PushConsumer/README.md) | Long-poll-based automatic dispatch through a message handler. |

Run a Remoting project with its directory as the project argument, for example:

```shell
dotnet run --project samples/remoting/PopConsumer
```

The Remoting projects are intended for the supplied NameServer/Broker test environment. They are separate from
the gRPC projects: use the matching sample for the protocol used by the application.
