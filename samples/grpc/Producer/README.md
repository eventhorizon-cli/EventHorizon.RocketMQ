# gRPC Producer sample

[English](README.md) | [简体中文](README.zh-CN.md)

This ASP.NET Core Minimal API sample registers `IGrpcProducer` through `AddRocketMQGrpc` and
`AddGrpcProducer`. It sends a standard RocketMQ message only when an HTTP endpoint is called; it
does not publish messages in a background loop.

## Configuration

All values below come from `appsettings.json` and can be overridden with normal .NET configuration,
for example `RocketMQ__Client__Endpoint=proxy.example:8081`.

| Key | Default | Purpose |
| --- | --- | --- |
| `RocketMQ:Client:Endpoint` | `localhost:8081` | RocketMQ Proxy gRPC endpoint. |
| `RocketMQ:Client:RequestTimeout` | `00:00:03` | Deadline for ordinary client requests. |
| `RocketMQ:Client:UseTLS` | `false` | Enables TLS for the Proxy connection. |
| `RocketMQ:Producer:SendMsgTimeout` | `00:00:03` | Send timeout for the default client registration. |
| `RocketMQ:Audit:Client:*` | Same as `RocketMQ:Client` | Connection settings for the independent keyed client registration with registration name `audit`. |
| `RocketMQ:Audit:Producer:SendMsgTimeout` | `00:00:03` | Send timeout for the keyed client registration with registration name `audit`. |
| `Sample:Topic` | `rocketmq-dotnet-manual` | Topic used when a request omits `topic`. |
| `Sample:Tag` | `sample` | Tag used when a request omits `tag`. An empty value removes the tag. |
| `Sample:Message` | `Hello from the RocketMQ gRPC producer sample.` | UTF-8 body used when a request omits `body`. |

This role has no consumer group. `POST /messages` uses the default client registration;
`POST /clients/audit/messages` resolves a separate `IGrpcProducer` from the .NET keyed service for registration name `audit`.
The registration name is application configuration, not a Broker-side audit feature, and is also used as the keyed-service key.

## Prerequisites

- A RocketMQ 5 Proxy must be reachable at `RocketMQ:Client:Endpoint`; gRPC clients connect to a
  Proxy, never directly to a NameServer.
- The target Broker must accept standard messages for `rocketmq-dotnet-manual`, or the topic must
  already exist. Create it explicitly when automatic topic creation is disabled:

  ```shell
  docker compose -f test-environments/rocketmq/compose.yaml exec broker sh mqadmin updateTopic \
    -n nameserver:9876 -c DefaultCluster -t rocketmq-dotnet-manual
  ```

- The supplied RocketMQ 5.5.0 Compose environment supports this sample. It exposes its
  cluster-mode Proxy at `localhost:8081` and enables automatic topic creation, although explicit
  topic creation is the safer production setup.

Start that environment from the repository root when using the defaults:

```shell
docker compose -f test-environments/rocketmq/compose.yaml up -d --wait
```

## Run

The HTTP launch profile opens Swagger at `http://localhost:5231/swagger` in launchers that honor
`launchBrowser`. From the repository root, start the project with:

```shell
dotnet run --project samples/grpc/Producer
```

For a predictable command-line address, disable the launch profile:

```shell
ASPNETCORE_URLS=http://localhost:5000 \
  dotnet run --no-launch-profile --project samples/grpc/Producer
```

Then send a message through the default client registration:

```shell
curl --request POST http://localhost:5000/messages \
  --header 'Content-Type: application/json' \
  --data '{"topic":"rocketmq-dotnet-manual","tag":"sample","body":"Hello from curl."}'
```

Use the same JSON shape with `POST /clients/audit/messages` to select the keyed client registration. Swagger
is available at `/swagger`; its OpenAPI document is `/swagger/v1/swagger.json`.

## Semantics and common pitfalls

- A successful HTTP response means `IGrpcProducer.SendAsync` returned a `GrpcSendReceipt`. A failed
  send is logged and returned as HTTP 503; it is not queued locally for a later retry.
- This sample creates a new message key for every HTTP request. It sends only standard messages;
  FIFO, delayed, priority, transactional, recall, and Lite message setup require additional
  message properties and, in some cases, Broker support.
- `topic`, `tag`, and `body` are optional request fields. Omitting a field uses the `Sample` default;
  specifying an empty tag deliberately clears the tag.
- The default and `audit` endpoints can point at different Proxies or clusters. They happen to use
  the same local defaults, but they are separate DI client registrations and neither route falls back to the
  other.

For production options and non-standard message types, see the
[gRPC guide](../../../src/EventHorizon.RocketMQ.Grpc/README.md).
