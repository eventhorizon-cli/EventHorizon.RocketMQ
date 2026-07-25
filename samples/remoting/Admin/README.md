# Remoting Admin sample

[简体中文](README.zh-CN.md)

This ASP.NET Core Minimal API resolves the read-only `IRemotingAdmin` role from dependency injection and exposes
classic RocketMQ queue, offset, and stored-message queries through HTTP. It does not create resources, change
consumer offsets, or mutate Broker state.

## HTTP API and Swagger

Swagger UI is available at `/swagger`, and the OpenAPI document is available at `/swagger/v1/swagger.json`. The
checked-in launch profile opens `http://localhost:5185/swagger` in launchers that honor `launchBrowser`.

| Route | Purpose |
| --- | --- |
| `GET /topics/{topic}/queues` | Discovers writable physical queues from NameServer routes. |
| `GET /topics/{topic}/queues/{brokerName}/{queueId}/offsets?committed=true` | Returns minimum offset, maximum offset, and earliest message-store time. Set `committed=false` to request the Broker's in-flight maximum offset. |
| `GET /topics/{topic}/queues/{brokerName}/{queueId}/offsets/search?timestamp={ISO-8601}&boundary=Lower` | Finds an offset at a required ISO 8601 timestamp. `boundary` defaults to `Lower`. |
| `GET /consumer-groups/{consumerGroup}/topics/{topic}/queues/{brokerName}/{queueId}/offset` | Reads a consumer group's committed offset; it is `null` when the group has not committed one. |
| `GET /topics/{topic}/messages/{offsetMessageId}` | Reads one physical message by the `OffsetMessageId` from a Remoting producer receipt. |

The route parameters `brokerName` and `queueId` come from the queue-discovery response. Broker failures are returned
as HTTP 502; invalid queue or `OffsetMessageId` input is returned as HTTP 400.

## Configuration

The sample reads only the Remoting connection section from `appsettings.json`:

| Key | Default | Purpose |
| --- | --- | --- |
| `RocketMQ:Remoting:NamesrvAddr` | `localhost:9876` | NameServer used to discover Broker routes. |

Override it with normal .NET configuration, for example
`RocketMQ__Remoting__NamesrvAddr=nameserver.example:9876`. This address is a **NameServer**, not a Proxy endpoint.

## Broker and network prerequisites

- Use a classic RocketMQ NameServer and Broker that accept Remoting clients. The API first queries the NameServer,
  then connects directly to the Broker addresses returned by route discovery.
- The process running this API must reach `NamesrvAddr` and every advertised Broker host and port. A reachable
  NameServer alone is not sufficient.
- Create the queried topic, such as `rocketmq-dotnet-manual`, before using production Brokers that disable automatic
  topic creation. The read-only Admin role itself does not need a Producer or consumer group. The consumer-offset
  route can query an existing group and returns `null` when it has no committed offset.
- Grant route-query and read/admin permissions when ACL or topic permissions are enabled.

`OffsetMessageId` has an additional networking requirement. It is the physical ID in a
`RemotingSendResult.OffsetMessageId`, not the producer-assigned `MessageId`; it encodes a Broker endpoint and a
commit-log offset. The message-view route decodes that endpoint and connects to it directly. The API host must be
able to reach the exact address and port embedded in the ID, even when NameServer route discovery works. A message
sent through a different network or a Broker that advertises an internal-only address cannot be viewed from this API.

The bundled [local RocketMQ environment](../../../test-environments/rocketmq/README.md) is suitable when this sample
runs on the host. Start it and create the topic from the repository root:

```shell
docker compose -f test-environments/rocketmq/compose.yaml up -d --wait
docker compose -f test-environments/rocketmq/compose.yaml exec broker sh mqadmin updateTopic \
  -n nameserver:9876 -c DefaultCluster -t rocketmq-dotnet-manual
```

The local stack exposes the NameServer on `localhost:9876` and advertises a host-reachable Broker on
`localhost:10911`. When the API runs in another container or network, configure a reachable NameServer and Broker
advertisement instead of using the host-only defaults.

## Run

```shell
dotnet run --project samples/remoting/Admin/EventHorizon.RocketMQ.Samples.Remoting.Admin.csproj
```

For a predictable command-line address, disable the launch profile:

```shell
ASPNETCORE_URLS=http://localhost:5000 \
  dotnet run --no-launch-profile --project samples/remoting/Admin/EventHorizon.RocketMQ.Samples.Remoting.Admin.csproj

curl http://localhost:5000/topics/rocketmq-dotnet-manual/queues
```

Use the returned `brokerName` and `queueId` to call the offset routes. To exercise the message-view route, first
send a message with the [Remoting Producer sample](../Producer/README.md), then use its returned
`OffsetMessageId` in `/topics/rocketmq-dotnet-manual/messages/{offsetMessageId}`.

## Important behavior

- This is a read-only inspection API. It does not send messages, create topics or groups, reset offsets, or
  acknowledge messages.
- `committed=true` is the default for the queue-offset route. Select `committed=false` only when the in-flight
  maximum offset is the intended Broker value.
- `MessageViewResponse.BodyBase64` is Base64 because a RocketMQ message body is arbitrary binary data. Decode it
  according to the producer's content contract rather than assuming UTF-8 text.

For the complete role API and connection options, see the
[Remoting guide](../../../src/EventHorizon.RocketMQ.Remoting/README.md).
