# Remoting Producer sample

[简体中文](README.zh-CN.md)

This ASP.NET Core minimal API resolves `IRemotingProducer` from dependency injection and sends normal Apache
RocketMQ messages through the classic Remoting protocol. It registers both the default producer profile and an
independent keyed `audit` profile.

## Public role and endpoints

| Route | Producer profile | Purpose |
| --- | --- | --- |
| `POST /messages` | Default `IRemotingProducer` | Sends one normal message. |
| `POST /profiles/audit/messages` | Keyed `"audit"` `IRemotingProducer` | Sends one normal message through the audit profile. |

Swagger UI is available at `/swagger`; the checked-in HTTP launch profile opens
`http://localhost:5184/swagger` when the launcher honors it. Both routes accept an optional JSON body with
`topic`, `tag`, and `body`. Supplied values override the matching `Sample` value.

```json
{
  "topic": "rocketmq-dotnet-manual",
  "tag": "sample",
  "body": "Hello from the Remoting producer."
}
```

## Default configuration

The sample reads these sections from `appsettings.json`.

| Key | Default | Purpose |
| --- | --- | --- |
| `RocketMQ:Remoting:NamesrvAddr` | `localhost:9876` | NameServer used to discover Broker routes for the default profile. |
| `RocketMQ:Producer:GroupName` | `remoting-sample-producer` | Default producer identity. |
| `RocketMQ:Audit:Remoting:NamesrvAddr` | `localhost:9876` | NameServer used by the keyed audit profile. |
| `RocketMQ:Audit:Producer:GroupName` | `remoting-sample-audit-producer` | Audit producer identity. |
| `Sample:Topic` | `rocketmq-dotnet-manual` | Topic used when the request omits `topic`. |
| `Sample:Tag` | `sample` | Tag used when the request omits `tag`. |
| `Sample:Message` | `Hello from the RocketMQ Remoting producer sample.` | Body used when the request omits `body`. |

Set values with normal .NET configuration overrides, for example
`RocketMQ__Remoting__NamesrvAddr=broker-nameserver:9876`.

## Broker and network prerequisites

- Use a classic RocketMQ NameServer and Broker that accept Remoting clients. The configured address is a
  **NameServer**, not a Proxy endpoint.
- The client asks the NameServer for routes and then connects directly to the advertised Broker address. The
  machine running this API must therefore reach both `NamesrvAddr` and every returned Broker host and port.
- Create a writable normal topic named `rocketmq-dotnet-manual`, or change `Sample:Topic` and the request. Do not
  depend on production Brokers allowing automatic topic creation.
- A producer group is a client identity; this sample does not require a consumer group or `updateSubGroup` call.
  If ACL or topic permissions are enabled, grant the configured producer groups permission to query routes and
  send to the topic.

The bundled [local RocketMQ environment](../../../test-environments/rocketmq/README.md) is suitable for this
sample when it runs on the host. Start it and create the default topic from the repository root:

```shell
docker compose -f test-environments/rocketmq/compose.yaml up -d --wait
docker compose -f test-environments/rocketmq/compose.yaml exec broker sh mqadmin updateTopic \
  -n nameserver:9876 -c DefaultCluster -t rocketmq-dotnet-manual
```

That environment advertises a host-reachable Broker address. A sample running inside another container needs a
different Broker advertisement and `NamesrvAddr`; `localhost:9876` is not a container-to-host address.

## Run

```shell
dotnet run --project samples/remoting/Producer/EventHorizon.RocketMQ.Samples.Remoting.Producer.csproj
```

For a predictable command-line address, disable the launch profile and set `ASPNETCORE_URLS`:

```shell
ASPNETCORE_URLS=http://localhost:5000 \
  dotnet run --no-launch-profile --project samples/remoting/Producer/EventHorizon.RocketMQ.Samples.Remoting.Producer.csproj

curl --request POST http://localhost:5000/messages \
  --header 'Content-Type: application/json' \
  --data '{"body":"Hello from curl."}'
```

## Important behavior

- The audit route is not a mirror or an automatic audit of `/messages`. It resolves only the keyed `audit`
  producer, so call that route explicitly when a message should use the audit profile.
- Both profiles use the same top-level `Sample` defaults. Configure the audit `Remoting` and `Producer` sections
  separately only when it must use a different cluster or producer identity.
- This is a normal-message example. It does not demonstrate transactions, delayed delivery, request-reply, or
  one-way sends.
