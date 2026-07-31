# Remoting Producer

[English](README.md) | [简体中文](README.zh-CN.md)

`IRemotingProducer` publishes messages through RocketMQ's classic Remoting protocol. It discovers routes from a
NameServer and then connects directly to the Broker endpoints in those routes. Choose this role for classic
RocketMQ deployments and for Producer operations that require physical queue selection, batches, one-way sends,
transactions, delayed-message recall, or request-reply.

Do not choose it for a Proxy-only deployment: reaching `NamesrvAddr` is not sufficient unless the application can
also reach every advertised Broker address. Use the [gRPC Producer](../../../grpc/GenericHost/Producer/README.md) when the deployment
exposes a RocketMQ 5 Proxy and classic-only operations are not required.

## Registration and lifecycle

Register the Remoting client, then add the Producer role:

```csharp
builder.Services
    .AddRocketMQRemoting(remotingSection.Bind)
    .AddRemotingProducer(static options =>
        options.GroupName = "remoting-sample-producer");
```

The role is exposed as `IRemotingProducer`. The .NET host owns the lifecycle of a DI-registered Producer, so
application services normally inject it and let the host call `StartAsync` and `StopAsync`.

The Producer group configured by `RemotingProducerOptions.GroupName` is the Producer's classic client identity. It
is not a consumer group. When one process needs independent clusters, credentials, or Producer identities, create a
named client registration and resolve its `IRemotingProducer` as a keyed service:

```csharp
builder.Services
    .AddRocketMQRemoting("audit", auditRemotingSection.Bind)
    .AddRemotingProducer(static options =>
        options.GroupName = "remoting-sample-audit-producer");

static Task<RemotingSendResult> PublishAsync(
    Message message,
    [FromKeyedServices("audit")] IRemotingProducer producer,
    CancellationToken cancellationToken) =>
    producer.SendAsync(message, cancellationToken);
```

The registration name is application composition metadata, not a Broker-side audit feature. Named and default
registrations have independent configuration and neither falls back to the other.

## Sending and outcomes

Create a Remoting `Message`, then send it through `IRemotingProducer`:

```csharp
var message = new Message("orders", Encoding.UTF8.GetBytes(orderId))
{
    Tag = "created"
};
message.Keys.Add(orderId);

RemotingSendResult result = await producer.SendAsync(message, cancellationToken);
```

`RemotingSendResult` identifies the selected Broker queue and offset and includes the Broker send `Status`. Always
inspect that status: `FlushDiskTimeout`, `FlushSlaveTimeout`, and `SlaveNotAvailable` can be returned as result values
rather than exceptions. A completed send does not mean a consumer processed the message.

The sample HTTP response also returns `OffsetMessageId`. Pass that physical ID to the
[Remoting Admin sample](../Admin/README.md) to read the stored message; it is not interchangeable with `MessageId`.

Configured send retries can produce duplicates, including when a previous attempt reached a Broker but its response
was lost. Consumers should therefore process messages idempotently. `SendOnewayAsync` does not wait for a Broker
response, so its completion does not confirm storage.

## Producer operations

| Requirement | SDK API | Important boundary |
| --- | --- | --- |
| Normal send | `SendAsync(Message, ...)` | Inspect `RemotingSendResult.Status`. |
| Explicit queue selection | `GetPublishMessageQueuesAsync` and queue or selector overloads of `SendAsync` | The selected queue must be writable for the message topic. |
| Batch send | Collection overloads of `SendAsync` | One topic per batch; delayed, retry, and transactional messages are not eligible. |
| FIFO, delayed, priority, or Lite messages | `MessageGroup`, `DeliveryTimestamp`, `Priority`, or `LiteTopic` | These specialized properties are mutually exclusive and require matching Broker support. |
| Transactional send | `SendTransactionAsync` | Configure both `LocalTransactionExecutor` and `TransactionChecker`; unresolved outcomes can be checked later by the Broker. |
| Delayed-message recall | `RecallAsync` | Preserve the opaque `RecallHandle` and use it with the same logical topic before delivery. |
| Request-reply | `RequestAsync` and `SendReplyAsync` | Requires a cooperating responder; timeout covers sending the request and receiving its reply. |
| One-way send | `SendOnewayAsync` | No Broker acknowledgement or storage confirmation is returned. |

Server version and Broker configuration determine whether specialized message modes are available. See the
[Remoting protocol guide](../../../../src/EventHorizon.RocketMQ.Remoting/README.md) for queue selector, transaction,
recall, request-reply, and compatibility details.

## Prerequisites and run

The default configuration expects a NameServer at `localhost:9876`, Broker endpoints reachable from the host, and a
writable normal topic named `eventhorizon-test-topic`. The repository environment prepares that topic:

```shell
docker compose -f test-environments/rocketmq/compose.yaml up -d --wait
dotnet run --project samples/remoting/GenericHost/Producer/EventHorizon.RocketMQ.Samples.Remoting.Producer.csproj
```

After the API starts, send through the default registration from a second terminal:

```shell
curl --request POST http://localhost:5184/messages \
  --header 'Content-Type: application/json' \
  --data '{"message":"hello from the Remoting Producer sample"}'
```

Use `/clients/audit/messages` instead of `/messages` to send through the independent keyed `audit` registration:

```shell
curl --request POST http://localhost:5184/clients/audit/messages \
  --header 'Content-Type: application/json' \
  --data '{"message":"hello from the Remoting audit Producer sample"}'
```

On the supplied healthy environment, each request returns HTTP 200 with `status` set to `SendOk`, the selected
Broker queue and offset, and an `offsetMessageId`. Preserve that physical ID to inspect the stored message with the
[Admin sample](../Admin/README.md). Swagger exposes the same operations at `http://localhost:5184/swagger`.

For an external cluster, configure `RocketMQ:Remoting:NamesrvAddr`, ensure every advertised Broker endpoint is
reachable, and create the topic when automatic resource creation is disabled. The sample keeps connection settings in
`appsettings.json` and declares its fixed Producer behavior next to each registration.
