# Remoting Admin

[English](README.md) | [简体中文](README.zh-CN.md)

`IRemotingAdmin` is the classic Remoting client's read-only inspection role. Use it when an application or
operations tool must discover a topic's physical queues, inspect queue offset bounds and timestamps, read a consumer
group's committed position, or retrieve a stored message by its physical identifier.

This role does not create topics or groups, reset offsets, acknowledge messages, or consume a stream. Use the
Producer or a Consumer role for those workflows, and use RocketMQ's resource-management tools when Broker state must
be changed.

## Registration

Register the Remoting client, then add the Admin role:

```csharp
builder.Services
    .AddRocketMQRemoting(remotingSection.Bind)
    .AddRemotingAdmin();
```

Inject `IRemotingAdmin` into the component that performs the query. Unlike hosted Producer and Consumer roles,
Admin does not start a background hosted service and does not require a Producer or consumer group.

The configured `NamesrvAddr` is a NameServer endpoint, not a Proxy. Admin first uses NameServer routes to identify
Brokers, then sends queue queries directly to the selected Broker. Both the NameServer and every advertised Broker
endpoint must therefore be reachable from the application.

## Public API

| SDK API | Result and use |
| --- | --- |
| `GetMessageQueuesAsync(topic)` | Returns the topic's currently writable physical queues, ordered by Broker and queue ID. Use these values to construct subsequent per-queue queries. |
| `GetMinOffsetAsync(queue)` | Returns the earliest offset still available in the queue; retention means this need not be zero. |
| `GetMaxOffsetAsync(queue, committed)` | Returns the highest committed, consumable offset by default. Pass `false` to request the current in-flight offset when the Broker supports it. |
| `GetEarliestMessageStoreTimeAsync(queue)` | Returns the earliest available message's UTC store time, or `null` when the Broker reports no available message. |
| `SearchOffsetAsync(queue, timestamp, boundary)` | Finds a queue offset for a timestamp. `Lower` selects the lowest matching offset and `Upper` the highest. |
| `GetConsumerOffsetAsync(group, queue)` | Returns that group's committed position for one physical queue, or `null` when no position has been committed. |
| `ViewMessageAsync(topic, offsetMessageId)` | Reads one stored message by the physical ID returned in `RemotingSendResult.OffsetMessageId`. |

A `RemotingMessageQueue` is identified by topic, Broker name, and queue ID. Offsets belong to that physical queue;
they are not global topic offsets and should not be compared across queues as one sequence.

## Physical message lookup

`ViewMessageAsync` requires `RemotingSendResult.OffsetMessageId`, not the producer-assigned `MessageId`. The physical
ID encodes a Broker endpoint and commit-log offset. The client decodes it and connects directly to that exact
endpoint without NameServer route discovery:

```csharp
RemotingSendResult sent = await producer.SendAsync(message, cancellationToken);
RemotingMessageView stored = await admin.ViewMessageAsync(
    message.Topic,
    sent.OffsetMessageId,
    cancellationToken);
```

An ID created in another network can encode an address that is unreachable from the inspecting application. A
working NameServer route does not replace that direct connection. Message bodies are arbitrary bytes; decode them
according to the Producer's content contract.

For the complete role contract, see the
[Remoting protocol guide](../../../src/EventHorizon.RocketMQ.Remoting/README.md).

## Prerequisites and run

The default configuration expects a NameServer at `localhost:9876` and host-reachable Broker endpoints. The
repository environment provides that topology and prepares `eventhorizon-test-topic`:

```shell
docker compose -f test-environments/rocketmq/compose.yaml up -d --wait
dotnet run --project samples/remoting/Admin/EventHorizon.RocketMQ.Samples.Remoting.Admin.csproj
```

For an external cluster, configure `RocketMQ:Remoting:NamesrvAddr`, create the topic when required, and grant the
route-query and read permissions required by that cluster. The runnable project exposes the Admin methods through a
small HTTP and Swagger surface; start with queue discovery because its Broker name and queue ID identify the inputs
for the per-queue APIs. A physical message ID can be obtained from the
[Remoting Producer](../Producer/README.md).
