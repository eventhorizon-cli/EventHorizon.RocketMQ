# Remoting POP Consumer without Generic Host

[English](README.md) | [简体中文](README.zh-CN.md)

This console project uses `IRemotingPopConsumer` in a process that does not start a .NET Generic Host. It explicitly
starts the Consumer, discovers readable physical queues, selects a queue for each POP request, processes returned
messages, acknowledges every receipt after processing, and stops on Ctrl+C.

## Lifecycle and workflow

`AddRemotingPopConsumer` adds an SDK `IHostedService`, but a plain `ServiceCollection.BuildServiceProvider()` does not
run it. The non-Host program therefore resolves `IRemotingPopConsumer`, calls `StartAsync`, runs its caller-owned POP
loop, and calls `StopAsync` in `finally`. The shutdown call uses `CancellationToken.None` because Ctrl+C has already
cancelled the loop token.
The `await using` service provider is asynchronously disposed after the role stops.

POP does not perform background assignment or settlement. The sample makes the application-owned steps explicit:

1. `GetMessageQueuesAsync` discovers readable physical queues for `eventhorizon-test-topic`.
2. The loop selects one queue and calls `PopAsync`.
3. Every returned message is processed, then its `RemotingPopReceipt` is passed to `AcknowledgeAsync`.

The receipt is valid only within the invisible duration. An unacknowledged message becomes available for redelivery
when that duration expires. Long-running work must renew a receipt with `ChangeInvisibleTimeAsync` and acknowledge the
replacement receipt. The sample does only short logging work, so direct acknowledgement is sufficient; production
processing must tolerate redelivery when acknowledgement cannot complete.

For a normal .NET `IHost`, use the [Generic Host POP Consumer sample](../../GenericHost/PopConsumer/README.md). Its host
drives the SDK lifecycle; application code must not manually call `IRemotingPopConsumer.StartAsync` or `StopAsync`.

## Run

Start the repository's local RocketMQ environment, then run this Consumer before publishing messages:

```shell
docker compose -f test-environments/rocketmq/compose.yaml up -d --wait
dotnet run --project samples/remoting/NonHost/PopConsumer/EventHorizon.RocketMQ.Samples.Remoting.NonHost.PopConsumer.csproj
```

Press Ctrl+C once for graceful shutdown. Use the [non-Host Producer sample](../Producer/README.md) to publish messages.
The default NameServer is `localhost:9876`; override it with standard configuration, for example
`RocketMQ__Remoting__NamesrvAddr=nameserver.example:9876`. An external cluster must expose its NameServer and every
advertised Broker endpoint, create the topic, and grant route-query, consume, POP, and acknowledgement permissions.

See the [Remoting protocol guide](../../../../src/EventHorizon.RocketMQ.Remoting/README.md) for receipt renewal,
invisibility, filtering, and the full POP API.
