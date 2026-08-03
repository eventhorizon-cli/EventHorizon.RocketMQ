# Remoting Push Consumer without Generic Host

[English](README.md) | [简体中文](README.zh-CN.md)

This console project uses `IRemotingPushConsumer` in a process that does not start a .NET Generic Host. It explicitly
starts the Consumer, then the SDK performs group assignment, Broker long polling, and handler dispatch until Ctrl+C.
The scoped handler processes batches tagged `sample` and returns `ConsumeResult.Success` after its work finishes.

## Lifecycle and workflow

`AddRemotingPushConsumer` adds an SDK `IHostedService`, but a plain `ServiceCollection.BuildServiceProvider()` never
starts it. The non-Host program therefore resolves `IRemotingPushConsumer`, calls `StartAsync`, waits for Ctrl+C, and
calls `StopAsync` in `finally`. Its shutdown token is `CancellationToken.None` because Ctrl+C has already cancelled the
wait token.
The `await using` service provider is asynchronously disposed after the role stops.

The `NonHostPushConsumerMessageHandler` is registered as scoped, so the SDK resolves it in a new asynchronous scope
for each delivery. It logs every `RemotingMessageView` in the batch. Returning `Success` advances the source queue
offset only after the entire batch has completed; a real handler should return a retry outcome instead when it cannot
complete the work and should tolerate duplicate delivery.

The configured group starts at `End` when it has no offset, explicitly uses the default
`RemotingPushQueueAssignmentMode.Client`, subscribes to `eventhorizon-test-topic` with the `sample` tag filter, and uses
bounded handler concurrency. `PullBatchSize` controls each Broker request. It is intentionally one Consumer role so
that registration, lifecycle, subscription, and settlement remain visible in one path.

For a normal .NET `IHost`, use the [Generic Host Push Consumer sample](../../GenericHost/PushConsumer/README.md). Its
host drives the SDK lifecycle; application code must not manually call `IRemotingPushConsumer.StartAsync` or
`StopAsync`.

## Run

Start the repository's local RocketMQ environment, then run this Consumer before publishing tagged messages:

```shell
docker compose -f test-environments/rocketmq/compose.yaml up -d --wait
dotnet run --project samples/remoting/NonHost/PushConsumer/EventHorizon.RocketMQ.Samples.Remoting.NonHost.PushConsumer.csproj
```

Press Ctrl+C once for graceful shutdown. Use the [non-Host Producer sample](../Producer/README.md) to publish messages
with the matching `sample` tag. The default NameServer is `localhost:9876`; override it with standard configuration,
for example `RocketMQ__Remoting__NamesrvAddr=nameserver.example:9876`. An external cluster must expose its NameServer
and every advertised Broker endpoint, create the topic, and grant route-query and consume permissions, including retry
and dead-letter access when those outcomes are enabled.

See the [Remoting protocol guide](../../../../src/EventHorizon.RocketMQ.Remoting/README.md) for the full Push Consumer
configuration and delivery contract.
