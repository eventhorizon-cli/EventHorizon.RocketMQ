# Remoting Lite Pull Consumer without Generic Host

[English](README.md) | [简体中文](README.zh-CN.md)

This console project uses `IRemotingLitePullConsumer` in a process that does not start a .NET Generic Host. It starts
the Consumer explicitly, lets the SDK assign subscribed queues, polls messages from those assignments, commits the
advanced local positions after processing, and stops on Ctrl+C.

## Lifecycle and workflow

`AddRemotingLitePullConsumer` adds an SDK `IHostedService`, but a plain `ServiceCollection.BuildServiceProvider()`
does not execute it. The non-Host application must resolve `IRemotingLitePullConsumer`, call `StartAsync`, run the poll
loop, and call `StopAsync` in `finally`. Shutdown uses `CancellationToken.None` after Ctrl+C so it is not cancelled
along with the polling loop.
The `await using` service provider is asynchronously disposed after the role stops.

The sample configures a `eventhorizon-test-topic` subscription. After startup, the SDK participates in group
assignment and `PollAsync` selects one active assigned queue. The program processes the returned messages and calls
`CommitAsync` only afterward. `PollAsync` advances local positions, but it does not persist them by itself.

At rebalance boundaries the Consumer can temporarily have no assignment. The sample treats the resulting
`InvalidOperationException` as a retryable condition and waits before polling again. Processing failures are logged and
retried after a short delay; a real application must still make its processing idempotent because a failure before a
successful commit can cause redelivery.

For a normal .NET `IHost`, use the [Generic Host Lite Pull Consumer sample](../../GenericHost/LitePullConsumer/README.md).
Its host drives the SDK lifecycle; application code must not manually call `IRemotingLitePullConsumer.StartAsync` or
`StopAsync`.

## Run

Start the repository's local RocketMQ environment, then run this Consumer before publishing messages:

```shell
docker compose -f test-environments/rocketmq/compose.yaml up -d --wait
dotnet run --project samples/remoting/NonHost/LitePullConsumer/EventHorizon.RocketMQ.Samples.Remoting.NonHost.LitePullConsumer.csproj
```

Press Ctrl+C once for graceful shutdown. Use the [non-Host Producer sample](../Producer/README.md) to send the
sample-tagged messages. The default NameServer is `localhost:9876`; standard configuration overrides include
`RocketMQ__Remoting__NamesrvAddr=nameserver.example:9876`. An external cluster must expose its NameServer and every
advertised Broker endpoint, create the topic, and grant route-query and consume permissions.

See the [Remoting protocol guide](../../../../src/EventHorizon.RocketMQ.Remoting/README.md) for subscription mode,
manual assignment, seeking, pausing, and the complete commit API.
