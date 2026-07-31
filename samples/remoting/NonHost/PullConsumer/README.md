# Remoting Pull Consumer without Generic Host

[English](README.md) | [简体中文](README.zh-CN.md)

This console project uses `IRemotingPullConsumer` in a process that does not start a .NET Generic Host. It explicitly
starts the Consumer, discovers physical queues, owns an offset for each queue, processes each pulled batch, persists
the next offset only after processing succeeds, and stops on Ctrl+C.

## Lifecycle and workflow

`AddRemotingPullConsumer` adds an SDK `IHostedService`, but a plain `ServiceCollection.BuildServiceProvider()` does not
run it. The non-Host program therefore resolves the protocol-specific role, calls `StartAsync`, and calls `StopAsync`
in `finally`. `StopAsync` uses `CancellationToken.None` because Ctrl+C has already cancelled the loop token.
The `await using` service provider is asynchronously disposed after the role stops.

Raw Pull gives the application complete queue and offset ownership. The main path makes that contract visible:

1. `GetMessageQueuesAsync` discovers readable physical queues for `eventhorizon-test-topic`.
2. `GetOffsetAsync` uses a committed group offset when one exists; otherwise `QueryOffsetAsync(..., End)` chooses the
   initial position.
3. `PullAsync` reads a chosen queue at its current offset.
4. The program logs every returned message, advances only that queue's local position, then calls `UpdateOffsetAsync`.

Processing happens before the commit. If the program fails before `UpdateOffsetAsync`, a later run can receive the
batch again, so real processing must tolerate duplicates. The sample is intentionally a single loop: applications that
run multiple instances or concurrent queue loops need an explicit queue-ownership and contiguous-offset policy.

For a normal .NET `IHost`, use the [Generic Host Pull Consumer sample](../../GenericHost/PullConsumer/README.md). Its
host drives the SDK lifecycle; application code must not manually call `IRemotingPullConsumer.StartAsync` or
`StopAsync`.

## Run

Start the repository's local RocketMQ environment, then run this Consumer before sending messages:

```shell
docker compose -f test-environments/rocketmq/compose.yaml up -d --wait
dotnet run --project samples/remoting/NonHost/PullConsumer/EventHorizon.RocketMQ.Samples.Remoting.NonHost.PullConsumer.csproj
```

Press Ctrl+C once for graceful shutdown. Use the [non-Host Producer sample](../Producer/README.md) to publish matching
messages. The default NameServer is `localhost:9876`; override it with
`RocketMQ__Remoting__NamesrvAddr=nameserver.example:9876`. An external cluster must expose its NameServer and every
advertised Broker address, create the topic, and grant route-query and consume permissions.

See the [Remoting protocol guide](../../../../src/EventHorizon.RocketMQ.Remoting/README.md) for the complete Pull
Consumer API and offset semantics.
