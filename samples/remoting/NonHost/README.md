# Remoting roles without Generic Host

[English](README.md) | [简体中文](README.zh-CN.md)

This collection shows every lifecycle-managed classic Remoting role in a process that does not use a .NET Generic
Host. Each leaf is an independently runnable console project that builds a plain `ServiceProvider`, resolves the
protocol-specific role, explicitly calls `StartAsync`, performs its core workflow, calls `StopAsync` in `finally`, and
asynchronously disposes the service provider.

Each role-specific `AddRemoting...` registration adds an `IHostedService`, but `ServiceCollection.BuildServiceProvider()`
does not run those services. The application therefore owns lifecycle only in this non-Host shape. When an application
starts a real `IHost` or `WebApplication`, the host invokes the registered services; do not call the role's
`StartAsync` or `StopAsync` yourself in that case.

## Included roles

| Role | Public API | Core workflow | Project |
| --- | --- | --- | --- |
| Producer | `IRemotingProducer` | Start, create and send one tagged message, inspect the result status, stop. | `Producer` |
| Lite Pull Consumer | `IRemotingLitePullConsumer` | Start, poll SDK-assigned queues, process messages, commit local positions, stop. | `LitePullConsumer` |
| Push Consumer | `IRemotingPushConsumer` | Start, let the SDK dispatch to a scoped handler, return `Success` after processing, stop. | `PushConsumer` |

The consumers run until Ctrl+C. Their shutdown paths deliberately use `CancellationToken.None` after the Ctrl+C token
is cancelled, so `StopAsync` can finish gracefully. The Producer sends one message and exits.

The category keeps these workflows separate because Lite Pull owns caller-driven polling and commits over SDK-managed
or manual assignment, while Push owns assignment, reception, handler dispatch, and settlement. The projects do not
abstract those protocol-specific responsibilities behind a shared sample layer.

## Run a project

Start the repository's local RocketMQ environment first:

```shell
docker compose -f test-environments/rocketmq/compose.yaml up -d --wait
```

Then run one project:

```shell
dotnet run --project samples/remoting/NonHost/Producer/EventHorizon.RocketMQ.Samples.Remoting.NonHost.Producer.csproj
dotnet run --project samples/remoting/NonHost/LitePullConsumer/EventHorizon.RocketMQ.Samples.Remoting.NonHost.LitePullConsumer.csproj
dotnet run --project samples/remoting/NonHost/PushConsumer/EventHorizon.RocketMQ.Samples.Remoting.NonHost.PushConsumer.csproj
```

Each project uses `localhost:9876` by default and accepts standard .NET configuration overrides, such as
`RocketMQ__Remoting__NamesrvAddr=nameserver.example:9876`. An external environment must expose the NameServer and all
Broker endpoints advertised by route discovery, create `eventhorizon-test-topic`, and grant the required producer or
consumer permissions.

For Generic Host counterparts and the role-specific delivery contracts, see the
[Remoting Producer](../GenericHost/Producer/README.md),
[Lite Pull Consumer](../GenericHost/LitePullConsumer/README.md),
[Push Consumer](../GenericHost/PushConsumer/README.md), and the
[Remoting protocol guide](../../../src/EventHorizon.RocketMQ.Remoting/README.md).
