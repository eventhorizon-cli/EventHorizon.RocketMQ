# Remoting Producer without Generic Host

[English](README.md) | [简体中文](README.zh-CN.md)

This console project uses `IRemotingProducer` in a process that does not start a .NET Generic Host. It builds a plain
`ServiceProvider`, starts the Producer itself, sends one normal message, checks the Broker result status, stops the
Producer, and asynchronously disposes the container.

## Lifecycle and workflow

`AddRemotingProducer` adds an SDK `IHostedService` for normal Host applications. Calling
`ServiceCollection.BuildServiceProvider()` alone does not execute that service, so this non-Host program must own both
lifecycle calls:

```csharp
await using var serviceProvider = services.BuildServiceProvider();
var producer = serviceProvider.GetRequiredService<IRemotingProducer>();

try
{
    await producer.StartAsync(CancellationToken.None);
    RemotingSendResult result = await producer.SendAsync(message, CancellationToken.None);
}
finally
{
    await producer.StopAsync(CancellationToken.None);
}
```

`StartAsync` prepares the Producer role before its first send; it is required even though the application, rather than
the SDK, initiates `SendAsync`. The sample creates a message on `eventhorizon-test-topic`, sets the `sample` tag and a
unique key, sends it, and prints the selected Broker queue, offset, and `RemotingSendStatus`. A completed send can carry
a non-success durability status, so the program returns a non-zero exit code unless the status is `SendOk`.

The `finally` block always calls `StopAsync` with a live token. That is important when a caller-owned operation token
has already been cancelled. The service provider is then asynchronously disposed so role-owned resources are released.

For a normal .NET `IHost` or `WebApplication`, use the [Generic Host Producer sample](../../GenericHost/Producer/README.md).
Its host starts and stops the SDK registration; application code must not call `IRemotingProducer.StartAsync` or
`StopAsync` a second time.

## Run

Start the repository's local RocketMQ environment, then run the project:

```shell
docker compose -f test-environments/rocketmq/compose.yaml up -d --wait
dotnet run --project samples/remoting/NonHost/Producer/EventHorizon.RocketMQ.Samples.Remoting.NonHost.Producer.csproj
```

The default NameServer address is `localhost:9876`. Override it with standard .NET configuration, for example
`RocketMQ__Remoting__NamesrvAddr=nameserver.example:9876`. An external cluster must expose its NameServer and every
Broker address advertised by route discovery, provide a writable `eventhorizon-test-topic`, and grant Producer
permissions.

See the [Remoting protocol guide](../../../../src/EventHorizon.RocketMQ.Remoting/README.md) for queue selection,
batching, transactions, delayed messages, and request-reply operations not included in this one-message workflow.
