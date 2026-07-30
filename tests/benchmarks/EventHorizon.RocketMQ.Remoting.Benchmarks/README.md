# Remoting benchmarks

This project contains local BenchmarkDotNet microbenchmarks for performance-sensitive Remoting paths. The benchmarks
do not require a NameServer or Broker:

- `RemotingRoundTripBenchmarks` measures request/response work against an in-process loopback TCP server.
- `RemotingPushDispatchBenchmarks` measures in-memory Push dispatch across one hot queue or nine queues.

Run benchmarks from the repository root in Release mode. Use a filter so the selected workload is explicit in the
recorded command:

```shell
dotnet run -c Release --project tests/benchmarks/EventHorizon.RocketMQ.Remoting.Benchmarks/EventHorizon.RocketMQ.Remoting.Benchmarks.csproj -- --filter '*RemotingRoundTripBenchmarks*'
dotnet run -c Release --project tests/benchmarks/EventHorizon.RocketMQ.Remoting.Benchmarks/EventHorizon.RocketMQ.Remoting.Benchmarks.csproj -- --filter '*RemotingPushDispatchBenchmarks*'
```

BenchmarkDotNet writes raw output under the ignored `BenchmarkDotNet.Artifacts/` directory. Do not commit that
machine-generated directory. Keep durable comparison points as concise Markdown records under [`baselines`](baselines)
using [`TEMPLATE.md`](baselines/TEMPLATE.md).

Name a record `YYYY-MM-DD-<short-source-sha>-<environment>.md`, where `environment` is a stable identifier such as a
CI runner class or lab machine name. Record the full source commit, working-tree state, environment, exact commands,
complete result tables, and any BenchmarkDotNet warnings. Compare records only when runtime, job, benchmark parameters,
and relevant machine conditions match. The `Baseline = true` attribute supplies an in-run ratio reference; it does
not replace a repository baseline from an earlier commit.
