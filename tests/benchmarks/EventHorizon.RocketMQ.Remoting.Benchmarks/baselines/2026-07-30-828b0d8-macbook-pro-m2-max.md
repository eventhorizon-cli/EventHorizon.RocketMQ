# Remoting benchmark baseline: multiple-consumer-rebalance

## Source

- Recorded (UTC): `2026-07-30T13:22:11Z`
- Commit: `828b0d86eb9266a68d15aff6f4770b8bc296c00c`
- Working tree: clean while the measurements ran; this durable record is added in the following commit.
- Comparison target: none; first durable record for the reorganized Remoting benchmark project.

## Environment

- Machine or runner: MacBook Pro, Apple M2 Max.
- OS and architecture: macOS Sequoia 15.7.7 (24G720), Darwin 24.6.0, arm64.
- CPU and logical core count: Apple M2 Max, 12 physical and 12 logical cores.
- Memory: 64 GiB.
- Power mode and relevant background-load notes: AC power, 80% battery; interactive development machine, so compare only against measurements made under comparable load.
- .NET SDK and runtime: SDK 10.0.100; .NET 10.0.0 (10.0.25.52411), Arm64 RyuJIT armv8.0-a.
- BenchmarkDotNet job/runtime: BenchmarkDotNet 0.15.8, `DefaultJob`, Concurrent Workstation GC.

## Commands

Run from the repository root:

```shell
dotnet run --configuration Release --project tests/benchmarks/EventHorizon.RocketMQ.Remoting.Benchmarks/EventHorizon.RocketMQ.Remoting.Benchmarks.csproj -- --filter "*RemotingRoundTripBenchmarks*"
dotnet run --configuration Release --project tests/benchmarks/EventHorizon.RocketMQ.Remoting.Benchmarks/EventHorizon.RocketMQ.Remoting.Benchmarks.csproj -- --filter "*RemotingPushDispatchBenchmarks*"
```

## Results

### RemotingRoundTripBenchmarks

| Method            | PayloadSize | Mean     | Error    | StdDev   | Ratio | RatioSD | Gen0   | Gen1   | Allocated | Alloc Ratio |
|------------------ |------------ |---------:|---------:|---------:|------:|--------:|-------:|-------:|----------:|------------:|
| Sequential        | 128         | 39.06 us | 0.769 us | 1.406 us |  1.00 |    0.05 | 1.9531 |      - |  16.53 KB |        1.00 |
| ThirtyTwoInFlight | 128         | 11.95 us | 0.169 us | 0.158 us |  0.31 |    0.01 | 2.0447 | 0.2747 |  16.31 KB |        0.99 |
|                  |             |          |          |          |       |         |        |        |           |             |
| Sequential        | 4096        | 48.75 us | 0.364 us | 0.323 us |  1.00 |    0.01 | 2.9297 | 0.1221 |  24.28 KB |        1.00 |
| ThirtyTwoInFlight | 4096        | 17.41 us | 0.175 us | 0.155 us |  0.36 |    0.00 | 3.0518 | 1.3428 |  24.08 KB |        0.99 |

### RemotingPushDispatchBenchmarks

| Method                 | ConsumeMessageBatchSize | MaxConcurrency | Mean        | Error     | StdDev    | Ratio | RatioSD | Gen0   | Gen1   | Allocated | Alloc Ratio |
|----------------------- |------------------------ |--------------- |------------:|----------:|----------:|------:|--------:|-------:|-------:|----------:|------------:|
| OneHotQueue            | 1                       | 1              | 31,725.2 ns | 343.04 ns | 320.88 ns |  1.00 |    0.01 | 0.1831 | 0.0610 |    2026 B |        1.00 |
| ThreeBrokersNineQueues | 1                       | 1              |  4,035.3 ns |  41.45 ns |  38.77 ns |  0.13 |    0.00 | 0.2060 | 0.0763 |    1735 B |        0.86 |
|                        |                         |                |             |           |           |       |         |        |        |           |             |
| OneHotQueue            | 1                       | 8              | 42,615.4 ns | 168.75 ns | 149.60 ns |  1.00 |    0.00 | 0.2441 | 0.0814 |    2637 B |        1.00 |
| ThreeBrokersNineQueues | 1                       | 8              |  1,680.1 ns |   8.14 ns |   6.80 ns |  0.04 |    0.00 | 0.2098 | 0.0782 |    1737 B |        0.66 |
|                        |                         |                |             |           |           |       |         |        |        |           |             |
| OneHotQueue            | 32                      | 1              |  1,165.5 ns |   8.43 ns |   7.88 ns |  1.00 |    0.01 | 0.0229 | 0.0076 |     204 B |        1.00 |
| ThreeBrokersNineQueues | 32                      | 1              |    248.8 ns |   2.55 ns |   2.39 ns |  0.21 |    0.00 | 0.0243 | 0.0091 |     204 B |        1.00 |
|                        |                         |                |             |           |           |       |         |        |        |           |             |
| OneHotQueue            | 32                      | 8              |  1,557.3 ns |   7.18 ns |   6.72 ns |  1.00 |    0.01 | 0.0267 | 0.0095 |     226 B |        1.00 |
| ThreeBrokersNineQueues | 32                      | 8              |    401.9 ns |  11.88 ns |  34.67 ns |  0.26 |    0.02 | 0.0248 | 0.0091 |     206 B |        0.91 |

## Warnings and notes

- BenchmarkDotNet could not raise the benchmark process to high priority. This is a machine-permission limitation and applies to every benchmark process in this record.
- `RemotingRoundTripBenchmarks` removed one sequential outlier and detected three outliers across the sequential and thirty-two-in-flight results.
- `RemotingPushDispatchBenchmarks.ThreeBrokersNineQueues` reported a bimodal distribution (`mValue = 3.52`) and BenchmarkDotNet removed or detected the outliers reported in its raw log. Treat that row as a directional comparison point and rerun it under an isolated load before making a small performance decision.
