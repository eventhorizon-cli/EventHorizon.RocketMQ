# Test Environments

[English](README.md) | [简体中文](README.zh-CN.md)

This directory contains Docker Compose environments for manual local testing. They are development and diagnostic
tools, not production deployment templates.

Each child directory is self-contained: it owns its Compose files, configuration, persistence override, and setup
guide. Add a new scenario as a new directory rather than combining unrelated services into an existing environment.

## Choose an environment

| Environment | Use it when | What starts | Guide |
| --- | --- | --- | --- |
| [`rocketmq`](rocketmq) | You need the normal starting point for standard Remoting or gRPC samples, or a single-Broker diagnostic environment. | NameServer, one Broker, a cluster-mode Proxy, RocketMQ Dashboard, and the standard sample Topic initializer. | [README](rocketmq/README.md) |
| [`rocketmq-litepush`](rocketmq-litepush) | You want to run the bundled gRPC LitePush sample without manually creating RocketMQ resources. | NameServer, one Lite-enabled Broker, a cluster-mode Proxy, RocketMQ Dashboard, and an initialization service that creates the sample's LITE parent Topic and Consumer Group. | [README](rocketmq-litepush/README.md) |
| [`rocketmq-multi-broker`](rocketmq-multi-broker) | You need to inspect route discovery, queue distribution, or behavior across three Brokers. | NameServer, three independent master Brokers with three queues each, a cluster-mode Proxy, and RocketMQ Dashboard. | [README](rocketmq-multi-broker/README.md) |
| [`otel-lgtm`](otel-lgtm) | You want to inspect local RocketMQ client traces and metrics in Grafana. | Grafana OTEL LGTM with an OTLP collector, Grafana, Tempo, Prometheus, Loki, and Pyroscope. | [README](otel-lgtm/README.md) |

The `rocketmq-litepush` environment is the recommended starting point for LitePush. Its resource initializer runs as
part of `docker compose up -d --wait`; no separate `mqadmin` command is needed before starting the sample.

The three Brokers in `rocketmq-multi-broker` are independent masters. That environment verifies routing and
partitioning behavior, not replication or high availability.

Every RocketMQ environment exposes RocketMQ Dashboard at `http://localhost:8082`. It is a local management interface
that can change Topics and Consumer Groups, so it binds only to `127.0.0.1` and is not a production administration
setup. The `otel-lgtm` environment is telemetry-only and exposes Grafana at `http://127.0.0.1:3000` plus OTLP on
ports `4317` and `4318`.

## Environment overview

```mermaid
flowchart TB
    Host[Developer host]
    Choose{Choose one RocketMQ environment}

    subgraph Single[rocketmq]
        SingleNs[NameServer :9876]
        SingleBroker[One Broker :10911]
        SingleProxy[cluster-mode Proxy :8081]
        SingleDashboard[RocketMQ Dashboard :8082]
    end

    subgraph Lite[rocketmq-litepush]
        LiteNs[NameServer :19876]
        LiteBroker[Lite-enabled Broker]
        LiteProxy[cluster-mode Proxy :8081]
        LiteInit[Resource initializer]
        LiteDashboard[RocketMQ Dashboard :8082]
    end

    subgraph Multi[rocketmq-multi-broker]
        MultiNs[NameServer :9876]
        BrokerA[broker-a :10911]
        BrokerB[broker-b :10921]
        BrokerC[broker-c :10931]
        MultiProxy[cluster-mode Proxy :8081]
        MultiDashboard[RocketMQ Dashboard :8082]
    end

    subgraph Lgtm[otel-lgtm]
        Collector[OTLP :4317 / :4318]
        Grafana[Grafana :3000]
        Tempo[Tempo]
        Prometheus[Prometheus]
    end

    Host --> Choose
    Choose --> Single
    Choose --> Lite
    Choose --> Multi
    Host --> Lgtm
```

Every RocketMQ environment uses a cluster-mode Proxy where gRPC is exposed. LitePush specifically needs this
deployment mode because RocketMQ 5.5.0's Broker-integrated Proxy does not implement `SyncLiteSubscription`.
The telemetry-only `otel-lgtm` environment can run with one RocketMQ environment and receives the applications'
OTLP data without starting a Broker or Proxy.

## Usage rules

- Run `docker compose` from the selected environment directory, or provide that directory's `compose.yaml` with `-f`.
- Do not run the three RocketMQ environments together. All three publish gRPC Proxy port `8081` and Dashboard port
  `8082`; the single- and multi-Broker environments also publish the same NameServer and Broker ports. `otel-lgtm`
  uses `3000`, `4317`, and `4318` instead, and is intended to run with one RocketMQ environment.
- Use each environment's `compose.host-volumes.yaml` when local files are required for inspection or backup. Its guide
  explains the resulting `data/` directory and reset behavior.
- Integration tests do not use these Compose environments. They start isolated Testcontainers fixtures, so no manual
  stack needs to be running before `dotnet test`.
