# Test Environments

[English](README.md) | [简体中文](README.zh-CN.md)

This directory groups Docker Compose environments for manual local testing. Each immediate child is a self-contained
environment with its own Compose files, configuration, and setup guide. Add a new environment as a new child directory
rather than mixing unrelated services into an existing stack.

## Current Topology

The current `rocketmq` environment is a LitePush-capable single-Broker development stack. Broker
and Proxy are separate processes inside the `broker` Compose service; this is intentionally not the
Broker-integrated Proxy mode.

```mermaid
flowchart TB
    Host[Developer host]

    subgraph Environment[test-environments/rocketmq]
        Init[volume-init<br/>one-shot ownership setup]
        NameServer[nameserver<br/>port 9876]

        subgraph BrokerService[broker Compose service]
            Broker[mqbroker<br/>port 10911]
            Proxy[mqproxy -pm cluster<br/>port 8081 gRPC]
        end

        Volumes[(Docker named volumes<br/>logs and Broker store)]
    end

    Host -->|Remoting route lookup :9876| NameServer
    Host -->|Remoting data plane :10911| Broker
    Host -->|gRPC :8081| Proxy
    Init --> Volumes
    Init -->|completes before startup| NameServer
    NameServer -->|healthy| Broker
    Broker -->|registers, then Proxy starts| Proxy
    Broker --> Volumes
    Proxy -->|route and Broker operations| NameServer
    Proxy --> Broker
```

| Environment | Purpose | Guide |
| --- | --- | --- |
| [`rocketmq`](rocketmq) | Local Apache RocketMQ 5.5.0 NameServer, Broker, and separately started cluster-mode Proxy for samples and manual client testing. This is the bundled LitePush-capable stack. | [README](rocketmq/README.md) |

Integration tests use Testcontainers and do not require one of these environments to be running.
