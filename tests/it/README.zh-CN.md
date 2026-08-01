# 集成测试

[English](README.md) | [简体中文](README.zh-CN.md)

`tests/it` 包含 `net10.0` Docker 集成测试项目。它们通过真实的 RocketMQ NameServer、Broker 和 Proxy
进程验证各协议行为，同时使容器编排与生产客户端代码保持分离。

## 项目

| 项目 | 职责 |
| --- | --- |
| `EventHorizon.RocketMQ.Grpc.IntegrationTests` | 通过真实的 cluster-mode Proxy 验证 gRPC 客户端。 |
| `EventHorizon.RocketMQ.Remoting.IntegrationTests` | 验证经典 NameServer 和 Broker Remoting 行为。 |
| `EventHorizon.RocketMQ.IntegrationTestInfrastructure` | 管理可复用的 Testcontainers 拓扑、主机端口分配和测试资源名称。 |
| `EventHorizon.RocketMQ.Remoting.CrossProcessTestHost` | 在独立操作系统进程中运行测试专用 Remoting consumer，用于跨进程 Rebalance 覆盖。 |

基础设施项目是不可打包的支持类库，而不是测试程序集。它刻意不引用任一生产客户端项目，也不包含协议客户端
断言。每个协议测试项目分别引用基础设施项目和自己的生产客户端；Remoting 测试还持有跨进程测试 Host：

```text
Grpc.IntegrationTests ------> EventHorizon.RocketMQ.Grpc
    `-----------------------> IntegrationTestInfrastructure

Remoting.IntegrationTests --> EventHorizon.RocketMQ.Remoting
    |-----------------------> IntegrationTestInfrastructure
    `-----------------------> Remoting.CrossProcessTestHost
                                  `--> EventHorizon.RocketMQ.Remoting

IntegrationTestInfrastructure --> Testcontainers
                              --> xUnit 生命周期抽象
```

仓库级测试拓扑和 CI 规则请参阅[本地测试与集成测试](../../docs/zh-CN/testing/local-and-integration-testing.md)。

## 基础设施结构

`EventHorizon.RocketMQ.IntegrationTestInfrastructure` 包含六个 public 顶层类型和一个 internal 辅助类型。

| 类型 | 职责 |
| --- | --- |
| `RocketMQSingleBrokerContainerFixtureRegistry` | 为单个测试程序集惰性持有共享的单 Broker Fixture。 |
| `RocketMQSingleBrokerContainerFixture` | 运行普通测试使用的 NameServer、单 Broker 和 cluster-mode Proxy 拓扑。 |
| `RocketMQTestScope` | 生成测试专用 topic 以及 producer 或 consumer group 名称。 |
| `RocketMQTestTopicType` | 选择普通、事务、FIFO、延时或 Lite topic 的预配方式。 |
| `RocketMQHostPortReservation` | 在当前测试进程内协调动态主机端口选择。 |
| `RocketMQMultiBrokerGrpcContainerFixture` | 运行位于 cluster-mode Proxy 后面的三个 Docker 网络 Broker。 |
| `RocketMQMultiBrokerRemotingContainerFixture` | 运行三个可从主机访问的经典 Remoting Broker。 |

内部类型依赖如下：

```text
RocketMQSingleBrokerContainerFixtureRegistry
    `-- 惰性持有 --> RocketMQSingleBrokerContainerFixture
                         |-- 使用 --> RocketMQHostPortReservation
                         |-- 接收 --> RocketMQTestTopicType
                         `-- 创建 --> RocketMQTestScope
                                          `-- 回调 --> RocketMQSingleBrokerContainerFixture

RocketMQMultiBrokerGrpcContainerFixture
    `-- 使用 --> RocketMQHostPortReservation

RocketMQMultiBrokerRemotingContainerFixture
    `-- 使用 --> RocketMQHostPortReservation
```

只有在创建需要 Broker 端 parent-topic 绑定的 Lite consumer group 时，`RocketMQTestScope` 才会回调
`RocketMQSingleBrokerContainerFixture`。它不持有也不释放 Broker 资源；topic 和 group 会一直存在，直到其所属容器
Fixture 被释放。

## Fixture 所有权与生命周期

### 单 Broker 基线拓扑

两个协议集成测试程序集都把 `RocketMQSingleBrokerContainerFixtureRegistry` 注册为 xUnit assembly fixture。因此 xUnit
会为每个测试程序集创建一个 Registry；两个程序集不会共享 Registry 实例或正在运行的 Docker 拓扑。

Registry 刻意设计为惰性持有者，而不是通用的 Fixture 目录：

```text
测试程序集启动
    -> xUnit 创建 RocketMQSingleBrokerContainerFixtureRegistry
    -> Registry.InitializeAsync 完成，但不启动 Docker
    -> 第一次 GetFixtureAsync 调用取得初始化锁
    -> Registry 创建并初始化 RocketMQSingleBrokerContainerFixture
         -> 预留三个主机端口
         -> 创建 Docker network
         -> 启动 NameServer
         -> 启动 Broker 和 Proxy 容器
         -> 预配固定 topic 和旧版 Remoting group
    -> 相互独立的测试类共享已初始化的 Fixture
测试程序集结束
    -> Registry 释放 Fixture、初始化锁及其持有的资源
```

`GetFixtureAsync` 使用 semaphore 和双重检查，确保并行测试类只初始化一套 Fixture。初始化失败时，Registry
会先释放部分初始化的 Fixture，再向上传递异常。只筛选 multi-Broker 测试时，xUnit 仍会创建开销很小的
assembly Registry，但由于没有调用 `GetFixtureAsync`，单 Broker 拓扑不会启动。

`RocketMQSingleBrokerContainerFixture` 会为每个 scope 预配唯一的普通 topic。事务、FIFO、延时和 Lite topic 因 Broker
语义需要配置而预先创建，但每个 scope 仍通过唯一 suffix 隔离 group 和消息标识。Broker 管理操作的并发数
限制为四，与集成测试的并行负载一致，同时避免串行化整个测试程序集。

### 多 Broker collection

两个多 Broker Fixture 都直接注册为 xUnit collection fixture：

```text
gRPC multi-Broker collection
    `-- RocketMQMultiBrokerGrpcContainerFixture

Remoting multi-Broker collection
    `-- RocketMQMultiBrokerRemotingContainerFixture
```

只有 collection 中存在被选中的测试时，xUnit 才会创建对应 Fixture，调用 `InitializeAsync`，向该 collection
的测试注入同一个实例，并在 collection 结束后调用 `DisposeAsync`。collection fixture 已经同时提供按需创建和
共享能力，再包装 Registry 不会增加生命周期能力。

gRPC 多 Broker 初始化顺序为：

```text
预留一个 Proxy 主机端口
    -> network -> NameServer -> 并行启动 Broker A/B/C
    -> 等待全部 Broker 注册
    -> 在每个 Broker 上创建 topic 并等待完整路由
    -> 启动 Proxy
```

释放按所有权的反方向执行：Proxy、各 Broker、NameServer、network，最后释放端口记录。

Remoting 多 Broker 初始化顺序为：

```text
预留一个 NameServer 和三个 Broker 主机端口
    -> network -> NameServer -> 并行启动 Broker A/B/C
    -> 等待全部 Broker 注册
    -> 在每个 Broker 上创建具有三个读队列和三个写队列的 topic
    -> 等待完整路由
```

释放时依次清理各 Broker、NameServer、network 和端口记录。

## 为什么单 Broker Fixture 可以复用

单 Broker Fixture 可以为两种协议提供一套始终有效的超集拓扑，因为 Broker 和 Proxy 运行在同一个容器中。
Broker 通告 `127.0.0.1`，并且容器内和主机上的 Broker 端口数字保持一致：

```text
主机 Remoting client -> 映射后的 Broker 端口 -> Broker

主机 gRPC client -> 映射后的 Proxy 端口 -> Proxy
                                          -> 同容器内的 loopback Broker 端口
```

Fixture 暴露的所有 endpoint 在整个生命周期内都有效。两个协议测试程序集复用 Fixture 类型和资源预配实现，
但生产客户端和断言仍保持分离；运行时每个程序集仍会启动自己的容器。Remoting 程序集也会启动一个基本不使用
的 Proxy，这是保持单 Broker 基线拓扑简单统一所接受的成本。

## 为什么多 Broker Fixture 保持分离

两个多 Broker 拓扑对 Broker 地址通告有互不兼容的要求：

| 关注点 | gRPC multi-Broker | Remoting multi-Broker |
| --- | --- | --- |
| 客户端入口 | Proxy | NameServer 和 Broker |
| Broker 通告主机 | `broker-a` 等 Docker alias | `127.0.0.1` |
| Broker 端口 | 各容器固定使用 `10911` | 各自使用动态分配的主机端口 |
| 主机端口映射 | 只映射 Proxy | 映射 NameServer 和每个 Broker |
| 附加行为 | 启动 Proxy 并检查各 Broker offset | 显式队列数和主机可达路由 |

独立 Proxy 容器可以解析 `broker-a`、`broker-b` 和 `broker-c`，但主机上的 Remoting client 无法解析这些
地址。如果 Broker 通告 `127.0.0.1`，主机可以通过不同的映射端口访问它们，但 Proxy 容器中的
`127.0.0.1` 指向 Proxy 容器自身，无法表示另外三个 Broker 容器。

合并 public Fixture 只会得到一个按模式切换的类型：其构造过程充满条件分支，并且会出现只在部分模式有效的
成员，例如 Remoting 模式下的 gRPC endpoint。这样只减少了类型数量，并没有统一运行时拓扑。因此应保留两个
协议专用 public Fixture。如果重复维护证明有实际收益，可以把注册轮询、路由轮询或容器编排提取到职责明确的
internal 协作者；不要只为删除相似代码而用通用模式选择器替代当前显式 Fixture。

## 运行集成测试

运行前必须确保 Docker 可用。在仓库根目录执行对应协议的集成测试项目：

```shell
dotnet test tests/it/EventHorizon.RocketMQ.Grpc.IntegrationTests/EventHorizon.RocketMQ.Grpc.IntegrationTests.csproj --no-restore
dotnet test tests/it/EventHorizon.RocketMQ.Remoting.IntegrationTests/EventHorizon.RocketMQ.Remoting.IntegrationTests.csproj --no-restore
```
