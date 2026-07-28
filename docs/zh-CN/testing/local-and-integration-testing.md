# 测试边界：快速单元测试与真实 RocketMQ 环境

[English](../../en-US/testing/local-and-integration-testing.md) | [简体中文](local-and-integration-testing.md)

RocketMQ 客户端的正确性分为两层。序列化、重试决策、生命周期和 DI 可以在进程内确定性验证；协议互操作、
Broker 路由、Proxy 语义和真实的长轮询则必须与实际服务端一起验证。本仓库把两层测试分开，避免“能跑完
Mock”被误认为“能连接到 RocketMQ”。

## 背景

classic Remoting 的行为同时受 NameServer route、Broker 广告地址、wire frame、网络连接和 callback 影响；
gRPC 的行为同时受 Proxy、Broker feature 与 protobuf API 影响。任一层的本地替身都很难完整模拟另一个层。

但把所有测试都放进 Docker 也不可取：测试会更慢、更脆弱，并把简单的错误变成环境问题。因此项目约定把
纯客户端行为留在 unit test，把 Docker 只用于验证真实部署边界。

## 决策

测试项目按协议和职责拆分：

| 位置 | 类型 | 责任 |
| --- | --- | --- |
| [`tests/ut/EventHorizon.RocketMQ.Grpc.Tests`](../../../tests/ut/EventHorizon.RocketMQ.Grpc.Tests) | 单元测试 | gRPC route、receipt、consumer 调度、DI 和错误处理。 |
| [`tests/ut/EventHorizon.RocketMQ.Remoting.Tests`](../../../tests/ut/EventHorizon.RocketMQ.Remoting.Tests) | 单元测试 | frame、连接、路由、经典 Consumer/Producer 和 Admin 行为。 |
| [`tests/ut/EventHorizon.RocketMQ.Compatibility.Tests`](../../../tests/ut/EventHorizon.RocketMQ.Compatibility.Tests) | 兼容性测试 | 同时引用两个协议 Project，验证两套公开 API、命名空间隔离与双 Package 共存。 |
| [`tests/it/EventHorizon.RocketMQ.Grpc.IntegrationTests`](../../../tests/it/EventHorizon.RocketMQ.Grpc.IntegrationTests) | Docker 集成测试 | Proxy gRPC 的发送、消费、事务、Lite、死信和三 Broker route 写入路径。 |
| [`tests/it/EventHorizon.RocketMQ.Remoting.IntegrationTests`](../../../tests/it/EventHorizon.RocketMQ.Remoting.IntegrationTests) | Docker 集成测试 | NameServer/Broker 的 Admin、Pull、发送、请求-响应、事务、撤回和三 Broker route 路径。 |
| [`tests/it/EventHorizon.RocketMQ.IntegrationTestInfrastructure`](../../../tests/it/EventHorizon.RocketMQ.IntegrationTestInfrastructure) | 测试基础设施库 | Testcontainers fixture 与可复用环境，不引用任一生产协议项目。 |
| [`tests/benchmarks/EventHorizon.RocketMQ.Benchmarks`](../../../tests/benchmarks/EventHorizon.RocketMQ.Benchmarks) | 基准测试 | 性能敏感路径的 BenchmarkDotNet 测量。 |

单元测试优先使用 `MockBehavior.Strict` 的 Moq，以明确可替换协作者的交互。对于流式 framing、竞争、
时序或有状态协议协作，保留小型专用 fake 比堆叠 Mock 更清晰时可以例外。

可发布的 gRPC 与 Remoting Package 提供 `net8.0` 和 `net10.0` 两个目标；三套单元测试与兼容性测试都会在这两个
目标上运行。Docker 驱动的集成测试继续使用 `net8.0`，以免专门优化过的四 job 集成测试矩阵把容器启动成本翻倍；
solution build 仍会编译两个 Package 目标。

BenchmarkDotNet 测量独立于 test runner，位于
[`tests/benchmarks/EventHorizon.RocketMQ.Benchmarks`](../../../tests/benchmarks/EventHorizon.RocketMQ.Benchmarks)。
在仓库根目录使用 Release 模式单独运行 Remoting Push 分发 benchmark：

```shell
dotnet run -c Release --project tests/benchmarks/EventHorizon.RocketMQ.Benchmarks/EventHorizon.RocketMQ.Benchmarks.csproj -- --filter '*RemotingPushDispatchBenchmarks*'
```

## 如何工作

### 1. 单元测试不依赖网络或 Docker

单元测试应在没有 NameServer、Broker、Proxy 或容器运行时的环境中稳定执行。它们针对的是客户端可控的
边界，例如：

- options 验证、默认客户端注册、keyed 客户端注册与重复角色注册；
- message 编解码、frame 长度限制、ACL 签名和 response correlation；
- route cache、重试、取消、连接故障和 handler 生命周期；
- Consumer 对 `Success`、部分批量确认、`Retry`、`DeadLetter` 的处理决策。

对于客户端能够确定性验证的行为，IT 覆盖不能替代 UT。即使 IT 已覆盖完整工作流，底层状态转换、分配、调度、
offset、重试和失败不变量仍必须保留聚焦的 UT；IT 只在此基础上补充真实 Broker、NameServer、Proxy、transport、
持久化和跨进程边界验证。

IT 必须同时覆盖这些真实边界上的正常链路与相关异常行为。改动涉及故障隔离、重试、阻塞、恢复或持久化时，
需要执行对应的真实失败路径，不能只验证成功路径。

例如，多 Broker、多 Queue 行为必须用 UT 覆盖 route 展开、每个队列只分配一次、Consumer 数量多于队列时的
空闲分配，以及逐队列调度与 offset 隔离；Docker IT 再验证对应的真实 NameServer route、Broker 持久化和
跨客户端进程协调，并验证单个队列阻塞或重试时不会干扰已成功的队列。

这样失败信息能直接指向客户端逻辑。若测试需要真实 endpoint 才能成立，它应迁移到对应的 integration
test 项目，而不是在 unit test 中偷偷连接本地 `localhost`。

### 2. Integration fixture 启动受控的 RocketMQ 集群

共享
[`RocketMQContainerFixture`](../../../tests/it/EventHorizon.RocketMQ.IntegrationTestInfrastructure/RocketMQContainerFixture.cs)
使用 Testcontainers 和 `apache/rocketmq:5.5.0`：

```text
测试进程
   │ 动态映射端口
   ├── NameServer 容器
   └── Broker + cluster-mode Proxy 容器
            ├── classic Remoting Broker 端口
            └── gRPC Proxy 端口
```

基础 fixture 创建隔离 Docker network，等待 NameServer 与 Broker 就绪，确认 Broker 已注册。每个 integration-test
assembly 通过惰性的 xUnit v3 assembly-fixture registry 共享一套基础容器，因此独立测试类可以复用 Docker stack，
而不必让整个项目串行。runner 最多并行四个 test collection；被筛选出的 multi-Broker job 不会初始化这套基础
fixture。

普通测试 scope 会生成唯一的 normal topic 和 consumer group。fixture 会在使用前显式创建 topic，因为 gRPC 的
route lookup 需要已有 route；普通 subscription group 则由 Broker 自动创建。事务、FIFO、延迟和 Lite topic 因为
依赖相应的 Broker 语义而保持预配置，测试通过唯一 group 和 message identifier 隔离。保留的 legacy Remoting
container suite 刻意继续使用专用 topic，且类内方法串行执行，因为这些 case 验证的是共享的 Admin 与 offset
语义。

端口由 Testcontainers 动态映射，测试不会依赖手工环境的固定端口。

Broker 与 cluster-mode Proxy 在同一容器中按顺序启动：Broker 先注册到 NameServer，Proxy 再启动。这既能
覆盖宿主机访问 Broker 的 route，又能覆盖 LitePush 所需的 cluster-mode Proxy 路径。

多 Broker 覆盖使用两套协议隔离的 fixture：

- Remoting fixture 启动 NameServer 和三个宿主机可达的 master Broker，并在每台 Broker 上显式创建三个读写队列。
  测试断言完整的九个物理队列 route，逐队列定向发送消息，验证一个 PushConsumer 会提交每个队列的 offset，
  并验证同组的三个及十个 PushConsumer 的分配。十个 Consumer 多于九个队列时，多出的实例保持未分配，
  且不会产生重复投递。
- gRPC fixture 在隔离 Docker network 中启动 NameServer、三个 master Broker 和 cluster-mode Proxy。测试通过 Proxy
  发送消息，再查询每台 Broker 的 topic 状态，确认三台 Broker 都有实际写入。

创建多 Broker topic 时，fixture 会逐台 Broker 执行 `mqadmin updateTopic -b` 并等待完整 route。不能假定
`updateTopic -c DefaultCluster` 会自动把 topic 创建到每台 master Broker。

### 3. 手工 Compose 环境服务于探索，不服务于集成测试前置条件

`test-environments` 集中存放自包含的手工 Compose 环境。其中的
[`rocketmq`](../../../test-environments/rocketmq) 是供开发者运行 sample、检查日志或手工复现问题的 RocketMQ 5.5.0
环境。它默认提供：

- NameServer：`localhost:9876`；
- Broker：`localhost:10911`；
- cluster-mode Proxy：gRPC 使用 `localhost:8081`。

它的[使用说明](../../../test-environments/rocketmq/README.zh-CN.md)还解释了 `volume-init`、默认 Docker named
volume 与可选的宿主机目录持久化覆盖文件。该 Compose 环境不应成为 CI 或 `dotnet test` 的前置条件：
集成测试总是使用自己的 Testcontainers fixture，避免共享 topic、端口和数据状态。

### 4. 验证顺序反映失败成本

修改 C# 后，先格式化和构建，再运行最窄的受影响 unit test；最后运行完整的匹配 unit test 项目。标准命令
在仓库根目录执行：

```shell
dotnet format EventHorizon.RocketMQ.slnx
dotnet restore EventHorizon.RocketMQ.slnx
dotnet build EventHorizon.RocketMQ.slnx --no-restore

dotnet test tests/ut/EventHorizon.RocketMQ.Grpc.Tests/EventHorizon.RocketMQ.Grpc.Tests.csproj --no-restore
dotnet test tests/ut/EventHorizon.RocketMQ.Remoting.Tests/EventHorizon.RocketMQ.Remoting.Tests.csproj --no-restore
dotnet test tests/ut/EventHorizon.RocketMQ.Compatibility.Tests/EventHorizon.RocketMQ.Compatibility.Tests.csproj --no-restore
```

涉及 Broker、NameServer、Proxy、wire interoperability 或实际协议行为时，再运行匹配的 integration test：

```shell
dotnet test tests/it/EventHorizon.RocketMQ.Grpc.IntegrationTests/EventHorizon.RocketMQ.Grpc.IntegrationTests.csproj --no-restore
dotnet test tests/it/EventHorizon.RocketMQ.Remoting.IntegrationTests/EventHorizon.RocketMQ.Remoting.IntegrationTests.csproj --no-restore
```

Docker 不可用时，应报告没有运行哪一个 integration test 以及原因，不能用 unit test 的成功替代它。

### 4.1 CI 也采集 integration coverage

GitHub Actions 会在一个 job 中完成格式化和全量构建，同时并行运行三个按协议/兼容性拆分的单元测试 job。每个
单元测试 job 都会 restore、build、运行两个目标框架的测试，并单独上传覆盖率报告。两层验证均通过后，再于四个
并行的 `ubuntu-latest` matrix job 运行 Docker 驱动的 gRPC 与 Remoting integration test：每个协议各有一个
single-Broker 与一个 multi-Broker job。类级别的 `Topology=MultiBroker` trait 选择 multi-Broker 测试；
single-Broker job 运行其余测试。每个 job 只 restore
和 build 对应协议的 integration-test 依赖图，使用独立超时预算，并复用仓库的 NuGet 包缓存。Testcontainers
仍会在每个 job 内独立启动隔离的 Broker、NameServer 与 Proxy fixture。single-Broker job 内，已经隔离的测试类
会在 runner 上限内并发执行；legacy Remoting suite 则按设计保持串行。四个 job 都会生成 Cobertura 报告，以不同
的上传名称归入共享的 `integration-tests` flag；Codecov 会将其与 `unit-tests` 报告合并。仓库配置仍会排除
`samples`，不会将 sample 计入覆盖率。

### 5. 测试范围也体现支持边界

集成测试只覆盖仓库真正公开且可由目标服务端运行的路径。gRPC 的 Simple、Push 和 LitePush 在匹配的
Proxy/Broker 配置下覆盖；已移除的 gRPC PullConsumer 没有 sample 或 integration test，因为它不是当前
可提供的 public role。显式 queue/offset Pull 的真实互操作覆盖属于 Remoting integration tests。

同样，手工 Compose 环境的初始化文档会列出 Lite、topic 类型、group 和 Proxy 前提。测试失败时先区分
客户端回归、服务端 feature 未开启、服务端版本不兼容和 Docker/network 环境问题，能显著缩短定位时间。

## 取舍与约束

**集成测试更慢，但不能省。** Testcontainers 需要 Docker、镜像和可用的容器网络；它会比 unit test 慢，
却能发现 Mock 无法代表的 route、broker advertised address、Proxy RPC 和 Broker 行为差异。

**Docker 环境是受控基线，不是任意生产集群的证明。** 当前 fixture 与手工环境使用 RocketMQ 5.5.0。生产
部署仍应在实际版本、TLS、ACL、拓扑与 topic 配置上做验收测试。

**数据隔离是刻意设计。** Testcontainers 通过每个 fixture 的网络和动态端口隔离测试；Compose 使用持久卷以
便于手工检查。不要把两者混用，否则会引入难以复现的 offset、topic 和消费者组状态。

## 延伸阅读

- [协议边界](../architecture/protocol-boundaries.md)
- [依赖注入与生命周期](../architecture/dependency-injection-and-lifetimes.md)
- [gRPC 消费模型](../grpc/consumer-model.md)
- [Remoting 传输与客户端角色](../remoting/transport-and-client-roles.md)
