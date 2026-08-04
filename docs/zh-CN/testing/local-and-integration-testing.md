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
| [`tests/it/EventHorizon.RocketMQ.Remoting.IntegrationTests`](../../../tests/it/EventHorizon.RocketMQ.Remoting.IntegrationTests) | Docker 集成测试 | NameServer/Broker 的 Admin、LitePull、Push PULL/POP、发送、请求-响应、事务、撤回和三 Broker route 路径。 |
| [`tests/it/EventHorizon.RocketMQ.Remoting.CrossProcessTestHost`](../../../tests/it/EventHorizon.RocketMQ.Remoting.CrossProcessTestHost) | 跨进程 IT 宿主 | 仅供 Remoting IT 启动独立 Push Consumer 进程，不包含测试断言且不打包。 |
| [`tests/it/EventHorizon.RocketMQ.IntegrationTestInfrastructure`](../../../tests/it/EventHorizon.RocketMQ.IntegrationTestInfrastructure) | 测试基础设施库 | Testcontainers fixture 与可复用环境，不引用任一生产协议项目。 |
| [`tests/benchmarks/EventHorizon.RocketMQ.Remoting.Benchmarks`](../../../tests/benchmarks/EventHorizon.RocketMQ.Remoting.Benchmarks) | 基准测试 | Remoting 性能敏感路径的 BenchmarkDotNet 测量。 |

单元测试优先使用 `MockBehavior.Strict` 的 Moq，以明确可替换协作者的交互。对于流式 framing、竞争、
时序或有状态协议协作，保留小型专用 fake 比堆叠 Mock 更清晰时可以例外。

测试方法采用 Microsoft 推荐的
[`MemberOrBehavior_Scenario_ExpectedOutcome` 三段式命名](https://learn.microsoft.com/en-us/dotnet/core/testing/unit-testing-best-practices#naming-your-tests)。
每段内部使用 PascalCase，段间以下划线分隔被测成员或行为、场景和可观察结果。没有单一被测方法的 IT 在第一段
写工作流名称。名称不添加 `Test` 前缀或 `Should` 套话，让 test runner 输出直接说明行为和失败条件。

可发布的 gRPC 与 Remoting Package 提供 `net8.0` 和 `net10.0` 两个目标。所有单元测试、兼容性测试、集成测试、
sample 和 BenchmarkDotNet 项目统一使用 `net10.0`；`net8.0` 只作为 Package 的受支持 target 保留。solution build
仍会编译两个 Package 目标，仓库内可运行项目则使用 `global.json` 选择的 SDK。
CI 还会通过命令行 target-framework override 在 `net8.0` 上执行 gRPC 和 Remoting 单元测试。测试项目本身仍声明
`net10.0`；该 override 用于验证已发布 Package 的 `net8.0` 资产，而不扩展仓库内可运行项目的 target 策略。

Remoting BenchmarkDotNet 测量独立于 test runner，位于
[`tests/benchmarks/EventHorizon.RocketMQ.Remoting.Benchmarks`](../../../tests/benchmarks/EventHorizon.RocketMQ.Remoting.Benchmarks)。
其目录与生产代码职责一致：Push 分发测量放在 `Consumer/Push`，wire 往返测量放在 `Protocol`。在仓库根目录使用
Release 模式单独运行 Push 分发 benchmark：

```shell
dotnet run -c Release --project tests/benchmarks/EventHorizon.RocketMQ.Remoting.Benchmarks/EventHorizon.RocketMQ.Remoting.Benchmarks.csproj -- --filter '*RemotingPushDispatchBenchmarks*'
```

可长期比较的运行环境、命令、源码 commit 和结果记录保存在 benchmark 项目的
[`baselines`](../../../tests/benchmarks/EventHorizon.RocketMQ.Remoting.Benchmarks/baselines) 目录；原始
`BenchmarkDotNet.Artifacts` 输出继续保持忽略。

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

IT 必须同时覆盖这些真实边界上可以确定性执行的正常链路与相关异常行为。改动涉及故障隔离、重试、阻塞、恢复或
持久化时，需要执行稳定的真实失败路径，不能只验证成功路径。若真实环境无法可靠注入底层 transport 或持久化失败，
这些场景仍必须由确定性 UT 覆盖，不能用易抖动的 Docker 故障注入替代。

例如，多 Broker、多 Queue 行为必须用 UT 覆盖 route 展开、每个队列只分配一次、Consumer 数量多于队列时的
空闲分配，以及逐队列调度与 offset 隔离；Docker IT 再验证对应的真实 NameServer route、Broker 持久化和
跨客户端进程协调，并验证单个队列阻塞或重试时不会干扰已成功的队列。

经典 Push 与 LitePull 的 Rebalance 集成 case 会刻意把路由轮询和心跳间隔设为 1 小时，并要求分配在 10 秒内发生
变化。生产周期兜底最长为 20 秒，因此 Broker `NotifyConsumerIdsChanged` 或客户端唤醒链路失效时，这些 case 不会
靠轮询误通过。测试会先等待队列分配互斥且并集完整，再发送消息，避免把启动收敛窗口内正常的 at-least-once 投递
误判为分配缺陷。

Push Rebalance 同时覆盖一个 `AddRocketMQRemoting` 注册下的两个 Consumer，以及运行在两个独立操作系统进程中的
两个 Consumer。跨进程 case 会用不同的客户端标识启动两个测试专用的 net10.0 可执行程序
`EventHorizon.RocketMQ.Remoting.CrossProcessTestHost`，通过重定向的标准输入输出等待分配快照，再正常停止其中
一个进程；这个仅用于测试的命令行由官方 `System.CommandLine` Package 负责解析和校验。测试会验证存活进程接管
全部队列，并确认成员变化前后的消息都只消费一次。该 case 会分别运行并发消费与
顺序消费模式，后者也覆盖跨进程的 Broker 队列锁。另有一个并发模式 case 会强制终止第二个子进程。顺序消费的
强制终止不使用短 Rebalance 超时断言，因为恢复时长由 Broker 队列锁 lease 过期而非通知链路决定。父测试会持续排空
两个输出流；清理超时时只终止对应的子进程树，因此断言失败不会在 CI 中遗留 Consumer 进程。

这样失败信息能直接指向客户端逻辑。若测试需要真实 endpoint 才能成立，它应迁移到对应的 integration
test 项目，而不是在 unit test 中偷偷连接本地 `localhost`。

### 2. Integration fixture 启动受控的 RocketMQ 集群

共享
[`RocketMQSingleBrokerContainerFixture`](../../../tests/it/EventHorizon.RocketMQ.IntegrationTestInfrastructure/RocketMQSingleBrokerContainerFixture.cs)
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

single-Broker fixture 会启用 Broker-side assignment 与 timer wheel，同时把消息请求模式默认保持为 PULL。
Remoting Broker-assigned Push case 仅通过测试管理操作把一个唯一 topic/group 切换为 POP，验证投递、确认、固定 deadline
过期、迟到结果拒绝、一次性重试、不对不确定失败重试和死信行为，并在清理时恢复 PULL。`SET_MESSAGE_REQUEST_MODE` 始终属于测试管理面，不会成为生产 Consumer
API。普通 LitePull 与默认 Push 工作流不依赖 Broker 侧 POP 配置。

single-Broker 的运行时间也属于需要持续测量的测试设计约束。在 commit `9ef119f` 上，本地使用
`Topology!=MultiBroker` 筛选条件运行 Remoting 测试，29 个测试全部通过，测试阶段耗时 167.7 秒，其中共享
fixture 启动约 44.6 秒。
使用 lazy assembly fixture 的测试时长都包含同一段启动时间，不能直接相加。容器就绪后，最明确的可避免开销来自
五个串行的 Broker-assigned POP 工作流：这些工作流多次使用 7 秒的结算后观察窗口，还叠加 retry 以及固定 deadline/迟到结果的观察等待。

经过优化的测试套件仍保留同等的真实 Broker 覆盖，并遵守以下规则：

- Broker-assigned POP 工作流使用独立 topic 和 consumer group，并拆分到可以彼此并行的测试类。公共 helper 可以复用
  setup，但不能把本来隔离的工作流重新放回一个串行测试类。
- 固定时长的静默等待不能证明 ACK、不可见时间变更、send-back 或 offset commit 已成功。测试应等待可观测的协议
  操作成功或 Broker 状态变化，再校验关联的 message、receipt、topic、group 与 outcome。如果现有 OpenTelemetry
  settlement completion 代表真实 Broker response，可以把它作为观察信号，但不能为测试在生产代码中增加捷径。
- timeout 只用于限定失败上限，不应作为预期 sleep。对于“没有重投”等负向断言，应使用能够证明条件的最短真实
  长轮询或状态查询，并为该等待单独设置上限。
- 重试、固定 deadline 过期、迟到结果拒绝、不对不确定失败重试、死信顺序、防重复、rebalance 恢复与持久化覆盖必须保留。
  只有确定性信号能证明同一不变量时，才可以删除等待时间。
- 性能改动应使用同一条筛选命令，并至少在镜像已预热的情况下比较测试阶段 wall-clock 时间。即使结果更快，只要
  聚焦工作流或完整筛选测试出现抖动，仍不能接受。

端口由 Testcontainers 动态映射，测试不会依赖手工环境的固定端口。

Broker 与 cluster-mode Proxy 在同一容器中按顺序启动：Broker 先注册到 NameServer，Proxy 再启动。这既能
覆盖宿主机访问 Broker 的 route，又能覆盖 LitePush 所需的 cluster-mode Proxy 路径。

多 Broker 覆盖使用两套协议隔离的 fixture：

- Remoting fixture 启动两个相互独立的 NameServer 和三个宿主机可达的 master Broker，并在每台 Broker 上显式创建三个读写队列。
  每台 Broker 都向两个 NameServer 注册。初始化会分别等待两个 NameServer 独立报告三台 Broker 以及完整的九个队列
  route。普通 multi-Broker Remoting client 使用分号分隔的 NameServer 地址（`NameServerA;NameServerB`）。Remoting 测试
  断言完整的九个物理队列 route，逐队列定向发送消息，验证一个 PushConsumer 会提交每个队列的 offset，并验证同组的
  三个及十个 PushConsumer 的分配。专门的故障切换集成测试使用同一个配置了两个地址的 client：先通过 A 和 B 各刷新
  一次 route，再停止 A，等待 10 毫秒的 route cache 过期，确认客户端对 A 的尝试失败并切换到 B，随后仍能刷新完整 route 并成功
  发送消息。最后重启 A，等待 A 再次报告三台 Broker 和完整的九个队列 topic route，再完成清理，避免污染共享 collection。
  Consumer 多于九个队列时，多出的实例保持未分配，且不会产生重复投递。
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

GitHub Actions 会在一个 job 中完成格式化和 solution 全量构建，同时并行运行三个按协议/兼容性拆分的单元测试
job。每个单元测试 job 都会 restore、build、运行对应的 `net10.0` 测试项目并单独上传覆盖率报告；solution build
另有两个不上传覆盖率的 job 会安装 .NET 8 runtime，并用 `-p:TargetFramework=net8.0` 运行两个协议的单元测试；
solution build 和发版 pack 负责验证两个可发布库的全部目标框架。两层验证均通过后，再于四个
并行的 `ubuntu-latest` matrix job 运行 Docker 驱动的 gRPC 与 Remoting integration test：每个协议各有一个
single-Broker 与一个 multi-Broker job。类级别的 `Topology=MultiBroker` trait 选择 multi-Broker 测试；
single-Broker job 运行其余测试。每个 job 只 restore
和 build 对应协议的 integration-test 依赖图，使用独立超时预算，并复用仓库的 NuGet 包缓存。Testcontainers
仍会在每个 job 内独立启动隔离的 Broker、NameServer 与 Proxy fixture；多 Broker Remoting fixture 包含两个相互独立的
NameServer。single-Broker job 内，已经隔离的测试类
会在 runner 上限内并发执行；legacy Remoting suite 则按设计保持串行。四个 job 都会生成 Cobertura 报告，以不同
的上传名称归入共享的 `integration-tests` flag；Codecov 会将其与 `unit-tests` 报告合并。仓库配置仍会排除
`samples`，不会将 sample 计入覆盖率。

### 5. 测试范围也体现支持边界

集成测试只覆盖仓库真正公开且可由目标服务端运行的路径。gRPC 的 Simple、Push 和 LitePush 在匹配的
Proxy/Broker 配置下覆盖；已移除的 gRPC PullConsumer 没有 sample 或 integration test，因为它不是当前
可提供的 public role。显式 queue/offset 工作流由 Remoting LitePull 的手工 assignment、seek 和 commit 覆盖；
Broker-assigned PULL/POP 则由 Remoting Push integration test 覆盖。

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

- [集成测试项目](../../../tests/it/README.zh-CN.md)
- [协议边界](../architecture/protocol-boundaries.md)
- [依赖注入与生命周期](../architecture/dependency-injection-and-lifetimes.md)
- [gRPC 消费模型](../grpc/consumer-model.md)
- [classic Remoting Consumer 模型](../remoting/consumer-model.md)
- [Remoting 传输与客户端角色](../remoting/transport-and-client-roles.md)
