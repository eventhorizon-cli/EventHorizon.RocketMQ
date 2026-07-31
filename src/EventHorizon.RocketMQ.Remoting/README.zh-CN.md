# EventHorizon.RocketMQ.Remoting

[English](https://github.com/eventhorizon-cli/EventHorizon.RocketMQ/blob/main/src/EventHorizon.RocketMQ.Remoting/README.md) |
[简体中文](https://github.com/eventhorizon-cli/EventHorizon.RocketMQ/blob/main/src/EventHorizon.RocketMQ.Remoting/README.zh-CN.md)

`EventHorizon.RocketMQ.Remoting` 是提供 `net8.0` 和 `net10.0` 目标的非官方 Apache RocketMQ classic Remoting
Project 和 NuGet Package，并集成 Microsoft 依赖注入、Options、日志记录和 Generic Host 生命周期管理。

客户端先连接 RocketMQ NameServer，再根据路由信息发现 Broker 并直接建立连接。应用需要使用经典协议时，
请使用该 Package；需要通过 Proxy 使用 RocketMQ 5 protobuf/gRPC API 时，请改用 `EventHorizon.RocketMQ.Grpc`。

客户端行为由高覆盖率单元测试保障，并通过 Docker 驱动的集成测试验证真实的 NameServer 和 Broker 链路。

> **请先阅读[仓库总览](https://github.com/eventhorizon-cli/EventHorizon.RocketMQ/blob/main/README.zh-CN.md)和
> [可运行示例](https://github.com/eventhorizon-cli/EventHorizon.RocketMQ/blob/main/samples/README.zh-CN.md)。**
> 仓库总览说明两种协议各自支持的功能以及如何选择 Package；可运行示例展示各个客户端角色的实际接入方式。

## 快速开始

### 安装

配置当前环境使用的 NuGet source，然后安装 Remoting Package：

```shell
dotnet add package EventHorizon.RocketMQ.Remoting
```

该 Package 在客户端 API 层面完全自包含：所有公开模型都在自己的程序集内，不依赖其他
EventHorizon.RocketMQ Package。

所有公开客户端模型都位于 `EventHorizon.RocketMQ.Remoting` 命名空间树下。Remoting 与 gRPC Package 可以在
同一应用中使用，但两套协议命名空间中的同名模型仍是不同的 CLR 类型，不能直接互换。如果同一个文件
导入了两边的协议命名空间，应为重复的短类型名设置 alias：

```csharp
using GrpcMessage = EventHorizon.RocketMQ.Grpc.Producer.Message;
using RemotingMessage = EventHorizon.RocketMQ.Remoting.Producer.Message;
```

### 最小注册与使用

通过 `AddRocketMQRemoting` 创建客户端注册，再通过 `AddRemotingProducer` 添加角色，并在端点中注入其协议专用接口。
下面的 ASP.NET Core Minimal API 会在每次 `POST /messages` 请求时发送一条消息。

```csharp
using EventHorizon.RocketMQ.Remoting;
using EventHorizon.RocketMQ.Remoting.Producer;
using Microsoft.AspNetCore.Builder;

var builder = WebApplication.CreateBuilder(args);

var rocketMQ = builder.Services.AddRocketMQRemoting(options =>
{
    options.NamesrvAddr = "nameserver-a:9876;nameserver-b:9876";
});

rocketMQ.AddRemotingProducer(options =>
{
    options.GroupName = "orders-producer";
});

var app = builder.Build();

app.MapPost("/messages", async (IRemotingProducer producer, CancellationToken cancellationToken) =>
{
    return await producer.SendAsync(
        new Message("orders", "hello"u8.ToArray()),
        cancellationToken);
});

await app.RunAsync();
```

默认客户端注册通过常规参数注入或构造函数注入进行解析。每个客户端注册最多分别添加一个 Producer 和 Admin；
同一个 builder 可重复调用 Consumer 注册方法，并为每个实例配置不同选项。

## 支持的功能

`✅` 表示该 Project 已实现客户端 API。目标 RocketMQ NameServer 和 Broker 仍必须支持并启用相应的服务端功能。

| 功能 | 状态 | 条件和说明 |
| --- | :---: | --- |
| 普通 Producer 发送和重试 | ✅ | `IRemotingProducer.SendAsync` 返回 `RemotingSendResult`；应检查其状态，因为部分 Broker 发送结果不会抛出异常。 |
| 单向 Producer 发送 | ✅ | `SendOnewayAsync` 不接收响应，因此任务完成不代表 Broker 已存储消息。 |
| 指定队列和队列选择器 | ✅ | 路由发现必须返回所请求的可写 Broker 队列。 |
| 批量 Producer 发送 | ✅ | 所有消息必须使用同一主题，且不能是延迟、重试或事务消息。 |
| FIFO Producer 消息 | ✅ | 设置 `MessageGroup` 以选择稳定的队列亲和性。 |
| 定时或延迟 Producer 消息及撤回 | ✅ | 设置 `DeliveryTimestamp`；撤回需要在投递前使用返回的 handle，并要求 Broker 已启用对应功能。 |
| 优先级和 Lite Producer 消息 | ✅ | 设置 `Priority` 或 `LiteTopic`；这些专用消息类型不能组合使用，并要求 Broker 支持。 |
| 事务 Producer 消息 | ✅ | 配置 `LocalTransactionExecutor` 和 `TransactionChecker`；Broker 可以在之后检查未确定的结果。 |
| Producer 请求-响应 | ✅ | 应用网络必须允许 Broker 为响应所需的回调。 |
| 只读 `IRemotingAdmin` | ✅ | 支持队列发现、通过 `OffsetMessageId` 查询物理消息，以及位点/时间查询。`ViewMessageAsync` 会直接连接 ID 中编码的 Broker 端点。 |
| `IRemotingPullConsumer` | ✅ | 支持显式队列和位点，以及 tag 或 SQL 过滤。SQL 过滤要求目标 Broker 配置过滤能力。 |
| `IRemotingLitePullConsumer` | ✅ | 支持订阅后的自动分配，或通过 `AssignAsync` 手动分配；客户端维护位点、提交、暂停、恢复和 seek。目前仅支持集群消费。 |
| `IRemotingPopConsumer` | ✅ | 支持手动选择物理队列、receipt 确认以及普通 topic POP 消息的可见期续租；Broker 必须支持 POP。 |
| `IRemotingPushConsumer` | ✅ | 支持集群或广播消费、运行时订阅、可配置起始位点、可部分确认前缀的并发批量回调或单消息 FIFO 分发、重试、死信处理，以及 Broker 触发的重平衡或位点重置。 |
| 队列有序 Push 消费 | ✅ | 使用可选的 Broker 队列锁实现有序消费；仅在目标 Broker 支持经典锁定流程时配置。 |
| 内置 Socket 传输 | ✅ | 基于 `System.IO.Pipelines`，支持可选 TLS、多个 NameServer 地址、Broker 故障转移、ACL 签名、命名空间和可配置帧限制；不依赖 Bedrock Framework。 |
| 依赖注入、Options、日志、Generic Host 生命周期以及默认客户端注册和 keyed 客户端注册 | ✅ | 使用 `AddRocketMQRemoting` 创建客户端注册，再添加所需角色。 |
| OpenTelemetry tracing 和 metrics | ✅ | 在应用的 OpenTelemetry SDK 中订阅 `RemotingRocketMQInstrumentation` 提供的 source 和 meter 名称。 |
| RocketMQ 5 Proxy gRPC 和 `SimpleConsumer` API | — | 需要通过 Proxy 使用 protobuf/gRPC API 时，请使用 `EventHorizon.RocketMQ.Grpc`。 |

## 连接与安全

`RemotingClientOptions` 用于配置经典连接：

- `NamesrvAddr` 接受一个或多个以分号分隔的 NameServer 地址，默认值为 `localhost:9876`。
- `RequestTimeout`、`NameServerRequestTimeout`、`PollNameServerInterval` 和
  `HeartbeatBrokerInterval` 用于控制请求及维护任务的时间参数。
- `ClientIP`、`InstanceName`、`UnitMode` 和 `UnitName` 用于控制经典客户端标识。
- `Namespace` 会在传输时自动为主题和消费组添加前缀，同时公开结果仍保留逻辑资源名称。
- `MaxRemotingFrameSize` 限制接收的完整帧大小，默认值为 16 MiB；提高该值前，需要在 Broker
  配置相同限制。

### ACL

同时设置 `AccessKey` 和 `AccessSecret` 后，客户端会对 NameServer 与 Broker 请求签名。
`SecurityToken` 为可选配置，设置后会随签名请求发送；使用它时必须同时提供 access key 和 secret。

```csharp
builder.Services.AddRocketMQRemoting(options =>
{
    options.NamesrvAddr = "nameserver:9876";
    options.AccessKey = configuration["RocketMQ:AccessKey"];
    options.AccessSecret = configuration["RocketMQ:AccessSecret"];
    options.SecurityToken = configuration["RocketMQ:SecurityToken"];
    options.Namespace = "tenant-a";
});
```

注册校验要求 AccessKey 与 AccessSecret 必须同时提供。

### TLS

启用 `UseTLS` 后，如果需要私有根证书、mTLS、证书吊销检查、TLS 协议选择或不同的 SNI 名称，
可通过 `ConfigureLegacySslOptions` 定制每条连接。

```csharp
using System.Security.Cryptography.X509Certificates;

builder.Services.AddRocketMQRemoting(options =>
{
    options.NamesrvAddr = "nameserver.internal:9876";
    options.UseTLS = true;
    options.ConfigureLegacySslOptions = (_, tls) =>
    {
        tls.TargetHost = "broker.internal";
        tls.ClientCertificates = new X509CertificateCollection { clientCertificate };
        tls.CertificateChainPolicy = new X509ChainPolicy
        {
            TrustMode = X509ChainTrustMode.CustomRootTrust,
            RevocationMode = X509RevocationMode.Online
        };
        tls.CertificateChainPolicy.CustomTrustStore.Add(rootCertificate);
    };
});
```

该回调可能被并发调用，并且每次连接都会收到一个新的 `SslClientAuthenticationOptions` 实例。

## OpenTelemetry

该 Package 会发出 OpenTelemetry Activity 和 metrics，不需要安装单独的 instrumentation Package。在应用的
OpenTelemetry SDK 中订阅公开名称：

```csharp
builder.Services.AddOpenTelemetry()
    .WithTracing(tracing => tracing
        .AddRocketMQRemotingInstrumentation())
    .WithMetrics(metrics => metrics
        .AddRocketMQRemotingInstrumentation());
```

资源属性、采样器、processor 和 exporter 由应用负责。客户端会为 Producer send、非空 receive、自动 Push process
以及 Consumer 完结操作发出 telemetry；它会把分布式上下文注入出站消息属性，且不会覆盖已有传播字段。trace 模型和
metrics 请参阅 [OpenTelemetry 埋点设计](../../docs/zh-CN/architecture/opentelemetry-instrumentation.md)，通过 OTLP
接入 Grafana 的方式请参阅 [OpenTelemetry Web 示例](../../samples/opentelemetry/README.zh-CN.md)。

## Producer

注册并注入 `IRemotingProducer`；本项目不提供与传输无关的 Producer 接口。

```csharp
using System.Text;
using EventHorizon.RocketMQ.Remoting;
using EventHorizon.RocketMQ.Remoting.Producer;

rocketMQ.AddRemotingProducer(options =>
{
    options.GroupName = "orders-producer";
    options.RetryTimesWhenSendFailed = 2;
});

public sealed class OrderPublisher(IRemotingProducer producer)
{
    public Task<RemotingSendResult> PublishAsync(
        string orderId,
        CancellationToken cancellationToken)
    {
        var message = new Message("orders", Encoding.UTF8.GetBytes(orderId))
        {
            Tag = "created",
            MessageGroup = orderId
        };
        message.Keys.Add(orderId);
        message.Properties["source"] = "checkout";

        return producer.SendAsync(message, cancellationToken);
    }
}
```

`SendAsync` 返回 `RemotingSendResult`。调用方应始终检查 `Status`：经典 Broker 可能返回
`FlushDiskTimeout`、`FlushSlaveTimeout` 或 `SlaveNotAvailable`，但不会因此抛出异常。
`SendOnewayAsync` 不等待 Broker 响应，因此任务完成并不代表消息已存储。

`MessageGroup` 会为 FIFO 投递选择稳定队列。`DeliveryTimestamp`、`Priority` 和 `LiteTopic` 会映射到
对应的经典保留属性。这四种专用消息类型不能组合使用，并且目标 Broker 必须支持并启用相应能力。
发送重试可能产生重复消息，因此 Consumer 应保证处理幂等。

### 队列与批量

当应用需要明确指定目标队列时，使用 `GetPublishMessageQueuesAsync`。指定队列和 selector 重载会复用
普通发送的路由校验和重试行为。

```csharp
var queues = await producer.GetPublishMessageQueuesAsync("orders", cancellationToken);
var queue = queues.First(static value => value.BrokerName == "broker-a" && value.QueueId == 0);
await producer.SendAsync(new Message("orders", "first"u8.ToArray()), queue, cancellationToken);

await producer.SendAsync(
    [
        new Message("orders", "second"u8.ToArray()),
        new Message("orders", "third"u8.ToArray())
    ],
    cancellationToken);
```

批量消息必须使用同一主题，且不能是延迟、重试或事务消息。客户端使用经典 batch mini-record 格式，并为
每条消息保留唯一标识。

### 事务、撤回与请求-响应

`SendTransactionAsync` 遵循经典 Broker 事务流程：先发送 half message，再调用
`LocalTransactionExecutor`，并自动向 Broker 报告提交、回滚或未知结果。还必须配置
`TransactionChecker`，因为 Broker 之后可能检查未确定的本地事务结果。

```csharp
using EventHorizon.RocketMQ.Remoting.Producer.Transactions;

rocketMQ.AddRemotingProducer(options =>
{
    options.GroupName = "orders-producer";
    options.LocalTransactionExecutor = static (message, state, cancellationToken) =>
        ValueTask.FromResult(RemotingTransactionResolution.Commit);
    options.TransactionChecker = static (message, cancellationToken) =>
        ValueTask.FromResult(RemotingTransactionResolution.Unknown);
});
```

符合条件的延迟消息会在 `RemotingSendResult.RecallHandle` 中返回不透明的 Broker handle；保持其原样，
并使用同一个逻辑主题调用 `RecallAsync`。

```csharp
var sent = await producer.SendAsync(delayedMessage, cancellationToken);
if (sent.RecallHandle is { } recallHandle)
{
    await producer.RecallAsync(delayedMessage.Topic, recallHandle, cancellationToken);
}
```

`RequestAsync` 会以普通经典消息属性传递关联 ID、请求方 client ID 和存活时间。Producer 会注册所需的
Broker 回调、维护相关 Broker 的 producer heartbeat，并在完成、取消、超时或停止时移除待处理请求。

```csharp
var reply = await producer.RequestAsync(
    new Message("orders", "quote"u8.ToArray()),
    TimeSpan.FromSeconds(10),
    cancellationToken);
```

接收请求的应用从请求属性创建 `RemotingReply`，再通过 `SendReplyAsync` 发送。该方法发送经典
`SEND_REPLY_MESSAGE` 命令，并不会让 Producer API 耦合 Consumer 实现。

```csharp
using EventHorizon.RocketMQ.Remoting.Consumer;
using EventHorizon.RocketMQ.Remoting.Producer;

public sealed class OrderResponder(IRemotingProducer producer)
{
    public async ValueTask<ConsumeResult> HandleAsync(
        RemotingMessageView request,
        CancellationToken cancellationToken)
    {
        var reply = RemotingReply.FromRequestProperties(request.Properties, "quoted"u8.ToArray());
        await producer.SendReplyAsync(reply, cancellationToken);
        return ConsumeResult.Success;
    }
}
```

只应将实际的请求消息传给 `RemotingReply.FromRequestProperties`；该方法会校验所需的 `CLUSTER`、
`CORRELATION_ID`、`REPLY_TO_CLIENT` 和 `TTL` 属性。

## Admin

`IRemotingAdmin` 是独立的只读角色，不会启动托管后台服务。

```csharp
using EventHorizon.RocketMQ.Remoting.Admin;

rocketMQ.AddRemotingAdmin();

public sealed class OrderOffsets(IRemotingAdmin admin)
{
    public async Task<long> GetMaximumAsync(CancellationToken cancellationToken)
    {
        var queue = (await admin.GetMessageQueuesAsync("orders", cancellationToken))[0];
        return await admin.GetMaxOffsetAsync(queue, cancellationToken: cancellationToken);
    }
}
```

当消费组没有已提交位点时，`GetConsumerOffsetAsync` 返回 `null`；Broker 未报告可用消息时，
`GetEarliestMessageStoreTimeAsync` 返回 `null`。其余方法可查询队列最早位点、当前最大位点，或按时间以
`Lower`（下界）/`Upper`（上界）查找位点。

`ViewMessageAsync` 可通过经典发送返回的物理 `RemotingSendResult.OffsetMessageId` 读取一条已存储消息。
它不是 Producer 分配的 `MessageId`：其中编码了 Broker 端点和 CommitLog 位点。Admin 客户端会解析该端点并
直接连接，因此应用网络必须能访问 ID 内嵌的 Broker 地址和端口；NameServer 的路由发现无法替代该连接。

```csharp
var sent = await producer.SendAsync(new Message("orders", "receipt"u8.ToArray()), cancellationToken);
var stored = await admin.ViewMessageAsync("orders", sent.OffsetMessageId, cancellationToken);
```

可运行的 HTTP 和 Swagger 接口请参阅[Remoting Admin 示例](https://github.com/eventhorizon-cli/EventHorizon.RocketMQ/blob/main/samples/remoting/GenericHost/Admin/README.zh-CN.md)。

## PullConsumer

`IRemotingPullConsumer` 将队列分配、消息处理和位点提交时机交给应用程序控制。它是显式请求 API，不会注册后台
消费组 heartbeat。

```csharp
using EventHorizon.RocketMQ.Remoting;
using EventHorizon.RocketMQ.Remoting.Consumer;
using EventHorizon.RocketMQ.Remoting.Consumer.Pull;

rocketMQ.AddRemotingPullConsumer(options =>
{
    options.GroupName = "orders-pull-consumer";
    options.BatchSize = 32;
    options.Subscribe("orders", new FilterExpression("created"));
});

public sealed class OrderPuller(IRemotingPullConsumer consumer)
{
    public async Task PullOnceAsync(CancellationToken cancellationToken)
    {
        var queues = await consumer.GetMessageQueuesAsync("orders", cancellationToken);
        foreach (var queue in queues)
        {
            var offset = await consumer.GetOffsetAsync(queue, cancellationToken);
            if (offset < 0)
            {
                offset = await consumer.QueryOffsetAsync(
                    queue,
                    QueryOffsetPolicy.Beginning,
                    cancellationToken: cancellationToken);
            }

            var result = await consumer.PullAsync(
                queue,
                offset,
                cancellationToken: cancellationToken);

            foreach (var message in result.Messages)
            {
                await ProcessAsync(message.Body, cancellationToken);
            }

            await consumer.UpdateOffsetAsync(queue, result.NextOffset, cancellationToken);
        }
    }

    private static Task ProcessAsync(byte[] body, CancellationToken cancellationToken) =>
        Task.CompletedTask;
}
```

消费组没有已提交位点时，`GetOffsetAsync` 返回 `-1`。`PullAsync` 返回 `RemotingPullResult`，其中包含
`RemotingPullStatus`、下一位点和 `RemotingMessageView` 实例；队列句柄类型为
`RemotingPullMessageQueue`。

tag 表达式是默认过滤方式。SQL92 过滤可使用 `new FilterExpression("region = 'west'",
FilterExpressionType.Sql)`；目标 Broker 必须允许属性过滤。

## LitePullConsumer

`IRemotingLitePullConsumer` 是经典的面向分配的拉取模型。在订阅或手工分配队列处于活跃状态时，它会发送
`CONSUME_ACTIVELY` 心跳。订阅模式参与集群消费组分配；手工分配保留应用选择的队列。两种模式都对已分配队列
执行长轮询。`PollAsync` 仅推进本地队列位点；只有显式调用 `CommitAsync` 才会将该位点写入 Broker。

```csharp
using EventHorizon.RocketMQ.Remoting;
using EventHorizon.RocketMQ.Remoting.Consumer;
using EventHorizon.RocketMQ.Remoting.Consumer.Pull.Lite;

rocketMQ.AddRemotingLitePullConsumer(options =>
{
    options.GroupName = "orders-lite-pull-consumer";
    options.BatchSize = 32;
    options.InitialOffset = QueryOffsetPolicy.Beginning;
    options.Subscribe("orders", new FilterExpression("created"));
});

public sealed class OrderLitePuller(IRemotingLitePullConsumer consumer)
{
    public async Task PollOnceAsync(CancellationToken cancellationToken)
    {
        var result = await consumer.PollAsync(cancellationToken: cancellationToken);
        foreach (var message in result.Messages)
        {
            await ProcessAsync(message.Body, cancellationToken);
        }

        await consumer.CommitAsync(cancellationToken);
    }

    private static Task ProcessAsync(byte[] body, CancellationToken cancellationToken) =>
        Task.CompletedTask;
}
```

订阅和手动分配不能同时使用。手动模式不要配置订阅，通过 `GetMessageQueuesAsync` 发现队列后，将选中的
队列传给 `AssignAsync`。可使用 `Pause`、`Resume`、`Seek`、`SeekToBeginningAsync` 和
`SeekToEndAsync` 控制本地拉取位点。
向 `AssignAsync` 传入空集合会清除手动分配，并立即向已知 Broker 注销该 LitePull 消费组成员。

Lite Pull 当前仅支持集群消费。它仍是客户端发起的 Broker 长轮询，而不是协议层面的 Broker Push；当前
有意不提供广播模式的本地位点存储。

## POPConsumer

`IRemotingPopConsumer` 将经典 Broker POP 暴露为显式的 receipt 消费操作。应用选择物理队列、处理返回
消息，并确认每条 Broker 签发的 receipt。它不执行后台分配或自动分发；未确认的消息会在 receipt 可见期
到期后由 Broker 重新投递。它不会注册后台消费组 heartbeat。

经典 Broker 的单次 POP 最多接受 32 条消息；`BatchSize` 和每次调用的 `maxMessages` 参数均受该上限限制。

```csharp
using EventHorizon.RocketMQ.Remoting;
using EventHorizon.RocketMQ.Remoting.Consumer;
using EventHorizon.RocketMQ.Remoting.Consumer.Push.Pop;

rocketMQ.AddRemotingPopConsumer(options =>
{
    options.GroupName = "orders-pop-consumer";
    options.BatchSize = 32;
    options.InvisibleDuration = TimeSpan.FromSeconds(30);
    options.LongPollingTimeout = TimeSpan.FromSeconds(10);
});

public sealed class OrderPopper(IRemotingPopConsumer consumer)
{
    public async Task PopOnceAsync(CancellationToken cancellationToken)
    {
        var queue = (await consumer.GetMessageQueuesAsync("orders", cancellationToken))[0];
        var result = await consumer.PopAsync(
            queue,
            filter: new FilterExpression("created"),
            cancellationToken: cancellationToken);

        foreach (var received in result.Messages)
        {
            await ProcessAsync(received.Message.Body, cancellationToken);
            await consumer.AcknowledgeAsync(received.Receipt, cancellationToken);
        }
    }

    private static Task ProcessAsync(byte[] body, CancellationToken cancellationToken) =>
        Task.CompletedTask;
}
```

处理需要更长时间时，应在当前可见期到期前调用 `ChangeInvisibleTimeAsync`；它会返回新的 receipt，后续确认
必须使用该新 receipt。该初始 API 仅支持普通物理 topic 的直接 checkpoint，并有意不提供自动分配、广播模式、
有序 POP、批量确认，以及 retry/logical topic 的 checkpoint 处理。

POP 命令码超出了 RocketMQ 可选二进制 command-header 格式保留的有符号 16 位 code 字段。已注册的默认
序列化器使用 JSON，POP 必须使用它；不要为该角色替换为 `RocketMQ` 二进制 header 序列化器。

## PushConsumer

经典 `IRemotingPushConsumer` 由客户端主动拉取和长轮询实现，并非协议层面的 Broker Push；但它还会
处理经典 Broker 的重平衡和位点重置回调。

Remoting 对外提供一套 PushConsumer API 和一套基于列表的 message-handler 合约。增大
`ConsumeMessageBatchSize` 只会改变该合约单次收到的最大消息数，不会选择另一个 Consumer 实现。

```csharp
using System.Text;
using Microsoft.Extensions.DependencyInjection;
using EventHorizon.RocketMQ.Remoting;
using EventHorizon.RocketMQ.Remoting.Consumer;
using EventHorizon.RocketMQ.Remoting.Consumer.Push;

rocketMQ.AddRemotingPushConsumer<OrderMessageHandler>(ServiceLifetime.Scoped, options =>
{
    options.GroupName = "orders-push-consumer";
    options.MaxConcurrency = 8;
    options.BatchSize = 32;
    options.ConsumeMessageBatchSize = 16;
    options.MaxDeliveryAttempts = 16;
    options.Subscribe("orders", new FilterExpression("created"));
});

public sealed class OrderMessageHandler : IRemotingPushMessageHandler
{
    public ValueTask<ConsumeResult> HandleAsync(
        IReadOnlyList<RemotingMessageView> messages,
        RemotingPushConsumeContext context,
        CancellationToken cancellationToken)
    {
        foreach (var message in messages)
        {
            Console.WriteLine(Encoding.UTF8.GetString(message.Body));
        }

        return ValueTask.FromResult(ConsumeResult.Success);
    }
}
```

`BatchSize` 是单次长轮询从 Broker 请求的最大消息数，默认值为 32。`ConsumeMessageBatchSize` 是单次
handler 调用收到的最大消息数，默认值为 1。并发分发只会将来自同一个 Broker 物理队列、且不带
`MessageGroup` 的消息合并为一批；带 `MessageGroup` 的 FIFO 消息以及所有 `ConsumeOrderly` 投递都会以
只包含一条消息的列表传递，以保持顺序和重试语义。符合并发条件的批次可在 `MaxConcurrency` 范围内并行处理。

并发分发会为每个已分配的 Broker 物理队列维护一个客户端侧 `ProcessQueue`。它缓存本地已拉取的消息及其消费
状态；它不是另一个 Broker 队列，也不会创建服务端资源。拉取位点可以独立于 handler 完成而继续推进。
`MaxCachedMessages` 限制等待首次分发的新拉取消息准入，在途批次不会继续占用该容量。处置尚未解决，或 FIFO
消息存在未完成的前驱（包括位于另一个物理队列的前驱）时，该 `ProcessQueue` 中已经缓存的消息会释放准入配额，
并暂停该队列的新准入，直到可以继续推进。这样被阻塞的队列不会独占健康队列所需的容量。因此该设置也不是进程内
所有驻留消息的严格总数上限。

每个 `ProcessQueue` 的就绪通知会被合并为最多一个。共享的逻辑消费循环每次从一个就绪队列取得一个批次，并把
仍有工作的队列放回队尾，从而在活跃队列之间提供公平轮询分发。`MaxConcurrency` 限制的是整个 Consumer 共享的
这些循环，而不是每个队列各自的并发度。它们是由 .NET ThreadPool 调度的异步任务，并非独占或绑定固定线程。

位点根据每个 `ProcessQueue` 独立维护的连续完成水位持久化。后面的批次或其他队列的批次即使先完成，也不能
越过前面尚未解决的 offset。retry/dead-letter 处置失败时会在 `RetryDelay` 后仅重试处置，不会再次调用应用
handler，也不能推进水位；位点持久化失败同样会重试。分配被撤销后，延迟返回的 handler 结果会被忽略，
不能影响替代它的新分配。

`ProcessQueue` 的名称和职责沿用了 RocketMQ 官方 Java 客户端的概念。公平 ready-queue 调度器以及每轮从
`ProcessQueue` 取一个批次是 .NET 的设计；Java 的并发路径会直接从每次刚拉取的消息列表创建 consume request，
因此两者的数据路径并非逐项复制。

`ConsumeTimeout` 默认值为 15 分钟。并发集群、非 FIFO 批次超过该时间后，Consumer 会取消 handler token，并为
整批消息请求 Broker 重新投递。它不能强制终止忽略取消信号的应用代码，因此 handler 必须响应该 token，并保持幂等。
延迟返回的结果会被忽略，并可能与重新投递的调用重叠。FIFO 和顺序投递为保持顺序保证而不会应用此机制。

handler 还会收到 `RemotingPushConsumeContext`。默认返回 `Success` 会提交整个批次。对于并发、非 FIFO 批次，可将
`context.AckIndex` 设为最后一条成功处理消息的从零开始索引，再返回 `Success`：客户端会提交这个连续前缀，并仅将其后的
尾部消息交由 Broker 重试。`AckIndex` 默认是 `int.MaxValue`（全部消息），当尚未处理任何消息时可设为 `-1`。返回
`Retry` 或 `DeadLetter` 时会忽略 `AckIndex`，并将结果应用到批次中的每条消息。

`context.DelayLevelWhenNextConsume` 控制将要重试的消息。它初始为由 `RetryDelay` 映射得到的 Broker 延迟级别；可设为
`0` 以让 Broker 选择重试策略，设为正的 RocketMQ 延迟级别以请求该级别，或设为负值以将这些消息直接发送到死信队列。
广播模式没有 Broker 重试或死信流程，因此未确认的批次尾部会被跳过，同时推进本地位点。`MessageGroup` FIFO 与
`ConsumeOrderly` 路径始终收到单消息列表，不使用部分批量确认。

运行时调用 `SubscribeAsync` 和 `UnsubscribeAsync` 会更新经典心跳和当前队列接收器。泛型注册会为当前 Consumer 实例选择
handler 生命周期：`Singleton` 会为每个 Consumer 实例创建一个 handler，且必须线程安全；`Scoped` 和
`Transient` 会在每次批量处理尝试中创建新的异步 DI scope，再从其中解析 handler。自动分发必须使用由应用容器解析的
`IRemotingPushMessageHandler` typed handler；handler 需要数据库上下文等 scoped 依赖时应选择 `Scoped`。

> **0.2.0 破坏性迁移：** `RemotingPushConsumerOptions.MessageHandler` 和非泛型
> `AddRemotingPushConsumer` overload 已被移除。请将委托逻辑移入 `IRemotingPushMessageHandler` 实现，并通过
> `AddRemotingPushConsumer<THandler>` 注册。handler 依赖 scoped 应用服务时应选择 `Scoped`。

### 队列有序消费

当经典 PushConsumer 需要让每个已分配的物理队列一次只处理一条消息时，设置 `ConsumeOrderly`。无论
`ConsumeMessageBatchSize` 如何配置，它都会传递只包含一条消息的列表：

```csharp
rocketMQ.AddRemotingPushConsumer<OrderMessageHandler>(ServiceLifetime.Scoped, options =>
{
    options.GroupName = "orders-orderly-consumer";
    options.ConsumeOrderly = true;
    options.MaxConcurrency = 8; // Limits concurrent queues, not a single queue's order.
    options.Subscribe("orders");
});
```

集群模式下，客户端会在拉取或提交已分配队列之前获取并续约经典 Broker 队列锁，并在重平衡、取消订阅或
正常停止后释放锁。返回 `Retry` 会在本地暂停当前队列，后续消息不会越过它；返回 `DeadLetter` 会先将
当前消息发送回 Broker，再推进队列。广播模式也会保持每个本地队列的串行处理，但不会获取 Broker 锁。

`ConsumeOrderly` 与 Producer 的 `MessageGroup` 不同。`MessageGroup` 提供稳定的生产端队列亲和性
和本地 FIFO 分发，但不会获取经典 Broker 队列锁。

队列锁仍是至少一次语义。若 handler 在锁丢失后继续执行，其外部副作用仍可能与新的锁持有者重叠，
因此有序 handler 必须保持幂等。

新消费组默认从末尾开始。当消费组没有已提交位点时，可配置其他起点：

```csharp
options.InitialPosition = ConsumeFromPosition.Beginning;

// Or start at the first offset at or after a timestamp.
options.InitialPosition = ConsumeFromPosition.Timestamp;
options.ConsumeTimestamp = DateTimeOffset.UtcNow.AddHours(-1);
```

起始位置不会重置已有的消费组位点，重试队列也会保留其恢复位置。

`StartAsync` 会启动 Consumer 生命周期并安排接收器初始化，但不保证每个新分配队列都已完成首个 offset 的查询和持久化，
也不保证已经发起首个拉取。默认的 `End` 策略下，若消息在 `StartAsync` 返回后、首个 offset 查询完成前写入，它可能位于
最终解析出的队尾之前，因此会被有意跳过。这与经典 RocketMQ 的 `CONSUME_FROM_LAST_OFFSET` 语义一致。部署切换必须包含
每条消息时，应使用 `Beginning`、`Timestamp`、已有的消费组已提交位点，或显式的应用就绪握手。并非测试启动时序的测试应
设置确定性的初始位置，或在发送前建立就绪条件。

广播消费会把每个可读队列分配给每个 Consumer 实例，并在本地存储位点：

```csharp
options.ConsumerMode = ConsumerMode.Broadcasting;
options.InitialPosition = ConsumeFromPosition.Beginning;
options.LocalOffsetStorePath = Path.Combine(
    Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData),
    "orders-broadcast-offsets.json");
```

如果默认客户端标识在不同启动之间会变化，请使用稳定且特定于实例的路径。广播模式没有消费组拥有的
重试或死信队列，因此 `Retry` 和 `DeadLetter` 结果会被丢弃，本地位点仍会向前推进。

## 多 Consumer、keyed 客户端注册与生命周期

现有方法签名可以分别配置每个 Consumer。以下两个 PushConsumer 在同一个 `AddRocketMQRemoting` 注册下使用
不同的 group 和 topic：

```csharp
var rocketMQ = builder.Services.AddRocketMQRemoting(options =>
{
    options.NamesrvAddr = "nameserver:9876";
});

rocketMQ.AddRemotingPushConsumer<OrderHandler>(ServiceLifetime.Scoped, options =>
{
    options.GroupName = "orders-consumer";
    options.Subscribe("orders", new FilterExpression("created"));
});

rocketMQ.AddRemotingPushConsumer<AuditHandler>(ServiceLifetime.Scoped, options =>
{
    options.GroupName = "audit-consumer";
    options.Subscribe("audit");
});
```

每个 Consumer 实例都有相互隔离的 options、逻辑 client ID、Engine、handler 绑定和 hosted 生命周期。默认
客户端注册可注入 `IEnumerable<IRemotingPushConsumer>` 取得全部 Push 实例；keyed 客户端注册则调用
`GetKeyedServices<IRemotingPushConsumer>(registrationName)`。按照 Microsoft DI 的注册顺序，单实例解析会返回
最后注册的 Consumer。

普通 Push 订阅组合具有以下语义：

| Consumer 注册组合 | 行为 |
| --- | --- |
| 相同 group、相同 topic/filter 订阅 | 合法的集群副本，分配会在实例间负载均衡。 |
| 不同 group、相同 topic | 扇出消费，每个 group 独立收到该 topic 的消息流。 |
| 不同 group、不同 topic | 相互隔离地消费。 |
| 相同 group、不同 topic 或 filter | 非法组合；注册会拒绝不一致的普通 Push 订阅。 |

同组 Push 实例还必须使用相同的集群/广播模式、顺序消费模式和初始位点；同组 LitePull 实例必须使用相同的订阅和
初始位点策略。Push 与 LitePull 会向经典 Broker 上报不同的消费类型，因此不能共用 group。并发度、超时和 handler
等实例本地配置仍可不同。注册层只能对当前进程和当前客户端注册内声明的成员做 fail-fast；其他进程中的成员仍需由
部署配置保证一致，因为经典 Broker heartbeat 不会携带包括顺序消费模式在内的所有客户端本地设置。

同一客户端注册会在经典协议允许时共享 NameServer 发现、路由状态和 Broker 连接。不同消费组的 Consumer
可以共享连接；同一消费组下重复注册的活跃组成员（Push 或 LitePull）则使用不同 Broker 连接，因为经典
Broker 以连接 channel 标识消费组成员。这种物理连接隔离不会改变上述集群副本语义。

分配给活跃 Push 或 LitePull 成员的每个物理 Client 都绑定一个内部 `RemotingRebalanceService`，由它持有唯一的 Broker
`NotifyConsumerIdsChanged` handler 和最长 20 秒的周期兜底。通知只写入可合并的唤醒信号；每个消费组通过自己
独立、单飞的 participant 完成收敛，因此阻塞的 group 不会占住入站请求循环，也不会串行阻塞无关 group。同一
group 的重复成员使用不同物理 Client，所以也分别拥有独立的 Rebalance service。

物理 `RemotingClient` 只负责传输，并不单独拥有消费组 identity。每个活跃的 Push 或 LitePull 会在初始和周期协调时，
经由分配给自己的 Client 发送 heartbeat。共享 Client 因而可以维护多个不同 group；同组的隔离成员则分别维护自己的
membership。Producer 维护自己的 producer heartbeat 生命周期。

同一应用连接多个集群、使用独立连接配置或添加另一个 Producer/Admin 时，请使用 keyed 客户端注册。注册名称即
`registrationName`；该 `registrationName` 同时作为 keyed service 的 key。

```csharp
using Microsoft.Extensions.DependencyInjection;
using EventHorizon.RocketMQ.Remoting;
using EventHorizon.RocketMQ.Remoting.Producer;

builder.Services
    .AddRocketMQRemoting("audit", options =>
    {
        options.NamesrvAddr = "audit-nameserver:9876";
    })
    .AddRemotingProducer(options => options.GroupName = "audit-producer");

public sealed class AuditPublisher(
    [FromKeyedServices("audit")] IRemotingProducer producer)
{
}
```

从独立 `ServiceProvider` 而不是 Generic Host 解析客户端时，请显式调用 `StartAsync` 和 `StopAsync`。Generic Host
已启动已注册角色后，应用代码不应再次调用这些方法。

## 兼容性与异常

- 该 Project 直接连接经典 NameServer 和 Broker 端点，不会连接 RocketMQ Proxy。
- `IRemotingProducer` 独立实现经典 half-message 事务、延迟消息撤回和请求-响应；它们的 wire contract
  和服务端兼容性与 gRPC Producer API 各自独立。
- PullConsumer、LitePullConsumer、POPConsumer 和 PushConsumer 在适用时都使用 Broker 长轮询。
  LitePullConsumer 增加主动的经典消费组协调、客户端本地位点和显式提交；POPConsumer 使用逐消息的
  可见 receipt；PushConsumer 还会执行自动分发，并支持经典 Broker 回调兼容。
- POP 使用 RocketMQ 5 Broker 引入的经典 `POP_MESSAGE`、确认和可见期变更命令。它要求 JSON 命令序列化，
  因为这些请求码超出了可选二进制 command-header 格式的有符号 16 位 code 字段。
- 本仓库提供的本地 Docker Compose 测试环境使用 Apache RocketMQ 5.5.0。SQL 过滤、TLS、ACL、优先级和 Lite 消息等能力
  仍取决于目标集群配置和服务端支持。
- `RemotingCommandException` 表示 NameServer 或 Broker 拒绝了命令，并保留原始 `ResponseCode`；
  它继承当前 Package 中的 `RocketMQClientException` 基类。

## 本地测试

仓库在 [`test-environments/rocketmq`](https://github.com/eventhorizon-cli/EventHorizon.RocketMQ/tree/main/test-environments/rocketmq)
下提供固定使用 Apache
RocketMQ 5.5.0 的 Docker Compose 环境。从仓库根目录运行：

```shell
docker compose -f test-environments/rocketmq/compose.yaml config --quiet
docker compose -f test-environments/rocketmq/compose.yaml up -d --wait
```

Remoting 客户端使用 `localhost:9876` 的 NameServer。通过
[环境说明](https://github.com/eventhorizon-cli/EventHorizon.RocketMQ/blob/main/test-environments/rocketmq/README.zh-CN.md)
中的命令创建主题和消费组，然后移除环境：

```shell
docker compose -f test-environments/rocketmq/compose.yaml down -v --remove-orphans
```

Remoting 集成测试使用相互隔离的 Testcontainers Fixture，不要求预先启动上述手动环境：

```shell
dotnet test tests/it/EventHorizon.RocketMQ.Remoting.IntegrationTests/EventHorizon.RocketMQ.Remoting.IntegrationTests.csproj
```

## 许可证

本项目基于 [Apache License 2.0](https://github.com/eventhorizon-cli/EventHorizon.RocketMQ/blob/main/LICENSE) 授权。
