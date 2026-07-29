# Remoting POP Consumer

[English](README.md) | [简体中文](README.zh-CN.md)

`IRemotingPopConsumer` 将 classic Broker POP 暴露为由调用方驱动、基于 receipt 的消费模式。应用选择一个物理队列并
调用 `PopAsync`；Broker 会让返回的消息在有限时间内不可见，并为每条消息签发 receipt。处理通过确认 receipt 完成，
而不是通过提交队列位点完成。

对此角色，SDK 不会在后台执行队列分配、接收分发或自动确认。

## 什么时候使用 POP Consumer

当工作需要暂时领取并逐条结算时，尤其是处理时长可能变化、应用需要延长消息不可见时间时，适合选择 POP。它适用于由调用方
控制的 worker：希望 Broker 管理重新投递，但不希望自己管理下一队列位点。

如果希望 SDK 管理 consumer group 分配、后台长轮询、handler 并发和重试结果，应选择 Push Consumer。如果需要的是
队列位置、重放和显式位点提交，应选择 Pull 或 Lite Pull。

POP 要求 Broker 支持 POP、确认和不可见时间变更命令。此客户端当前只支持普通物理 topic 的直接 checkpoint；不提供
自动分配、广播 POP、有序 POP、批量确认，也不处理 retry/logical topic checkpoint。

## SDK 工作流

通过 `AddRemotingPopConsumer` 注册此角色。在其 options 上调用 `ConsumerOptions.Subscribe` 只会记录该 topic
的默认过滤条件；它**不会**启动订阅、创建分配或启动接收循环。

公共 API 会显式呈现 receipt 生命周期：

1. `GetMessageQueuesAsync` 发现 topic 的可读物理队列。
2. `PopAsync` 以一个队列为目标，返回 `RemotingPopResult`，其中包含 `Status`、`RestNum` 和
   `RemotingPopMessage`。
3. 每个返回项都包含 `RemotingMessageView` 和 Broker 签发的 `RemotingPopReceipt`。
4. 处理成功后，以 `AcknowledgeAsync` 结算这一个 receipt。
5. 如果处理需要更长时间，应在到期前调用 `ChangeInvisibleTimeAsync`。它会返回一个**替代 receipt**；之后的任何
   延期或确认都必须使用这个替代值。

每次调用 `PopAsync` 时，可以覆盖已配置的 batch 大小、不可见时长和过滤条件。classic Broker 的单次 POP 最多接受
32 条消息。

## 确认凭据、失败与并发

未确认消息会在不可见时间到期后重新具备投递资格。因此，业务副作用发生后、确认前进程崩溃，确认请求失败，或处理超过不可见
时间，都可能导致重复处理。handler 应保持幂等；对耗时任务续期时还应留出足够余量，以便处理一次续期失败。

此模式没有应用可见的位点提交。不能把获得 receipt 当作处理完成：只有确认成功才会结算本次投递。如果
`ChangeInvisibleTimeAsync` 成功，旧 receipt 就已失效。

调用方决定队列选择、同时发起多少个 `PopAsync`，以及处理并发度。Broker 的不可见机制会让正常领取的消息不再立即对
其他 POP 请求可用，但不会消除至少一次重新投递，也不能代替应用限制本地并发。

POP 命令码要求使用注册时的默认 JSON command serializer。不要为此角色改用 RocketMQ 可选的 binary
command-header serializer。

## 运行示例

可运行示例轮询 `eventhorizon-test-topic` 的物理队列，并以 group `remoting-sample-pop-consumer` 确认消息。

启动提供兼容 RocketMQ 5.5.0 Broker 的
[本地 RocketMQ 环境](../../../test-environments/rocketmq/README.zh-CN.md)，然后运行：

```shell
docker compose -f test-environments/rocketmq/compose.yaml up -d --wait
dotnet run --project samples/remoting/PopConsumer/EventHorizon.RocketMQ.Samples.Remoting.PopConsumer.csproj
```

默认 NameServer 为 `localhost:9876`。使用外部环境时，必须同时暴露 NameServer 和所有公布的 Broker 地址，并为
外部环境创建普通 topic，再为 group 授予路由查询、POP、确认和不可见时间变更权限。

完整配置和 API 示例请参阅
[Remoting 指南](../../../src/EventHorizon.RocketMQ.Remoting/README.zh-CN.md)。
