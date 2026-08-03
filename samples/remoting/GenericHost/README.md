# Remoting Samples with Generic Host

[English](README.md) | [Simplified Chinese](README.zh-CN.md)

These projects run in a .NET Generic Host or `WebApplication`. For lifecycle-managed roles, the SDK hosted service
registered by `AddRocketMQRemoting` starts and stops with the host; application code must not invoke the same
`StartAsync` or `StopAsync` methods itself. The Admin sample has no long-lived role lifecycle, but is grouped here
because it is also a hosted Web application.

| Sample | Public role | Workflow |
| --- | --- | --- |
| [Producer](Producer/README.md) | `IRemotingProducer` | Exposes classic Remoting sends through a small HTTP API. |
| [Admin](Admin/README.md) | `IRemotingAdmin` | Reads physical queues, offsets, and stored messages through an HTTP API. |
| [LitePullConsumer](LitePullConsumer/README.md) | `IRemotingLitePullConsumer` | Lets application code poll and commit SDK-managed or manually assigned queues. |
| [PushConsumer](PushConsumer/README.md) | `IRemotingPushConsumer` | Lets the SDK coordinate client or Broker assignment, reception, dispatch, and settlement. |

For lifecycle-managed roles in a process that only builds a plain `ServiceProvider`, use the sibling
[non-Host samples](../NonHost/README.md).
