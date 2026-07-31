# OpenTelemetry Samples with Generic Host

[OpenTelemetry overview](../README.md) | [All samples](../../README.md) | [Simplified Chinese](README.zh-CN.md)

Both ASP.NET Core applications run in a Generic Host. Their registered RocketMQ producer and consumer roles are
started and stopped by the host; endpoint handlers and application code do not call role lifecycle methods directly.

| Sample | Workflow |
| --- | --- |
| [Producer Web API](ProducerWebApi/README.md) | Sends through explicit gRPC and Remoting routes while exporting traces and metrics. |
| [Consumer Web API](ConsumerWebApi/README.md) | Hosts independent gRPC and Remoting Push consumers and exports their delivery signals. |

Start the Consumer first so both Push roles are long-polling before the Producer publishes a request. The Consumer
groups are independent subscriptions to the same topic, so a message sent through either Producer route can be
processed by both groups. See the [OpenTelemetry overview](../README.md) for the local environments, requests, and
Grafana dashboards.
