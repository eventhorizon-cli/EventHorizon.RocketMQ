// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"). You may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

using System.Diagnostics;
using System.Diagnostics.Metrics;
using EventHorizon.RocketMQ.Remoting.Consumer;
using EventHorizon.RocketMQ.Remoting.Instrumentation;
using Microsoft.Extensions.DependencyInjection;
using OpenTelemetry;
using OpenTelemetry.Metrics;
using OpenTelemetry.Trace;
using Xunit;

namespace EventHorizon.RocketMQ.Remoting.Tests.Instrumentation;

public sealed class RemotingRocketMQTelemetryTests
{
    [Fact]
    public void StartSend_WithoutSubscribers_DoesNotCreateOrInjectContext()
    {
        using var provider = CreateServiceProvider();
        var telemetry = CreateTelemetry(provider);

        using var operation = telemetry.StartSend("orders", 1, 24);
        var properties = new Dictionary<string, string>();

        telemetry.InjectContext(operation.Activity, properties);
        operation.Complete();

        Assert.Null(operation.Activity);
        Assert.Empty(properties);
    }

    [Fact]
    public void StartSend_ExistingContext_InjectsAndPreserves()
    {
        using var listener = CreateActivityListener();
        using var provider = CreateServiceProvider();
        var telemetry = CreateTelemetry(provider);

        using var operation = telemetry.StartSend("orders", 1, 24);
        var activity = Assert.IsType<Activity>(operation.Activity);
        var properties = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        telemetry.InjectContext(activity, properties);

        var propagationField = DistributedContextPropagator.Current.Fields.First();
        var existingProperties = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
        {
            [propagationField] = "already-injected"
        };
        telemetry.InjectContext(activity, existingProperties);
        operation.Complete();

        Assert.Equal("send orders", activity.DisplayName);
        Assert.Equal("rocketmq", activity.GetTagItem("messaging.system"));
        Assert.Contains(
            DistributedContextPropagator.Current.Fields,
            field => properties.ContainsKey(field));
        Assert.Equal("already-injected", existingProperties[propagationField]);
    }

    [Fact]
    public void AddRocketMQRemotingInstrumentation_TracingAndMetricsBuilders_Configures()
    {
        var tracingBuilder = Sdk.CreateTracerProviderBuilder();
        var metricsBuilder = Sdk.CreateMeterProviderBuilder();

        Assert.Same(tracingBuilder, tracingBuilder.AddRocketMQRemotingInstrumentation());
        Assert.Same(metricsBuilder, metricsBuilder.AddRocketMQRemotingInstrumentation());

        using var tracingProvider = tracingBuilder.Build();
        using var metricsProvider = metricsBuilder.Build();
        using var provider = CreateServiceProvider();
        var telemetry = CreateTelemetry(provider);
        using var operation = telemetry.StartSend("orders", 1, 24);

        Assert.IsType<Activity>(operation.Activity);
        operation.Complete();
    }

    [Fact]
    public void StartProcess_ReceiveAndProducerContexts_UsesParentAndLink()
    {
        using var listener = CreateActivityListener();
        using var provider = CreateServiceProvider();
        var telemetry = CreateTelemetry(provider);
        var traceId = ActivityTraceId.CreateRandom();
        var spanId = ActivitySpanId.CreateRandom();
        var parentContext = new ActivityContext(
            ActivityTraceId.CreateRandom(),
            ActivitySpanId.CreateRandom(),
            ActivityTraceFlags.Recorded);
        var messageProperties = new IReadOnlyDictionary<string, string>[]
        {
            new Dictionary<string, string>
            {
                ["traceparent"] = $"00-{traceId}-{spanId}-01"
            },
            new Dictionary<string, string>
            {
                ["traceparent"] = "invalid"
            }
        };

        using var receive = telemetry.StartReceive(
            "orders",
            "orders-consumer",
            2,
            parentContext,
            DateTimeOffset.UtcNow,
            Stopwatch.GetTimestamp(),
            3);
        var receiveActivity = Assert.IsType<Activity>(receive.Activity);
        receive.Complete();

        using var process = telemetry.StartProcess(
            "orders",
            "orders-consumer",
            "message-1",
            3,
            24,
            messageProperties[0],
            receiveActivity.Context);
        var processActivity = Assert.IsType<Activity>(process.Activity);
        process.Complete(false, "retry");

        Assert.Empty(receiveActivity.Links);
        var link = Assert.Single(processActivity.Links);
        Assert.Equal(traceId, link.Context.TraceId);
        Assert.Equal(receiveActivity.TraceId, processActivity.TraceId);
        Assert.Equal(receiveActivity.SpanId, processActivity.ParentSpanId);
        Assert.Equal(ActivityStatusCode.Error, processActivity.Status);
        Assert.Equal("retry", processActivity.GetTagItem("messaging.rocketmq.consume.result"));
    }

    [Fact]
    public void StartReceive_EmptyResult_DoesNotCreateActivity()
    {
        using var listener = CreateActivityListener();
        using var provider = CreateServiceProvider();
        var telemetry = CreateTelemetry(provider);

        using var operation = telemetry.StartReceive(
            "orders",
            "orders-consumer",
            0,
            null,
            DateTimeOffset.UtcNow,
            Stopwatch.GetTimestamp(),
            createActivity: false);
        operation.Complete();

        Assert.Null(operation.Activity);
    }

    [Fact]
    public void StartReceive_ProducerContexts_LinksProducerContexts()
    {
        using var listener = CreateActivityListener();
        using var provider = CreateServiceProvider();
        var telemetry = CreateTelemetry(provider);
        var firstTraceId = ActivityTraceId.CreateRandom();
        var firstSpanId = ActivitySpanId.CreateRandom();
        var secondTraceId = ActivityTraceId.CreateRandom();
        var secondSpanId = ActivitySpanId.CreateRandom();
        var messageProperties = new IReadOnlyDictionary<string, string>[]
        {
            new Dictionary<string, string>
            {
                ["traceparent"] = $"00-{firstTraceId}-{firstSpanId}-01"
            },
            new Dictionary<string, string>
            {
                ["traceparent"] = "invalid"
            },
            new Dictionary<string, string>
            {
                ["traceparent"] = $"00-{secondTraceId}-{secondSpanId}-01"
            }
        };

        using var receive = telemetry.StartReceive(
            "orders",
            "orders-consumer",
            3,
            null,
            DateTimeOffset.UtcNow,
            Stopwatch.GetTimestamp(),
            3,
            messageProperties: messageProperties);
        var receiveActivity = Assert.IsType<Activity>(receive.Activity);
        receive.Complete();

        var links = receiveActivity.Links.ToArray();
        Assert.Equal(2, links.Length);
        Assert.Equal(firstTraceId, links[0].Context.TraceId);
        Assert.Equal(firstSpanId, links[0].Context.SpanId);
        Assert.Equal(secondTraceId, links[1].Context.TraceId);
        Assert.Equal(secondSpanId, links[1].Context.SpanId);
    }

    [Fact]
    public void StartProcess_ProducerWithoutReceiveContext_UsesProducerAsParent()
    {
        using var listener = CreateActivityListener();
        using var provider = CreateServiceProvider();
        var telemetry = CreateTelemetry(provider);
        var traceId = ActivityTraceId.CreateRandom();
        var spanId = ActivitySpanId.CreateRandom();
        var properties = new Dictionary<string, string>
        {
            ["traceparent"] = $"00-{traceId}-{spanId}-01"
        };

        using var process = telemetry.StartProcess(
            "orders",
            "orders-consumer",
            "message-1",
            3,
            24,
            properties,
            null);
        var processActivity = Assert.IsType<Activity>(process.Activity);
        process.Complete();

        Assert.Equal(traceId, processActivity.TraceId);
        Assert.Equal(spanId, processActivity.ParentSpanId);
        Assert.Empty(processActivity.Links);
    }

    [Fact]
    public void StartProcessBatch_ReceiveContextAndEveryProducerContext_UsesReceiveContextAndLinksProducers()
    {
        using var listener = CreateActivityListener();
        using var provider = CreateServiceProvider();
        var telemetry = CreateTelemetry(provider);
        var receiveContext = new ActivityContext(
            ActivityTraceId.CreateRandom(),
            ActivitySpanId.CreateRandom(),
            ActivityTraceFlags.Recorded);
        var firstTraceId = ActivityTraceId.CreateRandom();
        var firstSpanId = ActivitySpanId.CreateRandom();
        var secondTraceId = ActivityTraceId.CreateRandom();
        var secondSpanId = ActivitySpanId.CreateRandom();
        var messages = new[]
        {
            CreateMessage(
                "message-1",
                [1, 2],
                new Dictionary<string, string>
                {
                    ["traceparent"] = $"00-{firstTraceId}-{firstSpanId}-01"
                },
                receiveContext),
            CreateMessage(
                "message-2",
                [3, 4, 5],
                new Dictionary<string, string>
                {
                    ["traceparent"] = $"00-{secondTraceId}-{secondSpanId}-01"
                },
                receiveContext)
        };

        using var process = telemetry.StartProcessBatch("orders", "orders-consumer", messages);
        var processActivity = Assert.IsType<Activity>(process.Activity);
        process.Complete();

        var links = processActivity.Links.ToArray();
        Assert.Equal(receiveContext.TraceId, processActivity.TraceId);
        Assert.Equal(receiveContext.SpanId, processActivity.ParentSpanId);
        Assert.Equal(2, processActivity.GetTagItem("messaging.batch.message_count"));
        Assert.Equal(5L, processActivity.GetTagItem("messaging.message.body.size"));
        Assert.Equal(2, links.Length);
        Assert.Equal(firstTraceId, links[0].Context.TraceId);
        Assert.Equal(secondTraceId, links[1].Context.TraceId);
    }

    [Fact]
    public void StartProcessBatch_ProducerWithoutReceiveContext_UsesProducerAsParent()
    {
        using var listener = CreateActivityListener();
        using var provider = CreateServiceProvider();
        var telemetry = CreateTelemetry(provider);
        var firstTraceId = ActivityTraceId.CreateRandom();
        var firstSpanId = ActivitySpanId.CreateRandom();
        var secondTraceId = ActivityTraceId.CreateRandom();
        var secondSpanId = ActivitySpanId.CreateRandom();
        var messages = new[]
        {
            CreateMessage(
                "message-1",
                [1, 2],
                new Dictionary<string, string>
                {
                    ["traceparent"] = $"00-{firstTraceId}-{firstSpanId}-01"
                },
                default),
            CreateMessage(
                "message-2",
                [3, 4],
                new Dictionary<string, string>
                {
                    ["traceparent"] = $"00-{secondTraceId}-{secondSpanId}-01"
                },
                default)
        };

        using var process = telemetry.StartProcessBatch("orders", "orders-consumer", messages);
        var processActivity = Assert.IsType<Activity>(process.Activity);
        process.Complete();

        var link = Assert.Single(processActivity.Links);
        Assert.Equal(firstTraceId, processActivity.TraceId);
        Assert.Equal(firstSpanId, processActivity.ParentSpanId);
        Assert.Equal(secondTraceId, link.Context.TraceId);
        Assert.Equal(secondSpanId, link.Context.SpanId);
    }

    [Fact]
    public void StartProcessBatch_MessageIdForSingleMessage_PreservesSingleMessageId()
    {
        using var listener = CreateActivityListener();
        using var provider = CreateServiceProvider();
        var telemetry = CreateTelemetry(provider);
        var messages = new[]
        {
            CreateMessage(
                "message-1",
                [1, 2],
                new Dictionary<string, string>(),
                default)
        };

        using var process = telemetry.StartProcessBatch("orders", "orders-consumer", messages);
        var processActivity = Assert.IsType<Activity>(process.Activity);
        process.Complete();

        Assert.Equal("message-1", processActivity.GetTagItem("messaging.message.id"));
        Assert.Equal(1, processActivity.GetTagItem("messaging.batch.message_count"));
    }

    [Fact]
    public void Complete_FailedOperationMetrics_RecordsFailureMetrics()
    {
        var measurements = new List<(string InstrumentName, long Value, KeyValuePair<string, object?>[] Tags)>();
        var durations = new List<(string InstrumentName, double Value, KeyValuePair<string, object?>[] Tags)>();
        using var listener = CreateMeterListener(measurements, durations);
        using var provider = CreateServiceProvider();
        var telemetry = CreateTelemetry(provider);

        using (var operation = telemetry.StartReceive(
                   "orders",
                   "orders-consumer",
                   2,
                   null,
                   DateTimeOffset.UtcNow,
                   Stopwatch.GetTimestamp(),
                   3))
        {
            operation.Complete(new InvalidOperationException("broker rejected the request"));
        }

        var operationMeasurement = Assert.Single(
            measurements,
            static measurement => measurement.InstrumentName == "rocketmq.client.operations");
        var messageMeasurement = Assert.Single(
            measurements,
            static measurement => measurement.InstrumentName == "rocketmq.client.messages");
        var durationMeasurement = Assert.Single(
            durations,
            static measurement => measurement.InstrumentName == "rocketmq.client.operation.duration");

        Assert.Equal(1, operationMeasurement.Value);
        Assert.Equal(2, messageMeasurement.Value);
        Assert.True(durationMeasurement.Value >= 0);
        Assert.Contains(operationMeasurement.Tags, static tag => tag.Key == "success" && Equals(tag.Value, false));
        Assert.Contains(
            operationMeasurement.Tags,
            static tag => tag.Key == "error.type" &&
                          tag.Value is string errorType &&
                          errorType == typeof(InvalidOperationException).FullName);
    }

    [Fact]
    public void Dispose_AbandonedOperation_RecordsOnce()
    {
        var measurements = new List<(string InstrumentName, long Value, KeyValuePair<string, object?>[] Tags)>();
        var durations = new List<(string InstrumentName, double Value, KeyValuePair<string, object?>[] Tags)>();
        using var listener = CreateMeterListener(measurements, durations);
        using var provider = CreateServiceProvider();
        var telemetry = CreateTelemetry(provider);
        var operation = telemetry.StartSend("orders", 1, 24);

        operation.Dispose();
        operation.Dispose();

        var operationMeasurement = Assert.Single(
            measurements,
            static measurement => measurement.InstrumentName == "rocketmq.client.operations");
        Assert.Contains(operationMeasurement.Tags, static tag => tag.Key == "success" && Equals(tag.Value, false));
        Assert.Contains(
            operationMeasurement.Tags,
            static tag => tag.Key == "error.type" &&
                          tag.Value is string errorType &&
                          errorType == "abandoned");
    }

    private static ServiceProvider CreateServiceProvider()
    {
        var services = new ServiceCollection();
        services.AddMetrics();
        return services.BuildServiceProvider();
    }

    private static RemotingRocketMQTelemetry CreateTelemetry(ServiceProvider provider) =>
        new(provider.GetRequiredService<IMeterFactory>());

    private static RemotingMessageView CreateMessage(
        string messageId,
        byte[] body,
        IReadOnlyDictionary<string, string> properties,
        ActivityContext receiveActivityContext)
    {
        var message = new RemotingMessageView(
            "orders",
            body,
            messageId,
            $"offset-{messageId}",
            null,
            [],
            properties,
            1,
            null,
            3,
            "broker-a",
            0,
            0,
            DateTimeOffset.UnixEpoch,
            DateTimeOffset.UnixEpoch)
        {
            ReceiveActivityContext = receiveActivityContext
        };
        return message;
    }

    private static ActivityListener CreateActivityListener()
    {
        var listener = new ActivityListener
        {
            ShouldListenTo = static source => source.Name == RemotingRocketMQInstrumentation.ActivitySourceName,
            Sample = static (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllDataAndRecorded
        };
        ActivitySource.AddActivityListener(listener);
        return listener;
    }

    private static MeterListener CreateMeterListener(
        List<(string InstrumentName, long Value, KeyValuePair<string, object?>[] Tags)> measurements,
        List<(string InstrumentName, double Value, KeyValuePair<string, object?>[] Tags)> durations)
    {
        var listener = new MeterListener
        {
            InstrumentPublished = (instrument, meterListener) =>
            {
                if (instrument.Meter.Name == RemotingRocketMQInstrumentation.MeterName)
                {
                    meterListener.EnableMeasurementEvents(instrument);
                }
            }
        };
        listener.SetMeasurementEventCallback<long>((instrument, value, tags, _) =>
            measurements.Add((instrument.Name, value, tags.ToArray())));
        listener.SetMeasurementEventCallback<double>((instrument, value, tags, _) =>
            durations.Add((instrument.Name, value, tags.ToArray())));
        listener.Start();
        return listener;
    }
}
