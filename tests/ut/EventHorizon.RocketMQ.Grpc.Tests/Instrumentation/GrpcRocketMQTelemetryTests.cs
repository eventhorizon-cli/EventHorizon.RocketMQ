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
using EventHorizon.RocketMQ.Grpc.Instrumentation;
using Microsoft.Extensions.DependencyInjection;
using OpenTelemetry;
using OpenTelemetry.Metrics;
using OpenTelemetry.Trace;
using Xunit;

namespace EventHorizon.RocketMQ.Grpc.Tests.Instrumentation;

public sealed class GrpcRocketMQTelemetryTests
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
    public void AddRocketMQGrpcInstrumentation_TracingAndMetricsBuilders_ConfiguresTracingAndMetricsBuilders()
    {
        var tracingBuilder = Sdk.CreateTracerProviderBuilder();
        var metricsBuilder = Sdk.CreateMeterProviderBuilder();

        Assert.Same(tracingBuilder, tracingBuilder.AddRocketMQGrpcInstrumentation());
        Assert.Same(metricsBuilder, metricsBuilder.AddRocketMQGrpcInstrumentation());

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
            Stopwatch.GetTimestamp());
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
    public void StartReceive_ProducerContexts_LinksContexts()
    {
        using var listener = CreateActivityListener();
        using var provider = CreateServiceProvider();
        var telemetry = CreateTelemetry(provider);
        var traceId = ActivityTraceId.CreateRandom();
        var spanId = ActivitySpanId.CreateRandom();
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
            null,
            DateTimeOffset.UtcNow,
            Stopwatch.GetTimestamp(),
            messageProperties: messageProperties);
        var receiveActivity = Assert.IsType<Activity>(receive.Activity);
        receive.Complete();

        var link = Assert.Single(receiveActivity.Links);
        Assert.Equal(traceId, link.Context.TraceId);
        Assert.Equal(spanId, link.Context.SpanId);
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
                   Stopwatch.GetTimestamp()))
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

    private static GrpcRocketMQTelemetry CreateTelemetry(ServiceProvider provider) =>
        new(provider.GetRequiredService<IMeterFactory>());

    private static ActivityListener CreateActivityListener()
    {
        var listener = new ActivityListener
        {
            ShouldListenTo = static source => source.Name == GrpcRocketMQInstrumentation.ActivitySourceName,
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
                if (instrument.Meter.Name == GrpcRocketMQInstrumentation.MeterName)
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
