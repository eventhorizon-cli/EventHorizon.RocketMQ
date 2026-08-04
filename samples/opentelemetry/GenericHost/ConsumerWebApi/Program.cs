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

using EventHorizon.RocketMQ.Grpc;
using EventHorizon.RocketMQ.Grpc.Consumer.Push;
using EventHorizon.RocketMQ.Remoting;
using EventHorizon.RocketMQ.Remoting.Consumer.Push;
using EventHorizon.RocketMQ.Samples.OpenTelemetry.ConsumerWebApi;
using Microsoft.OpenApi;
using OpenTelemetry.Exporter;
using OpenTelemetry.Metrics;
using OpenTelemetry.Resources;
using OpenTelemetry.Trace;

const string topic = "eventhorizon-test-topic";

var builder = WebApplication.CreateBuilder(args);
var grpcClientSection = builder.Configuration.GetRequiredSection("RocketMQ:Grpc:Client");
var remotingClientSection = builder.Configuration.GetRequiredSection("RocketMQ:Remoting:Client");
var openTelemetrySection = builder.Configuration.GetRequiredSection("OpenTelemetry");
var serviceName = openTelemetrySection.GetValue<string>("ServiceName");
ArgumentException.ThrowIfNullOrWhiteSpace(serviceName);
var otlpEndpoint = GetOtlpEndpoint(openTelemetrySection.GetValue<string>("OtlpEndpoint"));

builder.Services.AddOpenTelemetry()
    .ConfigureResource(resource => resource.AddService(serviceName))
    .WithTracing(tracing => tracing
        .AddAspNetCoreInstrumentation()
        .AddRocketMQGrpcInstrumentation()
        .AddRocketMQRemotingInstrumentation()
        .AddOtlpExporter(options => ConfigureOtlpExporter(options, otlpEndpoint)))
    .WithMetrics(metrics => metrics
        .AddView(
            "rocketmq.client.operation.duration",
            new ExplicitBucketHistogramConfiguration
            {
                Boundaries = [0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 0.75, 1, 1.5, 2, 5, 10, 30]
            })
        .AddAspNetCoreInstrumentation()
        .AddRocketMQGrpcInstrumentation()
        .AddRocketMQRemotingInstrumentation()
        .AddOtlpExporter(options => ConfigureOtlpExporter(options, otlpEndpoint)));
builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen(static options =>
{
    options.SwaggerDoc("v1", new OpenApiInfo
    {
        Title = "RocketMQ OpenTelemetry Consumer API",
        Version = "v1",
        Description = "Runs separate gRPC and classic Remoting push consumers while exporting telemetry."
    });
});

builder.Services
    .AddRocketMQGrpc(grpcClientSection.Bind)
    .AddGrpcPushConsumer<GrpcConsumerMessageHandler>(ServiceLifetime.Scoped, options =>
    {
        options.GroupName = "eventhorizon-otel-grpc-consumer";
        options.BatchSize = 16;
        options.MaxConcurrency = 4;
        options.MaxDeliveryAttempts = 16;
        options.InvisibleDuration = TimeSpan.FromSeconds(30);
        options.ConsumeTimeout = TimeSpan.FromMinutes(15);
        options.LongPollingTimeout = TimeSpan.FromSeconds(15);
        options.Subscribe(topic);
    });
builder.Services
    .AddRocketMQRemoting(remotingClientSection.Bind)
    .AddRemotingPushConsumer<RemotingConsumerMessageHandler>(ServiceLifetime.Scoped, options =>
    {
        options.GroupName = "eventhorizon-otel-remoting-consumer";
        options.PullBatchSize = 16;
        options.ConsumeMessageBatchSize = 4;
        options.MaxConcurrency = 4;
        options.ConsumeTimeout = TimeSpan.FromMinutes(15);
        options.LongPollingTimeout = TimeSpan.FromSeconds(5);
        options.RetryDelay = TimeSpan.FromSeconds(1);
        options.Subscribe(topic);
    });

var app = builder.Build();
app.UseSwagger();
app.UseSwaggerUI();

app.MapGet("/health", static () => Results.Ok())
    .WithName("GetHealth")
    .WithSummary("Reports that the consumer sample host is running.")
    .Produces(StatusCodes.Status200OK);
app.MapGet("/consumers", GetConsumers)
    .WithName("GetConsumers")
    .WithSummary("Reports the two protocol-specific push consumers configured by this host.")
    .WithDescription("Both consumers are hosted services and begin long-polling during application startup.")
    .Produces<ConsumerStatusResponse>(StatusCodes.Status200OK);

// RunAsync starts and stops both SDK consumer roles through their registered hosted services.
// Do not manually call their StartAsync or StopAsync methods from this Host application.
await app.RunAsync();

static IResult GetConsumers() => Results.Ok(
    new ConsumerStatusResponse(
        topic,
        nameof(IGrpcPushConsumer),
        nameof(IRemotingPushConsumer)));

static Uri GetOtlpEndpoint(string? value)
{
    if (!Uri.TryCreate(value, UriKind.Absolute, out var endpoint) ||
        (endpoint.Scheme != Uri.UriSchemeHttp && endpoint.Scheme != Uri.UriSchemeHttps) ||
        string.IsNullOrWhiteSpace(endpoint.Host))
    {
        throw new InvalidOperationException(
            "OpenTelemetry:OtlpEndpoint must be an absolute HTTP or HTTPS URI.");
    }

    return endpoint;
}

static void ConfigureOtlpExporter(OtlpExporterOptions options, Uri endpoint)
{
    options.Protocol = OtlpExportProtocol.Grpc;
    options.Endpoint = endpoint;
}
