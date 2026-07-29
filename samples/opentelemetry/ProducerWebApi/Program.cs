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

using System.Text;
using EventHorizon.RocketMQ.Grpc;
using EventHorizon.RocketMQ.Grpc.Producer;
using EventHorizon.RocketMQ.Remoting;
using EventHorizon.RocketMQ.Remoting.Producer;
using EventHorizon.RocketMQ.Samples.OpenTelemetry.ProducerWebApi;
using Microsoft.OpenApi;
using OpenTelemetry.Exporter;
using OpenTelemetry.Metrics;
using OpenTelemetry.Resources;
using OpenTelemetry.Trace;
using GrpcMessage = EventHorizon.RocketMQ.Grpc.Producer.Message;
using RemotingMessage = EventHorizon.RocketMQ.Remoting.Producer.Message;

var builder = WebApplication.CreateBuilder(args);
var grpcClientSection = builder.Configuration.GetRequiredSection("RocketMQ:Grpc:Client");
var grpcProducerSection = builder.Configuration.GetRequiredSection("RocketMQ:Grpc:Producer");
var remotingClientSection = builder.Configuration.GetRequiredSection("RocketMQ:Remoting:Client");
var remotingProducerSection = builder.Configuration.GetRequiredSection("RocketMQ:Remoting:Producer");
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
        .AddAspNetCoreInstrumentation()
        .AddRocketMQGrpcInstrumentation()
        .AddRocketMQRemotingInstrumentation()
        .AddOtlpExporter(options => ConfigureOtlpExporter(options, otlpEndpoint)));
builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen(static options =>
{
    options.SwaggerDoc("v1", new OpenApiInfo
    {
        Title = "RocketMQ OpenTelemetry Producer API",
        Version = "v1",
        Description = "Sends messages through separate gRPC and classic Remoting routes while exporting telemetry."
    });
});

builder.Services
    .AddRocketMQGrpc(grpcClientSection.Bind)
    .AddGrpcProducer(grpcProducerSection.Bind);
builder.Services
    .AddRocketMQRemoting(remotingClientSection.Bind)
    .AddRemotingProducer(remotingProducerSection.Bind);

var app = builder.Build();
app.UseSwagger();
app.UseSwaggerUI();

app.MapGet("/health", static () => Results.Ok())
    .WithName("GetHealth")
    .WithSummary("Reports that the sample web host is running.")
    .Produces(StatusCodes.Status200OK);
app.MapPost("/messages/grpc", SendGrpcMessageAsync)
    .WithName("SendGrpcMessage")
    .WithSummary("Sends a message through the RocketMQ gRPC producer.")
    .WithDescription("Exports the HTTP request and gRPC RocketMQ client telemetry to the configured OTLP endpoint.")
    .Produces<GrpcSendMessageResponse>(StatusCodes.Status200OK)
    .ProducesValidationProblem(StatusCodes.Status400BadRequest)
    .ProducesProblem(StatusCodes.Status503ServiceUnavailable);
app.MapPost("/messages/remoting", SendRemotingMessageAsync)
    .WithName("SendRemotingMessage")
    .WithSummary("Sends a message through the RocketMQ classic Remoting producer.")
    .WithDescription("Exports the HTTP request and Remoting RocketMQ client telemetry to the configured OTLP endpoint.")
    .Produces<RemotingSendMessageResponse>(StatusCodes.Status200OK)
    .ProducesValidationProblem(StatusCodes.Status400BadRequest)
    .ProducesProblem(StatusCodes.Status503ServiceUnavailable);

await app.RunAsync();

static async Task<IResult> SendGrpcMessageAsync(
    SendMessageRequest? request,
    IGrpcProducer producer,
    ILogger<Program> logger,
    CancellationToken cancellationToken)
{
    var messageBody = request?.Message;
    if (string.IsNullOrWhiteSpace(messageBody))
    {
        return Results.ValidationProblem(new Dictionary<string, string[]>
        {
            ["message"] = ["A non-empty message is required."]
        });
    }

    var message = CreateGrpcMessage(messageBody);

    try
    {
        var receipt = await producer.SendAsync(message, cancellationToken).ConfigureAwait(false);
        logger.LogInformation(
            "Sent gRPC message {MessageId} to {Topic} at offset {Offset}.",
            receipt.MessageId,
            message.Topic,
            receipt.Offset);

        return Results.Ok(new GrpcSendMessageResponse(receipt.MessageId, message.Topic, receipt.Offset));
    }
    catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
    {
        throw;
    }
    catch (Exception exception)
    {
        logger.LogError(exception, "Failed to send a gRPC message to {Topic}.", message.Topic);
        return Results.Problem(
            title: "RocketMQ gRPC message send failed.",
            detail: "The message could not be sent to the RocketMQ service.",
            statusCode: StatusCodes.Status503ServiceUnavailable);
    }
}

static async Task<IResult> SendRemotingMessageAsync(
    SendMessageRequest? request,
    IRemotingProducer producer,
    ILogger<Program> logger,
    CancellationToken cancellationToken)
{
    var messageBody = request?.Message;
    if (string.IsNullOrWhiteSpace(messageBody))
    {
        return Results.ValidationProblem(new Dictionary<string, string[]>
        {
            ["message"] = ["A non-empty message is required."]
        });
    }

    var message = CreateRemotingMessage(messageBody);

    try
    {
        var result = await producer.SendAsync(message, cancellationToken).ConfigureAwait(false);
        logger.LogInformation(
            "Sent Remoting message {MessageId} to {Topic} via {BrokerName}/{QueueId} at offset {QueueOffset}.",
            result.MessageId,
            message.Topic,
            result.MessageQueue.BrokerName,
            result.MessageQueue.QueueId,
            result.QueueOffset);

        return Results.Ok(new RemotingSendMessageResponse(
            result.MessageId,
            message.Topic,
            result.MessageQueue.BrokerName,
            result.MessageQueue.QueueId,
            result.QueueOffset,
            result.Status.ToString()));
    }
    catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
    {
        throw;
    }
    catch (Exception exception)
    {
        logger.LogError(exception, "Failed to send a Remoting message to {Topic}.", message.Topic);
        return Results.Problem(
            title: "RocketMQ Remoting message send failed.",
            detail: "The message could not be sent to the RocketMQ service.",
            statusCode: StatusCodes.Status503ServiceUnavailable);
    }
}

static GrpcMessage CreateGrpcMessage(string body)
{
    var message = new GrpcMessage("eventhorizon-test-topic", Encoding.UTF8.GetBytes(body))
    {
        Tag = "observability"
    };
    message.Keys.Add($"otel-grpc-{Guid.NewGuid():N}");
    return message;
}

static RemotingMessage CreateRemotingMessage(string body)
{
    var message = new RemotingMessage("eventhorizon-test-topic", Encoding.UTF8.GetBytes(body))
    {
        Tag = "observability"
    };
    message.Keys.Add($"otel-remoting-{Guid.NewGuid():N}");
    return message;
}

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
