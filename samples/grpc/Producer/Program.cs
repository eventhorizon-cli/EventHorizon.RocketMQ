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
using EventHorizon.RocketMQ.Producer;
using EventHorizon.RocketMQ.Samples.Grpc.Producer;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Microsoft.OpenApi;

// The profile key selects an independent DI client; it does not enable a Broker audit feature.
const string AuditProfile = "audit";

var builder = WebApplication.CreateBuilder(args);
builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen(static options =>
{
    options.SwaggerDoc("v1", new OpenApiInfo
    {
        Title = "RocketMQ gRPC Producer Sample API",
        Version = "v1",
        Description = "Sends messages with the default and audit RocketMQ gRPC producer profiles."
    });
});

var clientSection = builder.Configuration.GetRequiredSection("RocketMQ:Client");
var producerSection = builder.Configuration.GetRequiredSection("RocketMQ:Producer");
var auditClientSection = builder.Configuration.GetRequiredSection("RocketMQ:Audit:Client");
var auditProducerSection = builder.Configuration.GetRequiredSection("RocketMQ:Audit:Producer");
var sampleSection = builder.Configuration.GetRequiredSection("Sample");

builder.Services
    .AddOptions<ProducerSampleOptions>()
    .Bind(sampleSection)
    .Validate(static options => !string.IsNullOrWhiteSpace(options.Topic), "RocketMQ topic is required.")
    .Validate(static options => !string.IsNullOrWhiteSpace(options.Message), "Message body is required.")
    .ValidateOnStart();

builder.Services
    .AddRocketMQGrpc(clientSection.Bind)
    .AddGrpcProducer(producerSection.Bind);
builder.Services
    .AddRocketMQGrpc(AuditProfile, auditClientSection.Bind)
    .AddGrpcProducer(auditProducerSection.Bind);

var app = builder.Build();
app.UseSwagger();
app.UseSwaggerUI();

app.MapPost("/messages", SendMessageAsync)
    .WithName("SendMessage")
    .WithSummary("Sends a message with the default gRPC producer.")
    .WithDescription(
        "Uses the default RocketMQ gRPC producer profile configured in " +
        "RocketMQ:Client and RocketMQ:Producer.")
    .Produces<SendMessageResponse>(StatusCodes.Status200OK)
    .ProducesProblem(StatusCodes.Status503ServiceUnavailable);
app.MapPost("/profiles/audit/messages", SendAuditMessageAsync)
    .WithName("SendAuditMessage")
    .WithSummary("Sends a message with the audit gRPC producer.")
    .WithDescription("Uses the audit keyed RocketMQ gRPC producer profile configured in RocketMQ:Audit.")
    .Produces<SendMessageResponse>(StatusCodes.Status200OK)
    .ProducesProblem(StatusCodes.Status503ServiceUnavailable);
await app.RunAsync();

static async Task<IResult> SendMessageAsync(
    SendMessageRequest? request,
    IGrpcProducer producer,
    IOptions<ProducerSampleOptions> sampleOptions,
    ILoggerFactory loggerFactory,
    CancellationToken cancellationToken)
{
    request ??= new SendMessageRequest();
    var settings = sampleOptions.Value;
    var topic = string.IsNullOrWhiteSpace(request.Topic) ? settings.Topic : request.Topic;
    var body = request.Body ?? settings.Message;
    var tag = request.Tag is null
        ? string.IsNullOrWhiteSpace(settings.Tag) ? null : settings.Tag
        : string.IsNullOrWhiteSpace(request.Tag) ? null : request.Tag;
    var message = new Message(topic, Encoding.UTF8.GetBytes(body))
    {
        Tag = tag
    };
    message.Keys.Add(Guid.NewGuid().ToString("N"));

    var logger = loggerFactory.CreateLogger("EventHorizon.RocketMQ.Samples.Grpc.Producer");
    try
    {
        var receipt = await producer.SendAsync(message, cancellationToken).ConfigureAwait(false);
        logger.LogInformation(
            "Sent message {MessageId} to {Topic} at offset {Offset}.",
            receipt.MessageId,
            message.Topic,
            receipt.Offset);

        return Results.Ok(new SendMessageResponse(receipt.MessageId, message.Topic, receipt.Offset));
    }
    catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
    {
        throw;
    }
    catch (Exception exception)
    {
        logger.LogError(exception, "Failed to send a message to {Topic}.", message.Topic);
        return Results.Problem(
            title: "RocketMQ message send failed.",
            detail: "The message could not be sent to the RocketMQ service.",
            statusCode: StatusCodes.Status503ServiceUnavailable);
    }
}

static Task<IResult> SendAuditMessageAsync(
    SendMessageRequest? request,
    [FromKeyedServices(AuditProfile)] IGrpcProducer producer,
    IOptions<ProducerSampleOptions> sampleOptions,
    ILoggerFactory loggerFactory,
    CancellationToken cancellationToken) =>
    SendMessageAsync(request, producer, sampleOptions, loggerFactory, cancellationToken);
