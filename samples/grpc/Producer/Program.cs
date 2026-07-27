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
using EventHorizon.RocketMQ.Samples.Grpc.Producer;
using Microsoft.OpenApi;

// A registration name identifies an independent client registration and doubles as its keyed-service key;
// "audit" is application composition metadata, not a Broker feature.
const string AuditRegistrationName = "audit";

var builder = WebApplication.CreateBuilder(args);
builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen(static options =>
{
    options.SwaggerDoc("v1", new OpenApiInfo
    {
        Title = "RocketMQ gRPC Producer Sample API",
        Version = "v1",
        Description = "Sends messages with the default and keyed audit RocketMQ gRPC producers."
    });
});

var clientSection = builder.Configuration.GetRequiredSection("RocketMQ:Client");
var producerSection = builder.Configuration.GetRequiredSection("RocketMQ:Producer");
var auditClientSection = builder.Configuration.GetRequiredSection("RocketMQ:Audit:Client");
var auditProducerSection = builder.Configuration.GetRequiredSection("RocketMQ:Audit:Producer");

// Endpoints must target RocketMQ Proxies; gRPC clients do not connect directly to Brokers.
// The first call creates the default unkeyed registration; the second creates an independent keyed registration.
builder.Services
    .AddRocketMQGrpc(clientSection.Bind)
    .AddGrpcProducer(producerSection.Bind);
builder.Services
    .AddRocketMQGrpc(AuditRegistrationName, auditClientSection.Bind)
    .AddGrpcProducer(auditProducerSection.Bind);

var app = builder.Build();
app.UseSwagger();
app.UseSwaggerUI();

app.MapPost("/messages", SendMessageAsync)
    .WithName("SendMessage")
    .WithSummary("Sends a message with the default gRPC producer.")
    .WithDescription(
        "Uses the default RocketMQ gRPC producer configured in " +
        "RocketMQ:Client and RocketMQ:Producer.")
    .Produces<SendMessageResponse>(StatusCodes.Status200OK)
    .ProducesValidationProblem(StatusCodes.Status400BadRequest)
    .ProducesProblem(StatusCodes.Status503ServiceUnavailable);
app.MapPost("/clients/audit/messages", SendAuditMessageAsync)
    .WithName("SendAuditMessage")
    .WithSummary("Sends a message with the audit gRPC producer.")
    .WithDescription(
        "Uses the RocketMQ gRPC producer registered with registration name 'audit' and configured in RocketMQ:Audit.")
    .Produces<SendMessageResponse>(StatusCodes.Status200OK)
    .ProducesValidationProblem(StatusCodes.Status400BadRequest)
    .ProducesProblem(StatusCodes.Status503ServiceUnavailable);
await app.RunAsync();

static async Task<IResult> SendMessageAsync(
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
            ["message"] = ["A message is required."]
        });
    }

    var message = new Message(
        "eventhorizon-test-topic",
        Encoding.UTF8.GetBytes(messageBody))
    {
        Tag = "sample"
    };
    message.Keys.Add(Guid.NewGuid().ToString("N"));

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
    [FromKeyedServices(AuditRegistrationName)] IGrpcProducer producer,
    ILogger<Program> logger,
    CancellationToken cancellationToken) =>
    SendMessageAsync(request, producer, logger, cancellationToken);
