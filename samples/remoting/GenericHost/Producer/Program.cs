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
using EventHorizon.RocketMQ.Remoting;
using EventHorizon.RocketMQ.Remoting.Producer;
using EventHorizon.RocketMQ.Samples.Remoting.Producer;
using Microsoft.OpenApi;

// This registration name is also the .NET keyed-service key for the explicit audit route.
const string AuditRegistrationName = "audit";

var builder = WebApplication.CreateBuilder(args);
// NamesrvAddr discovers routes; the producer then sends directly to advertised Broker endpoints.
var remotingSection = builder.Configuration.GetRequiredSection("RocketMQ:Remoting");
var auditRemotingSection = builder.Configuration.GetRequiredSection("RocketMQ:Audit:Remoting");
builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen(options =>
{
    options.SwaggerDoc("v1", new OpenApiInfo
    {
        Title = "RocketMQ Remoting Producer Sample",
        Description = "Sends Apache RocketMQ messages through the classic Remoting protocol.",
        Version = "v1"
    });
});

var rocketMQ = builder.Services.AddRocketMQRemoting(remotingSection.Bind);
rocketMQ.AddRemotingProducer(static options =>
    options.GroupName = "remoting-sample-producer");
builder.Services
    .AddRocketMQRemoting(AuditRegistrationName, auditRemotingSection.Bind)
    .AddRemotingProducer(static options =>
        options.GroupName = "remoting-sample-audit-producer");

var app = builder.Build();

app.UseSwagger();
app.UseSwaggerUI();

app.MapPost("/messages", SendMessageAsync)
    .WithName("SendRemotingMessage")
    .WithSummary("Send a message through the default Remoting producer.")
    .WithDescription(
        "Sends a message with the producer from the default client registration. " +
        "The sample uses its fixed shared topic and sample tag.")
    .Produces<SendMessageResponse>(StatusCodes.Status200OK)
    .ProducesValidationProblem(StatusCodes.Status400BadRequest)
    .ProducesProblem(StatusCodes.Status502BadGateway);

app.MapPost("/clients/audit/messages", SendAuditMessageAsync)
    .WithName("SendAuditRemotingMessage")
    .WithSummary("Send a message through the keyed audit Remoting producer.")
    .WithDescription(
        "Sends a message with the producer registered under registration name 'audit'. " +
        "The sample uses its fixed shared topic and sample tag.")
    .Produces<SendMessageResponse>(StatusCodes.Status200OK)
    .ProducesValidationProblem(StatusCodes.Status400BadRequest)
    .ProducesProblem(StatusCodes.Status502BadGateway);

// RunAsync starts and stops both Producer roles through their SDK hosted services.
// Do not manually call IRemotingProducer.StartAsync or StopAsync from this Host application.
await app.RunAsync();

static async Task<IResult> SendMessageAsync(
    SendMessageRequest? request,
    IRemotingProducer producer,
    ILogger<Program> logger,
    CancellationToken cancellationToken)
{
    var body = request?.Message;
    if (string.IsNullOrWhiteSpace(body))
    {
        return Results.ValidationProblem(
            new Dictionary<string, string[]>
            {
                ["message"] = ["A non-empty message is required."]
            });
    }

    var message = new Message("eventhorizon-test-topic", Encoding.UTF8.GetBytes(body))
    {
        Tag = "sample"
    };
    var messageKey = $"sample-{Guid.NewGuid():N}";
    message.Keys.Add(messageKey);
    message.Properties["sample"] = "remoting-producer";

    try
    {
        var result = await producer.SendAsync(message, cancellationToken).ConfigureAwait(false);
        logger.LogInformation(
            "Sent {MessageId} to {Topic} via {BrokerName}/{QueueId} at offset {QueueOffset} with {Status}",
            result.MessageId,
            message.Topic,
            result.MessageQueue.BrokerName,
            result.MessageQueue.QueueId,
            result.QueueOffset,
            result.Status);

        return Results.Ok(new SendMessageResponse
        {
            MessageId = result.MessageId,
            OffsetMessageId = result.OffsetMessageId,
            Topic = message.Topic,
            BrokerName = result.MessageQueue.BrokerName,
            QueueId = result.MessageQueue.QueueId,
            QueueOffset = result.QueueOffset,
            Status = result.Status.ToString()
        });
    }
    catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
    {
        throw;
    }
    catch (Exception exception)
    {
        logger.LogError(exception, "Unable to send a message to {Topic}", message.Topic);
        return Results.Problem(
            detail: exception.Message,
            statusCode: StatusCodes.Status502BadGateway,
            title: "Unable to send a message to RocketMQ.");
    }
}

static Task<IResult> SendAuditMessageAsync(
    SendMessageRequest? request,
    [FromKeyedServices(AuditRegistrationName)] IRemotingProducer producer,
    ILogger<Program> logger,
    CancellationToken cancellationToken) =>
    SendMessageAsync(request, producer, logger, cancellationToken);
