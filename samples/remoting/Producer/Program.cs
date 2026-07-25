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
using EventHorizon.RocketMQ.Producer;
using EventHorizon.RocketMQ.Remoting;
using EventHorizon.RocketMQ.Remoting.Producer;
using EventHorizon.RocketMQ.Samples.Remoting.Producer;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Microsoft.OpenApi;

// The audit route resolves this profile explicitly; it does not duplicate default sends.
const string AuditProfile = "audit";

var builder = WebApplication.CreateBuilder(args);
var remotingSection = builder.Configuration.GetRequiredSection("RocketMQ:Remoting");
var producerSection = builder.Configuration.GetRequiredSection("RocketMQ:Producer");
var auditRemotingSection = builder.Configuration.GetRequiredSection("RocketMQ:Audit:Remoting");
var auditProducerSection = builder.Configuration.GetRequiredSection("RocketMQ:Audit:Producer");
var sampleSection = builder.Configuration.GetRequiredSection("Sample");

builder.Services.AddOptions<ProducerSampleOptions>()
    .Bind(sampleSection)
    .Validate(static options => !string.IsNullOrWhiteSpace(options.Topic), "A message topic is required.")
    .Validate(static options => !string.IsNullOrWhiteSpace(options.Message), "A message body is required.")
    .ValidateOnStart();
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
rocketMQ.AddRemotingProducer(producerSection.Bind);
builder.Services
    .AddRocketMQRemoting(AuditProfile, auditRemotingSection.Bind)
    .AddRemotingProducer(auditProducerSection.Bind);

var app = builder.Build();

app.UseSwagger();
app.UseSwaggerUI();

app.MapPost("/messages", SendMessageAsync)
    .WithName("SendRemotingMessage")
    .WithSummary("Send a message through the default Remoting producer.")
    .WithDescription("Sends a message with the default producer profile. Request values override the Sample defaults.")
    .Produces<SendMessageResponse>(StatusCodes.Status200OK)
    .ProducesProblem(StatusCodes.Status502BadGateway);

app.MapPost("/profiles/audit/messages", SendAuditMessageAsync)
    .WithName("SendAuditRemotingMessage")
    .WithSummary("Send a message through the keyed audit Remoting producer.")
    .WithDescription("Sends a message with the keyed audit producer profile. Request values override the Sample defaults.")
    .Produces<SendMessageResponse>(StatusCodes.Status200OK)
    .ProducesProblem(StatusCodes.Status502BadGateway);

await app.RunAsync();

static async Task<IResult> SendMessageAsync(
    SendMessageRequest? request,
    IRemotingProducer producer,
    IOptions<ProducerSampleOptions> sampleOptions,
    ILoggerFactory loggerFactory,
    CancellationToken cancellationToken)
{
    request ??= new SendMessageRequest();
    var options = sampleOptions.Value;
    var topic = string.IsNullOrWhiteSpace(request.Topic) ? options.Topic : request.Topic;
    var tag = request.Tag is null
        ? string.IsNullOrWhiteSpace(options.Tag) ? null : options.Tag
        : string.IsNullOrWhiteSpace(request.Tag) ? null : request.Tag;
    var body = request.Body ?? options.Message;
    var messageKey = $"sample-{Guid.NewGuid():N}";
    var message = new Message(topic, Encoding.UTF8.GetBytes(body))
    {
        Tag = tag
    };
    message.Keys.Add(messageKey);
    message.Properties["sample"] = "remoting-producer";

    var logger = loggerFactory.CreateLogger("EventHorizon.RocketMQ.Samples.Remoting.Producer");

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
    [FromKeyedServices(AuditProfile)] IRemotingProducer producer,
    IOptions<ProducerSampleOptions> sampleOptions,
    ILoggerFactory loggerFactory,
    CancellationToken cancellationToken) =>
    SendMessageAsync(request, producer, sampleOptions, loggerFactory, cancellationToken);
