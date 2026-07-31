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
using EventHorizon.RocketMQ.Samples.Grpc.LiteProducer;
using Microsoft.OpenApi;

var builder = WebApplication.CreateBuilder(args);
builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen(static options =>
{
    options.SwaggerDoc("v1", new OpenApiInfo
    {
        Title = "RocketMQ gRPC Lite Producer Sample API",
        Version = "v1",
        Description = "Sends Lite messages through a LITE parent topic and logical LiteTopic."
    });
});

var clientSection = builder.Configuration.GetRequiredSection("RocketMQ:Client");

// The endpoint must target a RocketMQ Proxy; this transport does not connect directly to Brokers.
builder.Services
    .AddRocketMQGrpc(clientSection.Bind)
    .AddGrpcProducer(static options =>
        options.SendMsgTimeout = TimeSpan.FromSeconds(3));

var app = builder.Build();
app.UseSwagger();
app.UseSwaggerUI();

app.MapPost("/messages", SendLiteMessageAsync)
    .WithName("SendLiteMessage")
    .WithSummary("Sends a Lite message to the configured logical LiteTopic.")
    .WithDescription("Uses the gRPC producer and the LITE parent topic prepared by the LitePush environment.")
    .Produces<SendLiteMessageResponse>(StatusCodes.Status200OK)
    .ProducesValidationProblem(StatusCodes.Status400BadRequest)
    .ProducesProblem(StatusCodes.Status503ServiceUnavailable);

// RunAsync starts and stops IGrpcProducer through its SDK hosted service.
// Do not manually call IGrpcProducer.StartAsync or StopAsync from this Host application.
await app.RunAsync();

static async Task<IResult> SendLiteMessageAsync(
    SendLiteMessageRequest? request,
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

    const string parentTopic = "eventhorizon-test-lite-parent-topic";
    const string liteTopic = "eventhorizon-test-lite-topic";
    // LiteTopic selects a logical child of the LITE parent Topic. It is mutually exclusive with FIFO,
    // delayed, and priority message modes, and requires Broker and Proxy Lite support.
    var message = new Message(parentTopic, Encoding.UTF8.GetBytes(messageBody))
    {
        LiteTopic = liteTopic
    };
    message.Keys.Add(Guid.NewGuid().ToString("N"));

    try
    {
        var receipt = await producer.SendAsync(message, cancellationToken).ConfigureAwait(false);
        logger.LogInformation(
            "Sent Lite message {MessageId} through parent topic {ParentTopic} to LiteTopic {LiteTopic} at offset {Offset}.",
            receipt.MessageId,
            parentTopic,
            liteTopic,
            receipt.Offset);

        return Results.Ok(new SendLiteMessageResponse(receipt.MessageId, parentTopic, liteTopic, receipt.Offset));
    }
    catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
    {
        throw;
    }
    catch (Exception exception)
    {
        logger.LogError(
            exception,
            "Failed to send a Lite message through parent topic {ParentTopic} to LiteTopic {LiteTopic}.",
            parentTopic,
            liteTopic);
        return Results.Problem(
            title: "RocketMQ Lite message send failed.",
            detail: "The Lite message could not be sent to the RocketMQ service.",
            statusCode: StatusCodes.Status503ServiceUnavailable);
    }
}
