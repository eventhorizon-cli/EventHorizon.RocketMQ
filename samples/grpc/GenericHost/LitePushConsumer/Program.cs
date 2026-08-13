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
using EventHorizon.RocketMQ.Grpc.Consumer.LitePush;
using EventHorizon.RocketMQ.Samples.Grpc.LitePushConsumer;
using Microsoft.OpenApi;

const string parentTopic = "eventhorizon-test-lite-parent-topic";

var builder = WebApplication.CreateBuilder(args);
builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen(static options =>
{
    options.SwaggerDoc("v1", new OpenApiInfo
    {
        Title = "RocketMQ gRPC LitePushConsumer Sample API",
        Version = "v1",
        Description = "Adds and removes LiteTopic subscriptions beneath one LITE parent topic."
    });
});
var clientSection = builder.Configuration.GetRequiredSection("RocketMQ:Client");

// The endpoint must target a RocketMQ Proxy; this transport does not connect directly to Brokers.
// Lite Push uses client-initiated long polling.
// Handler results drive automatic acknowledgement, retry, or DLQ forwarding.
// This role starts with no LiteTopic subscriptions. Application events add and remove them at runtime.
builder.Services
    .AddRocketMQGrpc(clientSection.Bind)
    .AddGrpcLitePushConsumer<LitePushConsumerMessageHandler>(
        ServiceLifetime.Scoped,
        options =>
        {
            options.GroupName = "eventhorizon-test-lite-push-consumer";
            options.BindTopic = parentTopic;
            options.BatchSize = 16;
            options.MaxConcurrency = 4;
            options.MaxDeliveryAttempts = 16;
            options.InvisibleDuration = TimeSpan.FromSeconds(30);
            options.ConsumeTimeout = TimeSpan.FromMinutes(15);
            options.LongPollingTimeout = TimeSpan.FromSeconds(15);
            options.SubscriptionSyncInterval = TimeSpan.FromSeconds(30);
        });

var app = builder.Build();
app.UseSwagger();
app.UseSwaggerUI();

app.MapGet("/subscriptions", GetLiteTopicSubscriptions)
    .WithName("GetLiteTopicSubscriptions")
    .WithSummary("Lists LiteTopics currently subscribed by this consumer.")
    .WithDescription("Returns the local subscription snapshot under the configured LITE parent topic.")
    .Produces<string[]>(StatusCodes.Status200OK);
app.MapPost("/subscriptions/{liteTopic}", SubscribeLiteTopicAsync)
    .WithName("SubscribeLiteTopic")
    .WithSummary("Adds a LiteTopic subscription at runtime.")
    .WithDescription("Synchronizes the new LiteTopic with RocketMQ before returning. LiteTopic names use letters, digits, hyphens, and underscores.")
    .Produces(StatusCodes.Status204NoContent)
    .ProducesValidationProblem(StatusCodes.Status400BadRequest)
    .ProducesProblem(StatusCodes.Status503ServiceUnavailable);
app.MapDelete("/subscriptions/{liteTopic}", UnsubscribeLiteTopicAsync)
    .WithName("UnsubscribeLiteTopic")
    .WithSummary("Removes a LiteTopic subscription at runtime.")
    .WithDescription("Synchronizes removal of the LiteTopic with RocketMQ before returning. LiteTopic names use letters, digits, hyphens, and underscores.")
    .Produces(StatusCodes.Status204NoContent)
    .ProducesValidationProblem(StatusCodes.Status400BadRequest)
    .ProducesProblem(StatusCodes.Status503ServiceUnavailable);

// RunAsync starts and stops the registered Lite Push role through its SDK hosted service.
// Application code changes subscriptions but does not manage the consumer lifecycle directly.
await app.RunAsync();

static IResult GetLiteTopicSubscriptions(IGrpcLitePushConsumer consumer) =>
    Results.Ok(consumer.LiteTopics.OrderBy(static liteTopic => liteTopic, StringComparer.Ordinal).ToArray());

static async Task<IResult> SubscribeLiteTopicAsync(
    string liteTopic,
    IGrpcLitePushConsumer consumer,
    ILogger<Program> logger,
    CancellationToken cancellationToken)
{
    if (string.IsNullOrWhiteSpace(liteTopic))
    {
        return Results.ValidationProblem(new Dictionary<string, string[]>
        {
            ["liteTopic"] = ["A LiteTopic is required."]
        });
    }

    if (!IsValidLiteTopic(liteTopic))
    {
        return Results.ValidationProblem(new Dictionary<string, string[]>
        {
            ["liteTopic"] = ["A LiteTopic may contain only letters, digits, hyphens, and underscores."]
        });
    }

    try
    {
        await consumer.SubscribeLiteAsync(liteTopic, cancellationToken: cancellationToken).ConfigureAwait(false);
        logger.LogInformation("Subscribed to LiteTopic {LiteTopic}.", liteTopic);
        return Results.NoContent();
    }
    catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
    {
        throw;
    }
    catch (Exception exception)
    {
        logger.LogError(exception, "Failed to subscribe to LiteTopic {LiteTopic}.", liteTopic);
        return Results.Problem(
            title: "RocketMQ LiteTopic subscription failed.",
            detail: "The LiteTopic could not be synchronized with the RocketMQ service.",
            statusCode: StatusCodes.Status503ServiceUnavailable);
    }
}

static async Task<IResult> UnsubscribeLiteTopicAsync(
    string liteTopic,
    IGrpcLitePushConsumer consumer,
    ILogger<Program> logger,
    CancellationToken cancellationToken)
{
    if (string.IsNullOrWhiteSpace(liteTopic))
    {
        return Results.ValidationProblem(new Dictionary<string, string[]>
        {
            ["liteTopic"] = ["A LiteTopic is required."]
        });
    }

    if (!IsValidLiteTopic(liteTopic))
    {
        return Results.ValidationProblem(new Dictionary<string, string[]>
        {
            ["liteTopic"] = ["A LiteTopic may contain only letters, digits, hyphens, and underscores."]
        });
    }

    try
    {
        await consumer.UnsubscribeLiteAsync(liteTopic, cancellationToken).ConfigureAwait(false);
        logger.LogInformation("Unsubscribed from LiteTopic {LiteTopic}.", liteTopic);
        return Results.NoContent();
    }
    catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
    {
        throw;
    }
    catch (Exception exception)
    {
        logger.LogError(exception, "Failed to unsubscribe from LiteTopic {LiteTopic}.", liteTopic);
        return Results.Problem(
            title: "RocketMQ LiteTopic unsubscription failed.",
            detail: "The LiteTopic could not be removed from the RocketMQ service.",
            statusCode: StatusCodes.Status503ServiceUnavailable);
    }
}

// Apache RocketMQ 5.5.0 (rocketmq-all-5.5.0) validates LiteTopic characters as [A-Za-z0-9_-].
static bool IsValidLiteTopic(string liteTopic) =>
    liteTopic.All(static character => char.IsAsciiLetterOrDigit(character) || character is '-' or '_');
