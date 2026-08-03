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

using EventHorizon.RocketMQ.Remoting;
using EventHorizon.RocketMQ.Remoting.Admin;
using EventHorizon.RocketMQ.Remoting.Consumer;
using EventHorizon.RocketMQ.Samples.Remoting.Admin;
using Microsoft.OpenApi;

var builder = WebApplication.CreateBuilder(args);
// NamesrvAddr discovers routes; per-queue queries then connect directly to the selected Broker.
var remotingSection = builder.Configuration.GetRequiredSection("RocketMQ:Remoting");

builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen(static options =>
{
    options.SwaggerDoc("v1", new OpenApiInfo
    {
        Title = "RocketMQ Remoting Admin Sample",
        Description = "Reads Apache RocketMQ queue and message metadata through the classic Remoting protocol.",
        Version = "v1"
    });
});
builder.Services
    .AddRocketMQRemoting(remotingSection.Bind)
    .AddRemotingAdmin();

var app = builder.Build();

app.UseSwagger();
app.UseSwaggerUI();

// These endpoints only inspect existing state; they do not create resources, update offsets, or modify Broker state.
app.MapGet("/topics/{topic}/queues", GetMessageQueuesAsync)
    .WithName("GetRemotingMessageQueues")
    .WithSummary("Discover writable message queues for a topic.")
    .WithDescription("Queries NameServer routes and returns advertised physical queue endpoints; it does not probe Broker reachability.")
    .Produces<IReadOnlyList<AdminQueueResponse>>(StatusCodes.Status200OK)
    .ProducesProblem(StatusCodes.Status502BadGateway);

app.MapGet("/topics/{topic}/queues/{brokerName}/{queueId:int}/offsets", GetQueueOffsetsAsync)
    .WithName("GetRemotingQueueOffsets")
    .WithSummary("Read queue offset bounds and earliest message store time.")
    .WithDescription("Set the optional committed query parameter to false to request the broker's in-flight maximum offset.")
    .Produces<QueueOffsetsResponse>(StatusCodes.Status200OK)
    .ProducesProblem(StatusCodes.Status400BadRequest)
    .ProducesProblem(StatusCodes.Status502BadGateway);

app.MapGet("/topics/{topic}/queues/{brokerName}/{queueId:int}/offsets/search", SearchQueueOffsetAsync)
    .WithName("SearchRemotingQueueOffset")
    .WithSummary("Find a queue offset for a timestamp.")
    .WithDescription("The timestamp query parameter is required and must be ISO 8601. Boundary defaults to Lower.")
    .Produces<QueueOffsetSearchResponse>(StatusCodes.Status200OK)
    .ProducesProblem(StatusCodes.Status400BadRequest)
    .ProducesProblem(StatusCodes.Status502BadGateway);

app.MapGet("/consumer-groups/{consumerGroup}/topics/{topic}/queues/{brokerName}/{queueId:int}/offset", GetConsumerOffsetAsync)
    .WithName("GetRemotingConsumerOffset")
    .WithSummary("Read a consumer group's committed queue offset.")
    .WithDescription("Returns a null offset when the consumer group has not committed a position for the queue.")
    .Produces<ConsumerOffsetResponse>(StatusCodes.Status200OK)
    .ProducesProblem(StatusCodes.Status400BadRequest)
    .ProducesProblem(StatusCodes.Status502BadGateway);

app.MapGet("/topics/{topic}/messages/{offsetMessageId}", ViewMessageAsync)
    .WithName("ViewRemotingMessage")
    .WithSummary("View a message by its physical OffsetMessageId.")
    .WithDescription("Use the OffsetMessageId returned by a Remoting producer send receipt. The encoded broker endpoint must be reachable from this API.")
    .Produces<MessageViewResponse>(StatusCodes.Status200OK)
    .ProducesProblem(StatusCodes.Status400BadRequest)
    .ProducesProblem(StatusCodes.Status502BadGateway);

await app.RunAsync();

static async Task<IResult> GetMessageQueuesAsync(
    string topic,
    IRemotingAdmin admin,
    ILogger<Program> logger,
    CancellationToken cancellationToken)
{
    try
    {
        var queues = await admin.GetMessageQueuesAsync(topic, cancellationToken).ConfigureAwait(false);
        var response = queues.Select(static queue => AdminQueueResponse.From(queue)).ToArray();
        return Results.Ok(response);
    }
    catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
    {
        throw;
    }
    catch (ArgumentException exception)
    {
        logger.LogWarning(exception, "Invalid topic {Topic}", topic);
        return CreateBadRequestProblem(exception);
    }
    catch (Exception exception)
    {
        logger.LogError(exception, "Unable to discover queues for topic {Topic}", topic);
        return CreateBrokerProblem(exception, "Unable to discover RocketMQ message queues.");
    }
}

static async Task<IResult> GetQueueOffsetsAsync(
    string topic,
    string brokerName,
    int queueId,
    bool? committed,
    IRemotingAdmin admin,
    ILogger<Program> logger,
    CancellationToken cancellationToken)
{
    // The default reads the committed maximum; false requests the Broker's in-flight maximum.
    var useCommittedOffset = committed ?? true;

    try
    {
        var queue = new RemotingConsumerQueue(topic, brokerName, queueId);
        var minimumOffsetTask = admin.GetMinOffsetAsync(queue, cancellationToken);
        var maximumOffsetTask = admin.GetMaxOffsetAsync(queue, useCommittedOffset, cancellationToken);
        var earliestStoreTimeTask = admin.GetEarliestMessageStoreTimeAsync(queue, cancellationToken);
        await Task.WhenAll(minimumOffsetTask, maximumOffsetTask, earliestStoreTimeTask).ConfigureAwait(false);

        return Results.Ok(new QueueOffsetsResponse(
            topic,
            brokerName,
            queueId,
            minimumOffsetTask.Result,
            maximumOffsetTask.Result,
            earliestStoreTimeTask.Result,
            useCommittedOffset));
    }
    catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
    {
        throw;
    }
    catch (ArgumentException exception)
    {
        logger.LogWarning(exception, "Invalid queue {BrokerName}/{QueueId} for topic {Topic}", brokerName, queueId, topic);
        return CreateBadRequestProblem(exception);
    }
    catch (Exception exception)
    {
        logger.LogError(exception, "Unable to read offsets for queue {BrokerName}/{QueueId} on topic {Topic}", brokerName, queueId, topic);
        return CreateBrokerProblem(exception, "Unable to read RocketMQ queue offsets.");
    }
}

static async Task<IResult> SearchQueueOffsetAsync(
    string topic,
    string brokerName,
    int queueId,
    DateTimeOffset timestamp,
    RemotingOffsetBoundary? boundary,
    IRemotingAdmin admin,
    ILogger<Program> logger,
    CancellationToken cancellationToken)
{
    var selectedBoundary = boundary ?? RemotingOffsetBoundary.Lower;

    try
    {
        var queue = new RemotingConsumerQueue(topic, brokerName, queueId);
        var offset = await admin.SearchOffsetAsync(queue, timestamp, selectedBoundary, cancellationToken).ConfigureAwait(false);
        return Results.Ok(new QueueOffsetSearchResponse(topic, brokerName, queueId, timestamp, selectedBoundary, offset));
    }
    catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
    {
        throw;
    }
    catch (ArgumentException exception)
    {
        logger.LogWarning(exception, "Invalid queue {BrokerName}/{QueueId} for topic {Topic}", brokerName, queueId, topic);
        return CreateBadRequestProblem(exception);
    }
    catch (Exception exception)
    {
        logger.LogError(exception, "Unable to search an offset for queue {BrokerName}/{QueueId} on topic {Topic}", brokerName, queueId, topic);
        return CreateBrokerProblem(exception, "Unable to search the RocketMQ queue offset.");
    }
}

static async Task<IResult> GetConsumerOffsetAsync(
    string consumerGroup,
    string topic,
    string brokerName,
    int queueId,
    IRemotingAdmin admin,
    ILogger<Program> logger,
    CancellationToken cancellationToken)
{
    try
    {
        var queue = new RemotingConsumerQueue(topic, brokerName, queueId);
        var offset = await admin.GetConsumerOffsetAsync(consumerGroup, queue, cancellationToken).ConfigureAwait(false);
        return Results.Ok(new ConsumerOffsetResponse(consumerGroup, topic, brokerName, queueId, offset));
    }
    catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
    {
        throw;
    }
    catch (ArgumentException exception)
    {
        logger.LogWarning(exception, "Invalid consumer offset request for {ConsumerGroup}", consumerGroup);
        return CreateBadRequestProblem(exception);
    }
    catch (Exception exception)
    {
        logger.LogError(exception, "Unable to read the offset for consumer group {ConsumerGroup}", consumerGroup);
        return CreateBrokerProblem(exception, "Unable to read the RocketMQ consumer offset.");
    }
}

static async Task<IResult> ViewMessageAsync(
    string topic,
    string offsetMessageId,
    IRemotingAdmin admin,
    ILogger<Program> logger,
    CancellationToken cancellationToken)
{
    try
    {
        // OffsetMessageId embeds the target Broker endpoint, so this physical-message lookup bypasses
        // NameServer routing.
        var message = await admin.ViewMessageAsync(topic, offsetMessageId, cancellationToken).ConfigureAwait(false);
        return Results.Ok(MessageViewResponse.From(message));
    }
    catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
    {
        throw;
    }
    catch (ArgumentException exception)
    {
        logger.LogWarning(exception, "Invalid OffsetMessageId for topic {Topic}", topic);
        return CreateBadRequestProblem(exception);
    }
    catch (Exception exception)
    {
        logger.LogError(exception, "Unable to view OffsetMessageId {OffsetMessageId} for topic {Topic}", offsetMessageId, topic);
        return CreateBrokerProblem(exception, "Unable to view the RocketMQ message.");
    }
}

static IResult CreateBadRequestProblem(ArgumentException exception) =>
    Results.Problem(
        detail: exception.Message,
        statusCode: StatusCodes.Status400BadRequest,
        title: "The RocketMQ admin request is invalid.");

static IResult CreateBrokerProblem(Exception exception, string title) =>
    Results.Problem(
        detail: exception.Message,
        statusCode: StatusCodes.Status502BadGateway,
        title: title);
