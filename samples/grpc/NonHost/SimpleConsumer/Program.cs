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
using EventHorizon.RocketMQ.Grpc.Consumer;
using EventHorizon.RocketMQ.Grpc.Consumer.Simple;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

const string Topic = "eventhorizon-test-topic";

var configuration = new ConfigurationBuilder()
    .SetBasePath(AppContext.BaseDirectory)
    .AddJsonFile("appsettings.json", optional: false)
    .AddEnvironmentVariables()
    .AddCommandLine(args)
    .Build();
var clientSection = configuration.GetRequiredSection("RocketMQ:Client");

var services = new ServiceCollection();
services.AddLogging(logging => logging.AddSimpleConsole());
services
    .AddRocketMQGrpc(clientSection.Bind)
    .AddGrpcSimpleConsumer(options =>
    {
        options.GroupName = "rocketmq-dotnet-grpc-nonhost-simple-sample";
        options.AwaitDuration = TimeSpan.FromSeconds(5);
        options.InvisibleDuration = TimeSpan.FromSeconds(30);
        options.Subscribe(Topic, new FilterExpression("sample"));
    });

await using var serviceProvider = services.BuildServiceProvider(new ServiceProviderOptions
{
    ValidateOnBuild = true,
    ValidateScopes = true
});
var consumer = serviceProvider.GetRequiredService<IGrpcSimpleConsumer>();
var logger = serviceProvider.GetRequiredService<ILogger<Program>>();

using var stoppingSource = new CancellationTokenSource();
ConsoleCancelEventHandler cancelHandler = (_, eventArgs) =>
{
    eventArgs.Cancel = true;
    stoppingSource.Cancel();
};
Console.CancelKeyPress += cancelHandler;

try
{
    // BuildServiceProvider does not run the IHostedService registered by AddGrpcSimpleConsumer.
    // This non-Host process owns the Consumer lifecycle and the Receive/Ack/DLQ loop.
    await consumer.StartAsync(stoppingSource.Token).ConfigureAwait(false);
    Console.WriteLine("gRPC SimpleConsumer started. Press Ctrl+C to stop.");

    while (!stoppingSource.IsCancellationRequested)
    {
        IReadOnlyList<GrpcMessageView> messages;
        try
        {
            // ReceiveAsync long-polls the Proxy. Returned messages remain invisible until settled.
            messages = await consumer.ReceiveAsync(16, cancellationToken: stoppingSource.Token)
                .ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (stoppingSource.IsCancellationRequested)
        {
            break;
        }
        catch (Exception exception)
        {
            Environment.ExitCode = 1;
            logger.LogError(exception, "The non-Host SimpleConsumer sample failed to receive messages.");
            break;
        }

        foreach (var message in messages)
        {
            try
            {
                if (message.IsCorrupted)
                {
                    // SimpleConsumer never auto-settles: explicitly dead-letter an unsafe payload.
                    logger.LogWarning(
                        "Forwarding corrupted message {MessageId} to the dead-letter queue: {Reason}",
                        message.MessageId,
                        message.CorruptionReason);
                    await consumer.ForwardToDeadLetterQueueAsync(
                            message,
                            maxDeliveryAttempts: 16,
                            cancellationToken: stoppingSource.Token)
                        .ConfigureAwait(false);
                    continue;
                }

                logger.LogInformation(
                    "Received message {MessageId} from {Topic}: {Body}",
                    message.MessageId,
                    message.Topic,
                    Encoding.UTF8.GetString(message.Body));
                // Acknowledge only after the business effect is durable. This sample's effect is logging.
                await consumer.AckAsync(message, stoppingSource.Token).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (stoppingSource.IsCancellationRequested)
            {
                break;
            }
            catch (Exception exception)
            {
                logger.LogError(
                    exception,
                    "Message {MessageId} was not settled and will become visible again.",
                    message.MessageId);
            }
        }
    }
}
catch (OperationCanceledException) when (stoppingSource.IsCancellationRequested)
{
}
catch (Exception exception)
{
    Environment.ExitCode = 1;
    logger.LogError(exception, "The non-Host SimpleConsumer sample failed.");
}
finally
{
    Console.CancelKeyPress -= cancelHandler;
    // The Ctrl+C token may already be cancelled; use a live token so shutdown can finish gracefully.
    await consumer.StopAsync(CancellationToken.None).ConfigureAwait(false);
}
