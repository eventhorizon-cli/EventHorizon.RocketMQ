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
using EventHorizon.RocketMQ.Remoting.Consumer.Pull;
using EventHorizon.RocketMQ.Remoting.Consumer.Push.Pop;
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

var services = new ServiceCollection();
services.AddLogging(static logging => logging.AddSimpleConsole());
services
    .AddRocketMQRemoting(configuration.GetRequiredSection("RocketMQ:Remoting").Bind)
    .AddRemotingPopConsumer(options =>
    {
        options.GroupName = "rocketmq-dotnet-remoting-nonhost-pop-consumer-sample";
        options.BatchSize = 32;
        options.InvisibleDuration = TimeSpan.FromSeconds(30);
        options.LongPollingTimeout = TimeSpan.FromSeconds(5);
        options.Subscribe(Topic);
    });

await using var serviceProvider = services.BuildServiceProvider(new ServiceProviderOptions
{
    ValidateOnBuild = true,
    ValidateScopes = true
});

var consumer = serviceProvider.GetRequiredService<IRemotingPopConsumer>();
var logger = serviceProvider.GetRequiredService<ILoggerFactory>().CreateLogger("NonHostPopConsumer");
using var stoppingSource = new CancellationTokenSource();
ConsoleCancelEventHandler cancelHandler = (_, eventArgs) =>
{
    eventArgs.Cancel = true;
    stoppingSource.Cancel();
};
Console.CancelKeyPress += cancelHandler;

try
{
    // BuildServiceProvider does not run the IHostedService registered by AddRemotingPopConsumer.
    // This application does not use a Generic Host, so it explicitly owns the POP Consumer lifecycle.
    await consumer.StartAsync(stoppingSource.Token).ConfigureAwait(false);
    Console.WriteLine("Remoting POP Consumer started. Press Ctrl+C to stop.");
    await ConsumeAsync(consumer, logger, stoppingSource.Token).ConfigureAwait(false);
}
catch (OperationCanceledException) when (stoppingSource.IsCancellationRequested)
{
}
catch (Exception exception)
{
    Environment.ExitCode = 1;
    logger.LogError(exception, "The Remoting POP Consumer sample failed.");
}
finally
{
    Console.CancelKeyPress -= cancelHandler;
    // The Ctrl+C token may already be cancelled; use a live token for graceful shutdown.
    await consumer.StopAsync(CancellationToken.None).ConfigureAwait(false);
}

static async Task ConsumeAsync(
    IRemotingPopConsumer consumer,
    ILogger logger,
    CancellationToken cancellationToken)
{
    IReadOnlyList<RemotingPullMessageQueue> queues = [];
    var queueIndex = 0;

    while (!cancellationToken.IsCancellationRequested)
    {
        try
        {
            if (queues.Count == 0)
            {
                // POP does not assign queues. The application picks one readable queue for each request.
                queues = await consumer.GetMessageQueuesAsync(Topic, cancellationToken).ConfigureAwait(false);
                if (queues.Count == 0)
                {
                    logger.LogWarning("No readable queues were found for {Topic}.", Topic);
                    await DelayAsync(cancellationToken).ConfigureAwait(false);
                    continue;
                }
            }

            var queue = queues[queueIndex++ % queues.Count];
            var result = await consumer.PopAsync(queue, cancellationToken: cancellationToken).ConfigureAwait(false);
            foreach (var received in result.Messages)
            {
                var message = received.Message;
                logger.LogInformation(
                    "Received {MessageId} from {Topic}/{BrokerName}/{QueueId} at offset {QueueOffset}: {Body}",
                    message.MessageId,
                    message.Topic,
                    message.BrokerName,
                    message.QueueId,
                    message.QueueOffset,
                    Encoding.UTF8.GetString(message.Body));

                // Acknowledge the receipt only after processing. Unacknowledged messages reappear after invisibility.
                await consumer.AcknowledgeAsync(received.Receipt, cancellationToken).ConfigureAwait(false);
            }
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            return;
        }
        catch (Exception exception)
        {
            logger.LogError(exception, "POP request failed.");
            queues = [];
            await DelayAsync(cancellationToken).ConfigureAwait(false);
        }
    }
}

static async Task DelayAsync(CancellationToken cancellationToken)
{
    try
    {
        await Task.Delay(TimeSpan.FromSeconds(1), cancellationToken).ConfigureAwait(false);
    }
    catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
    {
    }
}
