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
using EventHorizon.RocketMQ.Remoting.Consumer;
using EventHorizon.RocketMQ.Remoting.Consumer.Pull;
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
    .AddRemotingPullConsumer(options =>
    {
        options.GroupName = "rocketmq-dotnet-remoting-nonhost-pull-consumer-sample";
        options.BatchSize = 32;
        options.LongPollingTimeout = TimeSpan.FromSeconds(5);
        options.Subscribe(Topic);
    });

await using var serviceProvider = services.BuildServiceProvider(new ServiceProviderOptions
{
    ValidateOnBuild = true,
    ValidateScopes = true
});

var consumer = serviceProvider.GetRequiredService<IRemotingPullConsumer>();
var logger = serviceProvider.GetRequiredService<ILoggerFactory>().CreateLogger("NonHostPullConsumer");
using var stoppingSource = new CancellationTokenSource();
ConsoleCancelEventHandler cancelHandler = (_, eventArgs) =>
{
    eventArgs.Cancel = true;
    stoppingSource.Cancel();
};
Console.CancelKeyPress += cancelHandler;

try
{
    // BuildServiceProvider does not run the IHostedService registered by AddRemotingPullConsumer.
    // This application does not use a Generic Host, so it explicitly owns the Pull Consumer lifecycle.
    await consumer.StartAsync(stoppingSource.Token).ConfigureAwait(false);
    Console.WriteLine("Remoting Pull Consumer started. Press Ctrl+C to stop.");
    await ConsumeAsync(consumer, logger, stoppingSource.Token).ConfigureAwait(false);
}
catch (OperationCanceledException) when (stoppingSource.IsCancellationRequested)
{
}
catch (Exception exception)
{
    Environment.ExitCode = 1;
    logger.LogError(exception, "The Remoting Pull Consumer sample failed.");
}
finally
{
    Console.CancelKeyPress -= cancelHandler;
    // The Ctrl+C token may already be cancelled; use a live token for graceful shutdown.
    await consumer.StopAsync(CancellationToken.None).ConfigureAwait(false);
}

static async Task ConsumeAsync(
    IRemotingPullConsumer consumer,
    ILogger logger,
    CancellationToken cancellationToken)
{
    while (!cancellationToken.IsCancellationRequested)
    {
        // Raw Pull exposes physical queues. This application owns both queue choice and one offset per queue.
        var queues = await consumer.GetMessageQueuesAsync(Topic, cancellationToken).ConfigureAwait(false);
        if (queues.Count == 0)
        {
            logger.LogWarning("No readable queues were found for {Topic}.", Topic);
            await Task.Delay(TimeSpan.FromSeconds(5), cancellationToken).ConfigureAwait(false);
            continue;
        }

        var offsets = new Dictionary<RemotingPullMessageQueue, long>();
        foreach (var queue in queues)
        {
            offsets[queue] = await GetInitialOffsetAsync(consumer, queue, cancellationToken).ConfigureAwait(false);
        }

        while (!cancellationToken.IsCancellationRequested)
        {
            foreach (var queue in queues)
            {
                var result = await consumer.PullAsync(queue, offsets[queue], cancellationToken: cancellationToken)
                    .ConfigureAwait(false);

                foreach (var message in result.Messages)
                {
                    logger.LogInformation(
                        "Received {MessageId} from {Topic}/{BrokerName}/{QueueId} at offset {QueueOffset}: {Body}",
                        message.MessageId,
                        message.Topic,
                        message.BrokerName,
                        message.QueueId,
                        message.QueueOffset,
                        Encoding.UTF8.GetString(message.Body));
                }

                // Commit only after every message returned in the batch has been processed.
                offsets[queue] = result.NextOffset;
                await consumer.UpdateOffsetAsync(queue, result.NextOffset, cancellationToken).ConfigureAwait(false);
            }
        }
    }
}

static async Task<long> GetInitialOffsetAsync(
    IRemotingPullConsumer consumer,
    RemotingPullMessageQueue queue,
    CancellationToken cancellationToken)
{
    var committed = await consumer.GetOffsetAsync(queue, cancellationToken).ConfigureAwait(false);
    return committed >= 0
        ? committed
        : await consumer.QueryOffsetAsync(
            queue,
            QueryOffsetPolicy.End,
            cancellationToken: cancellationToken).ConfigureAwait(false);
}
