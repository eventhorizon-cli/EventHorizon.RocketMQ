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
using EventHorizon.RocketMQ.Remoting.Consumer.LitePull;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

const string topic = "eventhorizon-test-topic";

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
    .AddRemotingLitePullConsumer(options =>
    {
        options.GroupName = "rocketmq-dotnet-remoting-nonhost-lite-pull-consumer-sample";
        options.PullBatchSize = 32;
        options.LongPollingTimeout = TimeSpan.FromSeconds(5);
        options.InitialPosition = ConsumeFromPosition.End;
        options.EnableAutoCommit = false;
        options.Subscribe(topic);
    });

await using var serviceProvider = services.BuildServiceProvider(new ServiceProviderOptions
{
    ValidateOnBuild = true,
    ValidateScopes = true
});

var consumer = serviceProvider.GetRequiredService<IRemotingLitePullConsumer>();
var logger = serviceProvider.GetRequiredService<ILoggerFactory>().CreateLogger("NonHostLitePullConsumer");
using var stoppingSource = new CancellationTokenSource();
ConsoleCancelEventHandler cancelHandler = (_, eventArgs) =>
{
    eventArgs.Cancel = true;
    stoppingSource.Cancel();
};
Console.CancelKeyPress += cancelHandler;

try
{
    // BuildServiceProvider does not run the IHostedService registered by AddRemotingLitePullConsumer.
    // This application does not use a Generic Host, so it explicitly owns the Lite Pull Consumer lifecycle.
    await consumer.StartAsync(stoppingSource.Token).ConfigureAwait(false);
    Console.WriteLine("Remoting Lite Pull Consumer started. Press Ctrl+C to stop.");
    await ConsumeAsync(consumer, logger, stoppingSource.Token).ConfigureAwait(false);
}
catch (OperationCanceledException) when (stoppingSource.IsCancellationRequested)
{
}
catch (Exception exception)
{
    Environment.ExitCode = 1;
    logger.LogError(exception, "The Remoting Lite Pull Consumer sample failed.");
}
finally
{
    Console.CancelKeyPress -= cancelHandler;
    // The Ctrl+C token may already be cancelled; use a live token for graceful shutdown.
    await consumer.StopAsync(CancellationToken.None).ConfigureAwait(false);
}

static async Task ConsumeAsync(
    IRemotingLitePullConsumer consumer,
    ILogger logger,
    CancellationToken cancellationToken)
{
    while (!cancellationToken.IsCancellationRequested)
    {
        try
        {
            // Poll reads the SDK's local buffer and advances only delivered local positions.
            var messages = await consumer.PollAsync(cancellationToken: cancellationToken).ConfigureAwait(false);
            foreach (var message in messages)
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

            // Persist the advanced local positions only after the returned messages have been processed.
            await consumer.CommitAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            return;
        }
        catch (InvalidOperationException exception)
        {
            logger.LogDebug(exception, "Waiting for Lite Pull queue assignment.");
            await DelayAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (Exception exception)
        {
            logger.LogError(exception, "Lite Pull polling failed.");
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
