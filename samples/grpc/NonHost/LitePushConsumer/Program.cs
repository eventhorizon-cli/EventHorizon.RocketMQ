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
using EventHorizon.RocketMQ.Samples.Grpc.NonHost.LitePushConsumer;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

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
    .AddGrpcLitePushConsumer<NonHostLitePushConsumerMessageHandler>(
        ServiceLifetime.Scoped,
        options =>
        {
            options.GroupName = "eventhorizon-test-lite-push-consumer";
            options.BindTopic = "eventhorizon-test-lite-parent-topic";
            options.LiteTopics.Add("eventhorizon-test-lite-topic");
            options.BatchSize = 16;
            options.MaxConcurrency = 4;
            options.MaxDeliveryAttempts = 16;
            options.InvisibleDuration = TimeSpan.FromSeconds(30);
            options.ConsumeTimeout = TimeSpan.FromMinutes(15);
            options.LongPollingTimeout = TimeSpan.FromSeconds(15);
            options.SubscriptionSyncInterval = TimeSpan.FromSeconds(30);
        });

await using var serviceProvider = services.BuildServiceProvider(new ServiceProviderOptions
{
    ValidateOnBuild = true,
    ValidateScopes = true
});
var consumer = serviceProvider.GetRequiredService<IGrpcLitePushConsumer>();
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
    // BuildServiceProvider does not run the IHostedService registered by AddGrpcLitePushConsumer.
    // This non-Host process owns the Lite Push lifecycle and initial LiteTopic synchronization.
    await consumer.StartAsync(stoppingSource.Token).ConfigureAwait(false);
    Console.WriteLine("gRPC LitePushConsumer started. Press Ctrl+C to stop.");
    await Task.Delay(Timeout.InfiniteTimeSpan, stoppingSource.Token).ConfigureAwait(false);
}
catch (OperationCanceledException) when (stoppingSource.IsCancellationRequested)
{
}
catch (Exception exception)
{
    Environment.ExitCode = 1;
    logger.LogError(exception, "The non-Host LitePushConsumer sample failed.");
}
finally
{
    Console.CancelKeyPress -= cancelHandler;
    // The Ctrl+C token may already be cancelled; use a live token so shutdown can finish gracefully.
    await consumer.StopAsync(CancellationToken.None).ConfigureAwait(false);
}
