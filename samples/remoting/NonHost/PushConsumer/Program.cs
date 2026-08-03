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
using EventHorizon.RocketMQ.Remoting.Consumer;
using EventHorizon.RocketMQ.Remoting.Consumer.Push;
using EventHorizon.RocketMQ.Samples.Remoting.NonHost.PushConsumer;
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
    .AddRemotingPushConsumer<NonHostPushConsumerMessageHandler>(ServiceLifetime.Scoped, options =>
    {
        options.GroupName = "rocketmq-dotnet-remoting-nonhost-push-consumer-sample";
        options.InitialPosition = ConsumeFromPosition.End;
        options.QueueAssignmentMode = RemotingPushQueueAssignmentMode.Client;
        options.MaxConcurrency = 4;
        options.PullBatchSize = 16;
        options.ConsumeMessageBatchSize = 4;
        options.Subscribe(Topic, new FilterExpression("sample"));
    });

await using var serviceProvider = services.BuildServiceProvider(new ServiceProviderOptions
{
    ValidateOnBuild = true,
    ValidateScopes = true
});

var consumer = serviceProvider.GetRequiredService<IRemotingPushConsumer>();
using var stoppingSource = new CancellationTokenSource();
ConsoleCancelEventHandler cancelHandler = (_, eventArgs) =>
{
    eventArgs.Cancel = true;
    stoppingSource.Cancel();
};
Console.CancelKeyPress += cancelHandler;

try
{
    // BuildServiceProvider does not run the IHostedService registered by AddRemotingPushConsumer.
    // This application does not use a Generic Host, so it explicitly owns the Push Consumer lifecycle.
    await consumer.StartAsync(stoppingSource.Token).ConfigureAwait(false);
    Console.WriteLine("Remoting Push Consumer started. Press Ctrl+C to stop.");
    await Task.Delay(Timeout.InfiniteTimeSpan, stoppingSource.Token).ConfigureAwait(false);
}
catch (OperationCanceledException) when (stoppingSource.IsCancellationRequested)
{
}
finally
{
    Console.CancelKeyPress -= cancelHandler;
    // The Ctrl+C token may already be cancelled; use a live token for graceful shutdown.
    await consumer.StopAsync(CancellationToken.None).ConfigureAwait(false);
}
