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
var clientSection = configuration.GetRequiredSection("RocketMQ:Client");

var services = new ServiceCollection();
services.AddLogging(logging => logging.AddSimpleConsole());
services
    .AddRocketMQGrpc(clientSection.Bind)
    .AddGrpcProducer(options => options.SendMsgTimeout = TimeSpan.FromSeconds(3));

await using var serviceProvider = services.BuildServiceProvider(new ServiceProviderOptions
{
    ValidateOnBuild = true,
    ValidateScopes = true
});
var producer = serviceProvider.GetRequiredService<IGrpcProducer>();
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
    // BuildServiceProvider does not run the IHostedService registered by AddGrpcProducer.
    // This non-Host process owns the Producer lifecycle.
    await producer.StartAsync(stoppingSource.Token).ConfigureAwait(false);

    var message = new Message(topic, Encoding.UTF8.GetBytes("Hello from the non-Host gRPC Producer sample."))
    {
        Tag = "sample"
    };
    message.Keys.Add(Guid.NewGuid().ToString("N"));

    var receipt = await producer.SendAsync(message, stoppingSource.Token).ConfigureAwait(false);
    logger.LogInformation(
        "Sent message {MessageId} to {Topic} at offset {Offset}.",
        receipt.MessageId,
        message.Topic,
        receipt.Offset);
}
catch (OperationCanceledException) when (stoppingSource.IsCancellationRequested)
{
}
catch (Exception exception)
{
    Environment.ExitCode = 1;
    logger.LogError(exception, "The non-Host Producer sample failed.");
}
finally
{
    Console.CancelKeyPress -= cancelHandler;
    // The Ctrl+C token may already be cancelled; use a live token so shutdown can finish gracefully.
    await producer.StopAsync(CancellationToken.None).ConfigureAwait(false);
}
