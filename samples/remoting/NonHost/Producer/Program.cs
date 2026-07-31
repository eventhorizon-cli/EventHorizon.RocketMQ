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
using EventHorizon.RocketMQ.Remoting.Producer;
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
    .AddRemotingProducer(static options =>
        options.GroupName = "rocketmq-dotnet-remoting-nonhost-producer-sample");

await using var serviceProvider = services.BuildServiceProvider(new ServiceProviderOptions
{
    ValidateOnBuild = true,
    ValidateScopes = true
});

var producer = serviceProvider.GetRequiredService<IRemotingProducer>();

try
{
    // BuildServiceProvider does not run the IHostedService registered by AddRemotingProducer.
    // This application does not use a Generic Host, so it explicitly owns the Producer lifecycle.
    await producer.StartAsync(CancellationToken.None).ConfigureAwait(false);

    var message = new Message(Topic, Encoding.UTF8.GetBytes("Hello from the Remoting non-Host Producer sample."))
    {
        Tag = "sample"
    };
    message.Keys.Add($"nonhost-producer-{Guid.NewGuid():N}");
    message.Properties["sample"] = "remoting-nonhost-producer";

    var result = await producer.SendAsync(message, CancellationToken.None).ConfigureAwait(false);
    Console.WriteLine(
        "Sent {0} to {1}/{2}/{3} at offset {4} with {5}.",
        result.MessageId,
        message.Topic,
        result.MessageQueue.BrokerName,
        result.MessageQueue.QueueId,
        result.QueueOffset,
        result.Status);

    // A completed SendAsync call can still carry a non-success durability status.
    if (result.Status != RemotingSendStatus.SendOk)
    {
        Environment.ExitCode = 1;
    }
}
finally
{
    // Keep shutdown independent from an operation token that might already be cancelled.
    await producer.StopAsync(CancellationToken.None).ConfigureAwait(false);
}
