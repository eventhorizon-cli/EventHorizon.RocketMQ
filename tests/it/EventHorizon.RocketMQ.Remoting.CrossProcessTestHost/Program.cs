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

using EventHorizon.RocketMQ.Remoting.Consumer;
using EventHorizon.RocketMQ.Remoting.Consumer.Push;
using Microsoft.Extensions.DependencyInjection;

namespace EventHorizon.RocketMQ.Remoting.CrossProcessTestHost;

internal static class Program
{
    public static Task<int> Main(string[] args) => CrossProcessHostCommand.InvokeAsync(args, RunCommandAsync);

    private static async Task<int> RunCommandAsync(
        CrossProcessHostOptions options,
        CancellationToken cancellationToken)
    {
        CrossProcessHostEventWriter? eventWriter = null;
        try
        {
            cancellationToken.ThrowIfCancellationRequested();
            eventWriter = new CrossProcessHostEventWriter(options.Member);
            return await RunAsync(options, eventWriter).ConfigureAwait(false);
        }
        catch (Exception exception)
        {
            if (eventWriter is not null)
            {
                try
                {
                    await eventWriter.WriteFaultedAsync(exception, CancellationToken.None).ConfigureAwait(false);
                }
                catch
                {
                }
            }

            await Console.Error.WriteLineAsync(exception.ToString()).ConfigureAwait(false);
            return 1;
        }
        finally
        {
            eventWriter?.Dispose();
        }
    }

    private static async Task<int> RunAsync(
        CrossProcessHostOptions hostOptions,
        CrossProcessHostEventWriter eventWriter)
    {
        var services = new ServiceCollection();
        services.AddSingleton(eventWriter);
        services.AddRocketMQRemoting(options =>
        {
            options.NamesrvAddr = hostOptions.NameServerAddress;
            options.InstanceName = hostOptions.InstanceName;
            options.HeartbeatBrokerInterval = TimeSpan.FromHours(1);
            options.PollNameServerInterval = TimeSpan.FromHours(1);
        }).AddRemotingPushConsumer<CrossProcessPushMessageHandler>(ServiceLifetime.Singleton, options =>
        {
            options.GroupName = hostOptions.ConsumerGroup;
            options.ConsumeOrderly = hostOptions.ConsumeOrderly;
            options.InitialPosition = ConsumeFromPosition.Beginning;
            options.MaxConcurrency = 1;
            options.BatchSize = 1;
            options.ConsumeMessageBatchSize = 1;
            options.LongPollingTimeout = TimeSpan.FromSeconds(1);
            options.RetryDelay = TimeSpan.FromMilliseconds(100);
            options.Subscribe(hostOptions.Topic, new FilterExpression(hostOptions.Tag));
        });

        await using var provider = services.BuildServiceProvider(
            new ServiceProviderOptions { ValidateOnBuild = true });
        var consumer = provider.GetRequiredService<IRemotingPushConsumer>();
        await consumer.StartAsync().ConfigureAwait(false);
        using var assignmentStopping = new CancellationTokenSource();
        var assignmentTask = ReportAssignmentsAsync(
            (RemotingPushConsumer)consumer,
            hostOptions.Topic,
            eventWriter,
            assignmentStopping.Token);
        try
        {
            await eventWriter.WriteStartedAsync(CancellationToken.None).ConfigureAwait(false);
            var command = await Console.In.ReadLineAsync().ConfigureAwait(false);
            if (command is not null && !string.Equals(command, "stop", StringComparison.Ordinal))
            {
                throw new InvalidOperationException($"Unsupported parent command '{command}'.");
            }
        }
        finally
        {
            try
            {
                await consumer.StopAsync(CancellationToken.None).ConfigureAwait(false);
            }
            finally
            {
                assignmentStopping.Cancel();
                await assignmentTask.ConfigureAwait(false);
            }
        }

        await eventWriter.WriteAssignmentAsync([], CancellationToken.None).ConfigureAwait(false);
        await eventWriter.WriteStoppedAsync(CancellationToken.None).ConfigureAwait(false);
        return 0;
    }

    private static async Task ReportAssignmentsAsync(
        RemotingPushConsumer consumer,
        string topic,
        CrossProcessHostEventWriter eventWriter,
        CancellationToken cancellationToken)
    {
        string[] previous = [];
        while (!cancellationToken.IsCancellationRequested)
        {
            var current = consumer.Assignment
                .Where(queue => string.Equals(queue.Topic, topic, StringComparison.Ordinal))
                .Select(queue => $"{queue.Topic}/{queue.BrokerName}/{queue.QueueId}")
                .Order(StringComparer.Ordinal)
                .ToArray();
            if (!current.SequenceEqual(previous, StringComparer.Ordinal))
            {
                previous = current;
                await eventWriter.WriteAssignmentAsync(current, CancellationToken.None).ConfigureAwait(false);
            }

            try
            {
                await Task.Delay(TimeSpan.FromMilliseconds(50), cancellationToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                return;
            }
        }
    }

}
