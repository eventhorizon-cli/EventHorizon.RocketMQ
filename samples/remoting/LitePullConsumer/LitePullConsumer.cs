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
using EventHorizon.RocketMQ.Remoting.Consumer.Pull.Lite;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace EventHorizon.RocketMQ.Samples.Remoting.LitePullConsumer;

internal sealed class LitePullConsumer(
    IRemotingLitePullConsumer consumer,
    IOptions<LitePullConsumerSampleOptions> options,
    ILogger<LitePullConsumer> logger) : BackgroundService
{
    private readonly LitePullConsumerSampleOptions _options = options.Value;

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                // Subscription mode polls only client-assigned queues; rebalance can temporarily leave none to poll.
                var result = await consumer.PollAsync(cancellationToken: stoppingToken).ConfigureAwait(false);
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

                // Commit the advanced local positions only after the returned messages have been processed.
                await consumer.CommitAsync(stoppingToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                return;
            }
            catch (InvalidOperationException exception)
            {
                logger.LogDebug(exception, "Waiting for Lite Pull queue assignment");
                await DelayAsync(stoppingToken).ConfigureAwait(false);
            }
            catch (Exception exception)
            {
                logger.LogError(exception, "Lite Pull polling failed");
                await DelayAsync(stoppingToken).ConfigureAwait(false);
            }
        }
    }

    private async Task DelayAsync(CancellationToken cancellationToken)
    {
        try
        {
            await Task.Delay(_options.RetryDelay, cancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
        }
    }
}
