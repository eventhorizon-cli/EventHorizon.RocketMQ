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
using EventHorizon.RocketMQ.Remoting.Consumer.Pull;
using EventHorizon.RocketMQ.Remoting.Consumer.Push.Pop;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace EventHorizon.RocketMQ.Samples.Remoting.PopConsumer;

internal sealed class PopConsumer(
    IRemotingPopConsumer consumer,
    IOptions<PopConsumerSampleOptions> options,
    ILogger<PopConsumer> logger) : BackgroundService
{
    private readonly PopConsumerSampleOptions _options = options.Value;

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var queues = await GetMessageQueuesAsync(stoppingToken).ConfigureAwait(false);
        var queueIndex = 0;

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                if (queues.Count == 0)
                {
                    queues = await GetMessageQueuesAsync(stoppingToken).ConfigureAwait(false);
                    await DelayAsync(stoppingToken).ConfigureAwait(false);
                    continue;
                }

                var queue = queues[queueIndex++ % queues.Count];
                var result = await consumer.PopAsync(queue, cancellationToken: stoppingToken).ConfigureAwait(false);

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
                    // Long-running work must renew the receipt and acknowledge its replacement before expiry.
                    await consumer.AcknowledgeAsync(received.Receipt, stoppingToken).ConfigureAwait(false);
                }
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                return;
            }
            catch (Exception exception)
            {
                logger.LogError(exception, "POP request failed");
                queues = [];
                await DelayAsync(stoppingToken).ConfigureAwait(false);
            }
        }
    }

    private async Task<IReadOnlyList<RemotingPullMessageQueue>> GetMessageQueuesAsync(CancellationToken cancellationToken)
    {
        try
        {
            return await consumer.GetMessageQueuesAsync(_options.Topic, cancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            return [];
        }
        catch (Exception exception)
        {
            logger.LogError(exception, "Unable to discover POP message queues for {Topic}", _options.Topic);
            return [];
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
