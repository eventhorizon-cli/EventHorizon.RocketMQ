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
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace EventHorizon.RocketMQ.Samples.Remoting.PullConsumer;

internal sealed class PullConsumer(
    IRemotingPullConsumer consumer,
    IOptions<PullConsumerSampleOptions> options,
    IHostApplicationLifetime applicationLifetime,
    ILogger<PullConsumer> logger) : BackgroundService
{
    private readonly PullConsumerSampleOptions _options = options.Value;

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                // NameServer discovery provides Broker queues; this pull loop owns queue selection and offsets.
                var queues = await consumer.GetMessageQueuesAsync(_options.Topic, stoppingToken).ConfigureAwait(false);
                if (queues.Count == 0)
                {
                    logger.LogWarning("No readable queues were found for {Topic}", _options.Topic);
                    await Task.Delay(TimeSpan.FromSeconds(5), stoppingToken).ConfigureAwait(false);
                    continue;
                }

                var offsets = new Dictionary<RemotingPullMessageQueue, long>();
                foreach (var queue in queues)
                {
                    offsets[queue] = await GetInitialOffsetAsync(queue, stoppingToken).ConfigureAwait(false);
                }

                while (!stoppingToken.IsCancellationRequested)
                {
                    foreach (var queue in queues)
                    {
                        var result = await consumer.PullAsync(queue, offsets[queue], cancellationToken: stoppingToken)
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

                        // Persist after processing so an earlier failure retries the batch from the previous offset.
                        offsets[queue] = result.NextOffset;
                        await consumer.UpdateOffsetAsync(queue, result.NextOffset, stoppingToken).ConfigureAwait(false);
                    }
                }
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                return;
            }
            catch (Exception exception)
            {
                Environment.ExitCode = 1;
                logger.LogError(exception, "The pull consumer sample failed.");
                applicationLifetime.StopApplication();
                return;
            }
        }
    }

    private async Task<long> GetInitialOffsetAsync(
        RemotingPullMessageQueue queue,
        CancellationToken cancellationToken)
    {
        // Existing group offsets take precedence over the configured initial-position policy.
        var committed = await consumer.GetOffsetAsync(queue, cancellationToken).ConfigureAwait(false);
        return committed >= 0
            ? committed
            : await consumer.QueryOffsetAsync(
                queue,
                _options.InitialOffset,
                _options.InitialTimestamp,
                cancellationToken).ConfigureAwait(false);
    }
}
