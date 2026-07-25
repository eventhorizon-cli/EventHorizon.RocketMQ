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
using EventHorizon.RocketMQ.Grpc.Consumer;
using EventHorizon.RocketMQ.Grpc.Consumer.Simple;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace EventHorizon.RocketMQ.Samples.Grpc.SimpleConsumer;

internal sealed class SimpleConsumer(
    IGrpcSimpleConsumer consumer,
    IOptions<SimpleConsumerSampleOptions> options,
    IHostApplicationLifetime applicationLifetime,
    ILogger<SimpleConsumer> logger) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var settings = options.Value;
        while (!stoppingToken.IsCancellationRequested)
        {
            IReadOnlyList<GrpcMessageView> messages;
            try
            {
                // ReceiveAsync long-polls the Proxy. Each result remains invisible for the configured duration
                // unless it is acknowledged or forwarded to the DLQ.
                messages = await consumer.ReceiveAsync(settings.BatchSize, cancellationToken: stoppingToken)
                    .ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                return;
            }
            catch (Exception exception)
            {
                Environment.ExitCode = 1;
                logger.LogError(exception, "The simple consumer sample failed to receive messages.");
                applicationLifetime.StopApplication();
                return;
            }

            foreach (var message in messages)
            {
                try
                {
                    if (message.IsCorrupted)
                    {
                        // SimpleConsumer never auto-settles messages; explicitly forward unsafe payloads to the DLQ.
                        logger.LogWarning(
                            "Forwarding corrupted message {MessageId} to the dead-letter queue: {Reason}",
                            message.MessageId,
                            message.CorruptionReason);
                        await consumer.ForwardToDeadLetterQueueAsync(
                                message,
                                settings.MaxDeliveryAttempts,
                                stoppingToken)
                            .ConfigureAwait(false);
                        continue;
                    }

                    logger.LogInformation(
                        "Received message {MessageId} from {Topic}: {Body}",
                        message.MessageId,
                        message.Topic,
                        Encoding.UTF8.GetString(message.Body));
                    // Acknowledgement is manual; production code should call AckAsync only after durable
                    // processing succeeds.
                    await consumer.AckAsync(message, stoppingToken).ConfigureAwait(false);
                }
                catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
                {
                    return;
                }
                catch (Exception exception)
                {
                    logger.LogError(
                        exception,
                        "Message {MessageId} was not acknowledged and will become visible again.",
                        message.MessageId);
                }
            }
        }
    }
}
