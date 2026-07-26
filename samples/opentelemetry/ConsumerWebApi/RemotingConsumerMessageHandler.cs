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
using EventHorizon.RocketMQ.Remoting.Consumer;
using EventHorizon.RocketMQ.Remoting.Consumer.Push;
using Microsoft.Extensions.Logging;

namespace EventHorizon.RocketMQ.Samples.OpenTelemetry.ConsumerWebApi;

internal sealed class RemotingConsumerMessageHandler(
    ILogger<RemotingConsumerMessageHandler> logger) : IRemotingPushMessageHandler
{
    public async ValueTask<ConsumeResult> HandleAsync(
        IReadOnlyList<RemotingMessageView> messages,
        RemotingPushConsumeContext context,
        CancellationToken cancellationToken)
    {
        foreach (var message in messages)
        {
            await Task.Delay(Random.Shared.Next(250, 1001), cancellationToken);
            logger.LogInformation(
                "Remoting consumer received {MessageId} from {Topic}/{BrokerName}/{QueueId} at offset {QueueOffset}: {Body}",
                message.MessageId,
                message.Topic,
                message.BrokerName,
                message.QueueId,
                message.QueueOffset,
                Encoding.UTF8.GetString(message.Body));
        }

        return ConsumeResult.Success;
    }
}
