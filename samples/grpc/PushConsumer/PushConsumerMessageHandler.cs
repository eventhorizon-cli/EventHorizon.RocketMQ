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
using EventHorizon.RocketMQ.Consumer;
using EventHorizon.RocketMQ.Grpc.Consumer;
using EventHorizon.RocketMQ.Grpc.Consumer.Push;
using Microsoft.Extensions.Logging;

namespace EventHorizon.RocketMQ.Samples.Grpc.PushConsumer;

internal sealed class PushConsumerMessageHandler(ILogger<PushConsumerMessageHandler> logger) : IGrpcPushMessageHandler
{
    public ValueTask<ConsumeResult> HandleAsync(GrpcMessageView message, CancellationToken cancellationToken)
    {
        if (message.IsCorrupted)
        {
            logger.LogWarning(
                "Forwarding corrupted message {MessageId} to the dead-letter queue: {Reason}",
                message.MessageId,
                message.CorruptionReason);
            // The consumer performs the dead-letter operation after this result is returned.
            return ValueTask.FromResult(ConsumeResult.DeadLetter);
        }

        logger.LogInformation(
            "Received message {MessageId} from {Topic}: {Body}",
            message.MessageId,
            message.Topic,
            Encoding.UTF8.GetString(message.Body));
        // The consumer acknowledges the message only after the handler returns Success.
        return ValueTask.FromResult(ConsumeResult.Success);
    }
}
