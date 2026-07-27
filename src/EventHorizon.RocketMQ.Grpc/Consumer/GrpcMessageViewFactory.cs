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

using EventHorizon.RocketMQ.Grpc.Protocol;
using Proto = Apache.Rocketmq.V2;

namespace EventHorizon.RocketMQ.Grpc.Consumer;

internal static class GrpcMessageViewFactory
{
    public static GrpcMessageView Create(Proto.Message message, Proto.MessageQueue queue, Uri endpoint)
    {
        var topic = message.Topic.Name;
        var messageId = message.SystemProperties.MessageId;
        var encodedBody = message.Body.ToByteArray();
        var body = encodedBody;
        var isCorrupted = false;
        string? corruptionReason = null;
        try
        {
            body = MessageBodyCodec.Decode(
                encodedBody,
                message.SystemProperties.BodyEncoding,
                message.SystemProperties.BodyDigest,
                topic,
                messageId);
        }
        catch (InvalidDataException exception)
        {
            isCorrupted = true;
            corruptionReason = exception.Message;
        }

        return new GrpcMessageView(
            topic,
            body,
            messageId,
            message.SystemProperties.HasReceiptHandle
                ? message.SystemProperties.ReceiptHandle
                : string.Empty,
            message.SystemProperties.HasTag ? message.SystemProperties.Tag : null,
            message.SystemProperties.Keys.ToArray(),
            new Dictionary<string, string>(message.UserProperties, StringComparer.Ordinal),
            message.SystemProperties.HasDeliveryAttempt ? message.SystemProperties.DeliveryAttempt : 1,
            message.SystemProperties.HasMessageGroup ? message.SystemProperties.MessageGroup : null,
            message.SystemProperties.InvisibleDuration?.ToTimeSpan(),
            message.SystemProperties.HasLiteTopic ? message.SystemProperties.LiteTopic : null,
            queue.Id,
            queue.Broker.Name,
            message.SystemProperties.HasQueueOffset ? message.SystemProperties.QueueOffset : null,
            message.SystemProperties.BornTimestamp?.ToDateTimeOffset(),
            message.SystemProperties.StoreTimestamp?.ToDateTimeOffset(),
            isCorrupted,
            corruptionReason,
            endpoint);
    }
}
