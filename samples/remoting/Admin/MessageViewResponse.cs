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

namespace EventHorizon.RocketMQ.Samples.Remoting.Admin;

internal sealed record MessageViewResponse(
    string Topic,
    string MessageId,
    string OffsetMessageId,
    string? Tag,
    IReadOnlyList<string> Keys,
    IReadOnlyDictionary<string, string> Properties,
    string BodyBase64,
    int DeliveryAttempt,
    string? MessageGroup,
    int QueueId,
    string? BrokerName,
    long QueueOffset,
    long CommitLogOffset,
    DateTimeOffset BornTimestamp,
    DateTimeOffset StoreTimestamp)
{
    internal static MessageViewResponse From(RemotingMessageView message) =>
        new(
            message.Topic,
            message.MessageId,
            message.OffsetMessageId,
            message.Tag,
            message.Keys,
            message.Properties,
            Convert.ToBase64String(message.Body),
            message.DeliveryAttempt,
            message.MessageGroup,
            message.QueueId,
            message.BrokerName,
            message.QueueOffset,
            message.CommitLogOffset,
            message.BornTimestamp,
            message.StoreTimestamp);
}
