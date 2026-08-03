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

using System.Globalization;

namespace EventHorizon.RocketMQ.Remoting.Consumer.Push.Pop;

/// <summary>
/// Represents the broker-issued receipt required to acknowledge or extend the invisibility of a POP message.
/// </summary>
internal sealed class RemotingPopReceipt
{
    internal RemotingPopReceipt(
        string topic,
        string brokerName,
        string brokerAddress,
        int queueId,
        long checkpointOffset,
        long queueOffset,
        DateTimeOffset popTime,
        TimeSpan invisibleTime,
        int reviveQueueId,
        string extraInfo,
        RemotingPopRetryTopicKind retryTopicKind = RemotingPopRetryTopicKind.Normal)
    {
        Topic = topic;
        BrokerName = brokerName;
        BrokerAddress = brokerAddress;
        QueueId = queueId;
        CheckpointOffset = checkpointOffset;
        QueueOffset = queueOffset;
        PopTime = popTime;
        InvisibleTime = invisibleTime;
        ReviveQueueId = reviveQueueId;
        ExtraInfo = extraInfo;
        RetryTopicKind = retryTopicKind;
    }

    /// <summary>
    /// Gets the logical topic to acknowledge.
    /// </summary>
    public string Topic { get; }

    /// <summary>
    /// Gets the broker that issued the receipt.
    /// </summary>
    public string BrokerName { get; }

    internal string BrokerAddress { get; }

    /// <summary>
    /// Gets the physical queue identifier.
    /// </summary>
    public int QueueId { get; }

    /// <summary>
    /// Gets the checkpoint's first queue offset.
    /// </summary>
    public long CheckpointOffset { get; }

    /// <summary>
    /// Gets the offset of the delivered message within the physical queue.
    /// </summary>
    public long QueueOffset { get; }

    /// <summary>
    /// Gets the broker time at which the message became invisible.
    /// </summary>
    public DateTimeOffset PopTime { get; }

    /// <summary>
    /// Gets the invisibility duration granted by the broker.
    /// </summary>
    public TimeSpan InvisibleTime { get; }

    /// <summary>
    /// Gets the revive queue identifier selected by the broker.
    /// </summary>
    public int ReviveQueueId { get; }

    internal string ExtraInfo { get; }

    internal RemotingPopRetryTopicKind RetryTopicKind { get; }

    internal RemotingPopReceipt Renew(
        long popTimeMilliseconds,
        long invisibleTimeMilliseconds,
        int reviveQueueId)
    {
        if (popTimeMilliseconds <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(popTimeMilliseconds));
        }

        if (invisibleTimeMilliseconds <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(invisibleTimeMilliseconds));
        }

        if (reviveQueueId < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(reviveQueueId));
        }

        var popTime = DateTimeOffset.FromUnixTimeMilliseconds(popTimeMilliseconds);
        var invisibleTime = TimeSpan.FromMilliseconds(invisibleTimeMilliseconds);
        var checkpointOffset = QueueOffset;
        var extraInfo = string.Join(
            ' ',
            checkpointOffset.ToString(CultureInfo.InvariantCulture),
            popTimeMilliseconds.ToString(CultureInfo.InvariantCulture),
            invisibleTimeMilliseconds.ToString(CultureInfo.InvariantCulture),
            reviveQueueId.ToString(CultureInfo.InvariantCulture),
            GetRetryTopicMarker(RetryTopicKind),
            BrokerName,
            QueueId.ToString(CultureInfo.InvariantCulture),
            QueueOffset.ToString(CultureInfo.InvariantCulture));
        return new RemotingPopReceipt(
            Topic,
            BrokerName,
            BrokerAddress,
            QueueId,
            checkpointOffset,
            QueueOffset,
            popTime,
            invisibleTime,
            reviveQueueId,
            extraInfo,
            RetryTopicKind);
    }

    internal string GetWireTopic(string wireTopic, string wireConsumerGroup) => RetryTopicKind switch
    {
        RemotingPopRetryTopicKind.Normal => wireTopic,
        RemotingPopRetryTopicKind.RetryV1 => $"%RETRY%{wireConsumerGroup}_{wireTopic}",
        RemotingPopRetryTopicKind.RetryV2 => $"%RETRY%{wireConsumerGroup}+{wireTopic}",
        _ => throw new InvalidOperationException($"Unsupported POP retry topic kind '{RetryTopicKind}'.")
    };

    private static string GetRetryTopicMarker(RemotingPopRetryTopicKind retryTopicKind) => retryTopicKind switch
    {
        RemotingPopRetryTopicKind.Normal => "0",
        RemotingPopRetryTopicKind.RetryV1 => "1",
        RemotingPopRetryTopicKind.RetryV2 => "2",
        _ => throw new InvalidOperationException($"Unsupported POP retry topic kind '{retryTopicKind}'.")
    };
}
