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

using BenchmarkDotNet.Attributes;
using EventHorizon.RocketMQ.Remoting.Consumer;
using EventHorizon.RocketMQ.Remoting.Consumer.Pull;
using EventHorizon.RocketMQ.Remoting.Consumer.Push;
using Microsoft.Extensions.Logging.Abstractions;

namespace EventHorizon.RocketMQ.Benchmarks.Protocols.Remoting;

[MemoryDiagnoser]
public class RemotingPushDispatchBenchmarks
{
    private const int MessageCount = 4096;
    private const int BrokerCount = 3;
    private const int QueueCountPerBroker = 3;
    private const string Topic = "BenchmarkTopic";
    private static readonly byte[] EmptyBody = [];
    private static readonly IReadOnlyList<string> EmptyKeys = [];
    private static readonly IReadOnlyDictionary<string, string> EmptyProperties =
        new Dictionary<string, string>();
    private static readonly bool[][] CompletedStates = Enumerable.Range(0, 33)
        .Select(static count => Enumerable.Repeat(true, count).ToArray())
        .ToArray();

    private QueueWorkload[] _hotQueue = null!;
    private QueueWorkload[] _nineQueues = null!;

    [Params(1, 32)]
    public int ConsumeMessageBatchSize { get; set; }

    [Params(1, 8)]
    public int MaxConcurrency { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        _hotQueue = [CreateWorkload("broker-0", 0, MessageCount)];

        var queueCount = BrokerCount * QueueCountPerBroker;
        var messagesPerQueue = MessageCount / queueCount;
        var queuesWithExtraMessage = MessageCount % queueCount;
        _nineQueues = Enumerable.Range(0, queueCount)
            .Select(index => CreateWorkload(
                $"broker-{index / QueueCountPerBroker}",
                index % QueueCountPerBroker,
                messagesPerQueue + (index < queuesWithExtraMessage ? 1 : 0)))
            .ToArray();
    }

    [Benchmark(Baseline = true, OperationsPerInvoke = MessageCount)]
    public Task OneHotQueue() => DispatchAsync(_hotQueue);

    [Benchmark(OperationsPerInvoke = MessageCount)]
    public Task ThreeBrokersNineQueues() => DispatchAsync(_nineQueues);

    private async Task DispatchAsync(IReadOnlyList<QueueWorkload> workloads)
    {
        using var stopping = new CancellationTokenSource();
        var completion = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var consumed = 0;
        var processQueues = new ProcessQueue[workloads.Count];
        var dispatcher = new ConcurrentConsumeDispatcher(
            MaxConcurrency,
            ConsumeMessageBatchSize,
            (request, _) =>
            {
                request.ProcessQueue.CompleteConsumeRequest(
                    request,
                    CompletedStates[request.Entries.Count]);
                if (Interlocked.Add(ref consumed, request.Entries.Count) == MessageCount)
                {
                    completion.TrySetResult();
                }

                return ValueTask.CompletedTask;
            },
            static _ => { },
            stopping.Token,
            NullLogger.Instance);

        try
        {
            for (var index = 0; index < workloads.Count; index++)
            {
                var workload = workloads[index];
                var processQueue = new ProcessQueue(workload.Queue, stopping.Token);
                processQueues[index] = processQueue;
                processQueue.Initialize(0, forcePersist: false);
                processQueue.PutMessages(workload.Messages);
                processQueue.AdvanceNextPullOffset(workload.Messages.Count);
                dispatcher.NotifyReady(processQueue);
            }

            await completion.Task.ConfigureAwait(false);
            dispatcher.Complete();
            await Task.WhenAll(dispatcher.ConsumeLoopTasks).ConfigureAwait(false);
        }
        finally
        {
            dispatcher.Complete();
            await stopping.CancelAsync().ConfigureAwait(false);

            try
            {
                await Task.WhenAll(dispatcher.ConsumeLoopTasks).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (stopping.IsCancellationRequested)
            {
            }

            foreach (var processQueue in processQueues)
            {
                processQueue?.MarkDropped();
            }
        }
    }

    private static QueueWorkload CreateWorkload(string brokerName, int queueId, int messageCount)
    {
        var queue = new RemotingPullMessageQueue(Topic, queueId, brokerName, "127.0.0.1:10911");
        var messages = Enumerable.Range(0, messageCount)
            .Select(offset => new RemotingMessageView(
                Topic,
                EmptyBody,
                $"{brokerName}-{queueId}-{offset}",
                $"offset-{brokerName}-{queueId}-{offset}",
                null,
                EmptyKeys,
                EmptyProperties,
                1,
                null,
                queueId,
                brokerName,
                offset,
                offset,
                DateTimeOffset.UnixEpoch,
                DateTimeOffset.UnixEpoch))
            .ToArray();
        return new QueueWorkload(queue, messages);
    }

    private sealed record QueueWorkload(
        RemotingPullMessageQueue Queue,
        IReadOnlyList<RemotingMessageView> Messages);
}
