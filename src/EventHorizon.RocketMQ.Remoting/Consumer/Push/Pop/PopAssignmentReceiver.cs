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

using EventHorizon.RocketMQ.Remoting.Consumer.Push.Assignment;
using Microsoft.Extensions.Logging;

namespace EventHorizon.RocketMQ.Remoting.Consumer.Push.Pop;

internal sealed class PopAssignmentReceiver : IRemotingPushReceiver
{
    private readonly RemotingPushConsumerOptions _options;
    private readonly PopWireClient _popClient;
    private readonly TimeProvider _timeProvider;
    private readonly ILogger _logger;
    private readonly FilterExpression _filter;
    private readonly PopDeliveryProcessor _deliveryProcessor;
    private readonly CancellationTokenSource _stopping;

    public PopAssignmentReceiver(
        PushReceiveTarget target,
        RemotingPushConsumerOptions options,
        PopWireClient popClient,
        TimeProvider timeProvider,
        ILogger logger,
        FilterExpression filter,
        PopDeliveryProcessor deliveryProcessor,
        CancellationToken stopping)
    {
        ArgumentNullException.ThrowIfNull(target);
        if (target.Assignment.Mode != RemotingPushReceiveMode.Pop)
        {
            throw new ArgumentException("A POP receiver requires a POP assignment.", nameof(target));
        }

        Target = target;
        _options = options;
        _popClient = popClient;
        _timeProvider = timeProvider;
        _logger = logger;
        _filter = filter;
        _deliveryProcessor = deliveryProcessor;
        _stopping = CancellationTokenSource.CreateLinkedTokenSource(stopping);
    }

    public RemotingPushAssignment Assignment => Target.Assignment;

    public PushReceiveTarget Target { get; }

    public CancellationToken Cancellation => _stopping.Token;

    public Task Completion { get; private set; } = Task.CompletedTask;

    public void Start()
    {
        if (!Completion.IsCompleted)
        {
            throw new InvalidOperationException("The POP receiver has already been started.");
        }

        Completion = RunAsync();
    }

    public void Stop() => _stopping.Cancel();

    public void Dispose() => _stopping.Dispose();

    private async Task RunAsync()
    {
        while (!_stopping.IsCancellationRequested)
        {
            try
            {
                var result = await _popClient.PopAsync(
                    Assignment,
                    Target.Broker.Address,
                    _filter,
                    _stopping.Token).ConfigureAwait(false);
                var deliveries = result.Messages
                    .Chunk(_options.ConsumeMessageBatchSize)
                    .Select(messages => _deliveryProcessor.ProcessBatchAsync(messages, _stopping.Token))
                    .ToArray();
                await Task.WhenAll(deliveries).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (_stopping.IsCancellationRequested)
            {
                return;
            }
            catch (Exception exception)
            {
                _logger.LogWarning(
                    exception,
                    "Legacy POP failed for {Topic}/{BrokerName}/{QueueId}; retrying",
                    Assignment.Topic,
                    Assignment.BrokerName,
                    Assignment.QueueId);
                await Task.Delay(_options.RetryDelay, _timeProvider, _stopping.Token).ConfigureAwait(false);
            }
        }
    }
}
