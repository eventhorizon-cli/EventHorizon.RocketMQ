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

using EventHorizon.RocketMQ.Remoting.Protocol;
using Microsoft.Extensions.Logging;

namespace EventHorizon.RocketMQ.Remoting.Consumer;

internal sealed class RemotingRebalanceService : IRemotingRebalanceService, IAsyncDisposable
{
    private readonly object _sync = new();
    private readonly Dictionary<long, RemotingRebalanceParticipant> _participants = [];
    private readonly Dictionary<string, RemotingRebalanceParticipant> _participantsByGroup =
        new(StringComparer.Ordinal);
    private readonly ILogger<RemotingRebalanceService> _logger;
    private readonly TimeSpan _rebalanceInterval;
    private readonly TimeSpan _retryInterval;
    private readonly CancellationTokenSource _stopping = new();
    private readonly IDisposable _requestHandlerRegistration;
    private readonly Task _periodicTask;
    private readonly Lazy<Task> _disposeTask;
    private long _nextRegistrationId;
    private int _disposed;

    public RemotingRebalanceService(
        IRemotingRequestHandlerRegistry requestHandlers,
        ILogger<RemotingRebalanceService> logger,
        TimeSpan rebalanceInterval,
        TimeSpan retryInterval)
    {
        ArgumentNullException.ThrowIfNull(requestHandlers);
        ArgumentNullException.ThrowIfNull(logger);
        if (rebalanceInterval <= TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(nameof(rebalanceInterval));
        }

        if (retryInterval <= TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(nameof(retryInterval));
        }

        _logger = logger;
        _rebalanceInterval = rebalanceInterval;
        _retryInterval = retryInterval;
        _requestHandlerRegistration = requestHandlers.RegisterRequestHandler(
            RequestCode.NotifyConsumerIdsChanged,
            HandleConsumerIdsChangedAsync);
        _periodicTask = RunPeriodicAsync();
        _disposeTask = new Lazy<Task>(DisposeCoreAsync, LazyThreadSafetyMode.ExecutionAndPublication);
    }

    public IRemotingRebalanceRegistration Register(
        string consumerGroup,
        Func<CancellationToken, Task<bool>> rebalance)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(consumerGroup);
        ArgumentNullException.ThrowIfNull(rebalance);
        lock (_sync)
        {
            ObjectDisposedException.ThrowIf(_disposed != 0, this);
            if (_participantsByGroup.ContainsKey(consumerGroup))
            {
                throw new InvalidOperationException(
                    $"Consumer group '{consumerGroup}' is already registered with this physical remoting client.");
            }

            var registrationId = ++_nextRegistrationId;
            var participant = new RemotingRebalanceParticipant(
                consumerGroup,
                rebalance,
                _retryInterval,
                _logger,
                _stopping.Token);
            _participants.Add(registrationId, participant);
            _participantsByGroup.Add(consumerGroup, participant);
            return new RemotingRebalanceRegistration(this, registrationId, participant);
        }
    }

    public ValueTask DisposeAsync() => new(_disposeTask.Value);

    private async Task DisposeCoreAsync()
    {
        RemotingRebalanceParticipant[] participants;
        lock (_sync)
        {
            _disposed = 1;
            participants = _participants.Values.ToArray();
            _participants.Clear();
            _participantsByGroup.Clear();
        }

        _requestHandlerRegistration.Dispose();
        _stopping.Cancel();
        try
        {
            await _periodicTask.ConfigureAwait(false);
            await Task.WhenAll(participants.Select(static participant => participant.DisposeAsync().AsTask()))
                .ConfigureAwait(false);
        }
        finally
        {
            _stopping.Dispose();
        }
    }

    internal async ValueTask UnregisterAsync(
        long registrationId,
        RemotingRebalanceParticipant participant)
    {
        lock (_sync)
        {
            if (_participants.Remove(registrationId, out var current) && ReferenceEquals(current, participant))
            {
                _participantsByGroup.Remove(participant.ConsumerGroup);
            }
        }

        await participant.DisposeAsync().ConfigureAwait(false);
    }

    private ValueTask<RemotingRequestResult> HandleConsumerIdsChangedAsync(RemotingRequestContext context)
    {
        RemotingRebalanceParticipant[] participants;
        lock (_sync)
        {
            if (context.Request.ExtFields.TryGetValue("consumerGroup", out var consumerGroup) &&
                consumerGroup is string group)
            {
                participants = _participantsByGroup.TryGetValue(group, out var participant)
                    ? [participant]
                    : [];
            }
            else
            {
                participants = _participants.Values.ToArray();
            }
        }

        foreach (var participant in participants)
        {
            participant.Wakeup();
        }

        return ValueTask.FromResult(RemotingRequestResult.NoResponse);
    }

    private async Task RunPeriodicAsync()
    {
        while (!_stopping.IsCancellationRequested)
        {
            try
            {
                await Task.Delay(_rebalanceInterval, _stopping.Token).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (_stopping.IsCancellationRequested)
            {
                return;
            }

            RemotingRebalanceParticipant[] participants;
            lock (_sync)
            {
                participants = _participants.Values.ToArray();
            }

            foreach (var participant in participants)
            {
                participant.Wakeup();
            }
        }
    }
}
