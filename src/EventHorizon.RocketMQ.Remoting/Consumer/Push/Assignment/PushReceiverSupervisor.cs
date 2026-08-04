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

using EventHorizon.RocketMQ.Remoting.Consumer.Push.Pull.Dispatch;
using Microsoft.Extensions.Logging;

namespace EventHorizon.RocketMQ.Remoting.Consumer.Push.Assignment;

internal sealed class PushReceiverSupervisor
{
    private readonly PushReceiverRegistry _registry;
    private readonly PullDeliveryCoordinator? _dispatchScheduler;
    private readonly CancellationToken _stopping;
    private readonly Func<IRemotingPushReceiver, Task> _releaseReceiver;
    private readonly Action _wakeup;
    private readonly ILogger _logger;
    // Shutdown and unexpected completion share this gate so exactly one path owns receiver release and disposal.
    private readonly object _ownershipGate = new();
    private readonly HashSet<Task> _ownedReceiverReleases = [];
    private bool _shutdownStarted;

    public PushReceiverSupervisor(
        PushReceiverRegistry registry,
        PullDeliveryCoordinator? dispatchScheduler,
        CancellationToken stopping,
        Func<IRemotingPushReceiver, Task> releaseReceiver,
        Action wakeup,
        ILogger logger)
    {
        ArgumentNullException.ThrowIfNull(registry);
        ArgumentNullException.ThrowIfNull(releaseReceiver);
        ArgumentNullException.ThrowIfNull(wakeup);
        ArgumentNullException.ThrowIfNull(logger);

        _registry = registry;
        _dispatchScheduler = dispatchScheduler;
        _stopping = stopping;
        _releaseReceiver = releaseReceiver;
        _wakeup = wakeup;
        _logger = logger;
    }

    public Task Observe(PushReceiverKey key, IRemotingPushReceiver receiver)
    {
        ArgumentNullException.ThrowIfNull(receiver);
        return ObserveAsync(key, receiver);
    }

    public IReadOnlyList<IRemotingPushReceiver> BeginShutdown()
    {
        lock (_ownershipGate)
        {
            _shutdownStarted = true;
            return _registry.TakeAll();
        }
    }

    public Task DrainOwnedReceiverReleasesAsync()
    {
        lock (_ownershipGate)
        {
            return Task.WhenAll(_ownedReceiverReleases);
        }
    }

    private async Task ObserveAsync(PushReceiverKey key, IRemotingPushReceiver receiver)
    {
        var receiverCancellation = receiver.Cancellation;
        Exception? receiverFailure = null;
        try
        {
            await receiver.Completion.ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (receiverCancellation.IsCancellationRequested)
        {
            return;
        }
        catch (Exception exception)
        {
            receiverFailure = exception;
        }

        TaskCompletionSource ownershipCompletion;
        lock (_ownershipGate)
        {
            if (_shutdownStarted || _stopping.IsCancellationRequested || !_registry.TryRemove(key, receiver))
            {
                return;
            }

            ownershipCompletion = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            _ownedReceiverReleases.Add(ownershipCompletion.Task);
        }

        try
        {
            try
            {
                _registry.Drop(receiver, _dispatchScheduler);
            }
            catch (Exception exception)
            {
                _logger.LogWarning(
                    exception,
                    "Unable to stop an unexpectedly completed legacy Push receiver for {Topic}/{BrokerName}/{QueueId}",
                    receiver.Assignment.Topic,
                    receiver.Assignment.BrokerName,
                    receiver.Assignment.QueueId);
            }

            try
            {
                await _releaseReceiver(receiver).ConfigureAwait(false);
            }
            catch (Exception exception)
            {
                _logger.LogWarning(
                    exception,
                    "Unable to release an unexpectedly completed legacy Push receiver for {Topic}/{BrokerName}/{QueueId}",
                    receiver.Assignment.Topic,
                    receiver.Assignment.BrokerName,
                    receiver.Assignment.QueueId);
            }

            try
            {
                receiver.Dispose();
            }
            catch (Exception exception)
            {
                _logger.LogWarning(
                    exception,
                    "Unable to dispose an unexpectedly completed legacy Push receiver for {Topic}/{BrokerName}/{QueueId}",
                    receiver.Assignment.Topic,
                    receiver.Assignment.BrokerName,
                    receiver.Assignment.QueueId);
            }
        }
        finally
        {
            ownershipCompletion.TrySetResult();
            lock (_ownershipGate)
            {
                _ownedReceiverReleases.Remove(ownershipCompletion.Task);
            }
        }

        lock (_ownershipGate)
        {
            if (_shutdownStarted || _stopping.IsCancellationRequested)
            {
                return;
            }
        }

        if (receiverFailure is null)
        {
            _logger.LogWarning(
                "Legacy Push receiver stopped unexpectedly for {Topic}/{BrokerName}/{QueueId}; reconciling a replacement",
                receiver.Assignment.Topic,
                receiver.Assignment.BrokerName,
                receiver.Assignment.QueueId);
        }
        else
        {
            _logger.LogWarning(
                receiverFailure,
                "Legacy Push receiver failed for {Topic}/{BrokerName}/{QueueId}; reconciling a replacement",
                receiver.Assignment.Topic,
                receiver.Assignment.BrokerName,
                receiver.Assignment.QueueId);
        }

        try
        {
            _wakeup();
        }
        catch (Exception exception)
        {
            _logger.LogWarning(
                exception,
                "Unable to wake legacy Push reconciliation after receiver completion for {Topic}/{BrokerName}/{QueueId}",
                receiver.Assignment.Topic,
                receiver.Assignment.BrokerName,
                receiver.Assignment.QueueId);
        }
    }
}
