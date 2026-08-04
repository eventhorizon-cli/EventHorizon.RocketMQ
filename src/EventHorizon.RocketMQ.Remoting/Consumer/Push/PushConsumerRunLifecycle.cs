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

using System.Runtime.ExceptionServices;
using EventHorizon.RocketMQ.Remoting.Consumer.Coordination;
using EventHorizon.RocketMQ.Remoting.Consumer.Coordination.Rebalance;
using EventHorizon.RocketMQ.Remoting.Consumer.Push.Assignment;
using EventHorizon.RocketMQ.Remoting.Consumer.Push.Processing;
using EventHorizon.RocketMQ.Remoting.Consumer.Push.Pull.Dispatch;
using EventHorizon.RocketMQ.Remoting.Consumer.Push.Pull.Offset;
using EventHorizon.RocketMQ.Remoting.Consumer.Push.Pull.Receive;
using EventHorizon.RocketMQ.Remoting.Consumer.Push.Pull.Settlement;
using EventHorizon.RocketMQ.Remoting.Instrumentation;
using Microsoft.Extensions.Logging;

namespace EventHorizon.RocketMQ.Remoting.Consumer.Push;

internal sealed class PushConsumerRunLifecycle
{
    private readonly RemotingPushConsumerOptions _options;
    private readonly IRemotingConsumerEngine _consumerEngine;
    private readonly IRemotingRebalanceService _rebalanceService;
    private readonly RemotingConsumerGroupClient _groupClient;
    private readonly PullOffsetManager _pullOffsetManager;
    private readonly PushAssignmentCoordinator _assignmentCoordinator;
    private readonly TimeProvider _timeProvider;
    private readonly IRemotingRocketMQTelemetry _telemetry;
    private readonly ILogger _logger;
    private readonly Func<IReadOnlyList<RemotingMessageView>, RemotingPushConsumeContext, CancellationToken,
        ValueTask<ConsumeResult>> _messageHandler;

    public PushConsumerRunLifecycle(
        RemotingPushConsumerOptions options,
        IRemotingConsumerEngine consumerEngine,
        IRemotingRebalanceService rebalanceService,
        RemotingConsumerGroupClient groupClient,
        PullOffsetManager pullOffsetManager,
        PushAssignmentCoordinator assignmentCoordinator,
        TimeProvider timeProvider,
        IRemotingRocketMQTelemetry telemetry,
        ILogger logger,
        Func<IReadOnlyList<RemotingMessageView>, RemotingPushConsumeContext, CancellationToken,
            ValueTask<ConsumeResult>> messageHandler)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(consumerEngine);
        ArgumentNullException.ThrowIfNull(rebalanceService);
        ArgumentNullException.ThrowIfNull(groupClient);
        ArgumentNullException.ThrowIfNull(pullOffsetManager);
        ArgumentNullException.ThrowIfNull(assignmentCoordinator);
        ArgumentNullException.ThrowIfNull(timeProvider);
        ArgumentNullException.ThrowIfNull(telemetry);
        ArgumentNullException.ThrowIfNull(logger);
        ArgumentNullException.ThrowIfNull(messageHandler);

        _options = options;
        _consumerEngine = consumerEngine;
        _rebalanceService = rebalanceService;
        _groupClient = groupClient;
        _pullOffsetManager = pullOffsetManager;
        _assignmentCoordinator = assignmentCoordinator;
        _timeProvider = timeProvider;
        _telemetry = telemetry;
        _logger = logger;
        _messageHandler = messageHandler;
    }

    public RemotingPushConsumerRun CreateRun()
    {
        _pullOffsetManager.BeginRun();
        var run = new RemotingPushConsumerRun(
            new RemotingConsumerGroupSession(
                _groupClient,
                _rebalanceService,
                _timeProvider.GetUtcNow().ToUnixTimeMilliseconds()),
            new PushMessageHandlerInvoker(
                _options,
                _messageHandler,
                _timeProvider,
                _telemetry,
                _logger));
        var sendBackSettlement = new PullSendBackSettlement(
            _options,
            _consumerEngine,
            _pullOffsetManager,
            _logger,
            run.Receivers.HasPullQueue,
            run.RequireRetryTopic,
            run.GroupSession.Wakeup);
        if (_options.ConsumeOrderly)
        {
            run.OrderlyPullReceiveLoop = new OrderlyPullReceiveLoop(
                _options,
                _consumerEngine,
                _pullOffsetManager,
                sendBackSettlement,
                run.MessageHandlerInvoker,
                _timeProvider,
                _logger);
        }
        else
        {
            run.PullDeliveryCoordinator = new PullDeliveryCoordinator(
                _options.PullMaxCachedMessages,
                _options.PullMaxCachedMessageBytes);
            run.ConcurrentPullReceiveLoop = new ConcurrentPullReceiveLoop(
                _options,
                _consumerEngine,
                _pullOffsetManager,
                run.PullDeliveryCoordinator,
                _logger);
            run.PullConsumeRequestProcessor = new PullConsumeRequestProcessor(
                _options,
                sendBackSettlement,
                run.MessageHandlerInvoker,
                run.PullDeliveryCoordinator,
                _logger);
        }

        run.ReceiverSupervisor = new PushReceiverSupervisor(
            run.Receivers,
            run.PullDeliveryCoordinator,
            run.Stopping.Token,
            receiver => _assignmentCoordinator.UnlockAsync(
                [receiver],
                oneway: true,
                CancellationToken.None),
            run.GroupSession.Wakeup,
            _logger);
        return run;
    }

    public async Task StartAsync(
        RemotingPushConsumerRun run,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(run);
        if (run.PullDeliveryCoordinator is { } dispatchScheduler)
        {
            dispatchScheduler.Start(
                _options.MaxConcurrency,
                _options.ConsumeMessageBatchSize,
                run.PullConsumeRequestProcessor!.ProcessAsync,
                run.Stopping.Token,
                _logger);
        }

        await _assignmentCoordinator.StartAsync(run, cancellationToken).ConfigureAwait(false);
    }

    public async Task ShutdownAsync(
        RemotingPushConsumerRun run,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(run);
        using var abandonHandlerResultsRegistration = cancellationToken.Register(
            run.MessageHandlerInvoker.AbandonResults);
        Task? detachedTasks = null;
        ExceptionDispatchInfo? failure = null;
        try
        {
            await run.GroupSession.StopRebalanceAsync().ConfigureAwait(false);
        }
        catch (Exception exception)
        {
            CaptureShutdownFailure(ref failure, exception, "stop rebalance");
        }

        var receiverSupervisor = run.ReceiverSupervisor ?? throw new InvalidOperationException(
            "The Push receiver supervisor has not been composed for this run.");
        var receivers = receiverSupervisor.BeginShutdown().ToArray();
        run.Stopping.Cancel();
        try
        {
            run.PullDeliveryCoordinator?.Complete();
        }
        catch (Exception exception)
        {
            CaptureShutdownFailure(ref failure, exception, "complete the PULL dispatcher");
        }

        foreach (var receiver in receivers)
        {
            try
            {
                run.Receivers.Drop(receiver, run.PullDeliveryCoordinator);
            }
            catch (Exception exception)
            {
                CaptureShutdownFailure(ref failure, exception, "stop a receiver");
            }
        }

        var activeTasks = receivers
            .Select(static receiver => receiver.Completion)
            .Concat(run.PullDeliveryCoordinator?.Tasks ?? [])
            .ToArray();
        var activeTasksCompletion = AwaitTasksAsync(activeTasks);
        try
        {
            await activeTasksCompletion.WaitAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            run.MessageHandlerInvoker.AbandonResults();
            detachedTasks = activeTasksCompletion;
        }
        catch (Exception exception)
        {
            CaptureShutdownFailure(ref failure, exception, "drain receiver and dispatch tasks");
        }

        try
        {
            await receiverSupervisor.DrainOwnedReceiverReleasesAsync().ConfigureAwait(false);
        }
        catch (Exception exception)
        {
            CaptureShutdownFailure(ref failure, exception, "drain supervisor-owned receiver releases");
        }

        try
        {
            await _assignmentCoordinator.UnlockAsync(
                receivers,
                oneway: false,
                CancellationToken.None).ConfigureAwait(false);
        }
        catch (Exception exception)
        {
            CaptureShutdownFailure(ref failure, exception, "unlock orderly PULL queues");
        }

        try
        {
            await run.GroupSession.UnregisterAsync().ConfigureAwait(false);
        }
        catch (Exception exception)
        {
            CaptureShutdownFailure(ref failure, exception, "unregister the consumer group");
        }

        try
        {
            await _pullOffsetManager.FlushAsync(CancellationToken.None).ConfigureAwait(false);
        }
        catch (Exception exception)
        {
            CaptureShutdownFailure(ref failure, exception, "flush PULL offsets");
        }

        try
        {
            await _consumerEngine.StopAsync(CancellationToken.None).ConfigureAwait(false);
        }
        catch (Exception exception)
        {
            CaptureShutdownFailure(ref failure, exception, "stop the consumer engine");
        }

        if (detachedTasks is null)
        {
            try
            {
                DisposeRunResources(run, receivers);
            }
            catch (Exception exception)
            {
                CaptureShutdownFailure(ref failure, exception, "dispose run resources");
            }
        }
        else
        {
            _ = ObserveDetachedConsumeLoopsAsync(run, receivers, detachedTasks);
        }

        if (detachedTasks is not null)
        {
            if (failure is not null)
            {
                _logger.LogWarning(
                    failure.SourceException,
                    "Legacy Push shutdown encountered a failure before caller cancellation detached active tasks");
            }

            cancellationToken.ThrowIfCancellationRequested();
        }

        failure?.Throw();
    }

    private void CaptureShutdownFailure(
        ref ExceptionDispatchInfo? firstFailure,
        Exception exception,
        string operation)
    {
        if (firstFailure is null)
        {
            firstFailure = ExceptionDispatchInfo.Capture(exception);
            return;
        }

        _logger.LogWarning(
            exception,
            "An additional failure occurred while attempting to {ShutdownOperation} during legacy Push shutdown",
            operation);
    }

    private static async Task AwaitTasksAsync(IEnumerable<Task> tasks)
    {
        try
        {
            await Task.WhenAll(tasks).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
        }
    }

    private async Task ObserveDetachedConsumeLoopsAsync(
        RemotingPushConsumerRun run,
        IRemotingPushReceiver[] receivers,
        Task consumeLoops)
    {
        try
        {
            await consumeLoops.ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
        }
        catch (Exception exception)
        {
            _logger.LogDebug(
                exception,
                "A legacy message processing task completed with an exception after consumer shutdown was canceled");
        }
        finally
        {
            try
            {
                DisposeRunResources(run, receivers);
            }
            catch (Exception exception)
            {
                _logger.LogWarning(
                    exception,
                    "Unable to dispose legacy Push run resources after detached message processing completed");
            }
        }
    }

    private static void DisposeRunResources(
        RemotingPushConsumerRun run,
        IEnumerable<IRemotingPushReceiver> receivers)
    {
        PushReceiverRegistry.DisposeAll(receivers);
        run.Dispose();
    }
}
