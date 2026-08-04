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

using EventHorizon.RocketMQ.Remoting.Consumer.Coordination;
using EventHorizon.RocketMQ.Remoting.Consumer.Coordination.Rebalance;
using EventHorizon.RocketMQ.Remoting.Consumer.LitePull.Assignment;
using EventHorizon.RocketMQ.Remoting.Consumer.LitePull.Offset;
using EventHorizon.RocketMQ.Remoting.Consumer.LitePull.Receive;
using Microsoft.Extensions.Logging;

namespace EventHorizon.RocketMQ.Remoting.Consumer.LitePull;

internal sealed class LitePullConsumerRunLifecycle
{
    private readonly RemotingLitePullConsumerOptions _options;
    private readonly RemotingClientOptions _clientOptions;
    private readonly IRemotingConsumerEngine _consumerEngine;
    private readonly IRemotingRebalanceService _rebalanceService;
    private readonly RemotingConsumerGroupClient _groupClient;
    private readonly LitePullSubscriptionState _subscriptions;
    private readonly TimeProvider _timeProvider;
    private readonly ILogger _logger;

    public LitePullConsumerRunLifecycle(
        RemotingLitePullConsumerOptions options,
        RemotingClientOptions clientOptions,
        IRemotingConsumerEngine consumerEngine,
        IRemotingRebalanceService rebalanceService,
        RemotingConsumerGroupClient groupClient,
        LitePullSubscriptionState subscriptions,
        TimeProvider timeProvider,
        ILogger logger)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(clientOptions);
        ArgumentNullException.ThrowIfNull(consumerEngine);
        ArgumentNullException.ThrowIfNull(rebalanceService);
        ArgumentNullException.ThrowIfNull(groupClient);
        ArgumentNullException.ThrowIfNull(subscriptions);
        ArgumentNullException.ThrowIfNull(timeProvider);
        ArgumentNullException.ThrowIfNull(logger);

        _options = options;
        _clientOptions = clientOptions;
        _consumerEngine = consumerEngine;
        _rebalanceService = rebalanceService;
        _groupClient = groupClient;
        _subscriptions = subscriptions;
        _timeProvider = timeProvider;
        _logger = logger;
    }

    public async Task<RemotingLitePullConsumerRun> StartAsync(CancellationToken cancellationToken)
    {
        var run = CreateRun();
        try
        {
            await _consumerEngine.StartAsync(cancellationToken).ConfigureAwait(false);
            run.AutoCommitTask = _options.EnableAutoCommit
                ? run.CommitLoop.RunAsync(run)
                : Task.CompletedTask;
            await run.AssignmentCoordinator.StartAsync(run, cancellationToken).ConfigureAwait(false);
            return run;
        }
        catch
        {
            try
            {
                await ShutdownAsync(run).ConfigureAwait(false);
            }
            catch (Exception exception)
            {
                _logger.LogWarning(exception, "Unable to clean up a failed legacy Lite Pull consumer start");
            }

            throw;
        }
    }

    public async Task ShutdownAsync(RemotingLitePullConsumerRun run)
    {
        ArgumentNullException.ThrowIfNull(run);
        run.PublicOperations.Close();
        run.OperationGate.Close();
        var receivers = run.CancelAllQueues();
        run.Stopping.Cancel();
        try
        {
            try
            {
                await run.GroupSession.StopRebalanceAsync().ConfigureAwait(false);
            }
            finally
            {
                await LitePullReceiverRegistry.AwaitAsync(receivers).ConfigureAwait(false);
            }

            try
            {
                await run.AutoCommitTask.ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
            }

            await run.OperationGate.DrainAsync().ConfigureAwait(false);
            await run.PublicOperations.DrainAsync().ConfigureAwait(false);
            if (_options.EnableAutoCommit)
            {
                await run.CommitLoop.CommitFinalAsync(run).ConfigureAwait(false);
            }
        }
        finally
        {
            try
            {
                await run.GroupSession.UnregisterAsync().ConfigureAwait(false);
            }
            finally
            {
                try
                {
                    await _consumerEngine.StopAsync(CancellationToken.None).ConfigureAwait(false);
                }
                finally
                {
                    run.Dispose();
                }
            }
        }
    }

    private RemotingLitePullConsumerRun CreateRun()
    {
        var queueReceiver = new LitePullQueueReceiver(
            _consumerEngine,
            _options.PullBatchSize,
            _options.MaxMessageBytes,
            _logger);
        var assignmentReconciler = new LitePullAssignmentReconciler(
            _options,
            _consumerEngine,
            queueReceiver,
            _logger);
        var assignmentCoordinator = new LitePullAssignmentCoordinator(
            _options,
            _clientOptions,
            _consumerEngine,
            _subscriptions,
            assignmentReconciler,
            _logger);
        var commitLoop = new LitePullCommitLoop(
            _consumerEngine,
            _options.AutoCommitInterval,
            _timeProvider,
            _options.GroupName,
            _logger);
        return new RemotingLitePullConsumerRun(
            new RemotingConsumerGroupSession(
                _groupClient,
                _rebalanceService,
                _timeProvider.GetUtcNow().ToUnixTimeMilliseconds()),
            new LitePullDeliveryState(
                new LitePullBuffer(_options.MaxCachedMessages, _options.MaxCachedMessageBytes)),
            assignmentCoordinator,
            assignmentReconciler,
            commitLoop);
    }
}
