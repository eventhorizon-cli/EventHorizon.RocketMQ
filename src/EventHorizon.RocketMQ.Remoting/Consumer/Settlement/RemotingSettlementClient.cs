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

using EventHorizon.RocketMQ.Remoting.Consumer.Route;
using EventHorizon.RocketMQ.Remoting.Exceptions;
using EventHorizon.RocketMQ.Remoting.Instrumentation;
using EventHorizon.RocketMQ.Remoting.Protocol;

namespace EventHorizon.RocketMQ.Remoting.Consumer.Settlement;

internal sealed class RemotingSettlementClient : IRemotingSettlementClient
{
    private readonly RemotingConsumerSettings _settings;
    private readonly RemotingClientOptions _clientOptions;
    private readonly RemotingConsumerRouteResolver _routes;
    private readonly IRemotingClient _remotingClient;
    private readonly IRemotingRocketMQTelemetry _telemetry;

    public RemotingSettlementClient(
        RemotingConsumerSettings settings,
        RemotingClientOptions clientOptions,
        RemotingConsumerRouteResolver routes,
        IRemotingClient remotingClient,
        IRemotingRocketMQTelemetry telemetry)
    {
        ArgumentNullException.ThrowIfNull(settings);
        ArgumentNullException.ThrowIfNull(clientOptions);
        ArgumentNullException.ThrowIfNull(routes);
        ArgumentNullException.ThrowIfNull(remotingClient);
        ArgumentNullException.ThrowIfNull(telemetry);

        _settings = settings;
        _clientOptions = clientOptions;
        _routes = routes;
        _remotingClient = remotingClient;
        _telemetry = telemetry;
    }

    public async Task SendBackAsync(
        RemotingConsumerQueue queue,
        RemotingMessageView message,
        int delayLevel,
        int maxReconsumeTimes,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(queue);
        ArgumentNullException.ThrowIfNull(message);
        using var telemetry = _telemetry.StartSettle(
            delayLevel < 0 ? "reject" : "nack",
            message.Topic,
            _settings.GroupName,
            message.MessageId,
            queue.QueueId,
            message.Properties);
        try
        {
            var broker = await _routes.ResolveBrokerAsync(
                queue,
                useSuggestedBroker: false,
                cancellationToken).ConfigureAwait(false);
            var response = await _remotingClient.InvokeAsync(
                EndpointParser.Parse(broker.Address),
                new RemotingCommand(RequestCode.ConsumerSendMsgBack, new ConsumerSendMessageBackRequestHeader
                {
                    Offset = message.CommitLogOffset,
                    Group = GetConsumerGroup(),
                    DelayLevel = delayLevel,
                    OriginMsgId = message.Properties.TryGetValue("ORIGIN_MESSAGE_ID", out var originMessageId)
                        ? originMessageId
                        : message.MessageId,
                    OriginTopic = GetWireTopic(message.Topic),
                    UnitMode = _clientOptions.UnitMode,
                    MaxReconsumeTimes = maxReconsumeTimes,
                    Bname = queue.BrokerName
                }),
                _clientOptions.RequestTimeout,
                cancellationToken).ConfigureAwait(false);
            EnsureSuccess(response, "send message back");
            telemetry.Complete();
        }
        catch (Exception exception)
        {
            telemetry.Complete(exception);
            throw;
        }
    }

    private static void EnsureSuccess(RemotingCommand response, string operation)
    {
        if (response.Code != ResponseCodes.ResSuccess)
        {
            throw new RemotingCommandException(
                response.Code,
                response.Remark ?? $"Broker rejected the {operation} request.");
        }
    }

    private string GetConsumerGroup() => LegacyNamespace.Wrap(_clientOptions.Namespace, _settings.GroupName);

    private string GetWireTopic(string topic) => LegacyNamespace.Wrap(_clientOptions.Namespace, topic);
}
