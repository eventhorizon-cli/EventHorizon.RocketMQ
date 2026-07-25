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

using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using EventHorizon.RocketMQ.Exceptions;
using EventHorizon.RocketMQ.Remoting.Exceptions;
using EventHorizon.RocketMQ.Remoting.Protocol;
using Microsoft.Extensions.Options;

namespace EventHorizon.RocketMQ.Remoting.Protocol.Route;
internal sealed class NameServer : INameServer
{
    private readonly IRemotingClient _remotingClient;
    private readonly RemotingClientOptions _options;
    private int _nameServerIndex = -1;

    public NameServer(IRemotingClient remotingClient, IOptions<RemotingClientOptions> options)
    {
        _remotingClient = remotingClient;
        _options = options.Value;
    }

    public async Task<TopicRouteData> GetTopicRouteInfoAsync(
        string topic,
        TimeSpan timeout,
        HashSet<int>? logicalQueueIdsFilter = null,
        CancellationToken cancellationToken = default)
    {
        var addresses = _options.NamesrvAddr.Split(
            ';',
            StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
        if (addresses.Length == 0)
        {
            throw new RocketMQClientException("No NameServer address is configured.");
        }

        Exception? lastException = null;
        var start = (Interlocked.Increment(ref _nameServerIndex) & int.MaxValue) % addresses.Length;
        for (var attempt = 0; attempt < addresses.Length; attempt++)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var address = addresses[(start + attempt) % addresses.Length];
            var request = new RemotingCommand(RequestCode.GetRouteInfoByTopic, new GetRouteInfoRequestHeader
            {
                Topic = topic,
                SysFlag = MessageSysFlag.LogicalQueueFlag,
                LogicalQueueIdsFilter = logicalQueueIdsFilter
            });
            try
            {
                var response = await _remotingClient.InvokeAsync(
                    EndpointParser.Parse(address),
                    request,
                    timeout,
                    cancellationToken).ConfigureAwait(false);
                if (response.Code != ResponseCodes.ResSuccess)
                {
                    lastException = new RemotingCommandException(
                        response.Code,
                        response.Remark ?? $"NameServer '{address}' rejected route request for topic '{topic}'.");
                    continue;
                }

                if (response.Body is null || response.Body.Length == 0)
                {
                    lastException = new InvalidDataException(
                        $"NameServer '{address}' returned an empty route for topic '{topic}'.");
                    continue;
                }

                return Encoding.UTF8.GetString(response.Body).FromJson<TopicRouteData>();
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception exception)
            {
                lastException = exception;
            }
        }

        throw new RocketMQClientException(
            $"Unable to query route for topic '{topic}' from any configured NameServer.",
            lastException);
    }
}
