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

using System.Net;
using System.Text;
using EventHorizon.RocketMQ.Remoting.Protocol;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;

namespace EventHorizon.RocketMQ.Remoting.IntegrationTests;

internal sealed class RemotingMessageRequestModeScope : IAsyncDisposable
{
    private static readonly TimeSpan RequestTimeout = TimeSpan.FromSeconds(10);

    private readonly IRemotingClient _client;
    private readonly EndPoint _brokerEndpoint;
    private readonly string _topic;
    private readonly string _consumerGroup;
    private int _disposed;

    private RemotingMessageRequestModeScope(
        IRemotingClient client,
        EndPoint brokerEndpoint,
        string topic,
        string consumerGroup)
    {
        _client = client;
        _brokerEndpoint = brokerEndpoint;
        _topic = topic;
        _consumerGroup = consumerGroup;
    }

    internal static async Task<RemotingMessageRequestModeScope> UsePopAsync(
        IServiceProvider provider,
        string brokerAddress,
        string topic,
        string consumerGroup,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(provider);
        ArgumentException.ThrowIfNullOrWhiteSpace(brokerAddress);
        ArgumentException.ThrowIfNullOrWhiteSpace(topic);
        ArgumentException.ThrowIfNullOrWhiteSpace(consumerGroup);

        var client = provider
            .GetRequiredKeyedService<RemotingClientRegistry>(Options.DefaultName)
            .SharedClient;
        var brokerEndpoint = EndpointParser.Parse(brokerAddress);
        await SetAsync(
            client,
            brokerEndpoint,
            topic,
            consumerGroup,
            "POP",
            popShareQueueNum: -1,
            cancellationToken).ConfigureAwait(false);
        return new RemotingMessageRequestModeScope(client, brokerEndpoint, topic, consumerGroup);
    }

    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
        {
            return;
        }

        await SetAsync(
            _client,
            _brokerEndpoint,
            _topic,
            _consumerGroup,
            "PULL",
            popShareQueueNum: 0,
            CancellationToken.None).ConfigureAwait(false);
    }

    private static async Task SetAsync(
        IRemotingClient client,
        EndPoint brokerEndpoint,
        string topic,
        string consumerGroup,
        string mode,
        int popShareQueueNum,
        CancellationToken cancellationToken)
    {
        var body = Encoding.UTF8.GetBytes(new
        {
            topic,
            consumerGroup,
            mode,
            popShareQueueNum
        }.ToJson());
        var response = await client.InvokeAsync(
            brokerEndpoint,
            new RemotingCommand(RequestCode.SetMessageRequestMode, new EmptyRequestHeader()) { Body = body },
            RequestTimeout,
            cancellationToken).ConfigureAwait(false);
        if (response.Code != ResponseCodes.ResSuccess)
        {
            throw new InvalidOperationException(
                $"Broker rejected request mode {mode} for {topic}/{consumerGroup}: " +
                $"{response.Code} {response.Remark}");
        }
    }

    private sealed class EmptyRequestHeader : CommandCustomHeader;
}
