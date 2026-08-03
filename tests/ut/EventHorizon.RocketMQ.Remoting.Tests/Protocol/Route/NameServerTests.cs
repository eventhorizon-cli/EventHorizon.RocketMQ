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
using EventHorizon.RocketMQ.Remoting.Exceptions;
using EventHorizon.RocketMQ.Remoting.Protocol;
using EventHorizon.RocketMQ.Remoting.Protocol.Route;
using Microsoft.Extensions.Options;
using Moq;
using Xunit;

namespace EventHorizon.RocketMQ.Remoting.Tests.Protocol.Route;

public sealed class NameServerTests
{
    [Fact]
    public async Task GetTopicRouteInfoAsync_MissingNameServerAddresses_Rejects()
    {
        var remoting = new Mock<IRemotingClient>(MockBehavior.Strict);
        var nameServer = new NameServer(remoting.Object, Options.Create(new RemotingClientOptions
        {
            NamesrvAddr = " ; "
        }));

        var exception = await Assert.ThrowsAsync<RocketMQClientException>(
            () => nameServer.GetTopicRouteInfoAsync(
                "orders",
                TimeSpan.FromSeconds(1),
                cancellationToken: TestContext.Current.CancellationToken));

        Assert.Contains("No NameServer address", exception.Message, StringComparison.Ordinal);
        remoting.VerifyNoOtherCalls();
    }

    [Fact]
    public async Task GetTopicRouteInfoAsync_ConfiguredNameServers_FailsOverAcrossAll()
    {
        var requests = new List<RemotingCommand>();
        var remoting = new Mock<IRemotingClient>(MockBehavior.Strict);
        remoting
            .Setup(value => value.InvokeAsync(
                It.IsAny<EndPoint>(),
                It.IsAny<RemotingCommand>(),
                It.IsAny<TimeSpan>(),
                It.IsAny<CancellationToken>()))
            .Returns<EndPoint, RemotingCommand, TimeSpan, CancellationToken>((_, request, _, _) =>
            {
                requests.Add(request);
                return requests.Count == 1
                    ? Task.FromException<RemotingCommand>(new IOException("NameServer is unavailable."))
                    : Task.FromResult(new RemotingCommand
                    {
                        Code = ResponseCodes.ResSuccess,
                        Body = Encoding.UTF8.GetBytes("{\"brokerDatas\":[],\"queueDatas\":[]}")
                    });
            });
        var nameServer = new NameServer(remoting.Object, Options.Create(new RemotingClientOptions
        {
            NamesrvAddr = "127.0.0.1:19876;127.0.0.1:29876"
        }));

        var route = await nameServer.GetTopicRouteInfoAsync(
            "orders",
            TimeSpan.FromSeconds(1),
            cancellationToken: TestContext.Current.CancellationToken);

        Assert.Empty(route.BrokerDatas);
        Assert.Empty(route.QueueDatas);
        Assert.Equal(2, requests.Count);
        Assert.Equal(2, requests.Select(static request => request.Opaque).Distinct().Count());
        remoting.VerifyAll();
    }

    [Fact]
    public async Task GetTopicRouteInfoAsync_LastFailureAfterAllNameServersRoute_ReportsLastFailureAndRejects()
    {
        var calls = 0;
        var remoting = new Mock<IRemotingClient>(MockBehavior.Strict);
        remoting
            .Setup(value => value.InvokeAsync(
                It.IsAny<EndPoint>(),
                It.IsAny<RemotingCommand>(),
                It.IsAny<TimeSpan>(),
                It.IsAny<CancellationToken>()))
            .Returns(() => Task.FromResult(++calls == 1
                ? new RemotingCommand
                {
                    Code = ResponseCodes.ResNoPermission,
                    Remark = "denied"
                }
                : new RemotingCommand
                {
                    Code = ResponseCodes.ResSuccess
                }));
        var nameServer = new NameServer(remoting.Object, Options.Create(new RemotingClientOptions
        {
            NamesrvAddr = "127.0.0.1:19876;127.0.0.1:29876"
        }));

        var exception = await Assert.ThrowsAsync<RocketMQClientException>(
            () => nameServer.GetTopicRouteInfoAsync(
                "orders",
                TimeSpan.FromSeconds(1),
                cancellationToken: TestContext.Current.CancellationToken));

        Assert.Equal(2, calls);
        Assert.IsType<InvalidDataException>(exception.InnerException);
        Assert.Contains("any configured NameServer", exception.Message, StringComparison.Ordinal);
        remoting.VerifyAll();
        remoting.VerifyNoOtherCalls();
    }

    [Fact]
    public async Task GetTopicRouteInfoAsync_CallerCancellationWithoutCallingNameServer_Propagates()
    {
        var remoting = new Mock<IRemotingClient>(MockBehavior.Strict);
        var nameServer = new NameServer(remoting.Object, Options.Create(new RemotingClientOptions
        {
            NamesrvAddr = "127.0.0.1:19876"
        }));
        using var cancellation = new CancellationTokenSource();
        cancellation.Cancel();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => nameServer.GetTopicRouteInfoAsync(
                "orders",
                TimeSpan.FromSeconds(1),
                cancellationToken: cancellation.Token));

        remoting.VerifyNoOtherCalls();
    }

    [Fact]
    public async Task GetTopicRouteInfoAsync_CancellationRaisedDuringRequest_Propagates()
    {
        using var cancellation = new CancellationTokenSource();
        var remoting = new Mock<IRemotingClient>(MockBehavior.Strict);
        remoting
            .Setup(value => value.InvokeAsync(
                It.IsAny<EndPoint>(),
                It.IsAny<RemotingCommand>(),
                It.IsAny<TimeSpan>(),
                cancellation.Token))
            .Returns(() =>
            {
                cancellation.Cancel();
                return Task.FromCanceled<RemotingCommand>(cancellation.Token);
            });
        var nameServer = new NameServer(remoting.Object, Options.Create(new RemotingClientOptions
        {
            NamesrvAddr = "127.0.0.1:19876"
        }));

        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => nameServer.GetTopicRouteInfoAsync(
                "orders",
                TimeSpan.FromSeconds(1),
                cancellationToken: cancellation.Token));

        remoting.VerifyAll();
        remoting.VerifyNoOtherCalls();
    }
}
