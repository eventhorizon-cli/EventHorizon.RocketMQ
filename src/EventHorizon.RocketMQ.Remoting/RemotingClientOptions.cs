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
using System.Net.Security;

namespace EventHorizon.RocketMQ.Remoting;

/// <summary>
/// Provides connection, identity, security, and transport options for a RocketMQ classic remoting client.
/// </summary>
public sealed class RemotingClientOptions : RocketMQClientOptions
{
    internal const int RemotingMessageFrameReserve = 64 * 1024;
    internal const int MinimumRemotingFrameSize = RemotingMessageFrameReserve + (2 * sizeof(int));
    internal const int DefaultRemotingFrameSize = 16 * 1024 * 1024;

    /// <summary>
    /// Gets or sets the semicolon-separated NameServer addresses used to discover brokers.
    /// </summary>
    public string NamesrvAddr { get; set; } = "localhost:9876";

    /// <summary>
    /// Gets or sets the interval at which topic route information is refreshed from a NameServer.
    /// </summary>
    public TimeSpan PollNameServerInterval { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>
    /// Gets or sets the timeout for each NameServer route request.
    /// </summary>
    public TimeSpan NameServerRequestTimeout { get; set; } = TimeSpan.FromSeconds(3);

    /// <summary>
    /// Gets or sets the interval between classic remoting heartbeats sent to brokers.
    /// </summary>
    public TimeSpan HeartbeatBrokerInterval { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>
    /// Gets or sets whether classic remoting requests use RocketMQ unit mode.
    /// </summary>
    public bool UnitMode { get; set; }

    /// <summary>
    /// Gets or sets the optional RocketMQ unit name appended to the classic client identifier.
    /// </summary>
    public string? UnitName { get; set; }

    /// <summary>
    /// Gets or sets a callback that configures TLS for each new classic remoting connection.
    /// </summary>
    /// <remarks>
    /// The callback is used when <see cref="RocketMQClientOptions.UseTLS"/> is enabled. The client supplies a fresh
    /// options instance whose target host is initialized from the broker endpoint. The callback may configure a
    /// different target host, custom trust, client certificates, certificate revocation, or enabled protocols. It may
    /// be invoked concurrently.
    /// </remarks>
    public Action<EndPoint, SslClientAuthenticationOptions>? ConfigureLegacySslOptions { get; set; }

    /// <summary>
    /// Gets or sets the maximum total frame size, in bytes, accepted by the classic remoting transport.
    /// </summary>
    /// <remarks>
    /// This includes the frame-length field, header-length field, serialized command header, and body.
    /// The value should match the broker's <c>com.rocketmq.remoting.frameMaxLength</c> setting.
    /// At least 64 KiB plus the protocol fields is required so commands retain a positive body budget.
    /// </remarks>
    public int MaxRemotingFrameSize { get; set; } = DefaultRemotingFrameSize;

    internal string BuildRemotingClientId()
    {
        var clientId = BuildClientId();
        return string.IsNullOrWhiteSpace(UnitName) ? clientId : $"{clientId}@{UnitName}";
    }

    internal RemotingClientOptions ForLogicalClient(string logicalClient) =>
        (RemotingClientOptions)CloneForLogicalClient(logicalClient);
}
