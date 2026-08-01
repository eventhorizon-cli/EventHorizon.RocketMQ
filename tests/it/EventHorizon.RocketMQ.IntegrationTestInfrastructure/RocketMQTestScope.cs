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

namespace EventHorizon.RocketMQ.IntegrationTestInfrastructure;

/// <summary>
/// Provides test-specific Broker resource names for one integration test.
/// </summary>
/// <remarks>
/// A scope does not own Broker resources and therefore has no disposal lifecycle. Its topics and groups remain until
/// the owning <see cref="RocketMQSingleBrokerContainerFixture"/> is disposed.
/// </remarks>
public sealed class RocketMQTestScope
{
    private readonly RocketMQSingleBrokerContainerFixture _fixture;
    private readonly string _suffix;

    internal RocketMQTestScope(RocketMQSingleBrokerContainerFixture fixture, string topic, string suffix)
    {
        _fixture = fixture;
        Topic = topic;
        _suffix = suffix;
    }

    /// <summary>
    /// Gets the unique normal topic or preconfigured special topic selected for the test.
    /// </summary>
    public string Topic { get; }

    /// <summary>
    /// Creates an isolated producer group name.
    /// </summary>
    /// <param name="role">The role label included in the group name.</param>
    /// <returns>A producer group name unique to this test scope.</returns>
    public string CreateProducerGroupName(string role)
    {
        return CreateGroupName(role);
    }

    /// <summary>
    /// Creates an isolated consumer group name.
    /// </summary>
    /// <param name="role">The role label included in the group name.</param>
    /// <returns>A consumer group name unique to this test scope.</returns>
    public string CreateConsumerGroupName(string role)
    {
        return CreateGroupName(role);
    }

    /// <summary>
    /// Creates and configures an isolated LitePush consumer group bound to this scope's parent topic.
    /// </summary>
    /// <param name="role">The role label included in the group name.</param>
    /// <param name="cancellationToken">The token used to cancel the Broker administration request.</param>
    /// <returns>A LitePush consumer group name unique to this test scope.</returns>
    public async Task<string> CreateLiteConsumerGroupAsync(string role, CancellationToken cancellationToken = default)
    {
        var group = CreateGroupName(role);
        await _fixture.CreateConsumerGroupAsync(group, Topic, cancellationToken).ConfigureAwait(false);
        return group;
    }

    private string CreateGroupName(string role)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(role);
        return $"rocketmq-dotnet-it-{role}-{_suffix}";
    }
}
