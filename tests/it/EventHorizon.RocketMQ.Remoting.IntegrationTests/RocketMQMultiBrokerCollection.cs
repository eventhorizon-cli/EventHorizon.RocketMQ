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

using EventHorizon.RocketMQ.IntegrationTestInfrastructure;
using Xunit;

namespace EventHorizon.RocketMQ.Remoting.IntegrationTests;

/// <summary>
/// Defines the test collection that directly owns the Remoting multi-Broker topology.
/// </summary>
/// <remarks>
/// Direct collection ownership lets xUnit create the topology only when selected tests run, so no registry wrapper is
/// required.
/// </remarks>
[CollectionDefinition(Name)]
public sealed class RocketMQMultiBrokerCollection : ICollectionFixture<RocketMQMultiBrokerRemotingContainerFixture>
{
    public const string Name = "RocketMQ Remoting multi-Broker integration";
}
