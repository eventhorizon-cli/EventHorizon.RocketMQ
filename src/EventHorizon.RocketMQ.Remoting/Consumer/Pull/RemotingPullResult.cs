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

namespace EventHorizon.RocketMQ.Remoting.Consumer.Pull;

/// <summary>
/// Represents the result of a classic remoting pull operation.
/// </summary>
/// <param name="Messages">The messages returned by the broker.</param>
/// <param name="NextOffset">The queue offset to use for the next pull operation.</param>
/// <param name="Status">The status reported for the pull operation.</param>
/// <param name="MinOffset">The minimum available offset reported by the broker.</param>
/// <param name="MaxOffset">The maximum available offset reported by the broker.</param>
public sealed record RemotingPullResult(
    IReadOnlyList<RemotingMessageView> Messages,
    long NextOffset,
    RemotingPullStatus Status = RemotingPullStatus.Found,
    long MinOffset = 0,
    long MaxOffset = 0);
