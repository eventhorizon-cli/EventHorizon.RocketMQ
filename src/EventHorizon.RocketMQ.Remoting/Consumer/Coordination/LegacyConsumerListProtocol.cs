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

using System.Text;
using EventHorizon.RocketMQ.Remoting.Protocol;

namespace EventHorizon.RocketMQ.Remoting.Consumer.Coordination;

internal static class LegacyConsumerListProtocol
{
    public static IReadOnlyList<string> Decode(byte[]? body)
    {
        if (body is not { Length: > 0 })
        {
            return Array.Empty<string>();
        }

        var response = Encoding.UTF8.GetString(body).FromJson<GetConsumerListByGroupResponseBody>();
        return response.ConsumerIdList?
            .Where(static value => !string.IsNullOrWhiteSpace(value))
            .Distinct(StringComparer.Ordinal)
            .OrderBy(static value => value, StringComparer.Ordinal)
            .ToArray() ?? Array.Empty<string>();
    }

    private sealed class GetConsumerListByGroupResponseBody
    {
        public string[]? ConsumerIdList { get; init; }
    }
}
