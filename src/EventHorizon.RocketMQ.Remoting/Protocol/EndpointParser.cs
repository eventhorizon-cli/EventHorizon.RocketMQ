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

namespace EventHorizon.RocketMQ.Remoting.Protocol;

internal static class EndpointParser
{
    public static EndPoint Parse(string value)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(value);
        if (IPEndPoint.TryParse(value, out var ipEndPoint))
        {
            return ipEndPoint;
        }

        var separator = value.LastIndexOf(':');
        if (separator <= 0 ||
            !int.TryParse(value[(separator + 1)..], out var port) ||
            port is <= 0 or > 65535)
        {
            throw new FormatException($"Invalid RocketMQ endpoint '{value}'. Expected host:port.");
        }

        var host = value[..separator].Trim('[', ']');
        return new DnsEndPoint(host, port);
    }
}
