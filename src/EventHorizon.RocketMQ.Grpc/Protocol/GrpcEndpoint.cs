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

using System.Collections.Concurrent;
using System.Net;
using Proto = Apache.Rocketmq.V2;

namespace EventHorizon.RocketMQ.Grpc.Protocol;

internal static class GrpcEndpoint
{
    private static readonly ConcurrentDictionary<string, EndpointCursor> BrokerCursors =
        new(StringComparer.Ordinal);

    public static IReadOnlyList<Uri> ParseMany(string endpoints, bool useTls)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(endpoints);
        var parsed = endpoints
            .Split(';', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
            .Select(endpoint => Parse(endpoint, useTls))
            .Distinct()
            .ToArray();
        return parsed.Length > 0
            ? parsed
            : throw new ArgumentException("At least one RocketMQ endpoint is required.", nameof(endpoints));
    }

    public static Uri Parse(string endpoint, bool useTls)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(endpoint);
        var hasExplicitScheme = endpoint.Contains("://", StringComparison.Ordinal);
        if (!hasExplicitScheme)
        {
            endpoint = $"{(useTls ? "https" : "http")}://{endpoint}";
        }

        if (!Uri.TryCreate(endpoint, UriKind.Absolute, out var uri) ||
            string.IsNullOrWhiteSpace(uri.Host) ||
            (!string.Equals(uri.Scheme, Uri.UriSchemeHttp, StringComparison.OrdinalIgnoreCase) &&
             !string.Equals(uri.Scheme, Uri.UriSchemeHttps, StringComparison.OrdinalIgnoreCase)))
        {
            throw new ArgumentException($"Invalid RocketMQ endpoint '{endpoint}'.", nameof(endpoint));
        }

        if (hasExplicitScheme &&
            (useTls != string.Equals(uri.Scheme, Uri.UriSchemeHttps, StringComparison.OrdinalIgnoreCase)))
        {
            throw new ArgumentException(
                $"RocketMQ endpoint scheme '{uri.Scheme}' conflicts with {nameof(GrpcClientOptions.UseTLS)}={useTls}.",
                nameof(endpoint));
        }

        return uri;
    }

    public static Proto.Endpoints ToProtobuf(IEnumerable<Uri> endpoints)
    {
        ArgumentNullException.ThrowIfNull(endpoints);
        var values = endpoints.Distinct().ToArray();
        if (values.Length == 0)
        {
            throw new ArgumentException("At least one RocketMQ endpoint is required.", nameof(endpoints));
        }

        var schemes = values.Select(GetAddressScheme).ToArray();
        var result = new Proto.Endpoints
        {
            Scheme = schemes.Contains(Proto.AddressScheme.Ipv6)
                ? Proto.AddressScheme.Ipv6
                : schemes.Contains(Proto.AddressScheme.Ipv4)
                    ? Proto.AddressScheme.Ipv4
                    : Proto.AddressScheme.DomainName
        };
        result.Addresses.AddRange(values.Select(static endpoint => new Proto.Address
        {
            Host = endpoint.Host,
            Port = endpoint.Port
        }));
        return result;
    }

    public static Uri FromProtobuf(Proto.Endpoints endpoints, bool useTls)
    {
        var values = FromProtobufAll(endpoints, useTls);
        if (values.Count == 1)
        {
            return values[0];
        }

        var key = string.Join(';', values.Select(static endpoint => endpoint.AbsoluteUri));
        var cursor = BrokerCursors.GetOrAdd(key, static _ => new EndpointCursor());
        var index = (Interlocked.Increment(ref cursor.Index) - 1) & int.MaxValue;
        return values[index % values.Count];
    }

    public static IReadOnlyList<Uri> FromProtobufAll(Proto.Endpoints endpoints, bool useTls)
    {
        ArgumentNullException.ThrowIfNull(endpoints);
        if (endpoints.Addresses.Count == 0)
        {
            throw new InvalidOperationException("RocketMQ returned an endpoint without an address.");
        }

        return endpoints.Addresses.Select(address =>
        {
            var host = address.Host.Contains(':', StringComparison.Ordinal) ? $"[{address.Host}]" : address.Host;
            return new Uri($"{(useTls ? "https" : "http")}://{host}:{address.Port}");
        }).ToArray();
    }

    private static Proto.AddressScheme GetAddressScheme(Uri endpoint) =>
        IPAddress.TryParse(endpoint.Host, out var address)
            ? address.AddressFamily == System.Net.Sockets.AddressFamily.InterNetworkV6
                ? Proto.AddressScheme.Ipv6
                : Proto.AddressScheme.Ipv4
            : Proto.AddressScheme.DomainName;

    private sealed class EndpointCursor
    {
        public int Index;
    }
}
