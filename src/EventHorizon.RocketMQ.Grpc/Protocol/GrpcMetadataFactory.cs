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

using System.Globalization;
using System.Reflection;
using System.Security.Cryptography;
using System.Text;
using Grpc.Core;
using Microsoft.Extensions.Options;

namespace EventHorizon.RocketMQ.Grpc.Protocol;

internal sealed class GrpcMetadataFactory
{
    private readonly GrpcClientOptions _options;
    private readonly string _clientId;
    private readonly string _version;

    internal GrpcMetadataFactory(IOptions<GrpcClientOptions> options, string logicalClient)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentException.ThrowIfNullOrWhiteSpace(logicalClient);
        _options = options.Value;
        var baseClientId = _options.BuildGrpcClientId();
        var logicalSuffix = $"@{logicalClient}";
        var suffix = baseClientId.EndsWith(logicalSuffix, StringComparison.Ordinal)
            ? string.Empty
            : logicalSuffix;
        _clientId = $"{baseClientId}{suffix}@{Guid.NewGuid():N}";
        _version = typeof(GrpcMetadataFactory).Assembly
            .GetCustomAttribute<AssemblyInformationalVersionAttribute>()?.InformationalVersion ?? "unknown";
    }

    public Metadata Create()
    {
        const string dateHeader = "x-mq-date-time";
        var timestamp = DateTime.UtcNow.ToString("yyyyMMdd'T'HHmmss'Z'", CultureInfo.InvariantCulture);
        var metadata = new Metadata
        {
            { "x-mq-language", "DOTNET" },
            { "x-mq-client-version", _version },
            { "x-mq-request-id", Guid.NewGuid().ToString() },
            { "x-mq-client-id", _clientId },
            { "x-mq-namespace", _options.Namespace ?? string.Empty },
            { "x-mq-protocol", "v2" },
            { dateHeader, timestamp }
        };

        if (!string.IsNullOrEmpty(_options.SecurityToken))
        {
            metadata.Add("x-mq-session-token", _options.SecurityToken);
        }

        if (!string.IsNullOrEmpty(_options.AccessKey) && !string.IsNullOrEmpty(_options.AccessSecret))
        {
            using var hmac = new HMACSHA1(Encoding.UTF8.GetBytes(_options.AccessSecret));
            var signature = Convert.ToHexString(hmac.ComputeHash(Encoding.UTF8.GetBytes(timestamp)))
                .ToLowerInvariant();
            metadata.Add("authorization",
                $"MQv2-HMAC-SHA1 Credential={_options.AccessKey}//Rocketmq, SignedHeaders={dateHeader}, Signature={signature}");
        }

        return metadata;
    }
}
