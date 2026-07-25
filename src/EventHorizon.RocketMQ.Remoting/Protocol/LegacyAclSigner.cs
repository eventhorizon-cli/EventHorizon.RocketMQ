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
using System.Security.Cryptography;
using System.Text;
using Microsoft.Extensions.Options;

namespace EventHorizon.RocketMQ.Remoting.Protocol;

internal sealed class LegacyAclSigner
{
    private const string AccessKeyField = "AccessKey";
    private const string SecurityTokenField = "SecurityToken";
    private const string SignatureField = "Signature";

    private readonly string? _accessKey;
    private readonly string? _accessSecret;
    private readonly string? _securityToken;

    public LegacyAclSigner(IOptions<RemotingClientOptions> options)
    {
        ArgumentNullException.ThrowIfNull(options);
        _accessKey = options.Value.AccessKey;
        _accessSecret = options.Value.AccessSecret;
        _securityToken = options.Value.SecurityToken;

        if (string.IsNullOrWhiteSpace(_accessKey) != string.IsNullOrWhiteSpace(_accessSecret))
        {
            throw new ArgumentException("RocketMQ ACL requires both an access key and an access secret.", nameof(options));
        }
    }

    public void Sign(RemotingCommand command)
    {
        ArgumentNullException.ThrowIfNull(command);
        if (string.IsNullOrWhiteSpace(_accessKey))
        {
            return;
        }

        command.ExtFields.Remove(SignatureField);
        command.ExtFields.Remove(AccessKeyField);
        command.ExtFields.Remove(SecurityTokenField);

        var fields = command.ExtFields
            .Select(static entry => new KeyValuePair<string, string>(entry.Key, FormatValue(entry.Value)))
            .Append(new KeyValuePair<string, string>(AccessKeyField, _accessKey))
            .OrderBy(static entry => entry.Key, StringComparer.Ordinal);
        var canonical = string.Concat(fields.Select(static entry => entry.Value));
        var canonicalBytes = Encoding.UTF8.GetBytes(canonical);
        var body = command.Body ?? [];
        var payload = new byte[canonicalBytes.Length + body.Length];
        canonicalBytes.CopyTo(payload, 0);
        body.CopyTo(payload, canonicalBytes.Length);

        command.ExtFields[SignatureField] = CalculateSignature(payload, Encoding.UTF8.GetBytes(_accessSecret!));
        command.ExtFields[AccessKeyField] = _accessKey;
        if (!string.IsNullOrWhiteSpace(_securityToken))
        {
            command.ExtFields[SecurityTokenField] = _securityToken;
        }
    }

    internal static string CalculateSignature(ReadOnlySpan<byte> data, ReadOnlySpan<byte> secret)
    {
        using var hmac = new HMACSHA1(secret.ToArray());
        return Convert.ToBase64String(hmac.ComputeHash(data.ToArray()));
    }

    private static string FormatValue(object? value) => value switch
    {
        null => string.Empty,
        bool boolean => boolean ? "true" : "false",
        string text => text,
        IFormattable formattable => formattable.ToString(null, CultureInfo.InvariantCulture) ?? string.Empty,
        _ => value.ToString() ?? string.Empty
    };
}
