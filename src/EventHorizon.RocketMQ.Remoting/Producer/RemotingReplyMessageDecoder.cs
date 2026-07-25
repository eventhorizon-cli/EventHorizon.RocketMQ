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
using EventHorizon.RocketMQ.Remoting.Protocol;

namespace EventHorizon.RocketMQ.Remoting.Producer;

internal static class RemotingReplyMessageDecoder
{
    public static RemotingReplyMessage Decode(
        RemotingCommand request,
        string? @namespace,
        DateTimeOffset arrivedAt)
    {
        ArgumentNullException.ThrowIfNull(request);
        var topic = RequireString(request.ExtFields, "topic");
        var sysFlag = ParseInt32(request.ExtFields, "sysFlag");
        var bornTimestamp = DateTimeOffset.FromUnixTimeMilliseconds(ParseInt64(request.ExtFields, "bornTimestamp"));
        var storeTimestamp = DateTimeOffset.FromUnixTimeMilliseconds(ParseInt64(request.ExtFields, "storeTimestamp"));
        var properties = new Dictionary<string, string>(
            MessagePropertyCodec.Deserialize(GetString(request.ExtFields, "properties") ?? string.Empty),
            StringComparer.Ordinal)
        {
            ["ARRIVE_TIME"] = arrivedAt.ToUnixTimeMilliseconds().ToString(CultureInfo.InvariantCulture)
        };
        if (!properties.TryGetValue("CORRELATION_ID", out var correlationId) ||
            string.IsNullOrWhiteSpace(correlationId))
        {
            throw new InvalidDataException("The reply callback did not contain a correlation ID.");
        }

        var body = MessageBodyCodec.Decompress(request.Body ?? [], sysFlag);
        var keys = Get(properties, "KEYS")?.Split(' ', StringSplitOptions.RemoveEmptyEntries) ?? [];
        return new RemotingReplyMessage(
            LegacyNamespace.WithoutNamespace(@namespace, topic),
            body,
            correlationId,
            Get(properties, "UNIQ_KEY"),
            Get(properties, "TAGS"),
            keys,
            properties,
            bornTimestamp,
            storeTimestamp);
    }

    private static string RequireString(IReadOnlyDictionary<string, object> fields, string name) =>
        GetString(fields, name) is { Length: > 0 } value
            ? value
            : throw new InvalidDataException($"The reply callback did not contain a valid '{name}' field.");

    private static int ParseInt32(IReadOnlyDictionary<string, object> fields, string name) =>
        int.TryParse(GetString(fields, name), NumberStyles.Integer, CultureInfo.InvariantCulture, out var value)
            ? value
            : throw new InvalidDataException($"The reply callback did not contain a valid '{name}' field.");

    private static long ParseInt64(IReadOnlyDictionary<string, object> fields, string name) =>
        long.TryParse(GetString(fields, name), NumberStyles.Integer, CultureInfo.InvariantCulture, out var value)
            ? value
            : throw new InvalidDataException($"The reply callback did not contain a valid '{name}' field.");

    private static string? GetString(IReadOnlyDictionary<string, object> fields, string name) =>
        fields.TryGetValue(name, out var value) ? Convert.ToString(value, CultureInfo.InvariantCulture) : null;

    private static string? Get(IReadOnlyDictionary<string, string> properties, string name) =>
        properties.TryGetValue(name, out var value) ? value : null;
}
