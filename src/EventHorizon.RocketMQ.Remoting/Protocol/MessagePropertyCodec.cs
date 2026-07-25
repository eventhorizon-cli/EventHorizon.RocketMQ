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

namespace EventHorizon.RocketMQ.Remoting.Protocol;

internal static class MessagePropertyCodec
{
    private const char NameValueSeparator = '\u0001';
    private const char PropertySeparator = '\u0002';

    public static string Serialize(IDictionary<string, string> properties)
    {
        if (properties.Count == 0)
        {
            return string.Empty;
        }

        var builder = new StringBuilder();
        foreach (var (key, value) in properties.OrderBy(static item => item.Key, StringComparer.Ordinal))
        {
            Validate(key, nameof(key));
            Validate(value, nameof(value));
            builder.Append(key)
                .Append(NameValueSeparator)
                .Append(value)
                .Append(PropertySeparator);
        }

        return builder.ToString();
    }

    public static IReadOnlyDictionary<string, string> Deserialize(string properties)
    {
        var result = new Dictionary<string, string>(StringComparer.Ordinal);
        foreach (var property in properties.Split(PropertySeparator, StringSplitOptions.RemoveEmptyEntries))
        {
            var separator = property.IndexOf(NameValueSeparator);
            if (separator <= 0 || separator == property.Length - 1)
            {
                continue;
            }

            result[property[..separator]] = property[(separator + 1)..];
        }

        return result;
    }

    private static void Validate(string value, string parameterName)
    {
        if (string.IsNullOrEmpty(value) || value.Contains(NameValueSeparator) || value.Contains(PropertySeparator))
        {
            throw new ArgumentException("RocketMQ message property names and values must be non-empty and cannot contain protocol separators.", parameterName);
        }
    }
}
