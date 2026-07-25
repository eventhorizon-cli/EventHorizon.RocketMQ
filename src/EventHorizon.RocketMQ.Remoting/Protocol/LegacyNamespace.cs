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

namespace EventHorizon.RocketMQ.Remoting.Protocol;

internal static class LegacyNamespace
{
    private const char Separator = '%';
    private const string RetryPrefix = "%RETRY%";
    private const string DeadLetterPrefix = "%DLQ%";

    public static string Wrap(string? @namespace, string resource)
    {
        if (string.IsNullOrWhiteSpace(@namespace) || string.IsNullOrEmpty(resource) || IsSystemResource(resource))
        {
            return resource;
        }

        var (prefix, name) = SplitPrefix(resource);
        var namespacePrefix = $"{@namespace}{Separator}";
        return name.StartsWith(namespacePrefix, StringComparison.Ordinal)
            ? resource
            : $"{prefix}{namespacePrefix}{name}";
    }

    public static string WithoutNamespace(string? @namespace, string resource)
    {
        if (string.IsNullOrWhiteSpace(@namespace) || string.IsNullOrEmpty(resource))
        {
            return resource;
        }

        var (prefix, name) = SplitPrefix(resource);
        var namespacePrefix = $"{@namespace}{Separator}";
        return name.StartsWith(namespacePrefix, StringComparison.Ordinal)
            ? $"{prefix}{name[namespacePrefix.Length..]}"
            : resource;
    }

    private static (string Prefix, string Name) SplitPrefix(string resource)
    {
        if (resource.StartsWith(RetryPrefix, StringComparison.Ordinal))
        {
            return (RetryPrefix, resource[RetryPrefix.Length..]);
        }

        return resource.StartsWith(DeadLetterPrefix, StringComparison.Ordinal)
            ? (DeadLetterPrefix, resource[DeadLetterPrefix.Length..])
            : (string.Empty, resource);
    }

    private static bool IsSystemResource(string resource) =>
        resource.StartsWith("RMQ_SYS_", StringComparison.OrdinalIgnoreCase) ||
        resource.StartsWith("CID_RMQ_SYS_", StringComparison.Ordinal) ||
        resource is "TBW102" or "SELF_TEST_TOPIC" or "BenchmarkTest" or
            "OFFSET_MOVED_EVENT" or "SCHEDULE_TOPIC_XXXX" or
            "TRANS_CHECK_MAX_TIME_TOPIC" or "CHECKPOINT_TOPIC";
}
