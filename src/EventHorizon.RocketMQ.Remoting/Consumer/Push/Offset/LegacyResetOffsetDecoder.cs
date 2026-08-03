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
using System.Text;
using System.Text.Json;

namespace EventHorizon.RocketMQ.Remoting.Consumer.Push.Offset;

internal static class LegacyResetOffsetDecoder
{
    private const string OffsetTableProperty = "\"offsetTable\"";

    public static IReadOnlyList<LegacyResetOffset> Decode(byte[]? body)
    {
        if (body is not { Length: > 0 })
        {
            return Array.Empty<LegacyResetOffset>();
        }

        JsonDocument document;
        try
        {
            document = JsonDocument.Parse(body);
        }
        catch (JsonException exception)
        {
            return DecodeFastJsonObjectKeys(body, exception);
        }

        using (document)
        {
            if (document.RootElement.ValueKind != JsonValueKind.Object ||
                !document.RootElement.TryGetProperty("offsetTable", out var offsetTable))
            {
                throw new InvalidDataException("The reset-offset body does not contain an 'offsetTable' object.");
            }

            var entries = new List<LegacyResetOffset>();
            switch (offsetTable.ValueKind)
            {
                case JsonValueKind.Null:
                    break;
                case JsonValueKind.Array:
                    DecodeArray(offsetTable, entries);
                    break;
                case JsonValueKind.Object:
                    DecodeStringKeyedObject(offsetTable, entries);
                    break;
                default:
                    throw new InvalidDataException("The reset-offset 'offsetTable' has an unsupported JSON shape.");
            }

            return Validate(entries);
        }
    }

    private static void DecodeArray(JsonElement offsetTable, ICollection<LegacyResetOffset> entries)
    {
        foreach (var item in offsetTable.EnumerateArray())
        {
            if (item.ValueKind == JsonValueKind.Array && item.GetArrayLength() == 2)
            {
                entries.Add(CreateEntry(item[0], item[1]));
                continue;
            }

            // ResetOffsetBodyForC uses a flat list instead of Java's complex map.
            if (item.ValueKind == JsonValueKind.Object && item.TryGetProperty("offset", out var offset))
            {
                entries.Add(CreateEntry(item, offset));
                continue;
            }

            throw new InvalidDataException("The reset-offset array contains an invalid entry.");
        }
    }

    private static void DecodeStringKeyedObject(JsonElement offsetTable, ICollection<LegacyResetOffset> entries)
    {
        foreach (var property in offsetTable.EnumerateObject())
        {
            JsonDocument keyDocument;
            try
            {
                keyDocument = JsonDocument.Parse(property.Name);
            }
            catch (JsonException exception)
            {
                throw new InvalidDataException(
                    "The reset-offset object contains a message-queue key that is not JSON.",
                    exception);
            }

            using (keyDocument)
            {
                entries.Add(CreateEntry(keyDocument.RootElement, property.Value));
            }
        }
    }

    private static IReadOnlyList<LegacyResetOffset> DecodeFastJsonObjectKeys(
        byte[] body,
        JsonException jsonException)
    {
        string json;
        try
        {
            json = new UTF8Encoding(encoderShouldEmitUTF8Identifier: false, throwOnInvalidBytes: true).GetString(body);
        }
        catch (DecoderFallbackException exception)
        {
            throw new InvalidDataException("The reset-offset body is not valid UTF-8.", exception);
        }

        var index = json.IndexOf(OffsetTableProperty, StringComparison.Ordinal);
        if (index < 0)
        {
            throw new InvalidDataException(
                "The reset-offset body is neither JSON nor a supported Fastjson complex map.",
                jsonException);
        }

        index += OffsetTableProperty.Length;
        SkipWhitespace(json, ref index);
        Require(json, ref index, ':', jsonException);
        SkipWhitespace(json, ref index);
        Require(json, ref index, '{', jsonException);

        var entries = new List<LegacyResetOffset>();
        while (true)
        {
            SkipWhitespace(json, ref index);
            if (TryRead(json, ref index, '}'))
            {
                return Validate(entries);
            }

            if (index >= json.Length || json[index] != '{')
            {
                throw InvalidFastJson(jsonException);
            }

            var keyStart = index;
            index = FindObjectEnd(json, index, jsonException);
            using var keyDocument = JsonDocument.Parse(json[keyStart..index]);

            SkipWhitespace(json, ref index);
            Require(json, ref index, ':', jsonException);
            SkipWhitespace(json, ref index);
            var valueStart = index;
            if (index < json.Length && json[index] == '"')
            {
                index = FindStringEnd(json, index, jsonException);
            }
            else
            {
                while (index < json.Length && json[index] is not ',' and not '}')
                {
                    index++;
                }
            }

            var valueText = json[valueStart..index].Trim();
            if (valueText.Length == 0)
            {
                throw InvalidFastJson(jsonException);
            }

            using var valueDocument = JsonDocument.Parse(valueText);
            entries.Add(CreateEntry(keyDocument.RootElement, valueDocument.RootElement));

            SkipWhitespace(json, ref index);
            if (TryRead(json, ref index, ','))
            {
                continue;
            }

            if (TryRead(json, ref index, '}'))
            {
                return Validate(entries);
            }

            throw InvalidFastJson(jsonException);
        }
    }

    private static LegacyResetOffset CreateEntry(JsonElement queue, JsonElement offsetElement)
    {
        if (queue.ValueKind != JsonValueKind.Object)
        {
            throw new InvalidDataException("A reset-offset message-queue key must be a JSON object.");
        }

        var topic = GetRequiredString(queue, "topic");
        var brokerName = GetRequiredString(queue, "brokerName");
        if (!queue.TryGetProperty("queueId", out var queueIdElement) ||
            !TryGetInt32(queueIdElement, out var queueId) || queueId < 0)
        {
            throw new InvalidDataException("A reset-offset entry contains an invalid 'queueId'.");
        }

        if (!TryGetInt64(offsetElement, out var offset) || offset < 0)
        {
            throw new InvalidDataException("A reset-offset entry contains an invalid 'offset'.");
        }

        return new LegacyResetOffset(topic, brokerName, queueId, offset);
    }

    private static IReadOnlyList<LegacyResetOffset> Validate(List<LegacyResetOffset> entries)
    {
        var keys = new HashSet<(string Topic, string BrokerName, int QueueId)>();
        foreach (var entry in entries)
        {
            if (!keys.Add((entry.Topic, entry.BrokerName, entry.QueueId)))
            {
                throw new InvalidDataException(
                    $"The reset-offset body contains duplicate queue {entry.Topic}/{entry.BrokerName}/{entry.QueueId}.");
            }
        }

        return entries;
    }

    private static string GetRequiredString(JsonElement element, string propertyName)
    {
        if (!element.TryGetProperty(propertyName, out var property) ||
            property.ValueKind != JsonValueKind.String ||
            string.IsNullOrWhiteSpace(property.GetString()))
        {
            throw new InvalidDataException($"A reset-offset entry contains an invalid '{propertyName}'.");
        }

        return property.GetString()!;
    }

    private static bool TryGetInt32(JsonElement element, out int value) =>
        element.ValueKind == JsonValueKind.Number
            ? element.TryGetInt32(out value)
            : int.TryParse(element.GetString(), NumberStyles.Integer, CultureInfo.InvariantCulture, out value);

    private static bool TryGetInt64(JsonElement element, out long value) =>
        element.ValueKind == JsonValueKind.Number
            ? element.TryGetInt64(out value)
            : long.TryParse(element.GetString(), NumberStyles.Integer, CultureInfo.InvariantCulture, out value);

    private static int FindObjectEnd(string json, int start, Exception innerException)
    {
        var depth = 0;
        var inString = false;
        var escaped = false;
        for (var index = start; index < json.Length; index++)
        {
            var character = json[index];
            if (inString)
            {
                if (escaped)
                {
                    escaped = false;
                }
                else if (character == '\\')
                {
                    escaped = true;
                }
                else if (character == '"')
                {
                    inString = false;
                }

                continue;
            }

            switch (character)
            {
                case '"':
                    inString = true;
                    break;
                case '{':
                    depth++;
                    break;
                case '}':
                    if (--depth == 0)
                    {
                        return index + 1;
                    }

                    break;
            }
        }

        throw InvalidFastJson(innerException);
    }

    private static int FindStringEnd(string json, int start, Exception innerException)
    {
        var escaped = false;
        for (var index = start + 1; index < json.Length; index++)
        {
            var character = json[index];
            if (escaped)
            {
                escaped = false;
            }
            else if (character == '\\')
            {
                escaped = true;
            }
            else if (character == '"')
            {
                return index + 1;
            }
        }

        throw InvalidFastJson(innerException);
    }

    private static void SkipWhitespace(string json, ref int index)
    {
        while (index < json.Length && char.IsWhiteSpace(json[index]))
        {
            index++;
        }
    }

    private static void Require(string json, ref int index, char expected, Exception innerException)
    {
        if (!TryRead(json, ref index, expected))
        {
            throw InvalidFastJson(innerException);
        }
    }

    private static bool TryRead(string json, ref int index, char expected)
    {
        if (index >= json.Length || json[index] != expected)
        {
            return false;
        }

        index++;
        return true;
    }

    private static InvalidDataException InvalidFastJson(Exception innerException) =>
        new("The reset-offset body contains an invalid Fastjson complex map.", innerException);
}
