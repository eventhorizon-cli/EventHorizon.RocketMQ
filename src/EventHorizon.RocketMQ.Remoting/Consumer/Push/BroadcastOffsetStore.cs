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

using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using EventHorizon.RocketMQ.Remoting.Consumer.Pull;

namespace EventHorizon.RocketMQ.Remoting.Consumer.Push;

internal sealed class BroadcastOffsetStore
{
    private readonly string _path;
    private readonly SemaphoreSlim _gate = new(1, 1);
    private readonly Dictionary<OffsetKey, long> _offsets = new();
    private bool _loaded;

    public BroadcastOffsetStore(string? configuredPath, string clientId, string consumerGroup)
    {
        _path = ResolvePath(configuredPath, clientId, consumerGroup);
    }

    public async Task<long?> ReadAsync(RemotingPullMessageQueue queue, CancellationToken cancellationToken)
    {
        await _gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            await LoadAsync(cancellationToken).ConfigureAwait(false);
            return _offsets.TryGetValue(OffsetKey.Create(queue), out var offset) ? offset : null;
        }
        finally
        {
            _gate.Release();
        }
    }

    public Task UpdateAsync(RemotingPullMessageQueue queue, long offset, CancellationToken cancellationToken) =>
        UpdateAsync(OffsetKey.Create(queue), offset, cancellationToken);

    internal Task UpdateAsync(
        string topic,
        string brokerName,
        int queueId,
        long offset,
        CancellationToken cancellationToken) =>
        UpdateAsync(new OffsetKey(topic, brokerName, queueId), offset, cancellationToken);

    private async Task UpdateAsync(OffsetKey key, long offset, CancellationToken cancellationToken)
    {
        await _gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            await LoadAsync(cancellationToken).ConfigureAwait(false);
            _offsets[key] = offset;
            await PersistAsync(cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            _gate.Release();
        }
    }

    public async Task FlushAsync(CancellationToken cancellationToken)
    {
        await _gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            await LoadAsync(cancellationToken).ConfigureAwait(false);
            await PersistAsync(cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            _gate.Release();
        }
    }

    internal static string ResolvePath(string? configuredPath, string clientId, string consumerGroup)
    {
        if (!string.IsNullOrWhiteSpace(configuredPath))
        {
            return System.IO.Path.GetFullPath(configuredPath);
        }

        var root = Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData);
        if (string.IsNullOrWhiteSpace(root))
        {
            root = System.IO.Path.GetTempPath();
        }

        var identity = Encoding.UTF8.GetBytes($"{clientId}\0{consumerGroup}");
        var hash = SHA256.HashData(identity);
        var fileName = $"broadcast-{Convert.ToHexString(hash.AsSpan(0, 16)).ToLowerInvariant()}.json";
        return System.IO.Path.Combine(root, "EventHorizon.RocketMQ", "offsets", fileName);
    }

    internal static bool IsValidPath(string? path)
    {
        if (string.IsNullOrWhiteSpace(path))
        {
            return true;
        }

        try
        {
            _ = System.IO.Path.GetFullPath(path);
            return true;
        }
        catch (Exception exception) when (exception is ArgumentException or NotSupportedException or
                                          PathTooLongException)
        {
            return false;
        }
    }

    private async Task LoadAsync(CancellationToken cancellationToken)
    {
        if (_loaded)
        {
            return;
        }

        if (File.Exists(_path))
        {
            await using var stream = new FileStream(
                _path,
                FileMode.Open,
                FileAccess.Read,
                FileShare.Read,
                4096,
                FileOptions.Asynchronous | FileOptions.SequentialScan);
            var document = await JsonSerializer.DeserializeAsync<OffsetDocument>(
                stream,
                cancellationToken: cancellationToken).ConfigureAwait(false)
                ?? throw new InvalidDataException($"Broadcast offset file '{_path}' is empty.");
            if (document.Version != 1)
            {
                throw new InvalidDataException(
                    $"Broadcast offset file '{_path}' has unsupported version {document.Version}.");
            }

            foreach (var entry in document.Offsets)
            {
                if (entry.Offset >= 0)
                {
                    _offsets[new OffsetKey(entry.Topic, entry.BrokerName, entry.QueueId)] = entry.Offset;
                }
            }
        }

        _loaded = true;
    }

    private async Task PersistAsync(CancellationToken cancellationToken)
    {
        var directory = System.IO.Path.GetDirectoryName(_path)
            ?? throw new InvalidOperationException($"Broadcast offset path '{_path}' has no directory.");
        Directory.CreateDirectory(directory);
        var temporaryPath = $"{_path}.{Guid.NewGuid():N}.tmp";
        try
        {
            var document = new OffsetDocument
            {
                Offsets = _offsets
                    .OrderBy(static entry => entry.Key.Topic, StringComparer.Ordinal)
                    .ThenBy(static entry => entry.Key.BrokerName, StringComparer.Ordinal)
                    .ThenBy(static entry => entry.Key.QueueId)
                    .Select(static entry => new OffsetEntry
                    {
                        Topic = entry.Key.Topic,
                        BrokerName = entry.Key.BrokerName,
                        QueueId = entry.Key.QueueId,
                        Offset = entry.Value
                    })
                    .ToArray()
            };
            await using (var stream = new FileStream(
                             temporaryPath,
                             FileMode.CreateNew,
                             FileAccess.Write,
                             FileShare.None,
                             4096,
                             FileOptions.Asynchronous | FileOptions.WriteThrough))
            {
                await JsonSerializer.SerializeAsync(stream, document, cancellationToken: cancellationToken)
                    .ConfigureAwait(false);
                await stream.FlushAsync(cancellationToken).ConfigureAwait(false);
            }

            File.Move(temporaryPath, _path, true);
        }
        finally
        {
            if (File.Exists(temporaryPath))
            {
                File.Delete(temporaryPath);
            }
        }
    }

    private readonly record struct OffsetKey(string Topic, string BrokerName, int QueueId)
    {
        public static OffsetKey Create(RemotingPullMessageQueue queue) =>
            new(queue.Topic, queue.BrokerName, queue.QueueId);
    }

    private sealed class OffsetDocument
    {
        public int Version { get; init; } = 1;
        public OffsetEntry[] Offsets { get; init; } = [];
    }

    private sealed class OffsetEntry
    {
        public string Topic { get; init; } = string.Empty;
        public string BrokerName { get; init; } = string.Empty;
        public int QueueId { get; init; }
        public long Offset { get; init; }
    }
}
