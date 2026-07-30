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
using System.Diagnostics;
using System.Text;
using System.Text.Json;

namespace EventHorizon.RocketMQ.Remoting.IntegrationTests;

internal sealed class RemotingConsumerProcess : IAsyncDisposable
{
    private const string HostAssemblyName = "EventHorizon.RocketMQ.Remoting.CrossProcessTestHost.dll";
    private static readonly TimeSpan StopTimeout = TimeSpan.FromSeconds(10);
    private readonly object _stateSync = new();
    private readonly Process _process;
    private readonly string _member;
    private readonly ConcurrentDictionary<string, int> _deliveries = new(StringComparer.Ordinal);
    private readonly StringBuilder _standardError = new();
    private readonly TaskCompletionSource _started = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private readonly TaskCompletionSource _stopped = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private readonly Task _standardOutputTask;
    private readonly Task _standardErrorTask;
    private readonly Task _exitTask;
    private string[] _assignment = [];
    private Exception? _failure;
    private int _stopRequested;
    private int _terminationExpected;
    private int _disposed;

    private RemotingConsumerProcess(Process process, string member)
    {
        _process = process;
        _member = member;
        _standardOutputTask = ReadStandardOutputAsync();
        _standardErrorTask = ReadStandardErrorAsync();
        _exitTask = ObserveExitAsync();
    }

    public int ProcessId => _process.Id;

    public IReadOnlyList<string> Assignment
    {
        get
        {
            lock (_stateSync)
            {
                return [.. _assignment];
            }
        }
    }

    public static string HostAssemblyPath => Path.Combine(AppContext.BaseDirectory, HostAssemblyName);

    public static async Task<RemotingConsumerProcess> StartAsync(
        string member,
        string nameServerAddress,
        string instanceName,
        string consumerGroup,
        string topic,
        string tag,
        bool consumeOrderly,
        TimeSpan timeout,
        CancellationToken cancellationToken)
    {
        var hostAssemblyPath = HostAssemblyPath;
        if (!File.Exists(hostAssemblyPath))
        {
            throw new FileNotFoundException(
                $"The cross-process Remoting consumer host was not found at '{hostAssemblyPath}'.",
                hostAssemblyPath);
        }

        var testAssemblyPath = typeof(RemotingConsumerProcess).Assembly.Location;
        var runtimeConfigPath = Path.ChangeExtension(testAssemblyPath, ".runtimeconfig.json");
        var dependenciesPath = Path.ChangeExtension(testAssemblyPath, ".deps.json");
        var startInfo = new ProcessStartInfo
        {
            FileName = Environment.GetEnvironmentVariable("DOTNET_HOST_PATH") ?? "dotnet",
            UseShellExecute = false,
            RedirectStandardInput = true,
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            CreateNoWindow = true
        };
        startInfo.ArgumentList.Add("exec");
        startInfo.ArgumentList.Add("--runtimeconfig");
        startInfo.ArgumentList.Add(runtimeConfigPath);
        startInfo.ArgumentList.Add("--depsfile");
        startInfo.ArgumentList.Add(dependenciesPath);
        startInfo.ArgumentList.Add(hostAssemblyPath);
        AddArgument(startInfo, "--member", member);
        AddArgument(startInfo, "--nameserver", nameServerAddress);
        AddArgument(startInfo, "--instance", instanceName);
        AddArgument(startInfo, "--group", consumerGroup);
        AddArgument(startInfo, "--topic", topic);
        AddArgument(startInfo, "--tag", tag);
        AddArgument(startInfo, "--orderly", consumeOrderly.ToString());

        var process = new Process { StartInfo = startInfo };
        if (!process.Start())
        {
            process.Dispose();
            throw new InvalidOperationException($"Unable to start Remoting consumer process '{member}'.");
        }

        var consumerProcess = new RemotingConsumerProcess(process, member);
        try
        {
            await consumerProcess._started.Task.WaitAsync(timeout, cancellationToken).ConfigureAwait(false);
            consumerProcess.ThrowIfFaulted();
            return consumerProcess;
        }
        catch
        {
            await consumerProcess.DisposeAsync().ConfigureAwait(false);
            throw;
        }
    }

    public int GetDeliveryCount(string body) => _deliveries.GetValueOrDefault(body);

    public void ThrowIfFaulted()
    {
        if (Volatile.Read(ref _failure) is { } failure)
        {
            throw new InvalidOperationException(
                $"Remoting consumer process '{_member}' failed. {GetDiagnostics()}",
                failure);
        }
    }

    public async Task StopAsync(CancellationToken cancellationToken)
    {
        if (_process.HasExited)
        {
            EnsureSuccessfulExit();
            return;
        }

        if (Interlocked.Exchange(ref _stopRequested, 1) == 0)
        {
            await _process.StandardInput.WriteLineAsync("stop".AsMemory(), cancellationToken)
                .ConfigureAwait(false);
            await _process.StandardInput.FlushAsync(cancellationToken).ConfigureAwait(false);
        }

        await _stopped.Task.WaitAsync(StopTimeout, cancellationToken).ConfigureAwait(false);
        await _exitTask.WaitAsync(StopTimeout, cancellationToken).ConfigureAwait(false);
        EnsureSuccessfulExit();
    }

    public async Task TerminateAsync(CancellationToken cancellationToken)
    {
        if (_process.HasExited)
        {
            throw new InvalidOperationException(
                $"Remoting consumer process '{_member}' has already exited. {GetDiagnostics()}");
        }

        Interlocked.Exchange(ref _terminationExpected, 1);
        _process.Kill(entireProcessTree: true);
        await _exitTask.WaitAsync(StopTimeout, cancellationToken).ConfigureAwait(false);
        if (!_process.HasExited)
        {
            throw new InvalidOperationException(
                $"Remoting consumer process '{_member}' did not terminate. {GetDiagnostics()}");
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
        {
            return;
        }

        try
        {
            if (!_process.HasExited)
            {
                try
                {
                    using var timeout = new CancellationTokenSource(StopTimeout);
                    await StopAsync(timeout.Token).ConfigureAwait(false);
                }
                catch
                {
                    if (!_process.HasExited)
                    {
                        _process.Kill(entireProcessTree: true);
                    }
                }
            }

            await Task.WhenAll(_exitTask, _standardOutputTask, _standardErrorTask).ConfigureAwait(false);
        }
        finally
        {
            _process.Dispose();
        }
    }

    public string GetDiagnostics()
    {
        string assignment;
        string standardError;
        lock (_stateSync)
        {
            assignment = string.Join(", ", _assignment);
            standardError = _standardError.ToString();
        }

        var exit = _process.HasExited ? _process.ExitCode.ToString() : "running";
        return $"member={_member}, pid={_process.Id}, exit={exit}, " +
               $"assignment=[{assignment}], stderr=[{standardError}]";
    }

    private static void AddArgument(ProcessStartInfo startInfo, string name, string value)
    {
        startInfo.ArgumentList.Add(name);
        startInfo.ArgumentList.Add(value);
    }

    private async Task ReadStandardOutputAsync()
    {
        try
        {
            while (await _process.StandardOutput.ReadLineAsync().ConfigureAwait(false) is { } line)
            {
                CrossProcessHostEvent? hostEvent;
                try
                {
                    hostEvent = JsonSerializer.Deserialize<CrossProcessHostEvent>(line);
                }
                catch (JsonException exception)
                {
                    SetFailure(new InvalidOperationException(
                        $"Consumer host '{_member}' wrote an invalid protocol line: {line}",
                        exception));
                    continue;
                }

                if (hostEvent is null || !string.Equals(hostEvent.Member, _member, StringComparison.Ordinal))
                {
                    SetFailure(new InvalidOperationException(
                        $"Consumer host '{_member}' wrote an invalid event: {line}"));
                    continue;
                }

                switch (hostEvent.Kind)
                {
                    case "started":
                        _started.TrySetResult();
                        break;
                    case "assignment":
                        lock (_stateSync)
                        {
                            _assignment = hostEvent.Queues ?? [];
                        }

                        break;
                    case "delivery" when hostEvent.Body is not null:
                        _deliveries.AddOrUpdate(hostEvent.Body, 1, static (_, count) => count + 1);
                        break;
                    case "stopped":
                        _stopped.TrySetResult();
                        break;
                    case "faulted":
                        SetFailure(new InvalidOperationException(hostEvent.Message ?? "Consumer host faulted."));
                        break;
                    default:
                        SetFailure(new InvalidOperationException(
                            $"Consumer host '{_member}' wrote an unsupported event: {line}"));
                        break;
                }
            }
        }
        catch (Exception exception)
        {
            SetFailure(exception);
        }
    }

    private async Task ReadStandardErrorAsync()
    {
        while (await _process.StandardError.ReadLineAsync().ConfigureAwait(false) is { } line)
        {
            lock (_stateSync)
            {
                if (_standardError.Length > 0)
                {
                    _standardError.AppendLine();
                }

                _standardError.Append(line);
            }
        }
    }

    private async Task ObserveExitAsync()
    {
        await _process.WaitForExitAsync().ConfigureAwait(false);
        if (_process.ExitCode != 0 &&
            Volatile.Read(ref _terminationExpected) == 0 &&
            Volatile.Read(ref _failure) is null)
        {
            SetFailure(new InvalidOperationException(
                $"Consumer host '{_member}' exited with code {_process.ExitCode}."));
        }
        else if (Volatile.Read(ref _stopRequested) == 0 &&
                 Volatile.Read(ref _terminationExpected) == 0 &&
                 Volatile.Read(ref _failure) is null)
        {
            SetFailure(new InvalidOperationException($"Consumer host '{_member}' exited unexpectedly."));
        }

        _started.TrySetException(
            Volatile.Read(ref _failure) ??
            new InvalidOperationException($"Consumer host '{_member}' exited before it started."));
        _stopped.TrySetResult();
    }

    private void EnsureSuccessfulExit()
    {
        ThrowIfFaulted();
        if (!_process.HasExited || _process.ExitCode != 0)
        {
            throw new InvalidOperationException(
                $"Remoting consumer process '{_member}' did not stop successfully. {GetDiagnostics()}");
        }
    }

    private void SetFailure(Exception failure)
    {
        Interlocked.CompareExchange(ref _failure, failure, null);
        _started.TrySetException(failure);
    }

    private sealed class CrossProcessHostEvent
    {
        public string Kind { get; init; } = string.Empty;

        public string Member { get; init; } = string.Empty;

        public string[]? Queues { get; init; }

        public string? Body { get; init; }

        public string? Message { get; init; }
    }
}
