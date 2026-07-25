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

using System.Buffers;
using System.Net;
using System.Net.Sockets;
using BenchmarkDotNet.Attributes;
using EventHorizon.RocketMQ.Remoting;
using EventHorizon.RocketMQ.Remoting.Protocol;
using Microsoft.Extensions.Options;

namespace EventHorizon.RocketMQ.Benchmarks.Protocols.Remoting;

[MemoryDiagnoser]
public class RemotingRoundTripBenchmarks
{
    private const int Operations = 256;
    private const int MaximumInFlight = 32;
    private static readonly TimeSpan OperationTimeout = TimeSpan.FromSeconds(30);
    private static readonly BenchmarkHeader Header = new() { Topic = "BenchmarkTest" };

    private readonly Task<RemotingCommand>[] _pending = new Task<RemotingCommand>[MaximumInFlight];
    private byte[] _payload = null!;
    private TcpListener? _listener;
    private CancellationTokenSource? _shutdown;
    private Task? _serverTask;
    private RemotingClient? _client;
    private IPEndPoint? _endPoint;

    [Params(128, 4096)]
    public int PayloadSize { get; set; }

    [GlobalSetup]
    public async Task SetupAsync()
    {
        _payload = GC.AllocateUninitializedArray<byte>(PayloadSize);
        for (var index = 0; index < _payload.Length; index++)
        {
            _payload[index] = unchecked((byte)index);
        }

        _shutdown = new CancellationTokenSource();
        _listener = new TcpListener(IPAddress.Loopback, 0);
        _listener.Start();
        _endPoint = (IPEndPoint)_listener.LocalEndpoint;
        _serverTask = RunServerAsync(_listener, _shutdown.Token);
        _client = new RemotingClient(
            new RemoteCommandSerializer(),
            Options.Create(new RemotingClientOptions()));

        try
        {
            await RoundTripAsync().ConfigureAwait(false);
        }
        catch
        {
            await CleanupAsync().ConfigureAwait(false);
            throw;
        }
    }

    [Benchmark(Baseline = true, OperationsPerInvoke = Operations)]
    public async Task Sequential()
    {
        for (var index = 0; index < Operations; index++)
        {
            await RoundTripAsync().ConfigureAwait(false);
        }
    }

    [Benchmark(OperationsPerInvoke = Operations)]
    public async Task ThirtyTwoInFlight()
    {
        for (var batch = 0; batch < Operations / MaximumInFlight; batch++)
        {
            for (var index = 0; index < MaximumInFlight; index++)
            {
                _pending[index] = InvokeAsync();
            }

            await Task.WhenAll(_pending).ConfigureAwait(false);
            for (var index = 0; index < MaximumInFlight; index++)
            {
                EnsureValidResponse(_pending[index].Result);
            }
        }
    }

    [GlobalCleanup]
    public async Task CleanupAsync()
    {
        _shutdown?.Cancel();
        _listener?.Stop();

        if (_client is not null)
        {
            await _client.DisposeAsync().ConfigureAwait(false);
            _client = null;
        }

        if (_serverTask is not null)
        {
            await _serverTask.ConfigureAwait(false);
            _serverTask = null;
        }

        _shutdown?.Dispose();
        _shutdown = null;
        _listener = null;
        _endPoint = null;
    }

    private async Task RoundTripAsync()
    {
        var response = await InvokeAsync().ConfigureAwait(false);
        EnsureValidResponse(response);
    }

    private Task<RemotingCommand> InvokeAsync()
    {
        var request = new RemotingCommand(RequestCode.GetRouteInfoByTopic, Header)
        {
            Body = _payload
        };
        return _client!.InvokeAsync(
            _endPoint!,
            request,
            OperationTimeout,
            CancellationToken.None);
    }

    private void EnsureValidResponse(RemotingCommand response)
    {
        if (response.Body is null || response.Body.Length != PayloadSize)
        {
            throw new InvalidDataException(
                $"Expected a {PayloadSize}-byte response, but received {response.Body?.Length ?? 0} bytes.");
        }
    }

    private static async Task RunServerAsync(TcpListener listener, CancellationToken cancellationToken)
    {
        try
        {
            while (true)
            {
                using var client = await listener.AcceptTcpClientAsync(cancellationToken).ConfigureAwait(false);
                client.NoDelay = true;
                await ServeConnectionAsync(client.GetStream(), cancellationToken).ConfigureAwait(false);
            }
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
        }
        catch (ObjectDisposedException) when (cancellationToken.IsCancellationRequested)
        {
        }
        catch (SocketException) when (cancellationToken.IsCancellationRequested)
        {
        }
    }

    private static async Task ServeConnectionAsync(Stream stream, CancellationToken cancellationToken)
    {
        var connection = new BenchmarkCommandConnection(stream);
        while (!cancellationToken.IsCancellationRequested)
        {
            RemotingCommand request;
            try
            {
                request = await connection.ReadAsync(cancellationToken).ConfigureAwait(false);
            }
            catch (EndOfStreamException)
            {
                return;
            }

            await connection.WriteAsync(
                new RemotingCommand
                {
                    Code = ResponseCodes.ResSuccess,
                    Opaque = request.Opaque,
                    Flag = 1,
                    Body = request.Body
                },
                cancellationToken).ConfigureAwait(false);
        }
    }

    private sealed class BenchmarkHeader : CommandCustomHeader
    {
        public required string Topic { get; init; }
    }

    private sealed class BenchmarkCommandConnection(Stream stream)
    {
        private readonly RemoteCommandSerializer _serializer = new();
        private readonly ArrayBufferWriter<byte> _output = new();
        private byte[] _input = new byte[8192];
        private int _bufferedLength;

        public async Task<RemotingCommand> ReadAsync(CancellationToken cancellationToken)
        {
            while (true)
            {
                var input = new ReadOnlySequence<byte>(_input.AsMemory(0, _bufferedLength));
                var consumed = input.Start;
                var examined = input.Start;
                if (_serializer.TryParseMessage(input, ref consumed, ref examined, out var command))
                {
                    var consumedLength = checked((int)input.Slice(0, consumed).Length);
                    _input.AsSpan(consumedLength, _bufferedLength - consumedLength).CopyTo(_input);
                    _bufferedLength -= consumedLength;
                    return command;
                }

                if (_bufferedLength == _input.Length)
                {
                    Array.Resize(ref _input, checked(_input.Length * 2));
                }

                var read = await stream.ReadAsync(
                    _input.AsMemory(_bufferedLength, _input.Length - _bufferedLength),
                    cancellationToken).ConfigureAwait(false);
                if (read == 0)
                {
                    throw new EndOfStreamException("The benchmark client closed the remoting connection.");
                }

                _bufferedLength += read;
            }
        }

        public async Task WriteAsync(RemotingCommand response, CancellationToken cancellationToken)
        {
            _output.Clear();
            _serializer.WriteMessage(response, _output);
            await stream.WriteAsync(_output.WrittenMemory, cancellationToken).ConfigureAwait(false);
        }
    }
}
