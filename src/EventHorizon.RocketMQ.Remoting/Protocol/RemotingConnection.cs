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

using System.IO;
using System.IO.Pipelines;
using System.Net;
using System.Net.Security;
using System.Net.Sockets;

namespace EventHorizon.RocketMQ.Remoting.Protocol;

internal sealed class RemotingConnection : IAsyncDisposable
{
    private readonly Stream _stream;
    private readonly PipeWriter _writer;
    private readonly PipeReader _reader;
    private readonly IRemoteCommandSerializer _serializer;
    private readonly int _maximumFrameLength;
    private readonly SemaphoreSlim _writeLock = new(1, 1);
    private readonly SemaphoreSlim _readLock = new(1, 1);
    private Exception? _terminalException;
    private int _faulted;
    private int _disposed;

    internal RemotingConnection(
        Stream stream,
        IRemoteCommandSerializer serializer,
        int maximumFrameLength = RemotingClientOptions.DefaultRemotingFrameSize)
    {
        ArgumentNullException.ThrowIfNull(stream);
        ArgumentNullException.ThrowIfNull(serializer);
        ArgumentOutOfRangeException.ThrowIfLessThan(
            maximumFrameLength,
            RemotingClientOptions.MinimumRemotingFrameSize);
        _stream = stream;
        _writer = PipeWriter.Create(stream, new StreamPipeWriterOptions(leaveOpen: true));
        _reader = PipeReader.Create(stream, new StreamPipeReaderOptions(leaveOpen: true));
        _serializer = serializer;
        _maximumFrameLength = maximumFrameLength;
    }

    internal static async ValueTask<RemotingConnection> ConnectAsync(
        EndPoint endPoint,
        bool useTls,
        IRemoteCommandSerializer serializer,
        CancellationToken cancellationToken,
        Action<EndPoint, SslClientAuthenticationOptions>? configureSslOptions = null,
        int maximumFrameLength = RemotingClientOptions.DefaultRemotingFrameSize)
    {
        ArgumentNullException.ThrowIfNull(endPoint);
        ArgumentNullException.ThrowIfNull(serializer);
        ArgumentOutOfRangeException.ThrowIfLessThan(
            maximumFrameLength,
            RemotingClientOptions.MinimumRemotingFrameSize);

        var socket = CreateSocket(endPoint);
        Stream? stream = null;
        try
        {
            socket.NoDelay = true;
            await socket.ConnectAsync(endPoint, cancellationToken).ConfigureAwait(false);
            stream = new NetworkStream(socket, ownsSocket: true);
            if (useTls)
            {
                var authenticationOptions = new SslClientAuthenticationOptions
                {
                    TargetHost = GetTargetHost(endPoint)
                };
                configureSslOptions?.Invoke(CloneEndPoint(endPoint), authenticationOptions);
                if (string.IsNullOrWhiteSpace(authenticationOptions.TargetHost))
                {
                    throw new InvalidOperationException(
                        "Classic remoting TLS requires a non-empty target host.");
                }

                var sslStream = new SslStream(stream, leaveInnerStreamOpen: false);
                stream = sslStream;
                await sslStream.AuthenticateAsClientAsync(
                    authenticationOptions,
                    cancellationToken).ConfigureAwait(false);
            }

            return new RemotingConnection(stream, serializer, maximumFrameLength);
        }
        catch
        {
            try
            {
                if (stream is null)
                {
                    socket.Dispose();
                }
                else
                {
                    await stream.DisposeAsync().ConfigureAwait(false);
                }
            }
            catch
            {
                // Preserve the connect or TLS handshake exception.
            }

            throw;
        }
    }

    internal bool IsUsable =>
        Volatile.Read(ref _disposed) == 0 && Volatile.Read(ref _faulted) == 0;

    public async ValueTask SendAsync(RemotingCommand request, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(request);
        ThrowIfUnavailable();
        await _writeLock.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            ThrowIfUnavailable();
            try
            {
                _serializer.WriteMessage(request, _writer, _maximumFrameLength);
            }
            catch (InvalidDataException)
            {
                // The built-in serializer validates the complete frame before mutating the writer.
                throw;
            }
            catch (OperationCanceledException exception) when (!cancellationToken.IsCancellationRequested)
            {
                var transportException = new IOException(
                    "The RocketMQ remoting connection was aborted while writing a frame.",
                    Volatile.Read(ref _terminalException) ?? exception);
                Fault(transportException);
                throw transportException;
            }
            catch (Exception exception)
            {
                Fault(exception);
                throw;
            }

            try
            {
                var result = await _writer.FlushAsync(cancellationToken).ConfigureAwait(false);
                if (result.IsCanceled)
                {
                    if (cancellationToken.IsCancellationRequested)
                    {
                        throw new OperationCanceledException(cancellationToken);
                    }

                    throw new IOException(
                        "The RocketMQ remoting connection was aborted while writing a frame.",
                        Volatile.Read(ref _terminalException));
                }

                if (result.IsCompleted)
                {
                    throw new IOException("The RocketMQ remoting connection was closed while writing a frame.");
                }
            }
            catch (OperationCanceledException exception) when (!cancellationToken.IsCancellationRequested)
            {
                var transportException = new IOException(
                    "The RocketMQ remoting connection was aborted while writing a frame.",
                    Volatile.Read(ref _terminalException) ?? exception);
                Fault(transportException);
                throw transportException;
            }
            catch (Exception exception)
            {
                Fault(exception);
                throw;
            }
        }
        finally
        {
            _writeLock.Release();
        }
    }

    public async ValueTask<RemotingCommand> ReceiveAsync()
    {
        ThrowIfUnavailable();
        await _readLock.WaitAsync().ConfigureAwait(false);
        try
        {
            ThrowIfUnavailable();
            while (true)
            {
                var result = await _reader.ReadAsync().ConfigureAwait(false);
                var buffer = result.Buffer;
                var consumed = buffer.Start;
                var examined = buffer.End;
                try
                {
                    if (result.IsCanceled)
                    {
                        throw new OperationCanceledException("The RocketMQ remoting read was canceled.");
                    }

                    if (_serializer.TryParseMessage(
                            buffer,
                            ref consumed,
                            ref examined,
                            out var command,
                            _maximumFrameLength))
                    {
                        return command;
                    }

                    if (result.IsCompleted)
                    {
                        if (!buffer.IsEmpty)
                        {
                            throw new InvalidDataException(
                                "The RocketMQ remoting connection closed in the middle of a frame.");
                        }

                        throw new EndOfStreamException("The RocketMQ remoting connection was closed by the peer.");
                    }
                }
                finally
                {
                    _reader.AdvanceTo(consumed, examined);
                }
            }
        }
        finally
        {
            _readLock.Release();
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
        {
            return;
        }

        CancelPendingOperations();
        var completionException = Volatile.Read(ref _terminalException) ??
            new ObjectDisposedException(nameof(RemotingConnection));

        try
        {
            await _stream.DisposeAsync().ConfigureAwait(false);
        }
        finally
        {
            try
            {
                await CompleteWriterAsync(completionException).ConfigureAwait(false);
            }
            finally
            {
                await CompleteReaderAsync(completionException).ConfigureAwait(false);
            }
        }
    }

    internal void Fault(Exception exception)
    {
        ArgumentNullException.ThrowIfNull(exception);
        Interlocked.CompareExchange(ref _terminalException, exception, null);
        if (Interlocked.Exchange(ref _faulted, 1) != 0)
        {
            return;
        }

        CancelPendingOperations();
        try
        {
            _stream.Dispose();
        }
        catch
        {
            // The original transport failure is more useful than a secondary close failure.
        }
    }

    private void CancelPendingOperations()
    {
        try
        {
            _reader.CancelPendingRead();
        }
        catch (ObjectDisposedException)
        {
        }

        try
        {
            _writer.CancelPendingFlush();
        }
        catch (ObjectDisposedException)
        {
        }
    }

    private static Socket CreateSocket(EndPoint endPoint) => endPoint switch
    {
        IPEndPoint ipEndPoint => new Socket(ipEndPoint.AddressFamily, SocketType.Stream, ProtocolType.Tcp),
        DnsEndPoint { AddressFamily: AddressFamily.InterNetwork or AddressFamily.InterNetworkV6 } dnsEndPoint =>
            new Socket(dnsEndPoint.AddressFamily, SocketType.Stream, ProtocolType.Tcp),
        DnsEndPoint => new Socket(SocketType.Stream, ProtocolType.Tcp),
        _ => new Socket(endPoint.AddressFamily, SocketType.Stream, ProtocolType.Tcp)
    };

    private static string GetTargetHost(EndPoint endPoint) => endPoint switch
    {
        DnsEndPoint dnsEndPoint => dnsEndPoint.Host,
        IPEndPoint ipEndPoint => ipEndPoint.Address.ToString(),
        _ => throw new InvalidOperationException("RocketMQ TLS requires a DNS or IP remote endpoint.")
    };

    private static EndPoint CloneEndPoint(EndPoint endPoint) => endPoint switch
    {
        DnsEndPoint dnsEndPoint =>
            new DnsEndPoint(dnsEndPoint.Host, dnsEndPoint.Port, dnsEndPoint.AddressFamily),
        IPEndPoint ipEndPoint => new IPEndPoint(CloneAddress(ipEndPoint.Address), ipEndPoint.Port),
        _ => throw new InvalidOperationException("RocketMQ TLS requires a DNS or IP remote endpoint.")
    };

    private static IPAddress CloneAddress(IPAddress address)
    {
        var clone = new IPAddress(address.GetAddressBytes());
        if (address.AddressFamily == AddressFamily.InterNetworkV6)
        {
            clone.ScopeId = address.ScopeId;
        }

        return clone;
    }

    private void ThrowIfUnavailable()
    {
        ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);
        if (Volatile.Read(ref _faulted) != 0)
        {
            throw new IOException(
                "The RocketMQ remoting connection is no longer usable.",
                Volatile.Read(ref _terminalException));
        }
    }

    private async ValueTask CompleteWriterAsync(Exception exception)
    {
        await _writeLock.WaitAsync().ConfigureAwait(false);
        try
        {
            await _writer.CompleteAsync(exception).ConfigureAwait(false);
        }
        finally
        {
            _writeLock.Release();
        }
    }

    private async ValueTask CompleteReaderAsync(Exception exception)
    {
        await _readLock.WaitAsync().ConfigureAwait(false);
        try
        {
            await _reader.CompleteAsync(exception).ConfigureAwait(false);
        }
        finally
        {
            _readLock.Release();
        }
    }
}
