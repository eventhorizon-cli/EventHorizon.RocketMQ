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
using EventHorizon.RocketMQ.Remoting.Protocol;
using Xunit;

namespace EventHorizon.RocketMQ.Remoting.Tests.Protocol;

public sealed class RemotingConnectionTests
{
    [Fact]
    public void Constructor_RejectsFrameSizeWithoutMessageBudget()
    {
        var exception = Assert.Throws<ArgumentOutOfRangeException>(() =>
            new RemotingConnection(
                new MemoryStream(),
                new RemoteCommandSerializer(),
                RemotingClientOptions.MinimumRemotingFrameSize - 1));

        Assert.Equal("maximumFrameLength", exception.ParamName);
    }

    [Fact]
    public async Task SerializerValidationFailure_DoesNotFaultConnectionBeforePipeMutation()
    {
        const int maximumFrameLength = 128 * 1024;
        var stream = new MemoryStream();
        var connection = new RemotingConnection(stream, new RemoteCommandSerializer(), maximumFrameLength);
        var oversized = new RemotingCommand
        {
            Code = RequestCode.GetRouteInfoByTopic,
            Body = GC.AllocateUninitializedArray<byte>(maximumFrameLength)
        };

        await Assert.ThrowsAsync<InvalidDataException>(() =>
            connection.SendAsync(oversized, CancellationToken.None).AsTask());

        Assert.True(connection.IsUsable);
        await connection.DisposeAsync();
    }

    [Fact]
    public async Task WriterFailureDuringSerialization_FaultsConnection()
    {
        var serializer = new RemoteCommandSerializer();
        var connection = new RemotingConnection(
            new MemoryStream(),
            new ThrowingBufferSerializer(serializer));
        var command = new RemotingCommand
        {
            Code = RequestCode.GetRouteInfoByTopic,
            Body = new byte[128 * 1024]
        };

        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            connection.SendAsync(command, CancellationToken.None).AsTask());

        Assert.False(connection.IsUsable);
        await connection.DisposeAsync();
    }

    [Fact]
    public async Task ConcurrentTransportFault_DoesNotOverrideACompletedFlush()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var stream = new BlockingWriteStream();
        var connection = new RemotingConnection(stream, new RemoteCommandSerializer());
        var send = connection.SendAsync(
            new RemotingCommand
            {
                Code = RequestCode.GetRouteInfoByTopic,
                Body = new byte[4096]
            },
            cancellationToken).AsTask();

        await stream.WriteStarted.WaitAsync(TimeSpan.FromSeconds(5), cancellationToken);
        var terminalFailure = new IOException("simulated receive failure");
        connection.Fault(terminalFailure);
        stream.ReleaseWrite();

        await send;
        Assert.False(connection.IsUsable);
        await connection.DisposeAsync();
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task SendFailureAfterWriting_FaultsConnectionAndDiscardsBufferedFrame(bool cancellation)
    {
        Exception failure = cancellation
            ? new OperationCanceledException(new CancellationToken(canceled: true))
            : new IOException("simulated partial socket write");
        var stream = new FailingWriteStream(failure);
        var connection = new RemotingConnection(stream, new RemoteCommandSerializer());
        var command = new RemotingCommand
        {
            Code = RequestCode.GetRouteInfoByTopic,
            Body = Encoding.UTF8.GetBytes("frame-that-must-not-be-reused")
        };

        var exception = await Assert.ThrowsAsync<IOException>(() =>
            connection.SendAsync(command, CancellationToken.None).AsTask());
        if (cancellation)
        {
            Assert.IsType<OperationCanceledException>(exception.InnerException);
        }

        Assert.False(connection.IsUsable);
        var writeCountAfterFailure = stream.WriteCount;
        await Assert.ThrowsAsync<IOException>(() =>
            connection.SendAsync(
                new RemotingCommand { Code = RequestCode.GetRouteInfoByTopic },
                CancellationToken.None).AsTask());

        await connection.DisposeAsync();

        Assert.True(stream.IsDisposed);
        Assert.Equal(writeCountAfterFailure, stream.WriteCount);
    }

    private sealed class FailingWriteStream(Exception failure) : Stream
    {
        public int WriteCount { get; private set; }

        public bool IsDisposed { get; private set; }

        public override bool CanRead => true;

        public override bool CanSeek => false;

        public override bool CanWrite => true;

        public override long Length => throw new NotSupportedException();

        public override long Position
        {
            get => throw new NotSupportedException();
            set => throw new NotSupportedException();
        }

        public override void Flush()
        {
        }

        public override Task FlushAsync(CancellationToken cancellationToken) => Task.CompletedTask;

        public override int Read(byte[] buffer, int offset, int count) => 0;

        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();

        public override void SetLength(long value) => throw new NotSupportedException();

        public override void Write(byte[] buffer, int offset, int count)
        {
            WriteCount++;
            throw failure;
        }

        public override ValueTask WriteAsync(
            ReadOnlyMemory<byte> buffer,
            CancellationToken cancellationToken = default)
        {
            WriteCount++;
            return ValueTask.FromException(failure);
        }

        protected override void Dispose(bool disposing)
        {
            IsDisposed = true;
            base.Dispose(disposing);
        }
    }

    private sealed class ThrowingBufferSerializer(IRemoteCommandSerializer inner) : IRemoteCommandSerializer
    {
        public bool TryParseMessage(
            in System.Buffers.ReadOnlySequence<byte> input,
            ref SequencePosition consumed,
            ref SequencePosition examined,
            out RemotingCommand command,
            int maximumFrameLength = RemotingClientOptions.DefaultRemotingFrameSize) =>
            inner.TryParseMessage(
                input,
                ref consumed,
                ref examined,
                out command,
                maximumFrameLength);

        public void WriteMessage(
            RemotingCommand command,
            System.Buffers.IBufferWriter<byte> output,
            int maximumFrameLength = RemotingClientOptions.DefaultRemotingFrameSize)
        {
            output.GetSpan(sizeof(int));
            output.Advance(sizeof(int));
            throw new InvalidOperationException("simulated buffer writer failure");
        }
    }

    private sealed class BlockingWriteStream : Stream
    {
        private readonly TaskCompletionSource _writeStarted =
            new(TaskCreationOptions.RunContinuationsAsynchronously);
        private readonly TaskCompletionSource _releaseWrite =
            new(TaskCreationOptions.RunContinuationsAsynchronously);

        public Task WriteStarted => _writeStarted.Task;

        public override bool CanRead => false;

        public override bool CanSeek => false;

        public override bool CanWrite => true;

        public override long Length => throw new NotSupportedException();

        public override long Position
        {
            get => throw new NotSupportedException();
            set => throw new NotSupportedException();
        }

        public void ReleaseWrite() => _releaseWrite.TrySetResult();

        public override void Flush()
        {
        }

        public override Task FlushAsync(CancellationToken cancellationToken) => Task.CompletedTask;

        public override int Read(byte[] buffer, int offset, int count) => throw new NotSupportedException();

        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();

        public override void SetLength(long value) => throw new NotSupportedException();

        public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();

        public override async ValueTask WriteAsync(
            ReadOnlyMemory<byte> buffer,
            CancellationToken cancellationToken = default)
        {
            _writeStarted.TrySetResult();
            await _releaseWrite.Task.ConfigureAwait(false);
        }
    }
}
