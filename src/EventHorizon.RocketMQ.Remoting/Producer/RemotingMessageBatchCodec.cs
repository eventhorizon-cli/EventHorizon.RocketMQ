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

using System.Buffers.Binary;
using System.Text;
using EventHorizon.RocketMQ.Remoting.Protocol;

namespace EventHorizon.RocketMQ.Remoting.Producer;

internal static class RemotingMessageBatchCodec
{
    public static byte[] Encode(
        IReadOnlyList<Message> messages,
        Func<Message, IDictionary<string, string>> propertiesFactory)
    {
        ArgumentNullException.ThrowIfNull(messages);
        ArgumentNullException.ThrowIfNull(propertiesFactory);

        var records = new byte[messages.Count][];
        var totalLength = 0;
        for (var index = 0; index < messages.Count; index++)
        {
            var message = messages[index];
            ArgumentNullException.ThrowIfNull(message);
            var properties = propertiesFactory(message);
            ArgumentNullException.ThrowIfNull(properties);
            var record = EncodeRecord(message.Body, MessagePropertyCodec.Serialize(properties));
            records[index] = record;
            totalLength = checked(totalLength + record.Length);
        }

        var batch = new byte[totalLength];
        var offset = 0;
        foreach (var record in records)
        {
            Buffer.BlockCopy(record, 0, batch, offset, record.Length);
            offset += record.Length;
        }

        return batch;
    }

    private static byte[] EncodeRecord(byte[] body, string properties)
    {
        var propertiesBytes = Encoding.UTF8.GetBytes(properties);
        if (propertiesBytes.Length > short.MaxValue)
        {
            throw new ArgumentException(
                $"Batch message properties cannot exceed {short.MaxValue} bytes.",
                nameof(properties));
        }

        var recordLength = checked(
            (sizeof(int) * 5) +
            body.Length +
            sizeof(short) +
            propertiesBytes.Length);
        var record = new byte[recordLength];
        var offset = 0;
        WriteInt32(record, ref offset, recordLength);
        WriteInt32(record, ref offset, 0);
        WriteInt32(record, ref offset, 0);
        WriteInt32(record, ref offset, 0);
        WriteInt32(record, ref offset, body.Length);
        body.CopyTo(record, offset);
        offset += body.Length;
        BinaryPrimitives.WriteInt16BigEndian(record.AsSpan(offset), checked((short)propertiesBytes.Length));
        offset += sizeof(short);
        propertiesBytes.CopyTo(record, offset);
        return record;
    }

    private static void WriteInt32(byte[] buffer, ref int offset, int value)
    {
        BinaryPrimitives.WriteInt32BigEndian(buffer.AsSpan(offset), value);
        offset += sizeof(int);
    }
}
