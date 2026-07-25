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

internal static class MessageSysFlag
{
    public const int CompressedFlag = 0x1;
    public const int TransactionNotType = 0;
    public const int TransactionPreparedType = 0x1 << 2;
    public const int TransactionCommitType = 0x2 << 2;
    public const int TransactionRollbackType = 0x3 << 2;
    public const int BornhostV6Flag = 0x1 << 4;
    public const int StorehostaddressV6Flag = 0x1 << 5;
    public const int LogicalQueueFlag = 0x1 << 6;
}
