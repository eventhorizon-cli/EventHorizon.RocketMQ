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

internal static class RequestCode
{
    public const int SendMessage = 10;
    public const int PullMessage = 11;
    public const int QueryConsumerOffset = 14;
    public const int UpdateConsumerOffset = 15;
    public const int SearchOffsetByTimestamp = 29;
    public const int GetMaxOffset = 30;
    public const int GetMinOffset = 31;
    public const int GetEarliestMessageStoreTime = 32;
    public const int ViewMessageById = 33;
    public const int HeartBeat = 34;
    public const int UnregisterClient = 35;
    public const int ConsumerSendMsgBack = 36;
    public const int EndTransaction = 37;
    public const int GetConsumerListByGroup = 38;
    public const int GetRouteInfoByTopic = 105;
    public const int CheckTransactionState = 39;
    public const int NotifyConsumerIdsChanged = 40;
    public const int LockBatchMq = 41;
    public const int UnlockBatchMq = 42;
    public const int ResetConsumerOffset = 220;
    public const int SendReplyMessage = 324;
    public const int PushReplyMessageToClient = 326;
    public const int RecallMessage = 370;
    public const int PopMessage = 200050;
    public const int AckMessage = 200051;
    public const int ChangeMessageInvisibleTime = 200053;
}
