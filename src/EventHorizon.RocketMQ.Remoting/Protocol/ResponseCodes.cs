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
internal static class ResponseCodes
{
    public const int ResSuccess = 0;
    public const int ResError = 1;
    public const int ResSystemBusy = 2;
    public const int ResRequestCodeNotSupported = 3;
    public const int ResFlushDiskTimeout = 10;
    public const int ResSlaveNotAvailable = 11;
    public const int ResFlushSlaveTimeout = 12;
    public const int ResServiceNotAvailable = 14;
    public const int ResNoPermission = 16;
    public const int ResTopicNotExist = 17;
    public const int ResPullNotFound = 19;
    public const int ResPullRetryImmediately = 20;
    public const int ResPullOffsetMoved = 21;
    public const int ResQueryNotFound = 22;
    public const int ResNoMessage = 208;
    public const int ResPollingFull = 209;
    public const int ResPollingTimeout = 210;
}
