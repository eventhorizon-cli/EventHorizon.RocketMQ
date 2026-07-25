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

internal readonly record struct RemotingRequestResult
{
    private RemotingRequestResult(bool isHandled, bool suppressResponse, RemotingCommand? response)
    {
        IsHandled = isHandled;
        SuppressResponse = suppressResponse;
        Response = response;
    }

    public static RemotingRequestResult NotHandled => default;

    public static RemotingRequestResult NoResponse => new(isHandled: true, suppressResponse: true, response: null);

    public bool IsHandled { get; }

    public bool SuppressResponse { get; }

    public RemotingCommand? Response { get; }

    public static RemotingRequestResult Respond(RemotingCommand response)
    {
        ArgumentNullException.ThrowIfNull(response);
        return new RemotingRequestResult(isHandled: true, suppressResponse: false, response);
    }
}
